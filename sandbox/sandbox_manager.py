from typing import List, Dict, Any

from sandbox.executor import DBExecutor
from sandbox.mysql_executor import MySQLExecutor
from sandbox.pg_executor import PGExecutor
from sandbox.statistics import inject_statistics
from utils.logger import default_logger
from utils.sandbox_fidelity import empty_fidelity
from utils.timing import timed_stage


def _slot_db_missing(exc: BaseException, db_name: str) -> bool:
    if not db_name.startswith("astratune_sandbox_"):
        return False
    msg = str(exc).lower()
    return "does not exist" in msg and db_name.lower() in msg


class SandboxManager:
    """沙箱环境管理器，负责搭建和清理沙箱环境"""
    
    def __init__(self, target_executor: DBExecutor, sandbox_executor: DBExecutor, config: Dict[str, Any]):
        """
        初始化沙箱管理器
        
        Args:
            target_executor: 目标数据库执行器（只读，用于读取原表数据）
            sandbox_executor: 沙箱数据库执行器（读写，用于创建和操作沙箱表）
            config: 沙箱配置
        """
        self.target_executor = target_executor
        self.sandbox_executor = sandbox_executor
        self.config = config
        
        # 从配置中读取参数
        self.copy_threshold = config.get("copy_thr", 10000)
        self.sample_size = config.get("sample_size", 10000)
        self.sampling_strategy = config.get("sampling_strategy", "random")
        self.sampling_params = config.get("sampling_params", {})
        self.batch_size = config.get("batch_size", 1000)  # 批量插入大小
        
        # 根据执行器类型确定引用符号
        self.target_quote = self._get_quote_char(target_executor)
        self.sandbox_quote = self._get_quote_char(sandbox_executor)
    
    def _get_quote_char(self, executor: DBExecutor) -> str:
        """
        根据执行器类型返回对应的引用符号

        Args:
            executor: 数据库执行器

        Returns:
            引用符号（MySQL: `, PostgreSQL: "）
        """
        if isinstance(executor, MySQLExecutor):
            return "`"
        elif isinstance(executor, PGExecutor):
            return '"'
        else:
            # 默认使用双引号（标准SQL）
            return '"'
    
 
    def _batch_copy_data(self, table_name: str, limit: int = None) -> int:
        """
        批量拷贝数据（跨数据库）
        
        Args:
            original_table: 原表名（在目标数据库）
            sandbox_table: 沙箱表名（在沙箱数据库）
            limit: 限制拷贝的行数（None表示全部拷贝）
        
        Returns:
            实际拷贝的行数
        """
        default_logger.info(f"开始批量拷贝数据: {table_name}")
        
        try:
            with timed_stage("sandbox_copy_table", table=table_name, sampled=bool(limit)):
                if limit:
                    if self.sampling_strategy == "time_based":
                        time_column = self.sampling_params.get("time_column", "created_at")
                        select_sql = (
                            f"SELECT * FROM {self.target_quote}{table_name}{self.target_quote} "
                            f"ORDER BY {self.target_quote}{time_column}{self.target_quote} DESC "
                            f"LIMIT {limit}"
                        )
                    elif self.target_quote == "`":
                        # 避免 ORDER BY RAND() 对大表 filesort；WHERE RAND()<p 只需扫描
                        try:
                            n_rows = int(self.target_executor.get_table_count(table_name))
                        except Exception:
                            n_rows = 0
                        if n_rows <= 0 or n_rows <= int(limit) * 2:
                            select_sql = f"SELECT * FROM `{table_name}` LIMIT {limit}"
                        else:
                            p = min(1.0, (float(limit) * 2.5) / float(n_rows))
                            select_sql = (
                                f"SELECT * FROM `{table_name}` "
                                f"WHERE RAND() < {p:.8f} LIMIT {limit}"
                            )
                    else:
                        try:
                            n_rows = int(self.target_executor.get_table_count(table_name))
                        except Exception:
                            n_rows = 0
                        if n_rows > 0:
                            pct = min(100.0, max(1.0, (float(limit) / float(n_rows)) * 100.0 * 1.8))
                        else:
                            pct = 25.0
                        q = self.target_quote
                        select_sql = (
                            f"SELECT * FROM {q}{table_name}{q} "
                            f"TABLESAMPLE SYSTEM ({pct:.4f}) REPEATABLE (42) "
                            f"LIMIT {limit}"
                        )
                else:
                    select_sql = f"SELECT * FROM {self.target_quote}{table_name}{self.target_quote}"

                default_logger.info(f"从目标数据库读取数据: {select_sql[:120]}...")
                results = self.target_executor.execute(select_sql, fetch=True)

                got_n = len(results[0]["rows"]) if results and results[0].get("rows") else 0
                if limit and self.sampling_strategy != "time_based" and got_n < max(1, int(limit * 0.3)):
                    default_logger.warning(
                        f"采样仅得 {got_n} 行，回退 ORDER BY RAND/RANDOM() LIMIT {limit}"
                    )
                    q = self.target_quote
                    if q == "`":
                        select_sql = (
                            f"SELECT * FROM `{table_name}` ORDER BY RAND() LIMIT {limit}"
                        )
                    else:
                        select_sql = (
                            f"SELECT * FROM {q}{table_name}{q} "
                            f"ORDER BY RANDOM() LIMIT {limit}"
                        )
                    results = self.target_executor.execute(select_sql, fetch=True)

                if not results or not results[0].get("rows"):
                    default_logger.warning(f"表 {table_name} 没有数据")
                    return 0

                rows = results[0]["rows"]
                total_rows = len(rows)
                default_logger.info(f"成功读取 {total_rows} 行数据")

                if total_rows > 0:
                    columns = list(rows[0].keys())
                    inserted_count = self.sandbox_executor.insert_rows(
                        table_name,
                        columns,
                        rows,
                        quote=self.sandbox_quote,
                        batch_size=self.batch_size,
                    )
                    default_logger.info(f"成功插入 {inserted_count} 行数据到沙箱表")
                    return inserted_count

                return 0

        except Exception as e:
            default_logger.error(f"批量拷贝数据失败: {e}")
            raise
    
    def _create_table_structure(self, table_name: str) -> None:
        """在沙箱创建表结构：先序列，再 CREATE TABLE；失败则 strip nextval 重试。"""
        create_ddl = self.target_executor.get_create_table_ddl(table_name)
        is_pg = isinstance(self.target_executor, PGExecutor) and isinstance(
            self.sandbox_executor, PGExecutor
        )

        if is_pg:
            for seq_ddl in self.target_executor.get_sequence_create_ddls(table_name):
                try:
                    self.sandbox_executor.execute(seq_ddl, fetch=False)
                    default_logger.info(f"沙箱创建序列: {seq_ddl[:80]}...")
                except Exception as e:
                    default_logger.warning(f"创建序列失败（继续）: {e}")

        try:
            self.sandbox_executor.execute(create_ddl, fetch=False)
        except Exception as e:
            if is_pg and "nextval" in create_ddl.lower():
                stripped = PGExecutor.strip_nextval_defaults(create_ddl)
                default_logger.warning(
                    f"建表失败，尝试剥离 nextval DEFAULT 后重试 {table_name}: {e}"
                )
                self.sandbox_executor.execute(stripped, fetch=False)
            else:
                raise
        default_logger.info(f"成功在沙箱数据库创建表结构: {table_name}")

    def _sync_sequences_after_load(self, table_name: str) -> None:
        """插入数据后恢复 OWNED BY / setval（尽力）。"""
        if not isinstance(self.target_executor, PGExecutor):
            return
        if not isinstance(self.sandbox_executor, PGExecutor):
            return
        for ddl in self.target_executor.get_sequence_owned_by_ddls(table_name):
            try:
                self.sandbox_executor.execute(ddl, fetch=True)
            except Exception as e:
                default_logger.warning(f"序列 OWNED BY/setval 失败（不阻断）: {e}")

    def create_full_copy_table(self, table_name: str) -> Dict[str, Any]:
        """创建完整拷贝表（小表）- 跨数据库版本。"""
        default_logger.info(f"开始完整拷贝表: {table_name}")
        
        try:
            self._create_table_structure(table_name)
            count = self._batch_copy_data(table_name, limit=None)
            default_logger.info(f"成功复制{count}行数据到沙箱表: {table_name}")
            self._sync_sequences_after_load(table_name)
            return {"sandbox_rows": int(count or 0), "sample_method": "full"}
            
        except Exception as e:
            default_logger.error(f"完整拷贝表失败: {e}")
            raise
    
    def create_sampled_table(self, table_name: str) -> Dict[str, Any]:
        """创建采样表（大表）- 跨数据库版本。"""
        default_logger.info(f"开始创建采样表: {table_name} (策略: {self.sampling_strategy})")
        
        try:
            self._create_table_structure(table_name)
            count = self._batch_copy_data(table_name, limit=self.sample_size)
            default_logger.info(f"成功采样{count}行数据到沙箱表: {table_name}")
            self._sync_sequences_after_load(table_name)
            if self.sampling_strategy == "time_based":
                method = "time_based"
            elif self.target_quote == "`":
                method = "mysql_rand"
            else:
                method = "pg_tablesample"
            return {"sandbox_rows": int(count or 0), "sample_method": method}
            
        except Exception as e:
            default_logger.error(f"创建采样表失败: {e}")
            raise
    
    def setup_sandbox(self, tables: List[str]) -> Dict[str, Any]:
        """
        搭建沙箱环境（跨数据库版本）
        
        Args:
            tables: 需要复制到沙箱的表名列表
        
        Returns:
            沙箱信息字典，包含采样信息
        
        Raises:
            Exception: 沙箱搭建失败时抛出异常
        """
        default_logger.info(f"开始搭建沙箱环境，涉及{len(tables)}张表")
        slot_db = ""
        try:
            slot_db = str(getattr(self.sandbox_executor, "config", {}).get("database") or "")
        except Exception:
            slot_db = ""

        dialect = "mysql" if self.target_quote == "`" else "postgresql"

        fidelity = empty_fidelity(dialect=dialect, slot_db=slot_db)
        fidelity["copy_threshold"] = self.copy_threshold
        fidelity["sample_size"] = self.sample_size

        sandbox_info = {
            "tables": [],
            "sampled_tables": [],
            "fidelity": fidelity,
        }
        
        try:
            if isinstance(self.sandbox_executor, MySQLExecutor):
                self.sandbox_executor.execute("SET FOREIGN_KEY_CHECKS = 0", fetch=False)

            for table in tables:
                # 检查目标数据库中表是否存在
                resolved = table
                if hasattr(self.target_executor, "resolve_table_name"):
                    hit = self.target_executor.resolve_table_name(table)
                    if hit:
                        resolved = hit
                if not self.target_executor.table_exists(resolved):
                    raise ValueError(f"目标数据库中表不存在: {table}")
                
                # 从目标数据库获取表的行数
                row_count = self.target_executor.get_table_count(resolved)
                default_logger.info(f"目标数据库中表 {resolved} 有 {row_count} 行数据")
                
                # 根据行数决定是完整拷贝还是采样
                with timed_stage(
                    "sandbox_setup_table",
                    table=resolved,
                    original_rows=row_count,
                ) as bag:
                    if row_count <= self.copy_threshold:
                        copy_meta = self.create_full_copy_table(resolved)
                        is_sampled = False
                    else:
                        copy_meta = self.create_sampled_table(resolved)
                        is_sampled = True
                        sandbox_info["sampled_tables"].append(resolved)
                    bag["sampled"] = is_sampled
                    bag["sandbox_rows"] = copy_meta.get("sandbox_rows")

                stats_report = {"ok": [], "fail": [], "skip": []}
                # 统计信息克隆：采样后伪装生产态基数，保障 EXPLAIN 保真度
                try:
                    with timed_stage("sandbox_stats_inject", table=resolved):
                        stats_report = inject_statistics(
                            self.target_executor,
                            self.sandbox_executor,
                            resolved,
                            target_quote=self.target_quote,
                            sandbox_quote=self.sandbox_quote,
                        ) or stats_report
                except Exception as e:
                    default_logger.warning(f"统计信息克隆异常（不阻断）: {resolved}: {e}")
                    stats_report = {"ok": [], "fail": [str(e)], "skip": []}

                table_entry = {
                    "original": table,
                    "resolved": resolved,
                    "original_rows": row_count,
                    "sandbox_rows": copy_meta.get("sandbox_rows"),
                    "is_sampled": is_sampled,
                    "sample_method": copy_meta.get("sample_method"),
                    "stats": stats_report,
                }
                sandbox_info["tables"].append(table_entry)
                fidelity["tables"].append(table_entry)

            if isinstance(self.sandbox_executor, MySQLExecutor):
                try:
                    self.sandbox_executor.execute("SET FOREIGN_KEY_CHECKS = 1", fetch=False)
                except Exception:
                    pass

            default_logger.info(f"沙箱环境搭建完成，共{len(tables)}张表 slot={slot_db}")
            return sandbox_info

        except Exception as e:
            default_logger.error(f"沙箱环境搭建失败: {e}")
            # 尝试清理已创建的表
            try:
                if isinstance(self.sandbox_executor, MySQLExecutor):
                    try:
                        self.sandbox_executor.execute("SET FOREIGN_KEY_CHECKS = 1", fetch=False)
                    except Exception:
                        pass
                self.cleanup_sandbox()
            except Exception:
                pass
            raise
    
    def _ensure_sandbox_executor_ready(self) -> None:
        """清理/重建前确保连接可用，避免 aborted 事务或 autocommit 残留拖死 DROP。"""
        ex = self.sandbox_executor
        conn = getattr(ex, "connection", None)
        db_name = str((getattr(ex, "config", {}) or {}).get("database") or "")

        def _connect_or_recreate() -> None:
            try:
                ex.connect()
            except Exception as e:
                if _slot_db_missing(e, db_name):
                    from sandbox.db_admin import ensure_sandbox_database

                    default_logger.warning(f"沙箱槽位库缺失，重建: {db_name} ({e})")
                    ensure_sandbox_database(dict(ex.config), db_name)
                    ex.connect()
                else:
                    raise

        if conn is None or getattr(conn, "closed", 1):
            _connect_or_recreate()
            return
        try:
            from psycopg2.extensions import TRANSACTION_STATUS_INERROR

            if conn.get_transaction_status() == TRANSACTION_STATUS_INERROR:
                conn.rollback()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                try:
                    ex.close()
                except Exception:
                    pass
                _connect_or_recreate()
                return
        try:
            conn.autocommit = False
        except Exception:
            pass

    def reset_sandbox(self, tables: List[str]) -> Dict[str, Any]:
        """清空并重建沙箱。优先从只读模板库恢复本槽位，否则 cleanup+setup。"""
        restore = getattr(self, "_template_restore_fn", None)
        if callable(restore):
            try:
                return restore() or self.setup_sandbox(tables)
            except Exception as e:
                default_logger.warning(f"模板恢复失败，回退 target 克隆: {e}")
                from sandbox.template_cache import release_template_binding

                release_template_binding(self)
        slot_db = str((getattr(self.sandbox_executor, "config", {}) or {}).get("database") or "")
        if slot_db:
            from sandbox.db_admin import ensure_sandbox_database

            ensure_sandbox_database(dict(self.sandbox_executor.config), slot_db)
        with timed_stage("sandbox_reset_full"):
            self.cleanup_sandbox()
            return self.setup_sandbox(tables)

    def cleanup_sandbox(self) -> None:
        """清理沙箱环境，直接清空整个沙箱数据库"""
        default_logger.info("开始清空沙箱数据库")
        self._ensure_sandbox_executor_ready()
        with timed_stage("sandbox_cleanup"):
            try:
                if isinstance(self.sandbox_executor, MySQLExecutor):
                    self._cleanup_mysql_database()
                elif isinstance(self.sandbox_executor, PGExecutor):
                    self._cleanup_postgres_database()
                else:
                    raise ValueError("未知的数据库类型")
                default_logger.info("沙箱数据库清空完成")
            except Exception as e:
                default_logger.error(f"清空沙箱数据库失败: {e}")
                raise
    
    def _cleanup_mysql_database(self) -> None:
        """清空MySQL数据库的所有表"""
        try:
            # 禁用外键检查
            self.sandbox_executor.execute("SET FOREIGN_KEY_CHECKS = 0", fetch=False)

            # 获取当前数据库中的所有表（库名/表名均做标识符转义，防注入）
            db_name = str(self.sandbox_executor.config.get("database") or "")
            db_lit = db_name.replace("'", "''")
            get_tables_sql = (
                "SELECT TABLE_NAME "
                "FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = '{db_lit}' "
                "AND TABLE_TYPE = 'BASE TABLE'"
            )
            results = self.sandbox_executor.execute(get_tables_sql, fetch=True)

            if not results or not results[0].get("rows"):
                default_logger.info("沙箱数据库中没有表需要删除")
                return

            tables = [row["TABLE_NAME"] for row in results[0]["rows"]]
            default_logger.info(f"找到 {len(tables)} 张表需要删除")

            # 删除所有表
            for table in tables:
                try:
                    safe_table = str(table).replace("`", "``")
                    drop_sql = f"DROP TABLE IF EXISTS `{safe_table}`"
                    self.sandbox_executor.execute(drop_sql, fetch=False)
                    default_logger.info(f"成功删除表: {table}")
                except Exception as e:
                    default_logger.error(f"删除表失败 {table}: {e}")

            # 恢复外键检查
            self.sandbox_executor.execute("SET FOREIGN_KEY_CHECKS = 1", fetch=False)

        except Exception as e:
            # 确保恢复外键检查
            try:
                self.sandbox_executor.execute("SET FOREIGN_KEY_CHECKS = 1", fetch=False)
            except Exception:
                pass
            raise
    
    def _cleanup_postgres_database(self) -> None:
        """清空 PostgreSQL 沙箱：DROP SCHEMA public CASCADE，避免逐表残留与目录膨胀。"""
        ex = self.sandbox_executor
        self._ensure_sandbox_executor_ready()

        def _run_autocommit(sql: str) -> None:
            if hasattr(ex, "execute_autocommit"):
                ex.execute_autocommit(sql)
            else:
                ex.execute(sql, fetch=False)

        try:
            # DDL 须 autocommit；若会话曾 aborted 或 autocommit=True 残留，execute() 会连环失败
            _run_autocommit("DROP SCHEMA IF EXISTS public CASCADE")
            _run_autocommit("CREATE SCHEMA public")
            try:
                _run_autocommit("GRANT ALL ON SCHEMA public TO postgres")
                _run_autocommit("GRANT ALL ON SCHEMA public TO public")
            except Exception:
                pass
            try:
                _run_autocommit("CHECKPOINT")
            except Exception as e:
                default_logger.debug(f"CHECKPOINT 跳过: {e}")
            self._ensure_sandbox_executor_ready()
            default_logger.info("PostgreSQL 沙箱 public schema 已重建")
        except Exception:
            # 连接可能已 poisoned，重连后再试一次
            try:
                ex.close()
            except Exception:
                pass
            ex.connect()
            _run_autocommit("DROP SCHEMA IF EXISTS public CASCADE")
            _run_autocommit("CREATE SCHEMA public")
            self._ensure_sandbox_executor_ready()
            default_logger.info("PostgreSQL 沙箱 public schema 已重建（重连后）")
    