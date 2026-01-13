from typing import List, Dict, Any
from sandbox.executor import DBExecutor
from utils.logger import default_logger


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
        from sandbox.mysql_executor import MySQLExecutor
        from sandbox.pg_executor import PGExecutor
        
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
            # 从目标数据库读取数据
            if limit:
                if self.sampling_strategy == "random":
                    # MySQL使用RAND(), PostgreSQL使用RANDOM()
                    rand_func = "RAND()" if self.target_quote == "`" else "RANDOM()"
                    select_sql = f"SELECT * FROM {self.target_quote}{table_name}{self.target_quote} ORDER BY {rand_func} LIMIT {limit}"
                elif self.sampling_strategy == "time_based":
                    time_column = self.sampling_params.get("time_column", "created_at")
                    select_sql = f"SELECT * FROM {self.target_quote}{table_name}{self.target_quote} ORDER BY {self.target_quote}{time_column}{self.target_quote} DESC LIMIT {limit}"
                else:
                    rand_func = "RAND()" if self.target_quote == "`" else "RANDOM()"
                    select_sql = f"SELECT * FROM {self.target_quote}{table_name}{self.target_quote} ORDER BY {rand_func} LIMIT {limit}"
            else:
                select_sql = f"SELECT * FROM {self.target_quote}{table_name}{self.target_quote}"
            
            default_logger.info(f"从目标数据库读取数据: {select_sql[:100]}...")
            results = self.target_executor.execute(select_sql, fetch=True)
            
            if not results or not results[0].get("rows"):
                default_logger.warning(f"表 {table_name} 没有数据")
                return 0
            
            rows = results[0]["rows"]
            total_rows = len(rows)
            default_logger.info(f"成功读取 {total_rows} 行数据")
            
            # 批量插入到沙箱数据库
            if total_rows > 0:
                # 获取列名
                columns = list(rows[0].keys())
                columns_str = ", ".join([f"{self.sandbox_quote}{col}{self.sandbox_quote}" for col in columns])
                
                # 分批插入
                inserted_count = 0
                for i in range(0, total_rows, self.batch_size):
                    batch = rows[i:i + self.batch_size]
                    
                    # 构造VALUES部分
                    values_parts = []
                    for row in batch:
                        values = []
                        for col in columns:
                            val = row[col]
                            if val is None:
                                values.append("NULL")
                            elif isinstance(val, (int, float)):
                                values.append(str(val))
                            else:
                                # 转义字符串中的单引号和反斜杠
                                val_str = str(val).replace("\\", "\\\\").replace("'", "\\'")
                                values.append(f"'{val_str}'")
                        values_parts.append(f"({', '.join(values)})")
                    
                    # 构造INSERT语句
                    insert_sql = f"INSERT INTO {self.sandbox_quote}{table_name}{self.sandbox_quote} ({columns_str}) VALUES {', '.join(values_parts)}"
                    
                    # 执行插入
                    self.sandbox_executor.execute(insert_sql, fetch=False)
                    inserted_count += len(batch)
                    
                    # if total_rows > self.batch_size:
                    #     default_logger.info(f"已插入 {inserted_count}/{total_rows} 行")
                
                default_logger.info(f"成功插入 {inserted_count} 行数据到沙箱表")
                return inserted_count
            
            return 0
            
        except Exception as e:
            default_logger.error(f"批量拷贝数据失败: {e}")
            raise
    
    def create_full_copy_table(self, table_name: str) -> None:
        """
        创建完整拷贝表（小表）- 跨数据库版本
        
        Args:
            original_name: 原表名（在目标数据库）
            sandbox_name: 沙箱表名（在沙箱数据库）
        """
        default_logger.info(f"开始完整拷贝表: {table_name}")
        
        try:
            # 从目标数据库获取原表的CREATE TABLE语句
            create_ddl = self.target_executor.get_create_table_ddl(table_name)
        
            
            # 在沙箱数据库创建表结构
            self.sandbox_executor.execute(create_ddl, fetch=False)
            default_logger.info(f"成功在沙箱数据库创建表结构: {table_name}")
            
            # 批量拷贝数据（跨数据库）
            count = self._batch_copy_data(table_name, limit=None)
            default_logger.info(f"成功复制{count}行数据到沙箱表: {table_name}")
            
        except Exception as e:
            default_logger.error(f"完整拷贝表失败: {e}")
            raise
    
    def create_sampled_table(self, table_name: str) -> None:
        """
        创建采样表（大表）- 跨数据库版本
        
        Args:
            table_name: 表名
        """
        default_logger.info(f"开始创建采样表: {table_name} (策略: {self.sampling_strategy})")
        
        try:
            # 从目标数据库获取原表的CREATE TABLE语句
            create_ddl = self.target_executor.get_create_table_ddl(table_name)
            
            
            # 在沙箱数据库创建表结构
            self.sandbox_executor.execute(create_ddl, fetch=False)
            default_logger.info(f"成功在沙箱数据库创建表结构: {table_name}")
            
            # 批量拷贝采样数据（跨数据库）
            count = self._batch_copy_data(table_name, limit=self.sample_size)
            default_logger.info(f"成功采样{count}行数据到沙箱表: {table_name}")
            
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
        
        sandbox_info = {
            "tables": [],
            "sampled_tables": []  # 记录哪些表是采样的
        }
        
        try:
            for table in tables:
                # 检查目标数据库中表是否存在
                if not self.target_executor.table_exists(table):
                    raise ValueError(f"目标数据库中表不存在: {table}")
                
                # 从目标数据库获取表的行数
                row_count = self.target_executor.get_table_count(table)
                default_logger.info(f"目标数据库中表 {table} 有 {row_count} 行数据")
                
                # 根据行数决定是完整拷贝还是采样
                if row_count <= self.copy_threshold:
                    self.create_full_copy_table(table)
                    is_sampled = False
                else:
                    self.create_sampled_table(table)
                    is_sampled = True
                    sandbox_info["sampled_tables"].append(table)
                
                sandbox_info["tables"].append({
                    "original": table,
                    "original_rows": row_count,
                    "is_sampled": is_sampled
                })
            
            default_logger.info(f"沙箱环境搭建完成，共{len(tables)}张表")
            return sandbox_info
            
        except Exception as e:
            default_logger.error(f"沙箱环境搭建失败: {e}")
            # 尝试清理已创建的表
            try:
                self.cleanup_sandbox()
            except:
                pass
            raise
    
    def cleanup_sandbox(self) -> None:
        """清理沙箱环境，直接清空整个沙箱数据库"""
        from sandbox.mysql_executor import MySQLExecutor
        from sandbox.pg_executor import PGExecutor
        
        default_logger.info("开始清空沙箱数据库")
        
        try:
            if isinstance(self.sandbox_executor, MySQLExecutor):
                # MySQL: 获取所有表并删除
                self._cleanup_mysql_database()
            elif isinstance(self.sandbox_executor, PGExecutor):
                # PostgreSQL: 删除public schema中的所有表
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
            
            # 获取当前数据库中的所有表
            db_name = self.sandbox_executor.config.get("database")
            get_tables_sql = f"""
                SELECT TABLE_NAME 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = '{db_name}' 
                AND TABLE_TYPE = 'BASE TABLE'
            """
            results = self.sandbox_executor.execute(get_tables_sql, fetch=True)
            
            if not results or not results[0].get("rows"):
                default_logger.info("沙箱数据库中没有表需要删除")
                return
            
            tables = [row["TABLE_NAME"] for row in results[0]["rows"]]
            default_logger.info(f"找到 {len(tables)} 张表需要删除")
            
            # 删除所有表
            for table in tables:
                try:
                    drop_sql = f"DROP TABLE IF EXISTS `{table}`"
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
            except:
                pass
            raise
    
    def _cleanup_postgres_database(self) -> None:
        """清空PostgreSQL数据库的所有表和相关对象"""
        try:
            # 获取public schema中的所有表
            get_tables_sql = """
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
            """
            results = self.sandbox_executor.execute(get_tables_sql, fetch=True)
            
            if not results or not results[0].get("rows"):
                default_logger.info("沙箱数据库中没有表需要删除")
                return
            
            tables = [row["tablename"] for row in results[0]["rows"]]
            default_logger.info(f"找到 {len(tables)} 张表需要删除")
            
            # 使用CASCADE删除所有表（自动处理外键依赖）
            for table in tables:
                try:
                    drop_sql = f'DROP TABLE IF EXISTS "{table}" CASCADE'
                    self.sandbox_executor.execute(drop_sql, fetch=False)
                    default_logger.info(f"成功删除表: {table}")
                except Exception as e:
                    default_logger.error(f"删除表失败 {table}: {e}")
            
            # 删除所有序列（sequences）
            get_sequences_sql = """
                SELECT sequencename 
                FROM pg_sequences 
                WHERE schemaname = 'public'
            """
            results = self.sandbox_executor.execute(get_sequences_sql, fetch=True)
            
            if results and results[0].get("rows"):
                sequences = [row["sequencename"] for row in results[0]["rows"]]
                default_logger.info(f"找到 {len(sequences)} 个序列需要删除")
                for seq in sequences:
                    try:
                        drop_sql = f'DROP SEQUENCE IF EXISTS "{seq}" CASCADE'
                        self.sandbox_executor.execute(drop_sql, fetch=False)
                        default_logger.info(f"成功删除序列: {seq}")
                    except Exception as e:
                        default_logger.error(f"删除序列失败 {seq}: {e}")
            
        except Exception as e:
            raise
    