import re
import psycopg2
import psycopg2.extras
import time
from typing import Any, List, Dict, Optional
from psycopg2.extensions import TRANSACTION_STATUS_INERROR

from sandbox.executor import DBExecutor
from utils.logger import default_logger
from utils.sql_wrap import split_sql_statements


def _is_transient_pg_error(msg: str) -> bool:
    low = (msg or "").lower()
    return (
        "recovery mode" in low
        or "starting up" in low
        or "shutting down" in low
        or "connection refused" in low
        or "could not connect to server" in low
    )


class PGExecutor(DBExecutor):
    """PostgreSQL数据库执行器"""
    
    def connect(self) -> None:
        """连接 PostgreSQL；recovery mode 时短暂退避重试；沙箱可设 statement_timeout。"""
        max_attempts = int(self.config.get("connect_retries") or 5)
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                self._connect_once()
                return
            except Exception as e:
                last_exc = e
                if not _is_transient_pg_error(str(e)) or attempt >= max_attempts:
                    default_logger.error(f"连接PostgreSQL数据库失败: {e}")
                    raise
                wait_s = min(8.0, 0.5 * (2 ** (attempt - 1)))
                default_logger.warning(
                    f"PostgreSQL 暂不可用（{e}），{wait_s:.1f}s 后重试 {attempt}/{max_attempts}"
                )
                time.sleep(wait_s)
        assert last_exc is not None
        raise last_exc

    def _connect_once(self) -> None:
        self.connection = psycopg2.connect(
            host=self.config.get("host", "localhost"),
            port=self.config.get("port", 5432),
            user=self.config.get("user", "postgres"),
            password=self.config.get("password", ""),
            database=self.config.get("database", "postgres"),
        )
        self.connection.autocommit = False
        timeout_ms = self.config.get("statement_timeout_ms")
        if timeout_ms is not None and int(timeout_ms) > 0:
            with self.connection.cursor() as cursor:
                cursor.execute("SET statement_timeout = %s", (int(timeout_ms),))
                cursor.execute("SET lock_timeout = %s", (int(timeout_ms),))
            self.connection.commit()
        db_name = str(self.config.get("database") or "")
        if db_name.startswith("astratune_sandbox") or db_name.startswith("astratune_tpl_"):
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
                self.connection.commit()
            except Exception as e:
                try:
                    self.connection.rollback()
                except Exception:
                    pass
                default_logger.info(f"PostGIS 扩展跳过（fail-open）: {e}")
        default_logger.info(
            f"成功连接到PostgreSQL数据库: {self.config.get('host')}:{self.config.get('port')}"
        )
    
    def execute(self, sql: str, fetch: bool = True) -> Optional[List[Dict[str, Any]]]:
        """
        执行SQL语句, 并测量执行时间
        
        支持多语句执行(用分号分隔),会依次执行每条语句并返回所有结果
        每条语句的结果中都包含执行时间(秒)
        """
        if not self.connection or getattr(self.connection, "closed", 1):
            self.connect()

        # 上一语句失败后可能停在 INERROR，先清掉避免连环 aborted
        try:
            if self.connection.get_transaction_status() == TRANSACTION_STATUS_INERROR:
                self.connection.rollback()
        except Exception:
            try:
                self.connection.rollback()
            except Exception:
                try:
                    self.connect()
                except Exception:
                    pass

        # 按分号拆分（尊重引号 / dollar-quote，避免切断函数体）
        statements = split_sql_statements(sql)
        if not statements:
            raise RuntimeError("can't execute an empty query")

        all_results = []

        try:
            # 使用DictCursor以字典形式返回结果
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                for stmt in statements:
                    default_logger.debug(f"执行SQL: {stmt[:100]}...")
                    
                    # 记录开始时间
                    start_time = time.time()
                    
                    # 执行SQL
                    cursor.execute(stmt)
                    
                    # 记录结束时间
                    end_time = time.time()
                    execution_time = end_time - start_time
                    
                    # 判断是否是查询语句(去掉注释行再提取首字)
                    if fetch and re.sub(r'--.*', '', stmt).strip().upper().startswith(('SELECT', 'SHOW', 'EXPLAIN', 'WITH')):
                        result = cursor.fetchall()
                        # 将RealDictRow转换为普通字典
                        result_dicts = [dict(row) for row in result]
                        all_results.append({
                            "sql": stmt,
                            "rows": result_dicts,
                            "row_count": len(result_dicts),
                            "execution_time": round(execution_time, 4),  # 秒,保留4位小数
                            "execution_time_ms": round(execution_time * 1000, 2)  # 毫秒,保留2位小数
                        })
                        default_logger.debug(f"查询执行时间: {execution_time:.4f}秒 ({execution_time * 1000:.2f}ms)")
                    else:
                        # 非查询语句,返回影响的行数
                        all_results.append({
                            "sql": stmt,
                            "affected_rows": cursor.rowcount,
                            "message": "执行成功",
                            "execution_time": round(execution_time, 4),
                            "execution_time_ms": round(execution_time * 1000, 2)
                        })
                        default_logger.debug(f"语句执行时间: {execution_time:.4f}秒 ({execution_time * 1000:.2f}ms)")
                
                self.connection.commit()
                # default_logger.info(f"成功执行{len(statements)}条SQL语句")
                
                return all_results if fetch else None
                
        except Exception as e:
            try:
                if self.connection and not getattr(self.connection, "closed", 1):
                    self.connection.rollback()
            except Exception:
                pass
            # 连接被服务端掐断时自动重连一次再抛，便于上层重试/下一轮 Verify
            msg = str(e).lower()
            if _is_transient_pg_error(msg):
                try:
                    self.connect()
                    return self.execute(sql, fetch=fetch)
                except Exception:
                    pass
            if "connection already closed" in msg or "server closed the connection" in msg:
                try:
                    self.connect()
                except Exception:
                    pass
            raise
    
    def close(self) -> None:
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            default_logger.info("已关闭PostgreSQL数据库连接")

    def insert_rows(
        self,
        table_name: str,
        columns: List[str],
        rows: List[Dict[str, Any]],
        *,
        quote: str = '"',
        batch_size: int = 1000,
    ) -> int:
        if not self.connection:
            raise RuntimeError("数据库未连接,请先调用connect()")
        if not rows:
            return 0

        def _ident(name: str) -> str:
            # mogrify 会把标识符里的 % 当成占位符，列名含 Percent (%) 时必须写成 %%
            return f"{quote}{str(name).replace(quote, quote + quote)}{quote}".replace(
                "%", "%%"
            )

        cols = ", ".join(_ident(c) for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {_ident(table_name)} ({cols}) VALUES ({placeholders})"
        inserted = 0
        try:
            with self.connection.cursor() as cursor:
                for i in range(0, len(rows), batch_size):
                    chunk = rows[i : i + batch_size]
                    data = [tuple(r.get(c) for c in columns) for r in chunk]
                    psycopg2.extras.execute_batch(cursor, sql, data, page_size=batch_size)
                    inserted += len(chunk)
            self.connection.commit()
            return inserted
        except Exception:
            self.connection.rollback()
            raise

    def execute_autocommit(self, sql: str) -> None:
        """在 autocommit 下执行（用于 CREATE DATABASE 等）。"""
        if not self.connection:
            raise RuntimeError("数据库未连接,请先调用connect()")
        old = self.connection.autocommit
        try:
            self.connection.autocommit = True
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
        finally:
            self.connection.autocommit = old
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在（支持大小写折叠到 public 基表）。"""
        return self.resolve_table_name(table_name) is not None

    def list_base_tables(self) -> List[str]:
        """列出 public schema 下的基表名。"""
        try:
            sql = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
            results = self.execute(sql, fetch=True) or []
            rows = results[0].get("rows") if results else []
            return [str(r["table_name"]) for r in (rows or []) if r.get("table_name")]
        except Exception as e:
            default_logger.error(f"列出基表失败: {e}")
            return []

    def resolve_table_name(self, table_name: str) -> Optional[str]:
        """将候选表名解析为库内真实表名（剥 public.、大小写折叠）。"""
        raw = (table_name or "").strip().strip('"')
        if not raw:
            return None
        if "." in raw:
            parts = [p.strip().strip('"') for p in raw.split(".") if p.strip()]
            raw = parts[-1] if parts else raw
        low = raw.lower()
        if low.startswith("pg_") or low in {"information_schema"}:
            return None
        try:
            with self.connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_type = 'BASE TABLE'
                      AND (
                        table_name = %s
                        OR lower(table_name) = lower(%s)
                      )
                    ORDER BY CASE WHEN table_name = %s THEN 0 ELSE 1 END
                    LIMIT 1
                    """,
                    (raw, raw, raw),
                )
                row = cur.fetchone()
                return row[0] if row else None
        except Exception as e:
            default_logger.error(f"解析表名失败 {table_name}: {e}")
            return None
    
    def get_table_count(self, table_name: str) -> int:
        """获取表的行数"""
        try:
            resolved = self.resolve_table_name(table_name) or table_name
            sql = f'SELECT COUNT(*) as cnt FROM "{resolved}"'
            results = self.execute(sql, fetch=True) or []
            if not results or not isinstance(results[0], dict):
                return 0
            rows = results[0].get("rows") or []
            if not rows:
                return 0
            row0 = rows[0]
            if isinstance(row0, dict):
                return int(row0.get("cnt") or 0)
            if isinstance(row0, (list, tuple)) and row0:
                return int(row0[0])
            return 0
        except Exception as e:
            default_logger.error(f"获取表行数失败: {e}")
            raise

    def get_sequence_create_ddls(self, table_name: str) -> List[str]:
        """为表关联序列生成 CREATE SEQUENCE（建表前执行）。"""
        resolved = self.resolve_table_name(table_name) or table_name
        sql = """
            SELECT DISTINCT
                quote_ident(n.nspname) AS schema_ident,
                quote_ident(c.relname) AS seq_ident,
                s.seqstart,
                s.seqincrement,
                s.seqmax,
                s.seqmin,
                s.seqcache,
                s.seqcycle
            FROM pg_class t
            JOIN pg_depend d ON d.refobjid = t.oid AND d.deptype IN ('a', 'i')
            JOIN pg_class c ON c.oid = d.objid AND c.relkind = 'S'
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_sequence s ON s.seqrelid = c.oid
            WHERE t.oid = %s::regclass
            ORDER BY 2
        """
        ddls: List[str] = []
        try:
            with self.connection.cursor() as cur:
                cur.execute(sql, (resolved,))
                for row in cur.fetchall() or []:
                    schema_ident, seq_ident, start, inc, mx, mn, cache, cycle = row
                    cycle_sql = "CYCLE" if cycle else "NO CYCLE"
                    # IF NOT EXISTS 避免重复 reset 时冲突
                    ddls.append(
                        f"CREATE SEQUENCE IF NOT EXISTS {schema_ident}.{seq_ident} "
                        f"START WITH {int(start)} INCREMENT BY {int(inc)} "
                        f"MINVALUE {int(mn)} MAXVALUE {int(mx)} "
                        f"CACHE {int(cache)} {cycle_sql}"
                    )
            return ddls
        except Exception as e:
            default_logger.warning(f"获取序列 DDL 失败 {resolved}: {e}")
            return []

    def get_sequence_owned_by_ddls(self, table_name: str) -> List[str]:
        """建表后恢复 OWNED BY，并尽力 setval。"""
        resolved = self.resolve_table_name(table_name) or table_name
        sql = """
            SELECT
                n.nspname AS seq_schema,
                c.relname AS seq_name,
                a.attname AS col_name
            FROM pg_class t
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum > 0 AND NOT a.attisdropped
            JOIN pg_depend d ON d.refobjid = t.oid AND d.refobjsubid = a.attnum AND d.deptype = 'a'
            JOIN pg_class c ON c.oid = d.objid AND c.relkind = 'S'
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE t.oid = %s::regclass
        """
        ddls: List[str] = []
        try:
            with self.connection.cursor() as cur:
                cur.execute(sql, (resolved,))
                for seq_schema, seq_name, col_name in cur.fetchall() or []:
                    seq_qual = f'"{seq_schema}"."{seq_name}"'
                    col_qual = f'"{resolved}"."{col_name}"'
                    ddls.append(f"ALTER SEQUENCE {seq_qual} OWNED BY {col_qual}")
                    # setval 用 regclass，避免字面量引号坑
                    ddls.append(
                        "SELECT setval("
                        f"'{seq_schema}.{seq_name}'::regclass, "
                        f'COALESCE((SELECT MAX("{col_name}") FROM "{resolved}"), 1), true)'
                    )
            return ddls
        except Exception as e:
            default_logger.warning(f"获取序列 OWNED BY 失败 {resolved}: {e}")
            return []

    @staticmethod
    def strip_nextval_defaults(create_ddl: str) -> str:
        """去掉 DEFAULT nextval(...)，用于序列同步失败时的降级建表。"""
        return re.sub(
            r"\s+DEFAULT\s+nextval\s*\([^)]+\)",
            "",
            create_ddl,
            flags=re.IGNORECASE,
        )
        
    def get_create_table_ddl(self, table_name: str) -> str:
        """获取表的CREATE TABLE语句（列名 quote_ident，兼容 PG 12+）。"""
        try:
            resolved = self.resolve_table_name(table_name) or table_name
            sql = """
            SELECT 
                'CREATE TABLE ' || quote_ident(%s) || ' (' ||
                string_agg(
                    '    ' || column_def,
                    ',\n'
                ) ||
                CASE 
                    WHEN pk_cols IS NOT NULL THEN 
                        ',\n    CONSTRAINT ' || quote_ident(pk_name) || 
                        ' PRIMARY KEY (' || pk_cols || ')'
                    ELSE ''
                END ||
                '\n);' AS create_table
            FROM (
                SELECT 
                    a.attname AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                    CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END AS not_null,
                    CASE 
                        WHEN ad.adbin IS NOT NULL THEN 
                            ' DEFAULT ' || pg_get_expr(ad.adbin, ad.adrelid)
                        ELSE ''
                    END AS default_val,
                    a.attnum,
                    pk_info.pk_name,
                    pk_info.pk_cols
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_attrdef ad 
                    ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
                LEFT JOIN (
                    SELECT 
                        c.conname AS pk_name,           
                        string_agg(quote_ident(a2.attname), ', ' ORDER BY array_position(c.conkey, a2.attnum)) AS pk_cols,
                        c.conrelid                 
                    FROM pg_constraint c
                    JOIN pg_attribute a2 
                        ON a2.attrelid = c.conrelid AND a2.attnum = ANY(c.conkey)
                    WHERE c.contype = 'p' 
                    AND c.conrelid = %s::regclass
                    GROUP BY c.conname, c.conrelid
                ) pk_info ON pk_info.conrelid = a.attrelid
                WHERE a.attrelid = %s::regclass
                AND a.attnum > 0 
                AND NOT a.attisdropped
                ORDER BY a.attnum
            ) cols,
            LATERAL (
                SELECT 
                    quote_ident(cols.column_name) || ' ' || cols.data_type || cols.not_null || cols.default_val AS column_def
            ) col_format
            GROUP BY pk_name, pk_cols;
            """
            
            with self.connection.cursor() as cur:
                cur.execute(sql, (resolved, resolved, resolved))
                result = cur.fetchone()
                
                if not result or not result[0]:
                    raise ValueError(f"表 {resolved} 不存在或无法生成 DDL")
                    
                return result[0].strip()
                
        except Exception as e:
            default_logger.error(f"获取表DDL失败: {e}")
            raise