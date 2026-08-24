import re
import pymysql
import time
from typing import Any, List, Dict, Optional
from sandbox.executor import DBExecutor
from utils.logger import default_logger
from utils.sql_wrap import split_sql_statements


class MySQLExecutor(DBExecutor):
    """MySQL数据库执行器"""
    
    def connect(self) -> None:
        """连接MySQL数据库"""
        try:            
            self.connection = pymysql.connect(
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 3306),
                user=self.config.get("user", "root"),
                password=self.config.get("password", ""),
                database=self.config.get("database", "test"),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
                connect_timeout=30,
                read_timeout=120,
                write_timeout=120,
            )
            default_logger.info(f"成功连接到MySQL数据库: {self.config.get('host')}:{self.config.get('port')}")
            
            # 启用性能测量相关设置
            self._enable_profiling()
            
        except Exception as e:
            default_logger.error(f"连接MySQL数据库失败: {e}")
            raise

    def _ensure_connection(self) -> None:
        """连接断开时自动重连（pymysql InterfaceError (0, '') 常见）。"""
        if self.connection is None:
            self.connect()
            return
        try:
            self.connection.ping(reconnect=True)
        except Exception:
            try:
                self.connection.close()
            except Exception:
                pass
            self.connection = None
            self.connect()
    
    def _enable_profiling(self) -> None:
        """启用MySQL性能分析功能"""
        try:
            with self.connection.cursor() as cursor:
                # 启用profiling（用于详细的性能分析）
                cursor.execute("SET profiling = 1")
                # 设置profiling历史记录数量
                cursor.execute("SET profiling_history_size = 100")
            default_logger.info("已启用MySQL性能分析功能（profiling）")
        except Exception as e:
            default_logger.warning(f"启用性能分析功能失败: {e}")
            # 不抛出异常，因为这不是致命错误
    
    def execute(self, sql: str, fetch: bool = True) -> Optional[List[Dict[str, Any]]]:
        """
        执行SQL语句，并测量执行时间
        
        支持多语句执行（用分号分隔），会依次执行每条语句并返回所有结果
        每条语句的结果中都包含执行时间（秒）
        """
        self._ensure_connection()
        
        # 按分号拆分SQL语句
        statements = split_sql_statements(sql)
        
        all_results = []
        
        try:
            with self.connection.cursor() as cursor:
                for stmt in statements:
                    default_logger.debug(f"执行SQL: {stmt[:100]}...")
                    
                    # 记录开始时间
                    start_time = time.time()
                    
                    # 执行SQL
                    cursor.execute(stmt)
                    
                    # 记录结束时间
                    end_time = time.time()
                    execution_time = end_time - start_time
                    
                    # 判断是否是查询语句（去掉注释行再提取首字）
                    if fetch and re.sub(r'--.*', '', stmt).strip().upper().startswith(('SELECT', 'SHOW', 'EXPLAIN', 'DESC', 'DESCRIBE')):
                        result = cursor.fetchall()
                        all_results.append({
                            "sql": stmt,
                            "rows": result,
                            "row_count": len(result),
                            "execution_time": round(execution_time, 4),  # 秒，保留4位小数
                            "execution_time_ms": round(execution_time * 1000, 2)  # 毫秒，保留2位小数
                        })
                        default_logger.debug(f"查询执行时间: {execution_time:.4f}秒 ({execution_time * 1000:.2f}ms)")
                    else:
                        # 非查询语句，返回影响的行数
                        all_results.append({
                            "sql": stmt,
                            "affected_rows": cursor.rowcount,
                            "message": "执行成功",
                            "execution_time": round(execution_time, 4),
                            "execution_time_ms": round(execution_time * 1000, 2)
                        })
                        default_logger.debug(f"语句执行时间: {execution_time:.4f}秒 ({execution_time * 1000:.2f}ms)")
                
                return all_results if fetch else None
                
        except Exception as e:
            # 连接类错误：重连后重试一次
            err_s = str(e)
            if "(0, '')" in err_s or "Gone away" in err_s or "Lost connection" in err_s:
                default_logger.warning(f"MySQL 连接异常，尝试重连后重试: {e}")
                try:
                    self._ensure_connection()
                    with self.connection.cursor() as cursor:
                        all_results = []
                        for stmt in statements:
                            start_time = time.time()
                            cursor.execute(stmt)
                            execution_time = time.time() - start_time
                            if fetch and re.sub(r'--.*', '', stmt).strip().upper().startswith(
                                ('SELECT', 'SHOW', 'EXPLAIN', 'DESC', 'DESCRIBE')
                            ):
                                result = cursor.fetchall()
                                all_results.append({
                                    "sql": stmt,
                                    "rows": result,
                                    "row_count": len(result),
                                    "execution_time": round(execution_time, 4),
                                    "execution_time_ms": round(execution_time * 1000, 2),
                                })
                            else:
                                all_results.append({
                                    "sql": stmt,
                                    "affected_rows": cursor.rowcount,
                                    "message": "执行成功",
                                    "execution_time": round(execution_time, 4),
                                    "execution_time_ms": round(execution_time * 1000, 2),
                                })
                        return all_results if fetch else None
                except Exception as e2:
                    default_logger.error(f"执行SQL失败(重试后): {e2}")
                    raise
            try:
                if self.connection:
                    self.connection.rollback()
            except Exception:
                pass
            default_logger.error(f"执行SQL失败: {e}")
            raise
    def close(self) -> None:
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            default_logger.info("已关闭MySQL数据库连接")

    def insert_rows(
        self,
        table_name: str,
        columns: List[str],
        rows: List[Dict[str, Any]],
        *,
        quote: str = "`",
        batch_size: int = 1000,
    ) -> int:
        self._ensure_connection()
        if not rows:
            return 0
        cols = ", ".join(
            f"{quote}{c.replace(quote, quote + quote)}{quote}".replace("%", "%%")
            for c in columns
        )
        placeholders = ", ".join(["%s"] * len(columns))
        tq = f"{quote}{table_name.replace(quote, quote + quote)}{quote}".replace("%", "%%")
        sql = f"INSERT INTO {tq} ({cols}) VALUES ({placeholders})"
        inserted = 0
        with self.connection.cursor() as cursor:
            for i in range(0, len(rows), batch_size):
                chunk = rows[i : i + batch_size]
                data = [tuple(r.get(c) for c in columns) for r in chunk]
                cursor.executemany(sql, data)
                inserted += len(chunk)
        return inserted

    def execute_autocommit(self, sql: str) -> None:
        self._ensure_connection()
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            sql = f"SHOW TABLES LIKE '{table_name}'"
            results = self.execute(sql, fetch=True)
            return len(results[0]["rows"]) > 0
        except Exception as e:
            default_logger.error(f"检查表是否存在失败: {e}")
            return False
    
    def get_table_count(self, table_name: str) -> int:
        """获取表的行数"""
        try:
            sql = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
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
    
    def get_create_table_ddl(self, table_name: str) -> str:
        """获取表的CREATE TABLE语句"""
        try:
            sql = f"SHOW CREATE TABLE `{table_name}`"
            results = self.execute(sql, fetch=True)
            # SHOW CREATE TABLE返回的结果中，第二列是CREATE TABLE语句
            create_stmt = results[0]["rows"][0]["Create Table"]
            return create_stmt
        except Exception as e:
            default_logger.error(f"获取表DDL失败: {e}")
            raise

