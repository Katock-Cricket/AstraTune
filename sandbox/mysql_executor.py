import pymysql
import time
from typing import Any, List, Dict, Optional
from sandbox.executor import DBExecutor
from utils.logger import default_logger


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
                cursorclass=pymysql.cursors.DictCursor
            )
            default_logger.info(f"成功连接到MySQL数据库: {self.config.get('host')}:{self.config.get('port')}")
            
            # 启用性能测量相关设置
            self._enable_profiling()
            
        except Exception as e:
            default_logger.error(f"连接MySQL数据库失败: {e}")
            raise
    
    def _enable_profiling(self) -> None:
        """启用MySQL性能分析功能"""
        try:
            with self.connection.cursor() as cursor:
                # 启用profiling（用于详细的性能分析）
                cursor.execute("SET profiling = 1")
                # 设置profiling历史记录数量
                cursor.execute("SET profiling_history_size = 100")
                self.connection.commit()
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
        if not self.connection:
            raise RuntimeError("数据库未连接，请先调用connect()")
        
        # 按分号拆分SQL语句
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        
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
                    
                    # 判断是否是查询语句
                    if fetch and stmt.strip().upper().startswith(('SELECT', 'SHOW', 'EXPLAIN', 'DESC', 'DESCRIBE')):
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
                
                self.connection.commit()
                # default_logger.info(f"成功执行{len(statements)}条SQL语句")
                
                return all_results if fetch else None
                
        except Exception as e:
            self.connection.rollback()
            # default_logger.error(f"执行SQL失败: {e}")
    
    def close(self) -> None:
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            default_logger.info("已关闭MySQL数据库连接")
    
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
            results = self.execute(sql, fetch=True)
            return results[0]["rows"][0]["cnt"]
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

