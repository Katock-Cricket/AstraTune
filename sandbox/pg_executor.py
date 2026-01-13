import re
import psycopg2
import psycopg2.extras
import time
from typing import Any, List, Dict, Optional
from sandbox.executor import DBExecutor
from utils.logger import default_logger


class PGExecutor(DBExecutor):
    """PostgreSQL数据库执行器"""
    
    def connect(self) -> None:
        """连接PostgreSQL数据库"""
        try:
            self.connection = psycopg2.connect(
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 5432),
                user=self.config.get("user", "postgres"),
                password=self.config.get("password", ""),
                database=self.config.get("database", "postgres")
            )
            # 设置自动提交为False，手动控制事务
            self.connection.autocommit = False
            default_logger.info(f"成功连接到PostgreSQL数据库: {self.config.get('host')}:{self.config.get('port')}")
            
        except Exception as e:
            default_logger.error(f"连接PostgreSQL数据库失败: {e}")
            raise
    
    def execute(self, sql: str, fetch: bool = True) -> Optional[List[Dict[str, Any]]]:
        """
        执行SQL语句, 并测量执行时间
        
        支持多语句执行(用分号分隔),会依次执行每条语句并返回所有结果
        每条语句的结果中都包含执行时间(秒)
        """
        if not self.connection:
            raise RuntimeError("数据库未连接,请先调用connect()")
        
        # 按分号拆分SQL语句
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        
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
            self.connection.rollback()
            # default_logger.error(f"执行SQL失败: {e}")
            raise
    
    def close(self) -> None:
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            default_logger.info("已关闭PostgreSQL数据库连接")
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            # PostgreSQL使用information_schema查询表是否存在
            sql = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                )
            """
            results = self.execute(sql, fetch=True)
            return results[0]["rows"][0]["exists"]
        except Exception as e:
            default_logger.error(f"检查表是否存在失败: {e}")
            return False
    
    def get_table_count(self, table_name: str) -> int:
        """获取表的行数"""
        try:
            sql = f'SELECT COUNT(*) as cnt FROM "{table_name}"'
            results = self.execute(sql, fetch=True)
            return results[0]["rows"][0]["cnt"]
        except Exception as e:
            default_logger.error(f"获取表行数失败: {e}")
            raise
        
    def get_create_table_ddl(self, table_name: str) -> str:
        """获取表的CREATE TABLE语句（兼容 PostgreSQL 12+，修复 conrelid 引用）"""
        try:
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
                    -- 子查询：获取主键信息
                    SELECT 
                        c.conname AS pk_name,           
                        string_agg(a2.attname, ', ' ORDER BY array_position(c.conkey, a2.attnum)) AS pk_cols,
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
                    cols.column_name || ' ' || cols.data_type || cols.not_null || cols.default_val AS column_def
            ) col_format
            GROUP BY pk_name, pk_cols;
            """
            
            with self.connection.cursor() as cur:
                cur.execute(sql, (table_name, table_name, table_name))
                result = cur.fetchone()
                
                if not result or not result[0]:
                    raise ValueError(f"表 {table_name} 不存在或无法生成 DDL")
                    
                return result[0].strip()
                
        except Exception as e:
            default_logger.error(f"获取表DDL失败: {e}")
            raise