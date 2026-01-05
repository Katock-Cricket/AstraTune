from sandbox.executor import DBExecutor
from sandbox.table_mapper import TableMapper
from utils.logger import default_logger


class SandboxTool:
    """沙箱工具，封装数据库操作供Agent调用"""
    
    def __init__(self, executor: DBExecutor, table_mapper: TableMapper):
        """
        初始化沙箱工具
        
        Args:
            executor: 数据库执行器
            table_mapper: 表名映射器
        """
        self.executor = executor
        self.table_mapper = table_mapper

    def register_preprocess_sql(self, preprocess_sql: str) -> None:
        """
        注册前置sql
        
        Args:
            preprocess_sql: 前置sql
        """
        self.preprocess_sql = preprocess_sql

    def register_clean_up_sql(self, clean_up_sql: str) -> None:
        """
        注册清理sql
        
        Args:
            clean_up_sql: 清理sql
        """
        self.clean_up_sql = clean_up_sql

    def  execute_sql(self, sql: str) -> str:
        """
        执行SQL语句（Agent调用的主要接口）
        
        自动进行表名替换，将原表名替换为沙箱表名
        
        Args:
            sql: SQL语句（可以包含多条，用分号分隔）
        
        Returns:
            格式化的执行结果字符串
        """
        default_logger.info(f"Agent请求执行SQL: {sql[:100]}...")
        
        try:
            # 进行表名替换
            replaced_sql = self.table_mapper.replace_table_names(sql)
            
            # 执行前置sql
            if self.preprocess_sql:
                result_pre = self.executor.execute(self.preprocess_sql, fetch=True)
                default_logger.info(f"前置sql执行成功: {result_pre[:100]}...")

            # 执行SQL
            results = self.executor.execute(replaced_sql, fetch=True)

            # 执行清理sql
            if self.clean_up_sql:
                result_clean_up = self.executor.execute(self.clean_up_sql, fetch=True)
                default_logger.info(f"清理sql执行成功: {result_clean_up[:100]}...")
            
            # 格式化结果返回给Agent
            formatted_result = self._format_results(results)
            
            default_logger.info(f"SQL执行成功，返回结果长度: {len(formatted_result)}")
            return formatted_result
            
        except Exception as e:
            error_msg = f"SQL执行失败: {str(e)}"
            default_logger.error(error_msg)
            return error_msg
    
    def _format_results(self, results: list) -> str:
        """
        格式化执行结果为字符串
        
        Args:
            results: 执行结果列表
        
        Returns:
            格式化的结果字符串
        """
        if not results:
            return "执行成功，无返回结果"
        
        formatted_parts = []
        
        for i, result in enumerate(results, 1):
            sql = result.get("sql", "")
            
            # 获取执行时间
            exec_time = result.get("execution_time")
            exec_time_ms = result.get("execution_time_ms")
            time_str = f"⏱️ 执行时间: {exec_time}秒 ({exec_time_ms}ms)" if exec_time is not None else ""
            
            # 查询语句的结果
            if "rows" in result:
                rows = result["rows"]
                row_count = result["row_count"]
                
                formatted_parts.append(f"[语句 {i}] {sql}")
                if time_str:
                    formatted_parts.append(time_str)
                formatted_parts.append(f"返回 {row_count} 行:")
                
                # 限制显示的行数，避免结果过长
                max_display_rows = 20
                display_rows = rows[:max_display_rows]
                
                if display_rows:
                    # 显示结果
                    for row in display_rows:
                        formatted_parts.append(str(row))
                    
                    if row_count > max_display_rows:
                        formatted_parts.append(f"... (还有 {row_count - max_display_rows} 行未显示)")
                else:
                    formatted_parts.append("(空结果集)")
            
            # 非查询语句的结果
            elif "affected_rows" in result:
                affected = result["affected_rows"]
                message = result.get("message", "")
                
                formatted_parts.append(f"[语句 {i}] {sql}")
                if time_str:
                    formatted_parts.append(time_str)
                formatted_parts.append(f"影响 {affected} 行，{message}")
            
            formatted_parts.append("")  # 空行分隔
        
        return "\n".join(formatted_parts)


def create_sandbox_tool_function(sandbox_tool: SandboxTool):
    """
    创建供LangChain/LangGraph使用的工具函数
    
    Args:
        sandbox_tool: SandboxTool实例
    
    Returns:
        可以被LangChain调用的函数
    """
    def execute_sql(sql: str) -> str:
        """
        在沙箱数据库中执行SQL语句。
        
        你可以执行任何SQL操作，包括：
        - EXPLAIN 分析查询计划
        - 创建、删除索引
        - 调整会话参数（SET）
        - 查询统计信息
        - 执行测试SQL等...
        
        系统会自动将表名替换为沙箱表名，你只需使用原始表名即可。
        
        Args:
            sql: 要执行的SQL语句，可以包含多条语句（用分号分隔）
        
        Returns:
            执行结果的详细描述
        """
        return sandbox_tool.execute_sql(sql)
    
    return execute_sql

