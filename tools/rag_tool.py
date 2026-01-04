from typing import List, Dict, Any
from utils.logger import default_logger


class RAGTool:
    """RAG检索工具，用于检索相似的慢SQL诊断案例"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化RAG工具
        
        Args:
            config: RAG配置（预留）
        """
        self.config = config or {}
        default_logger.info("RAG工具已初始化（当前为接口模式，未实现实际检索）")
    
    def retrieve_similar_cases(
        self, 
        sql: str, 
        schema: str, 
        tables: List[str]
    ) -> List[Dict[str, Any]]:
        """
        检索相似的慢SQL诊断案例
        
        Args:
            sql: 当前的慢SQL语句
            schema: 相关表的schema信息
            tables: 相关表名列表
        
        Returns:
            相似案例列表，格式: [{"sql": "...", "problem": "...", "solution": "..."}, ...]
        """
        default_logger.info(f"RAG检索请求 - SQL: {sql[:100]}..., 表: {tables}")
        
        # 当前返回空列表，表示未找到相似案例
        # 未来可以实现向量数据库检索、知识图谱查询等
        return []


def create_rag_tool_function(rag_tool: RAGTool):
    """
    创建供LangChain/LangGraph使用的RAG工具函数
    
    Args:
        rag_tool: RAGTool实例
    
    Returns:
        可以被LangChain调用的函数
    """
    def retrieve_similar_cases(sql: str, schema: str, tables: str) -> str:
        """
        检索知识库中相似的慢SQL诊断案例。
        
        可以帮助你参考历史上类似问题的诊断经验和解决方案。
        
        Args:
            sql: 当前的慢SQL语句
            schema: 相关表的schema信息（DDL语句）
            tables: 相关表名，多个表用逗号分隔
        
        Returns:
            相似案例的描述，如果没有找到则返回提示信息
        """
        # 解析tables参数
        table_list = [t.strip() for t in tables.split(",") if t.strip()]
        
        # 调用RAG工具
        cases = rag_tool.retrieve_similar_cases(sql, schema, table_list)
        
        if not cases:
            return "未找到相似的历史案例。建议基于当前SQL和表结构进行分析。"
        
        # 格式化案例返回
        formatted_cases = []
        for i, case in enumerate(cases, 1):
            formatted_cases.append(f"案例 {i}:")
            formatted_cases.append(f"  SQL: {case.get('sql', 'N/A')}")
            formatted_cases.append(f"  问题: {case.get('problem', 'N/A')}")
            formatted_cases.append(f"  解决方案: {case.get('solution', 'N/A')}")
            formatted_cases.append("")
        
        return "\n".join(formatted_cases)
    
    return retrieve_similar_cases

