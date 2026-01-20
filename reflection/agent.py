import asyncio
from typing import Any, Dict, Optional
from langchain_core.messages import SystemMessage, HumanMessage

from reflection.graph import create_reflection_graph
from reflection.prompts import create_initial_message, create_system_prompt
from utils.logger import default_logger
from utils.stream_handler import StreamHandler


class ReflectAgent:
    """知识图谱更新反思Agent"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get("reflection", {})
        self.llm_config = config.get("llm", {})


        self.graph = create_reflection_graph(
            llm_config=self.llm_config,
            )

        default_logger.info(f"ReflectAgent初始化完成 - LLM: {self.llm_config}")

    async def reflect_from_solution_stream(
        self, 
        sql: str | list[str],
        sol_sql: str | list[str], 
        report: str = None, 
        schema: str = None,
        tables: list = None,
        exec_log: str = None,
        stream_handler: Optional[StreamHandler] = None
    ) -> str:
        """
        从诊断/修复报告中反思知识（流式）
        
        Args:
            sql: 原问题SQL
            sol_sql: 修复SQL
            report: 修复报告
            schema: 相关表结构（DDL）
            tables: 相关表信息
            exec_log: 相关执行日志

        Returns:
            结构化的诊断/修复经验
        """
        default_logger.info(f"SQL: {sql[:100]}...")
        default_logger.info(f"涉及表: {tables}")
        if exec_log:
            default_logger.info(f"执行日志: {exec_log[:100]}...")
        if report:
            default_logger.info(f"修复报告: {report[:100]}...")
        if schema:
            default_logger.info(f"相关表结构（DDL）: {schema[:100]}...")
        if sol_sql:
            default_logger.info(f"解决方案SQL: {sol_sql[:100]}...")

        try:
            system_prompt = create_system_prompt()
            initial_message = create_initial_message(
                sql, 
                sol_sql, 
                report, 
                schema, 
                tables, 
                exec_log)
            
            initial_state = {
                "messages": [
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=initial_message)
                ],
                "iteration": 0,
                "max_iter": self.max_iter,
                "error_message": None,
                "json_content": None
            }

            async for event in self.graph.astream_events(initial_state, version="v2"):
                # 通过 stream_handler 处理事件
                if stream_handler:
                    stream_handler.handle_event(event)
                
                # 提取最终状态（从 on_chain_end 事件）
                if event.get("event") == "on_chain_end":
                    event_data = event.get("data", {})
                    output = event_data.get("output", {})
                    if isinstance(output, dict) and "json_content" in output:
                        json_content = output.get("json_content", None)
                        return json_content

        except Exception as e:
            error_msg = f"反思/抽取过程发生错误: {str(e)}"
            default_logger.error(error_msg, exc_info=True)
            return None

    def reflect_from_solution(
        self, 
        sql: str | list[str],
        sol_sql: str | list[str], 
        report: str = None, 
        schema: str = None,
        tables: list = None,
        exec_log: str = None,
    ) -> str:
        """
        从诊断/修复报告中反思知识
        
        Args:
            sql: 原问题SQL
            sol_sql: 修复SQL
            report: 修复报告
            schema: 相关表结构（DDL）
            tables: 相关表信息
            exec_log: 相关执行日志

        Returns:
            结构化的诊断/修复经验
        """
        stream_handler = StreamHandler(mode="logger", logger=default_logger)
        json_content = asyncio.run(self.reflect_from_solution_stream(
            sql, 
            sol_sql, 
            report, 
            schema, 
            tables, 
            exec_log, 
            stream_handler=stream_handler))

        return json_content