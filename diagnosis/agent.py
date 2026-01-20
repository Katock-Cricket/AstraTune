from typing import List, Dict, Any, Optional
import asyncio
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import tool
from diagnosis.graph import create_diagnosis_graph
from diagnosis.prompts import create_system_prompt, create_initial_message
from tools.sandbox_tool import SandboxTool, create_sandbox_tool_function
from tools.rag_tool import RAGTool, create_rag_tool_function
from utils.logger import default_logger
from utils.stream_handler import StreamHandler


class DiagnosisAgent:
    """慢SQL诊断Agent"""
    
    def __init__(self, config: Dict[str, Any], sandbox_tool: SandboxTool = None, rag_tool: RAGTool = None):
        """
        初始化DiagnosisAgent实例
        
        Args:
            config: Agent配置，包含LLM配置和其他参数
            sandbox_tool: 沙箱工具实例
            rag_tool: RAG工具实例（可选）
        """
        self.config = config
        self.sandbox_tool = sandbox_tool
        self.rag_tool = rag_tool
        
        # 获取配置参数
        self.llm_config = config.get("llm", {})
        self.max_iter = config.get("max_iter", 10)
        self.enable_rag = config.get("enable_rag", False)
        self.enable_test = config.get("enable_test", False)
        
        # 创建工具列表
        self.tools = self._create_tools()
        
        # 创建状态图
        self.graph = create_diagnosis_graph(
            llm_config=self.llm_config,
            tools=self.tools
            )
        
        default_logger.info(f"DiagnosisAgent初始化完成 - 最大迭代次数: {self.max_iter}, RAG: {self.enable_rag}, 沙箱测试: {self.enable_test}")
    
    def _create_tools(self) -> list:
        """创建Agent可用的工具列表"""
        tools = []
        
        # 沙箱执行工具
        if self.enable_test:
            execute_sql_func = create_sandbox_tool_function(self.sandbox_tool)
            tools.append(tool(execute_sql_func))
            default_logger.info("已启用沙箱测试工具")
        
        # RAG检索工具
        if self.enable_rag:
            rag_func = create_rag_tool_function(self.rag_tool)
            tools.append(tool(rag_func))
            default_logger.info("已启用RAG检索工具")
        
        return tools
    
    async def diagnose_stream(
        self,
        ori_sql: str,
        schema: str,
        tables: List[str],
        exec_log: str = None,
        sampled_tables: List[str] = None,
        preprocess_sql: str = None,
        clean_up_sql: str = None,
        user_prompt: str = None,
        stream_handler: Optional[StreamHandler] = None
    ) -> str:
        """
        流式执行慢SQL诊断（异步）
        
        使用 astream_events(version="v2") 捕获完整的推理过程
        通过 stream_handler 实时显示（Rich美化）
        同时通过 logger 记录到文件（如果指定了 --log-file）
        
        Args:
            ori_sql: 原始慢SQL语句
            schema: 相关表的schema信息（DDL语句）
            tables: 相关表名列表
            exec_log: 执行日志（包含平均执行时间、执行次数等）
            sampled_tables: 采样表列表（可选）
            preprocess_sql: 测试本条sql的前置sql
            clean_up_sql: 测试本条sql的清理sql，用于恢复测试环境
            user_prompt: 用户提示
            stream_handler: 流式事件处理器

        Returns:
            诊断报告（自然语言）
        """
        default_logger.info(f"SQL: {ori_sql[:100]}...")
        default_logger.info(f"涉及表: {tables}")
        default_logger.info(f"采样表: {sampled_tables}")
        if exec_log:
            default_logger.info(f"执行日志: {exec_log[:100]}...")
        if preprocess_sql:
            default_logger.info(f"前置sql: {preprocess_sql[:100]}...")
        if clean_up_sql:
            default_logger.info(f"清理sql: {clean_up_sql[:100]}...")
        if user_prompt:
            default_logger.info(f"用户提示: {user_prompt[:100]}...")
            
        try:
            # 创建系统提示
            system_prompt = create_system_prompt(enable_test=self.enable_test, enable_rag=self.enable_rag)
            
            # 创建初始消息
            initial_message = create_initial_message(
                sql=ori_sql,
                schema=schema,
                tables=tables,
                exec_log=exec_log,
                sampled_tables=sampled_tables,
                preprocess_sql=preprocess_sql,
                clean_up_sql=clean_up_sql,
                user_prompt=user_prompt
            )
            
            # 构建初始状态
            initial_state = {
                "messages": [
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=initial_message)
                ],
                "iteration": 0,
                "max_iter": self.max_iter,
                "conclusion": ""
            }
            
            conclusion = ""
            final_state = None
            
            async for event in self.graph.astream_events(initial_state, version="v2"):
                # 通过 stream_handler 处理事件
                if stream_handler:
                    stream_handler.handle_event(event)
                
                # 提取最终状态（从 on_chain_end 事件）
                if event.get("event") == "on_chain_end":
                    event_data = event.get("data", {})
                    output = event_data.get("output", {})
                    if isinstance(output, dict) and "conclusion" in output:
                        conclusion = output.get("conclusion", "")
                        final_state = output
            
            # 如果没有从事件中提取到结论，则使用最后一条消息作为结论
            if not conclusion and final_state:
                messages = final_state.get("messages", [])
                if messages:
                    last_message = messages[-1]
                    if hasattr(last_message, "content"):
                        conclusion = last_message.content
            
            if not conclusion:
                conclusion = "诊断未能生成有效结论，请检查日志。"
            
            # default_logger.info("诊断完成（流式）")
            if stream_handler:
                summary = stream_handler.get_summary()
                default_logger.info(f"总事件数: {summary['total_events']}, 迭代次数: {summary['iterations']}, 工具调用: {summary['tool_calls']}")
            
            return conclusion
            
        except Exception as e:
            error_msg = f"诊断过程发生错误: {str(e)}"
            default_logger.error(error_msg, exc_info=True)
            return error_msg
       
    
    def diagnose(
        self,
        ori_sql: str,
        schema: str,
        tables: List[str],
        exec_log: str = None,
        sampled_tables: List[str] = None,
        preprocess_sql: str = None,
        clean_up_sql: str = None,
        user_prompt: str = None
    ) -> str:
        """
        执行慢SQL诊断（同步包装器，保持向后兼容）
        
        Args:
            ori_sql: 原始慢SQL语句
            schema: 相关表的schema信息（DDL语句）
            tables: 相关表名列表
            exec_log: 执行日志（包含平均执行时间、执行次数等）
            sampled_tables: 采样表列表（可选）
            preprocess_sql: 测试本条sql的前置sql
            clean_up_sql: 测试本条sql的清理sql，用于恢复测试环境
            user_prompt: 用户提示

        Returns:
            诊断报告（自然语言）
        """
         # 创建 logger 模式的 stream_handler
        stream_handler = StreamHandler(mode="logger", logger=default_logger)
        
        # 复用流式诊断方法
        conclusion = asyncio.run(self.diagnose_stream(
            ori_sql=ori_sql,
            schema=schema,
            tables=tables,
            exec_log=exec_log,
            sampled_tables=sampled_tables,
            preprocess_sql=preprocess_sql,
            clean_up_sql=clean_up_sql,
            user_prompt=user_prompt,
            stream_handler=stream_handler
        ))

        return conclusion