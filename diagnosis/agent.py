from typing import List, Dict, Any
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import tool
from diagnosis.graph import create_diagnosis_graph
from diagnosis.prompts import create_system_prompt, create_initial_message
from tools.sandbox_tool import SandboxTool, create_sandbox_tool_function
from tools.rag_tool import RAGTool, create_rag_tool_function
from utils.logger import default_logger


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
    
    def diagnose(
        self,
        ori_sql: str,
        schema: str,
        tables: List[str],
        exec_log: str = "",
        sampled_tables: List[str] = []
    ) -> str:
        """
        执行慢SQL诊断
        
        Args:
            ori_sql: 原始慢SQL语句
            schema: 相关表的schema信息（DDL语句）
            tables: 相关表名列表
            exec_log: 执行日志（包含平均执行时间、执行次数等）
            sampled_tables: 采样表列表（可选）
        
        Returns:
            诊断报告（自然语言）
        """
        default_logger.info("开始执行慢SQL诊断")
        default_logger.info(f"SQL: {ori_sql[:100]}...")
        default_logger.info(f"涉及表: {tables}")
        default_logger.info(f"采样表: {sampled_tables}")
        default_logger.info(f"执行日志: {exec_log[:100]}...")
                
        try:
            # 创建初始消息
            initial_message = create_initial_message(
                sql=ori_sql,
                schema=schema,
                tables=tables,
                exec_log=exec_log,
                sampled_tables=sampled_tables
            )
            
            # 构建初始状态
            initial_state = {
                "messages": [
                    SystemMessage(content=create_system_prompt()),
                    HumanMessage(content=initial_message)
                ],
                "iteration": 0,
                "max_iter": self.max_iter,
                "conclusion": ""
            }
            
            # 执行状态图
            default_logger.info("开始执行LangGraph状态图")
            final_state = self.graph.invoke(initial_state)
            
            # 提取诊断结论
            conclusion = final_state.get("conclusion", "")
            
            if not conclusion:
                # 如果没有结论，从最后一条消息中提取
                messages = final_state.get("messages", [])
                if messages:
                    last_message = messages[-1]
                    if hasattr(last_message, "content"):
                        conclusion = last_message.content
            
            if not conclusion:
                conclusion = "诊断未能生成有效结论，请检查日志。"
            
            default_logger.info("诊断完成")
            default_logger.info(f"总迭代次数: {final_state.get('iteration', 0)}")
            
            return conclusion
            
        except Exception as e:
            error_msg = f"诊断过程发生错误: {str(e)}"
            default_logger.error(error_msg)
            return error_msg
