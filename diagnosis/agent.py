from typing import List, Dict, Any, Optional
import asyncio
import json
import re
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import tool
from utils.llm import make_chat_openai
from diagnosis.graph import create_diagnosis_graph
from diagnosis.prompts import (
    create_system_prompt,
    create_initial_message,
    create_table_extraction_system_prompt,
    create_table_extraction_user_message,
    _tables_from_schema_ddl,
)
from tools.sandbox_tool import (
    SandboxTool,
    create_reset_sandbox_tool_function,
    create_sandbox_tool_function,
)
from utils.logger import default_logger
from utils.stream_handler import StreamHandler
from utils.timing import timed_stage


def _parse_tables_from_llm_text(content: str) -> List[str]:
    """从 LLM 回复中解析 {"tables":[...]}。"""
    if not content or not isinstance(content, str):
        return []
    text = content.strip()
    for m in re.finditer(r"\{[\s\S]*?\}", text):
        try:
            obj = json.loads(m.group())
            if isinstance(obj, dict) and "tables" in obj:
                raw = obj["tables"]
                if isinstance(raw, list):
                    return [str(t).strip() for t in raw if str(t).strip()]
        except json.JSONDecodeError:
            continue
    return []


def extract_involved_tables(
    sql: str,
    llm_config: dict,
    user_prompt: str = "",
) -> List[str]:
    """
    第一步：用 LLM 从 SQL + 用户问题描述中提取涉及的物理表名（不调用沙箱工具）。

    使用同步 invoke，避免 scripts 里 asyncio.run(抽表) 后再 asyncio.run(诊断)
    在 Windows 上触发 Event loop is closed → Connection error。
    """
    llm = make_chat_openai(llm_config, temperature=0)
    system = create_table_extraction_system_prompt()
    human = create_table_extraction_user_message(sql, user_prompt=user_prompt or "")
    with timed_stage("extract_tables_llm"):
        resp = llm.invoke(
            [SystemMessage(content=system), HumanMessage(content=human)]
        )
    content = resp.content if resp and isinstance(resp.content, str) else ""
    tables = _parse_tables_from_llm_text(content)
    return tables


async def extract_involved_tables_async(
    sql: str,
    llm_config: dict,
    user_prompt: str = "",
) -> List[str]:
    """异步版抽表；仅在已有 running loop 内使用，勿再包一层 asyncio.run。"""
    llm = make_chat_openai(llm_config, temperature=0)
    system = create_table_extraction_system_prompt()
    human = create_table_extraction_user_message(sql, user_prompt=user_prompt or "")
    with timed_stage("extract_tables_llm"):
        resp = await llm.ainvoke(
            [SystemMessage(content=system), HumanMessage(content=human)]
        )
    content = resp.content if resp and isinstance(resp.content, str) else ""
    tables = _parse_tables_from_llm_text(content)
    return tables


class DiagnosisAgent:
    """SQL诊断Agent"""
    
    def __init__(self, config: Dict[str, Any], sandbox_tool: SandboxTool = None):
        """
        初始化DiagnosisAgent实例
        
        Args:
            config: Agent配置，包含LLM配置和其他参数
            sandbox_tool: 沙箱工具实例
        """
        self.config = config
        self.sandbox_tool = sandbox_tool
        
        # 获取配置参数
        self.llm_config = config.get("llm", {})
        self.max_iter = config.get("max_iter", 10)
        self.enable_test = config.get("enable_test", False)
        
        # 创建工具列表
        self.tools = self._create_tools()
        
        # 创建状态图
        self.graph = create_diagnosis_graph(
            llm_config=self.llm_config,
            tools=self.tools
            )
        
        default_logger.info(f"DiagnosisAgent初始化完成 - 最大迭代次数: {self.max_iter}, 沙箱测试: {self.enable_test}")
    
    def _create_tools(self) -> list:
        """创建Agent可用的工具列表"""
        tools = []
        
        # 沙箱执行工具
        if self.enable_test and self.sandbox_tool:
            execute_sql_func = create_sandbox_tool_function(self.sandbox_tool)
            tools.append(tool(execute_sql_func))
            if getattr(self.sandbox_tool, "_manager", None) and getattr(
                self.sandbox_tool, "_sandbox_tables", None
            ):
                reset_fn = create_reset_sandbox_tool_function(self.sandbox_tool)
                tools.append(tool(reset_fn))
                default_logger.info("已启用沙箱 reset 工具")
            default_logger.info("已启用沙箱测试工具")
        
        return tools
    
    async def diagnose_stream(
        self,
        ori_sql: str,
        schema: str,
        exec_log: str = None,
        sampled_tables: List[str] = None,
        preprocess_sql: str = None,
        clean_up_sql: str = None,
        user_prompt: str = None,
        sandbox_fidelity: str = None,
        stream_handler: Optional[StreamHandler] = None
    ) -> str:
        """
        流式执行SQL诊断（异步）
        
        使用 astream_events(version="v2") 捕获完整的推理过程
        通过 stream_handler 实时显示（Rich美化）
        同时通过 logger 记录到文件（如果指定了 --log-file）
        
        Args:
            ori_sql: 原始问题SQL语句
            schema: 相关表的schema信息（DDL语句），须含 `-- 表: name` 行
            exec_log: 执行日志（包含平均执行时间、执行次数等）
            sampled_tables: 采样表列表（可选）
            preprocess_sql: 测试本条sql的前置sql
            clean_up_sql: 测试本条sql的清理sql，用于恢复测试环境
            user_prompt: 用户提示
            stream_handler: 流式事件处理器

        Returns:
            诊断报告（自然语言）
        """
        tables_in_schema = _tables_from_schema_ddl(schema)
        default_logger.info(f"SQL: {ori_sql[:100]}...")
        default_logger.info(f"涉及表(来自 schema): {tables_in_schema}")
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
            system_prompt = create_system_prompt(enable_test=self.enable_test)
            
            # 创建初始消息
            initial_message = create_initial_message(
                sql=ori_sql,
                schema=schema,
                exec_log=exec_log,
                sampled_tables=sampled_tables,
                preprocess_sql=preprocess_sql,
                clean_up_sql=clean_up_sql,
                user_prompt=user_prompt,
                sandbox_fidelity=sandbox_fidelity,
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

            with timed_stage("diagnosis_total"):
                async for event in self.graph.astream_events(initial_state, version="v2"):
                    if stream_handler:
                        stream_handler.handle_event(event)

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
        exec_log: str = None,
        sampled_tables: List[str] = None,
        preprocess_sql: str = None,
        clean_up_sql: str = None,
        user_prompt: str = None,
        sandbox_fidelity: str = None,
    ) -> str:
        """
        执行SQL诊断（同步包装器，保持向后兼容）
        
        Args:
            ori_sql: 原始问题SQL语句
            schema: 相关表的schema信息（DDL语句），须含 `-- 表: name` 行
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
            exec_log=exec_log,
            sampled_tables=sampled_tables,
            preprocess_sql=preprocess_sql,
            clean_up_sql=clean_up_sql,
            user_prompt=user_prompt,
            sandbox_fidelity=sandbox_fidelity,
            stream_handler=stream_handler
        ))

        return conclusion