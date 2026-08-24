from typing import TypedDict, Annotated, Sequence
import operator
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_core.runnables import RunnableConfig
from utils.llm import make_chat_openai
from utils.logger import default_logger


class AgentState(TypedDict):
    """Agent状态定义"""
    messages: Annotated[Sequence[BaseMessage], operator.add]
    iteration: int
    max_iter: int
    conclusion: str


def create_diagnosis_graph(llm_config: dict, tools: list):
    """
    创建诊断Agent的LangGraph状态图。
    涉及表名的解析、拉取 DDL、setup_sandbox 由调用方（如 scripts/diag）在入图之前完成。
    
    Args:
        llm_config: LLM配置
        tools: 工具列表
    
    Returns:
        编译后的状态图
    """
    # 初始化LLM
    llm = make_chat_openai(llm_config, default_temperature=0.2)
    
    # 绑定工具到LLM
    llm_with_tools = llm.bind_tools(tools)
    
    # 定义节点函数
    async def reasoning_node(state: AgentState, config: RunnableConfig) -> AgentState:
        """推理节点：调用LLM进行推理
        
        Args:
            state: Agent状态
            config: 运行时配置（包含回调），由 LangGraph 自动传递
        """
        
        messages = state["messages"]
        iteration = state["iteration"]
        
        full_response = await llm_with_tools.ainvoke(messages, config=config)

        # 检查是否输出了结论（兼容 content 为 str 或 multimodal list）
        conclusion = ""
        if full_response:
            raw = full_response.content
            if isinstance(raw, list):
                parts = []
                for p in raw:
                    if isinstance(p, str):
                        parts.append(p)
                    elif isinstance(p, dict) and p.get("type") == "text":
                        parts.append(str(p.get("text") or ""))
                content = "\n".join(parts)
            else:
                content = raw if isinstance(raw, str) else str(raw or "")
            idx = content.rfind("【诊断结论】")
            if idx != -1:
                conclusion = content[idx:]

        return {
            "messages": [full_response],
            "iteration": iteration + 1,
            "conclusion": conclusion,
        }

    def should_continue(state: AgentState) -> str:
        """判断是否继续迭代。"""
        messages = state["messages"]
        last_message = messages[-1]
        iteration = state["iteration"]
        max_iter = state["max_iter"]
        conclusion = state["conclusion"]

        if conclusion:
            return "end"

        if iteration >= max_iter:
            return "force_end"

        tool_calls = getattr(last_message, "tool_calls", None) or []
        if tool_calls:
            return "tool"

        # 空回复 / 无工具无结论：不可静默结束，否则丢诊断；改走强制结案
        default_logger.warning(
            "LLM 本轮未调用工具且未给出【诊断结论】"
            f"（content 空或非结论），iteration={iteration}，转入强制结案"
        )
        return "force_end"   
         
    async def force_conclusion_node(state: AgentState, config: RunnableConfig) -> AgentState:
        """强制输出结论节点：当达到最大迭代次数时调用
        
        Args:
            state: Agent状态
            config: 运行时配置（包含回调），由 LangGraph 自动传递
        """
        
        messages = state["messages"]
        
        # 添加强制总结的消息
        force_message = HumanMessage(
            content="你已经达到最大迭代次数。请基于目前收集的信息，立即输出诊断结论。使用【诊断结论】格式输出。"
        )
        
        # 调用LLM生成结论
        response = await llm.ainvoke(messages + [force_message], config=config)

        raw = response.content if response else ""
        if isinstance(raw, list):
            parts = []
            for p in raw:
                if isinstance(p, str):
                    parts.append(p)
                elif isinstance(p, dict) and p.get("type") == "text":
                    parts.append(str(p.get("text") or ""))
            conclusion = "\n".join(parts)
        else:
            conclusion = raw if isinstance(raw, str) else str(raw or "")
        idx = conclusion.rfind("【诊断结论】")
        if idx != -1:
            conclusion = conclusion[idx:]

        return {
            "messages": [force_message, response],
            "conclusion": conclusion,
        }
    
    # 创建工具节点
    tool_node = ToolNode(tools)
    
    # 构建状态图
    workflow = StateGraph(AgentState)
    
    # 添加节点
    workflow.add_node("reasoning", reasoning_node)
    workflow.add_node("tools", tool_node)
    workflow.add_node("force_conclusion", force_conclusion_node)
    
    # 设置入口点
    workflow.set_entry_point("reasoning")
    
    # 添加条件边：推理节点 -> 工具节点/结束/强制结论节点
    workflow.add_conditional_edges(
        "reasoning",
        should_continue,
        {
            "tool": "tools",
            "end": END,
            "force_end": "force_conclusion",
        }
    )
    
    # 添加边：工具节点 -> 推理节点
    workflow.add_edge("tools", "reasoning")
    
    # 添加边：强制结论节点 -> 结束
    workflow.add_edge("force_conclusion", END)
    
    app = workflow.compile()
    # default_logger.info("LangGraph状态图创建完成")
    
    return app

