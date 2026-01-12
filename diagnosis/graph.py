"""
LangGraph状态图定义
"""
from typing import TypedDict, Annotated, Sequence
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
import operator
from utils.logger import default_logger


class AgentState(TypedDict):
    """Agent状态定义"""
    messages: Annotated[Sequence[BaseMessage], operator.add]
    iteration: int
    max_iter: int
    conclusion: str


def create_diagnosis_graph(llm_config: dict, tools: list):
    """
    创建诊断Agent的LangGraph状态图
    
    Args:
        llm_config: LLM配置
        tools: 工具列表
    
    Returns:
        编译后的状态图
    """
    # 初始化LLM
    llm = ChatOpenAI(
        base_url=llm_config.get("base_url"),
        model=llm_config.get("model"),
        api_key=llm_config.get("api_key"),
        temperature=llm_config.get("temperature", 0.7)
    )
    
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
        
        # 检查是否输出了结论
        conclusion = ""
        if full_response and isinstance(full_response.content, str):
            content = full_response.content
            idx = content.rfind("【诊断结论】")
            if idx != -1:
                conclusion = content[idx:]
        
        return {
            "messages": [full_response],
            "iteration": iteration + 1,
            "conclusion": conclusion
        }
    
    def should_continue(state: AgentState) -> str:
        """判断是否继续迭代"""
        messages = state["messages"]
        last_message = messages[-1]
        iteration = state["iteration"]
        max_iter = state["max_iter"]
        conclusion = state["conclusion"]
        
        if conclusion:
            return "end"
        
        if iteration >= max_iter:
            return "force_end"
        
        if hasattr(last_message, "tool_calls") and last_message.tool_calls:
            return "tool"
        
        raise RuntimeError(f"LLM 未能成功调用工具，可能是型号问题。请检查日志: {last_message}")
    
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
        
        conclusion = response.content if response and isinstance(response.content, str) else ""
        idx = conclusion.rfind("【诊断结论】")
        if idx != -1:
            conclusion = conclusion[idx:]
        
        return {
            "messages": [force_message, response],
            "conclusion": conclusion
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
            "continue": "reasoning"
        }
    )
    
    # 添加边：工具节点 -> 推理节点
    workflow.add_edge("tools", "reasoning")
    
    # 添加边：强制结论节点 -> 结束
    workflow.add_edge("force_conclusion", END)
    
    app = workflow.compile()
    # default_logger.info("LangGraph状态图创建完成")
    
    return app

