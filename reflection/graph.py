import operator
from typing import Annotated, Sequence, TypedDict

from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph

from json_parser import parse_json


class AgentState(TypedDict):
    """知识图谱更新、反思Agent状态定义"""
    messages: Annotated[Sequence[BaseMessage], operator.add]
    iteration: int
    max_iter: int


def create_reflection_graph(llm_config: dict):
    """
    创建知识图谱更新、反思Agent的LangGraph状态图
    
    Args:
        llm_config: LLM配置
    """
    # 初始化LLM
    llm = ChatOpenAI(
        base_url=llm_config.get("base_url"),
        model=llm_config.get("model"),
        api_key=llm_config.get("api_key"),
        temperature=llm_config.get("temperature", 0.7)
    )

    async def reasoning_node(state: AgentState, config: RunnableConfig) -> AgentState:
        """推理节点：调用LLM进行推理
        
        Args:
            state: Agent状态
            config: 运行时配置（包含回调），由 LangGraph 自动传递
        """
        messages = state["messages"]
        
        full_response = await llm.ainvoke(messages, config=config)
        
        return {
            "messages": [full_response],
            "iteration": state["iteration"] + 1
        }

    def check_response(state: AgentState) -> str:
        """检查响应是否符合要求"""
        messages = state["messages"]
        last_message = messages[-1]
        iteration = state["iteration"]
        max_iter = state["max_iter"]
        
        if iteration >= max_iter:
            state["json_content"] = None
            return "end"
        
        json_result = parse_json(last_message.content)
        json_content = ""

        if json_result["success"]:
            json_content = json_result["data"]
            state["json_content"] = json_content
            return "end"

        error_message = json_result["error"]
        state["error_message"] = error_message
        return "fix_json"
        
    async def fix_json_node(state: AgentState, config: RunnableConfig) -> AgentState:
        """修复JSON节点"""
        
        fix_prompt = f"尝试解析你返回的json结果时出错，请尝试修复并重新返回。{state["error_message"]}"
        
        error_message = HumanMessage(content=fix_prompt)

        full_response = await llm.ainvoke([error_message], config=config)
        return {
            "messages": [full_response],
            "iteration": state["iteration"] + 1
        }

    workflow = StateGraph(AgentState)

    workflow.add_node("reasoning", reasoning_node)
    workflow.add_node("fix_json", fix_json_node)
    workflow.add_node("check_response", check_response)
    
    workflow.set_entry_point("reasoning")

    workflow.add_conditional_edges(
        "reasoning",
        check_response,
        {
            "end": END,
            "fix_json": "fix_json"
        }
    )
    workflow.add_conditional_edges(
        "fix_json",
        check_response,
        {
            "end": END,
            "fix_json": "fix_json"
        }
    )

    app = workflow.compile()
    return app