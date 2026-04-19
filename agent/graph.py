"""Kur — Multi-Agent LangGraph Orchestrator"""
from typing import Literal
from typing_extensions import TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import create_react_agent
from agent.utils.llm_factory import get_llm
from agent.tools.catalog_tools import get_database_schema, check_sql_syntax
from langchain_core.messages import SystemMessage

class KurState(TypedDict):
    messages: list
    intent: str

def build_kur_agent():
    graph = StateGraph(KurState)
    
    def router_node(state: KurState):
        # Extremely fast and cheap router model
        router_llm = get_llm(model_type="router", temperature=0)
        sys_prompt = "You are a data assistant Router. Analyze the user's latest query. If it asks for any data retrieval, metric calculation, or SQL generation, reply STRICTLY with 'SQL'. If it is just casual greeting or general non-data question, reply STRICTLY with 'CHAT'."
        
        messages = [SystemMessage(content=sys_prompt)] + state["messages"]
        response = router_llm.invoke(messages)
        content = response.content.upper()
        
        intent = "SQL" if "SQL" in content else "CHAT"
        
        if intent == "CHAT":
            # Answer immediately using cheap model
            chat_sys = "You are Kur AI, a highly technical data engineering assistant. Answer the user briefly and politely."
            chat_res = router_llm.invoke([SystemMessage(content=chat_sys)] + state["messages"])
            return {"intent": "meta_chat", "messages": [chat_res]}
            
        return {"intent": "sql_query"}

    def route_after_intent(state: KurState) -> Literal["generator", "__end__"]:
        if state.get("intent") == "meta_chat":
            return "__end__"
        return "generator"

    # Generator is the heavy lifter
    generator_llm = get_llm(model_type="generator", temperature=0)
    tools = [get_database_schema, check_sql_syntax]
    
    heavy_agent = create_react_agent(
        model=generator_llm,
        tools=tools,
        prompt="""You are Kur AI, an Expert Data Analyst. Your job is to help users analyze data.
1. Always call `get_database_schema` to explore tables before writing.
2. Use DuckDB/PostgreSQL SQL dialect. 
3. Call `check_sql_syntax` to verify your query doesn't have errors before answering. Correct any errors if it fails.
4. Output the finalized SQL within a markdown block (```sql ... ```). State any brief data observations in Vietnamese."""
    )

    def generator_node(state: KurState):
        res = heavy_agent.invoke({"messages": state["messages"]})
        # create_react_agent returns the full updated messages list under 'messages'
        return {"messages": res["messages"]}

    graph.add_node("router", router_node)
    graph.add_node("generator", generator_node)
    
    graph.add_edge(START, "router")
    graph.add_conditional_edges("router", route_after_intent)
    graph.add_edge("generator", END)
    
    return graph.compile()
