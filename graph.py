"""Kur — LangGraph Agent Orchestrator"""
from langgraph.graph import StateGraph, END
from typing import TypedDict, Optional
import os
import time
import pandas as pd

from nodes.intent import classify_intent_node
from nodes.schema_retriever import retrieve_schema_node
from nodes.rag_retriever import retrieve_examples_node
from nodes.sql_generator import generate_sql_node
from nodes.validator import validate_sql_node
from nodes.executor import execute_sql_node
from nodes.refiner import refine_sql_node
from nodes.formatter import format_response_node


class KurState(TypedDict):
    question: str
    intent: str
    schema_context: str
    rag_examples: str
    generated_sql: str
    validation_result: str
    query_result: Optional[pd.DataFrame]
    final_answer: str
    error_message: str
    retry_count: int
    debug_steps: list
    node_timings_ms: dict


def _with_timing(node_name: str, fn):
    def _wrapped(state: KurState):
        start = time.time()
        result = fn(state)
        elapsed_ms = int((time.time() - start) * 1000)

        if "node_timings_ms" not in result or result.get("node_timings_ms") is None:
            result["node_timings_ms"] = {}
        result["node_timings_ms"][node_name] = elapsed_ms

        if "debug_steps" not in result or result.get("debug_steps") is None:
            result["debug_steps"] = []
        result["debug_steps"].append(f"{node_name}: {elapsed_ms}ms")
        return result

    return _wrapped


def route_validation(state: KurState) -> str:
    return "pass" if state["validation_result"] == "PASS" else "fail"


def route_execution(state: KurState) -> str:
    return "success" if not state.get("error_message") else "error"


def route_retry(state: KurState) -> str:
    max_retries = int(os.getenv("MAX_RETRIES", "3"))
    return "retry" if state["retry_count"] < max_retries else "give_up"


def route_after_intent(state: KurState) -> str:
    return "sql" if state.get("intent") == "sql_query" else "respond"


def build_kur_graph():
    graph = StateGraph(KurState)

    # ──── Nodes ────
    graph.add_node("classify_intent", _with_timing("classify_intent", classify_intent_node))
    graph.add_node("retrieve_schema", _with_timing("retrieve_schema", retrieve_schema_node))
    graph.add_node("retrieve_examples", _with_timing("retrieve_examples", retrieve_examples_node))
    graph.add_node("generate_sql", _with_timing("generate_sql", generate_sql_node))
    graph.add_node("validate_sql", _with_timing("validate_sql", validate_sql_node))
    graph.add_node("execute_sql", _with_timing("execute_sql", execute_sql_node))
    graph.add_node("refine_sql", _with_timing("refine_sql", refine_sql_node))
    graph.add_node("format_response", _with_timing("format_response", format_response_node))

    # ──── Edges ────
    graph.set_entry_point("classify_intent")
    graph.add_conditional_edges("classify_intent", route_after_intent, {
        "sql": "retrieve_schema",
        "respond": "format_response",
    })
    graph.add_edge("retrieve_schema", "retrieve_examples")
    graph.add_edge("retrieve_examples", "generate_sql")
    graph.add_edge("generate_sql", "validate_sql")

    graph.add_conditional_edges("validate_sql", route_validation, {
        "pass": "execute_sql",
        "fail": "refine_sql",
    })

    graph.add_conditional_edges("execute_sql", route_execution, {
        "success": "format_response",
        "error": "refine_sql",
    })

    graph.add_conditional_edges("refine_sql", route_retry, {
        "retry": "generate_sql",
        "give_up": "format_response",
    })

    graph.add_edge("format_response", END)

    return graph.compile()
