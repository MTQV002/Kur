"""RAG Retriever Node — Tìm golden queries tương tự từ Vanna + ChromaDB"""
import os
from utils.llm_invoke import invoke_with_timeout


def retrieve_examples_node(state: dict) -> dict:
    """Retrieve similar past queries from Vanna RAG."""
    question = state["question"]

    # Fast guard: disable RAG unless explicitly enabled and OpenAI key exists
    if os.getenv("ENABLE_RAG", "false").lower() != "true":
        state["rag_examples"] = ""
        return state
    if not (os.getenv("OPENAI_API_KEY") or os.getenv("API_KEY")):
        state["rag_examples"] = ""
        return state

    try:
        from tools.rag import get_vanna_instance
        vn = get_vanna_instance()
        rag_timeout = float(os.getenv("RAG_TIMEOUT", "2"))
        similar = invoke_with_timeout(lambda: vn.get_similar_question_sql(question), rag_timeout)

        if similar:
            examples = []
            for item in similar[:3]:  # Top 3
                q = item.get("question", "")
                s = item.get("sql", "")
                examples.append(f"Q: {q}\nSQL: {s}")
            state["rag_examples"] = "\n\n".join(examples)
        else:
            state["rag_examples"] = ""
    except Exception:
        state["rag_examples"] = ""

    return state
