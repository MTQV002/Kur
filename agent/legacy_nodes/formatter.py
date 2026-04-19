"""Formatter Node — Format kết quả thành câu trả lời"""
import os
from utils.llm_invoke import invoke_with_timeout


def format_response_node(state: dict) -> dict:
    """Format query results into a human-readable Vietnamese/English answer."""
    question = state["question"]
    sql = state.get("generated_sql", "")
    result = state.get("query_result")
    error = state.get("error_message", "")
    max_retries = int(os.getenv("MAX_RETRIES", "3"))

    if state.get("final_answer"):
        return state

    if error and state.get("retry_count", 0) >= max_retries:
        state["final_answer"] = (
            f"❌ Không thể tạo SQL chính xác sau {max_retries} lần thử. "
            f"Lỗi cuối: {error}. "
            f"Thử diễn đạt lại câu hỏi hoặc hỏi cụ thể hơn."
        )
        return state

    if result is None or (hasattr(result, 'empty') and result.empty):
        state["final_answer"] = "📭 Truy vấn thành công nhưng không có dữ liệu phù hợp."
        return state

    # Summarize with LLM
    try:
        result_str = result.head(20).to_string() if hasattr(result, 'to_string') else str(result)

        from utils.llm_factory import get_llm
        llm = get_llm(model_type="small", temperature=0)

        llm_timeout = float(os.getenv("LLM_TIMEOUT", "20"))
        prompt = f"""Summarize these SQL results in a concise, friendly way. 
Answer in the same language as the question (Vietnamese or English).
Include key numbers and insights.
    Return plain text only. No markdown, no bullet symbols, no ** bold markers.

Question: {question}
Results:
{result_str}

Summary:"""

        summary = invoke_with_timeout(lambda: llm.invoke(prompt), llm_timeout)
        clean = summary.content.strip().replace("**", "")
        state["final_answer"] = clean
    except Exception:
        state["final_answer"] = f"✅ Truy vấn thành công — {len(result)} dòng kết quả."

    return state
