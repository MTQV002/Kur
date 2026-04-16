"""Intent Classification Node — production-style structured routing"""
import json
import re
from utils.llm_factory import get_llm


ALLOWED_INTENTS = {
    "sql_query",
    "sql_explain",
    "sql_rewrite",
    "system_info",
    "assistant_explain",
    "clarification",
    "greeting",
    "meta_chat",
    "out_of_scope",
}


def _extract_json_payload(raw_text: str) -> dict:
    text = (raw_text or "").strip()
    if not text:
        return {}

    try:
        return json.loads(text)
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except Exception:
            return {}
    return {}


def _intent_fallback(question: str, has_recent_sql: bool) -> str:
    q = (question or "").strip().lower()
    if not q:
        return "meta_chat"

    if re.search(r"\b(db|database|schema|catalog|engine|đang dùng db|đang ở schema)\b", q):
        return "system_info"

    if re.search(r"nghĩa là sao|nghia la sao|ý là gì|y la gi|what do you mean|explain your message", q):
        return "assistant_explain"

    return "sql_query"


def _followup_sql_gate(question: str, conversation_context: str) -> bool:
    """Use LLM to detect whether the message is a follow-up about prior SQL."""
    try:
        llm = get_llm(model_type="small", temperature=0)
        prompt = f"""Return ONLY JSON: {{"followup_sql": true|false, "confidence": 0.0}}

Conversation context:
{conversation_context}

Current message:
{question}

Definition:
- followup_sql=true when user is asking to explain/rewrite/optimize previously generated SQL,
  instead of asking a brand-new analytics question.
"""
        response = llm.invoke(prompt)
        payload = _extract_json_payload(response.content if hasattr(response, "content") else str(response))
        return bool(payload.get("followup_sql", False)) and float(payload.get("confidence", 0.0) or 0.0) >= 0.45
    except Exception:
        return False


def _semantic_sql_followup_guard(question: str, has_recent_sql: bool) -> str | None:
    """Detect follow-up intent only when user clearly refers to previous query context."""
    q = (question or "").strip().lower()
    if not has_recent_sql:
        return None

    asks_meaning = bool(re.search(r"ngh[ĩi]a.*sao|ý.*l[àa].*g[ìi]|what do you mean", q))
    if asks_meaning:
        return "assistant_explain"

    deictic_ref = bool(re.search(r"câu (này|đó)|query (này|đó)|sql (này|đó)|trên|vừa rồi|ở trên", q))
    mentions_sql = bool(re.search(r"\b(sql|query|truy vấn|câu query|câu sql)\b", q))
    asks_explain = bool(re.search(r"giải thích|phân tích|explain", q))
    asks_rewrite = bool(re.search(r"tối ưu hơn|viết lại|rewrite|cải thiện|cải tiến|phiên bản khác|query khác", q))

    refers_prev = deictic_ref or mentions_sql

    if refers_prev and asks_explain:
        return "sql_explain"

    if refers_prev and asks_rewrite:
        return "sql_rewrite"
    return None


def _semantic_runtime_guard(question: str) -> str | None:
    q = (question or "").strip().lower()
    if re.search(r"\b(db|database|schema|catalog|engine|model|provider)\b", q):
        return "system_info"
    if re.search(r"ngh[ĩi]a.*sao|ý.*l[àa].*g[ìi]|what do you mean|explain your message", q):
        return "assistant_explain"
    return None


def classify_intent_node(state: dict) -> dict:
    """Classify intent with LLM structured output + context-aware fallback."""
    question = state["question"]
    conversation_context = state.get("conversation_context", "")
    has_recent_sql = "SQL:" in conversation_context

    try:
        llm = get_llm(model_type="small", temperature=0)
        prompt = f"""You are an intent router for an analytics assistant.
Return ONLY valid JSON with this schema:
{{
    "intent": "sql_query|sql_explain|sql_rewrite|system_info|assistant_explain|clarification|greeting|meta_chat|out_of_scope",
  "confidence": 0.0,
  "needs_clarification": false,
  "clarification_question": "",
  "reason": ""
}}

Conversation context (recent turns):
{conversation_context if conversation_context else "<empty>"}

Current user message:
{question}

Routing rules:
- Use sql_explain when user asks to explain/evaluate previous SQL.
- Use sql_rewrite when user asks a better/alternative SQL for previous SQL.
- Use system_info when asking which DB/engine/schema/catalog/model is currently active.
- Use assistant_explain when user asks what the assistant's previous message means.
- Use clarification when asking schema/column/table definitions.
- If message is ambiguous and depends on missing prior SQL, set needs_clarification=true with a concise clarification_question.
- Prefer asking clarification instead of guessing.
- Avoid keyword-only routing; use context + intent semantics.
"""
        response = llm.invoke(prompt)
        payload = _extract_json_payload(response.content if hasattr(response, "content") else str(response))
    except Exception:
        payload = {}

    intent = str(payload.get("intent", "")).strip().lower()
    confidence = float(payload.get("confidence", 0.0) or 0.0)
    needs_clarification = bool(payload.get("needs_clarification", False))
    clarification_question = str(payload.get("clarification_question", "")).strip()

    if intent not in ALLOWED_INTENTS:
        intent = _intent_fallback(question, has_recent_sql)

    if confidence < 0.35 and intent in {"sql_explain", "sql_rewrite"} and not has_recent_sql:
        needs_clarification = True
        clarification_question = clarification_question or "Bạn muốn mình giải thích/tối ưu SQL nào? Hãy gửi lại query gần nhất hoặc yêu cầu mình tạo query mới."

    guard_intent = _semantic_sql_followup_guard(question, has_recent_sql)
    if guard_intent:
        intent = guard_intent
        if guard_intent == "meta_chat":
            needs_clarification = True
            clarification_question = clarification_question or "Mình cần câu SQL trước đó để tối ưu lại. Bạn gửi lại query vừa chạy hoặc bảo mình tạo query mới từ đầu."

    if (not has_recent_sql) and re.search(r"giải thích|phân tích|explain|tối ưu|rewrite|viết lại|query khác", (question or "").lower()):
        needs_clarification = True
        clarification_question = clarification_question or "Mình chưa có SQL gần nhất để giải thích/tối ưu. Bạn muốn mình tạo query mới từ câu hỏi business nào?"

    if has_recent_sql and intent == "sql_query" and _followup_sql_gate(question, conversation_context):
        if _semantic_sql_followup_guard(question, has_recent_sql) in {"sql_explain", "sql_rewrite"}:
            intent = _semantic_sql_followup_guard(question, has_recent_sql)

    runtime_intent = _semantic_runtime_guard(question)
    if runtime_intent:
        intent = runtime_intent

    if needs_clarification:
        state["intent"] = "meta_chat"
        state["final_answer"] = clarification_question or "Bạn có thể nói rõ hơn ý cần phân tích/tối ưu để mình xử lý đúng ngữ cảnh không?"
        return state

    state["intent"] = intent

    if intent == "greeting":
        state["final_answer"] = "👋 Chào bạn! Bạn cứ hỏi trực tiếp bài toán dữ liệu, mình sẽ tạo SQL và giải thích rõ theo ngữ cảnh."
    elif intent == "meta_chat":
        state["final_answer"] = "Mình có thể xử lý ngay theo ngữ cảnh hiện tại. Nếu bạn muốn tối ưu SQL vừa rồi, chỉ cần nói: 'viết lại query tối ưu hơn'."
    elif intent == "out_of_scope":
        state["final_answer"] = "Mình tập trung vào phân tích dữ liệu/SQL. Bạn thử hỏi theo dạng KPI, doanh thu, đơn hàng, khách hàng, funnel..."

    return state
