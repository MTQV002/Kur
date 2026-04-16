"""Kur API — FastAPI backend for Agentic Text-to-SQL"""
import os
import json
import time
import uuid
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

from graph import build_kur_graph
from nodes.intent import classify_intent_node
from nodes.schema_retriever import retrieve_schema_node
from nodes.rag_retriever import retrieve_examples_node
from nodes.sql_generator import generate_sql_node
from nodes.validator import validate_sql_node
from nodes.refiner import refine_sql_node
from nodes.executor import execute_sql_node
from nodes.formatter import format_response_node
from utils.llm_invoke import invoke_with_timeout

app = FastAPI(title="Kur API", version="2.0", description="Agentic Text-to-SQL Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Build graph once at startup
kur_graph = build_kur_graph()
PENDING_QUERIES = {}
PENDING_TTL_SECONDS = 1800
HISTORY_DB_PATH = Path(os.getenv("HISTORY_DB_PATH", "data/history.db"))

# ──── Settings Persistence ────
SETTINGS_PATH = Path(os.getenv("SETTINGS_PATH", "data/settings.json"))

DEFAULT_SETTINGS = {
    "llm_provider": os.getenv("LLM_PROVIDER", "openai"),
    "llm_model": "gpt-4o",
    "api_key": os.getenv("OPENAI_API_KEY", ""),
    "ollama_url": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
    "ollama_model": os.getenv("OLLAMA_MODEL", "snowflake-arctic-text2sql-r1:7b"),

    "db_engine": "duckdb",
    "duckdb_path": os.getenv("DUCKDB_PATH", "data/kur.db"),
    "db_host": os.getenv("DB_HOST", "localhost"),
    "db_port": int(os.getenv("DB_PORT", "5432")),
    "db_name": os.getenv("DB_NAME", "business_db"),
    "db_user": os.getenv("DB_USER", "analyst"),
    "db_password": os.getenv("DB_PASSWORD", ""),

    "uc_url": os.getenv("UC_SERVER_URL", "http://uc-server:8080"),
    "uc_catalog": os.getenv("UC_CATALOG", "kur_catalog"),
    "uc_schema": os.getenv("UC_SCHEMA", "public"),

    "max_retries": 3,
    "query_timeout": 30,
    "max_rows": 1000,
    "language": "auto",
}


def _init_history_db():
    HISTORY_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(HISTORY_DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question TEXT NOT NULL,
                answer TEXT,
                intent TEXT,
                sql_text TEXT,
                created_at REAL NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _save_history(question: str, answer: str, intent: str, sql_text: Optional[str] = None):
    now = time.time()
    conn = sqlite3.connect(HISTORY_DB_PATH)
    try:
        cur = conn.execute(
            "SELECT question, created_at FROM chat_history ORDER BY id DESC LIMIT 1"
        )
        latest = cur.fetchone()
        if latest:
            last_question, last_ts = latest
            if (last_question or "").strip().lower() == (question or "").strip().lower() and (now - float(last_ts)) < 2:
                return

        conn.execute(
            """
            INSERT INTO chat_history (question, answer, intent, sql_text, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (question, answer or "", intent or "", sql_text or "", now),
        )
        conn.commit()
    finally:
        conn.close()


def _load_history(limit: int = 30):
    conn = sqlite3.connect(HISTORY_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT id, question, answer, intent, sql_text, created_at
            FROM chat_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, min(int(limit), 200)),),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    items = []
    for row in rows:
        items.append({
            "id": row["id"],
            "question": row["question"],
            "answer": row["answer"],
            "intent": row["intent"],
            "sql": row["sql_text"],
            "timestamp": datetime.fromtimestamp(row["created_at"]).isoformat(),
        })
    return items


def _clear_history():
    conn = sqlite3.connect(HISTORY_DB_PATH)
    try:
        conn.execute("DELETE FROM chat_history")
        conn.commit()
    finally:
        conn.close()


def load_settings() -> dict:
    try:
        if SETTINGS_PATH.exists():
            with open(SETTINGS_PATH, "r") as f:
                saved = json.load(f)
            merged = {**DEFAULT_SETTINGS, **saved}
            return merged
    except Exception:
        pass
    return dict(DEFAULT_SETTINGS)


def save_settings(settings: dict):
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SETTINGS_PATH, "w") as f:
        json.dump(settings, f, indent=2)


def apply_settings(settings: dict):
    """Push settings to env vars so agent nodes pick them up."""
    env_map = {
        "llm_provider": "LLM_PROVIDER",
        "llm_model": "LLM_MODEL",
        "api_key": "API_KEY",
        "ollama_url": "OLLAMA_BASE_URL",
        "ollama_model": "OLLAMA_MODEL",
        "db_engine": "DB_ENGINE",
        "duckdb_path": "DUCKDB_PATH",
        "db_host": "DB_HOST",
        "db_port": "DB_PORT",
        "db_name": "DB_NAME",
        "db_user": "DB_USER",
        "db_password": "DB_PASSWORD",
        "uc_url": "UC_SERVER_URL",
        "uc_catalog": "UC_CATALOG",
        "uc_schema": "UC_SCHEMA",
        "max_retries": "MAX_RETRIES",
        "query_timeout": "QUERY_TIMEOUT",
        "max_rows": "MAX_ROWS",
    }
    for key, env_var in env_map.items():
        if key in settings and settings[key]:
            os.environ[env_var] = str(settings[key])


def mask_key(key: str) -> str:
    if not key or len(key) < 8:
        return "••••••••"
    return key[:4] + "•" * (len(key) - 8) + key[-4:]


def _clean_pending_queries():
    now = time.time()
    expired = [
        request_id for request_id, payload in PENDING_QUERIES.items()
        if now - payload.get("created_at", now) > PENDING_TTL_SECONDS
    ]
    for request_id in expired:
        PENDING_QUERIES.pop(request_id, None)


def _run_timed_step(state: dict, step_name: str, func):
    start = time.time()
    state = func(state)
    elapsed_ms = int((time.time() - start) * 1000)
    state.setdefault("debug_steps", []).append(f"{step_name}: {elapsed_ms}ms")
    return state


def _build_clarification_answer(question: str, schema_context: str) -> str:
    q = (question or "").lower()
    physical_context = schema_context
    if "[PHYSICAL TABLES - SOURCE OF TRUTH]" in schema_context:
        after = schema_context.split("[PHYSICAL TABLES - SOURCE OF TRUTH]", 1)[1]
        if "[UC SEMANTIC METADATA - FOR BUSINESS CONTEXT ONLY]" in after:
            physical_context = after.split("[UC SEMANTIC METADATA - FOR BUSINESS CONTEXT ONLY]", 1)[0]
        else:
            physical_context = after

    table_names = re.findall(r"TABLE:\s*([^\n]+)", physical_context)
    unique_tables = []
    seen = set()
    for name in table_names:
        normalized = name.strip()
        if normalized and normalized not in seen:
            unique_tables.append(normalized)
            seen.add(normalized)

    asks_table_count = bool(re.search(r"mấy bảng|bao nhiêu bảng|how many tables|số bảng", q))
    asks_column_types = bool(re.search(r"dạng dữ liệu|kiểu dữ liệu|data type|từng cột|mỗi cột|columns?", q))

    if not unique_tables:
        return "Hiện tại mình chưa đọc được schema từ Unity Catalog hoặc DB vật lý. Bạn kiểm tra lại UC URL/catalog/schema trong Settings."

    lines = []
    if asks_table_count or "table" in q or "bảng" in q or "database" in q:
        lines.append(f"Hiện tại có {len(unique_tables)} bảng khả dụng: {', '.join(unique_tables)}.")

    if asks_column_types:
        table_blocks = re.split(r"\n\n+", physical_context)
        rendered = []
        for block in table_blocks:
            match = re.search(r"TABLE:\s*([^\n]+)", block)
            if not match:
                continue
            table_name = match.group(1).strip()
            col_lines = []
            for raw in block.splitlines():
                striped = raw.strip()
                if not striped or striped.startswith("TABLE:") or striped.startswith("DESCRIPTION:") or striped.startswith("COLUMNS:"):
                    continue
                col_lines.append(striped)
            if col_lines:
                rendered.append(f"- {table_name}: " + ", ".join(col_lines[:40]))
        if rendered:
            lines.append("Kiểu dữ liệu theo cột:\n" + "\n".join(rendered[:20]))

    if not lines:
        lines.append(f"Schema hiện có {len(unique_tables)} bảng: {', '.join(unique_tables)}.")
    return "\n\n".join(lines)


def _build_conversation_context(limit: int = 4) -> str:
    history_items = _load_history(limit=limit)
    if not history_items:
        return ""

    lines = []
    for item in reversed(history_items):
        question = (item.get("question") or "").strip()
        answer = (item.get("answer") or "").strip()
        sql_text = (item.get("sql") or "").strip()
        if question:
            lines.append(f"User: {question}")
        if sql_text:
            lines.append(f"SQL: {sql_text[:400]}")
        if answer:
            lines.append(f"Assistant: {answer[:260]}")
    return "\n".join(lines)


def _find_latest_sql(limit: int = 20) -> str:
    for item in _load_history(limit=limit):
        sql_text = (item.get("sql") or "").strip()
        if sql_text:
            return sql_text
    return ""


def _find_latest_answer(limit: int = 20) -> str:
    for item in _load_history(limit=limit):
        answer = (item.get("answer") or "").strip()
        if answer:
            return answer
    return ""


def _build_runtime_info_answer() -> str:
    s = load_settings()
    engine = (s.get("db_engine") or "duckdb").lower()
    provider = s.get("llm_provider", "openai")
    model = s.get("llm_model", "")
    uc_catalog = s.get("uc_catalog", "kur_catalog")
    uc_schema = s.get("uc_schema", "public")

    if engine == "duckdb":
        db_info = f"DuckDB ({s.get('duckdb_path', 'data/kur.db')})"
    elif engine == "trino":
        db_info = f"Trino {s.get('db_host', 'trino')}:{s.get('db_port', 8080)} / catalog={s.get('db_name', 'memory')}"
    else:
        db_info = f"{engine.upper()} {s.get('db_host', 'localhost')}:{s.get('db_port', '')} / db={s.get('db_name', '')}"

    return (
        f"Hiện tại đang dùng DB engine: {db_info}. "
        f"Schema active: {uc_catalog}.{uc_schema}. "
        f"LLM đang dùng: {provider}/{model}."
    )


def _build_assistant_explain_answer(question: str) -> str:
    last_answer = _find_latest_answer(limit=30)
    if not last_answer:
        return "Mình chưa có câu trả lời trước đó để diễn giải. Bạn có thể nhắc lại nội dung cần mình giải thích."

    try:
        from utils.llm_factory import get_llm

        llm = get_llm(model_type="small", temperature=0)
        timeout_s = float(os.getenv("LLM_TIMEOUT", "20"))
        prompt = f"""Diễn giải lại câu trả lời trước của assistant bằng tiếng Việt đơn giản, 2-3 câu, rõ ý cho người dùng business.

Ngữ cảnh user mới:
{question}

Câu trả lời trước của assistant:
{last_answer}

Yêu cầu:
- Không nói lan man
- Không tạo SQL mới
- Chỉ giải thích ý nghĩa của câu trả lời trước
"""
        res = invoke_with_timeout(lambda: llm.invoke(prompt), timeout_s)
        answer = (res.content if hasattr(res, "content") else str(res)).strip()
        if answer:
            return answer.replace("**", "")
    except Exception:
        pass

    return f"Ý mình là: {last_answer}"


def _deterministic_sql_explain(sql_text: str) -> str:
    sql_upper = sql_text.upper()
    tables = []
    for pattern in [r"FROM\s+([A-Z0-9_\.]+)", r"JOIN\s+([A-Z0-9_\.]+)"]:
        for match in re.findall(pattern, sql_upper):
            if match not in tables:
                tables.append(match)

    has_where = " WHERE " in f" {sql_upper} "
    has_group = " GROUP BY " in f" {sql_upper} "
    has_order = " ORDER BY " in f" {sql_upper} "
    has_limit = " LIMIT " in f" {sql_upper} "
    has_agg = bool(re.search(r"\b(SUM|COUNT|AVG|MIN|MAX)\s*\(", sql_upper))

    suggestions = []
    if has_agg and has_limit and not has_group:
        suggestions.append("Bỏ LIMIT vì query aggregate một dòng không cần giới hạn bản ghi đầu ra.")
    if not has_where:
        suggestions.append("Thêm điều kiện lọc thời gian/miền dữ liệu để giảm lượng scan nếu bài toán cho phép.")
    if has_order and not has_limit:
        suggestions.append("Nếu chỉ cần top-k, thêm LIMIT để giảm chi phí sort.")

    if not suggestions:
        suggestions.append("Query hiện tại đã khá hợp lý cho mục tiêu hiện tại.")

    return (
        "1) Query đang đọc dữ liệu từ: " + (", ".join(tables) if tables else "(không xác định)") + ".\n"
        f"2) Đặc điểm: WHERE={has_where}, GROUP_BY={has_group}, ORDER_BY={has_order}, LIMIT={has_limit}, AGG={has_agg}.\n"
        "3) Gợi ý tối ưu: " + " ".join(suggestions)
    )


def _build_sql_explain_answer(question: str, sql_text: str) -> str:
    if not sql_text:
        return "Mình chưa có truy vấn SQL trước đó để giải thích/tối ưu. Bạn hãy chạy một câu hỏi dữ liệu trước, rồi hỏi lại về tối ưu query."

    db_engine = os.getenv("DB_ENGINE", "duckdb").lower()
    explain_output = ""
    if db_engine == "duckdb":
        db_path = os.getenv("DUCKDB_PATH", "data/kur.db")
        conn = None
        try:
            import duckdb
            conn = duckdb.connect(db_path, read_only=True)
            rows = conn.execute(f"EXPLAIN {sql_text}").fetchall()
            if rows:
                explain_output = "\n".join(str(r) for r in rows[:30])
        except Exception:
            explain_output = ""
        finally:
            if conn is not None:
                conn.close()

    try:
        from utils.llm_factory import get_llm
        llm = get_llm(model_type="small", temperature=0)
        timeout_s = float(os.getenv("LLM_TIMEOUT", "20"))
        prompt = f"""Bạn là Senior Data Analyst. Hãy giải thích ngắn gọn và thực dụng.

Yêu cầu user:
{question}

SQL cần phân tích:
{sql_text}

EXPLAIN PLAN (nếu có):
{explain_output if explain_output else "N/A"}

Trả lời theo format:
1) Query này đang làm gì
2) Điểm tốt
3) Điểm có thể tối ưu
4) SQL tối ưu đề xuất (nếu cần)

Ngắn gọn, rõ ràng, tiếng Việt.
"""
        res = invoke_with_timeout(lambda: llm.invoke(prompt), timeout_s)
        answer = (res.content if hasattr(res, "content") else str(res)).strip()
        if answer:
            return answer.replace("**", "")
    except Exception:
        pass

    return _deterministic_sql_explain(sql_text)


def _rewrite_sql_from_latest(question: str, sql_text: str, schema_context: str) -> tuple[str, str]:
    if not sql_text:
        return "", "Mình chưa có SQL trước đó để rewrite. Bạn hãy chạy một câu query trước rồi yêu cầu tối ưu lại."

    heuristic = sql_text.strip()
    if "SUM(" in heuristic.upper() and "GROUP BY" not in heuristic.upper() and "LIMIT" in heuristic.upper():
        lines = [line for line in heuristic.splitlines() if "LIMIT" not in line.upper()]
        heuristic = "\n".join(lines).rstrip(";") + ";"

    try:
        from utils.llm_factory import get_llm

        llm = get_llm(model_type="small", temperature=0)
        timeout_s = float(os.getenv("LLM_TIMEOUT", "20"))
        prompt = f"""Bạn là Senior Analytics Engineer.
Hãy viết một phiên bản SQL khác tối ưu hơn nhưng giữ đúng business meaning.

Yêu cầu user:
{question}

SQL cũ:
{sql_text}

Schema context:
{schema_context[:5000]}

Rules:
- Chỉ trả về SQL thuần (không markdown)
- Không DDL/DML
- Nếu query là aggregate toàn bảng, tránh LIMIT dư thừa
- Giữ logic nghiệp vụ tương đương
"""
        res = invoke_with_timeout(lambda: llm.invoke(prompt), timeout_s)
        candidate = (res.content if hasattr(res, "content") else str(res)).strip()
        candidate = candidate.removeprefix("```sql").removeprefix("```").removesuffix("```").strip()
        if candidate:
            return candidate, "Đã tạo phiên bản SQL tối ưu hơn từ query trước."
    except Exception:
        pass

    return heuristic, "Đã tạo phiên bản SQL tối ưu theo heuristic từ query trước."


def _prepare_sql_state(question: str, max_retries: int) -> dict:
    state = {
        "question": question,
        "conversation_context": _build_conversation_context(limit=4),
        "intent": "",
        "schema_context": "",
        "rag_examples": "",
        "generated_sql": "",
        "validation_result": "",
        "query_result": None,
        "final_answer": "",
        "error_message": "",
        "retry_count": 0,
        "debug_steps": [],
        "node_timings_ms": {},
    }

    state = _run_timed_step(state, "classify_intent", classify_intent_node)
    if state.get("intent") in {"greeting", "meta_chat", "out_of_scope"}:
        state = _run_timed_step(state, "format_response", format_response_node)
        return state

    if state.get("intent") == "system_info":
        state["final_answer"] = _build_runtime_info_answer()
        return state

    if state.get("intent") == "assistant_explain":
        state["final_answer"] = _build_assistant_explain_answer(question)
        return state

    if state.get("intent") == "sql_explain":
        state["final_answer"] = _build_sql_explain_answer(question, _find_latest_sql(limit=20))
        return state

    if state.get("intent") == "sql_rewrite":
        state = _run_timed_step(state, "retrieve_schema", retrieve_schema_node)
        latest_sql = _find_latest_sql(limit=20)
        rewritten_sql, rewrite_msg = _rewrite_sql_from_latest(
            question=question,
            sql_text=latest_sql,
            schema_context=state.get("schema_context", ""),
        )
        state["generated_sql"] = rewritten_sql
        state["final_answer"] = rewrite_msg
        if rewritten_sql:
            state = _run_timed_step(state, "validate_sql", validate_sql_node)
        else:
            state["validation_result"] = "FAIL"
        return state

    if state.get("intent") == "clarification":
        state = _run_timed_step(state, "retrieve_schema", retrieve_schema_node)
        state["final_answer"] = _build_clarification_answer(question, state.get("schema_context", ""))
        return state

    state = _run_timed_step(state, "retrieve_schema", retrieve_schema_node)
    state = _run_timed_step(state, "retrieve_examples", retrieve_examples_node)
    last_error = None
    repeated_error_count = 0

    while True:
        state = _run_timed_step(state, "generate_sql", generate_sql_node)

        if state.get("error_message"):
            current_error = state.get("error_message", "")
            if current_error == last_error:
                repeated_error_count += 1
            else:
                repeated_error_count = 0
            last_error = current_error

            # Fail fast for provider/network timeout patterns
            err_low = current_error.lower()
            timeout_like = ("timeout" in err_low) or ("timed out" in err_low)
            if timeout_like or repeated_error_count >= 1:
                state["retry_count"] = max_retries
                brief = current_error[:140]
                state["final_answer"] = (
                    "⏱️ Không thể sinh SQL ổn định với provider hiện tại. "
                    "Flow dừng ở bước generate_sql để tránh lặp vô nghĩa. "
                    f"Chi tiết: {brief}. Bạn đổi model nhẹ hơn hoặc thử lại sau."
                )
                return state

        state = _run_timed_step(state, "validate_sql", validate_sql_node)

        if state.get("validation_result") == "PASS":
            return state

        state = _run_timed_step(state, "refine_sql", refine_sql_node)
        if state.get("retry_count", 0) >= max_retries:
            state = _run_timed_step(state, "format_response", format_response_node)
            return state


def _execute_sql_state(state: dict, max_retries: int) -> dict:
    if state.get("intent") == "sql_rewrite":
        state["final_answer"] = ""

    while True:
        state = _run_timed_step(state, "execute_sql", execute_sql_node)
        if not state.get("error_message"):
            break

        state = _run_timed_step(state, "refine_sql", refine_sql_node)
        if state.get("retry_count", 0) >= max_retries:
            break

        state = _run_timed_step(state, "generate_sql", generate_sql_node)
        state = _run_timed_step(state, "validate_sql", validate_sql_node)
        if state.get("validation_result") != "PASS":
            continue

    state = _run_timed_step(state, "format_response", format_response_node)
    return state


# Apply settings on startup
apply_settings(load_settings())
_init_history_db()


# ──── Models ────
class AskRequest(BaseModel):
    question: str
    auto_execute: bool = False


class ExecuteRequest(BaseModel):
    request_id: str


class AskResponse(BaseModel):
    question: str
    sql: Optional[str] = None
    data: Optional[list] = None
    columns: Optional[list] = None
    answer: str
    intent: Optional[str] = None
    retries: int = 0
    latency_ms: int = 0
    timestamp: str = ""
    error: Optional[str] = None
    debug_steps: Optional[list] = None
    requires_approval: bool = False
    request_id: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    engine: str = "DuckDB"
    duckdb_path: str = ""
    tables: int = 0


# ──── Settings Endpoints ────
@app.get("/api/settings")
async def get_settings():
    s = load_settings()
    return {
        "llm_provider": s.get("llm_provider", "openai"),
        "llm_model": s.get("llm_model", "gpt-4o"),
        "api_key_masked": mask_key(s.get("api_key", "")),
        "ollama_url": s.get("ollama_url", "http://localhost:11434"),
        "ollama_model": s.get("ollama_model", ""),

        "db_engine": s.get("db_engine", "duckdb"),
        "duckdb_path": s.get("duckdb_path", "data/kur.db"),
        "db_host": s.get("db_host", "localhost"),
        "db_port": s.get("db_port", 5432),
        "db_name": s.get("db_name", "business_db"),
        "db_user": s.get("db_user", "analyst"),

        "uc_url": s.get("uc_url", "http://uc-server:8080"),
        "uc_catalog": s.get("uc_catalog", "kur_catalog"),
        "uc_schema": s.get("uc_schema", "public"),

        "max_retries": s.get("max_retries", 3),
        "query_timeout": s.get("query_timeout", 30),
        "max_rows": s.get("max_rows", 1000),
        "language": s.get("language", "auto"),
    }


@app.post("/api/settings")
async def update_settings(payload: dict):
    current = load_settings()

    # Merge — only overwrite keys that are present
    for key in ["llm_provider", "llm_model", "ollama_url", "ollama_model",
                 "db_engine", "duckdb_path", "db_host", "db_port", "db_name", "db_user",
                 "uc_url", "uc_catalog", "uc_schema",
                 "max_retries", "query_timeout", "max_rows", "language"]:
        if key in payload and payload[key] is not None:
            current[key] = payload[key]

    # Sensitive keys: only update if non-empty
    if payload.get("api_key"):
        current["api_key"] = payload["api_key"]
    if payload.get("db_password"):
        current["db_password"] = payload["db_password"]

    save_settings(current)
    apply_settings(current)

    return {"status": "ok", "message": "Settings saved"}


@app.post("/api/settings/reset")
async def reset_settings():
    save_settings(dict(DEFAULT_SETTINGS))
    apply_settings(DEFAULT_SETTINGS)
    return {"status": "ok", "message": "Settings reset to defaults"}


# ──── Core Endpoints ────
@app.get("/api/health", response_model=HealthResponse)
def health():
    s = load_settings()
    engine = s.get("db_engine", "duckdb")

    if engine == "duckdb":
        import duckdb
        db_path = s.get("duckdb_path", "data/kur.db")
        try:
            conn = duckdb.connect(db_path, read_only=True)
            tables = conn.execute("SHOW TABLES").fetchall()
            conn.close()
            return HealthResponse(
                status="healthy", version="2.0", engine="DuckDB",
                duckdb_path=db_path, tables=len(tables)
            )
        except Exception as e:
            return HealthResponse(
                status=f"degraded: {e}", version="2.0", engine="DuckDB",
                duckdb_path=db_path, tables=0
            )
    elif engine == "trino":
        try:
            import trino
            host = s.get("db_host", "trino")
            port = int(s.get("db_port", 8080))
            user = s.get("db_user", "analyst")
            catalog = s.get("db_name", "memory")
            schema = s.get("uc_schema", "default")

            conn = trino.dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
                schema=schema,
                http_scheme="http",
            )
            cur = conn.cursor()
            schema_safe = str(schema).replace("'", "''")
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = '{schema_safe}'
                """
            )
            count = int(cur.fetchone()[0])
            cur.close()
            conn.close()
            return HealthResponse(
                status="healthy", version="2.0", engine="TRINO",
                duckdb_path="", tables=count
            )
        except Exception as e:
            return HealthResponse(
                status=f"degraded: {e}", version="2.0", engine="TRINO",
                duckdb_path="", tables=0
            )
    else:
        return HealthResponse(
            status="configured", version="2.0", engine=engine.upper(),
            duckdb_path="", tables=0
        )


@app.post("/api/ask", response_model=AskResponse)
def ask(req: AskRequest):
    if not req.question.strip():
        raise HTTPException(400, "Question cannot be empty")

    start = time.time()
    _clean_pending_queries()
    settings = load_settings()
    apply_settings(settings)
    max_retries = int(settings.get("max_retries", 3))

    try:
        result = _prepare_sql_state(req.question, max_retries=max_retries)

        if result.get("intent") not in {"sql_query", "sql_rewrite"}:
            latency = int((time.time() - start) * 1000)
            answer = result.get("final_answer", "Không thể xử lý.")
            _save_history(req.question, answer, result.get("intent") or "")
            return AskResponse(
                question=req.question,
                sql=result.get("generated_sql"),
                data=None,
                columns=None,
                answer=answer,
                intent=result.get("intent"),
                retries=result.get("retry_count", 0),
                latency_ms=latency,
                timestamp=datetime.now().isoformat(),
                error=result.get("error_message") or None,
                debug_steps=None,
            )

        if result.get("intent") == "sql_rewrite" and result.get("validation_result") == "PASS" and not req.auto_execute:
            request_id = str(uuid.uuid4())
            PENDING_QUERIES[request_id] = {
                "state": result,
                "created_at": time.time(),
            }
            latency = int((time.time() - start) * 1000)
            return AskResponse(
                question=req.question,
                sql=result.get("generated_sql"),
                data=None,
                columns=None,
                answer="🛠️ Đây là phiên bản SQL tối ưu hơn. Nhấn 'Allow chạy query' để thực thi hoặc 'Skip'.",
                intent=result.get("intent"),
                retries=result.get("retry_count", 0),
                latency_ms=latency,
                timestamp=datetime.now().isoformat(),
                error=None,
                debug_steps=None,
                requires_approval=True,
                request_id=request_id,
            )

        if result.get("validation_result") != "PASS":
            latency = int((time.time() - start) * 1000)
            answer = result.get("final_answer", "Không thể tạo SQL hợp lệ.")
            _save_history(req.question, answer, result.get("intent") or "", result.get("generated_sql"))
            return AskResponse(
                question=req.question,
                sql=result.get("generated_sql"),
                data=None,
                columns=None,
                answer=answer,
                intent=result.get("intent"),
                retries=result.get("retry_count", 0),
                latency_ms=latency,
                timestamp=datetime.now().isoformat(),
                error=result.get("error_message") or None,
                debug_steps=None,
            )

        if not req.auto_execute:
            request_id = str(uuid.uuid4())
            PENDING_QUERIES[request_id] = {
                "state": result,
                "created_at": time.time(),
            }
            latency = int((time.time() - start) * 1000)
            return AskResponse(
                question=req.question,
                sql=result.get("generated_sql"),
                data=None,
                columns=None,
                answer="📝 SQL đã sẵn sàng. Nhấn 'Allow chạy query' để thực thi hoặc 'Skip'.",
                intent=result.get("intent"),
                retries=result.get("retry_count", 0),
                latency_ms=latency,
                timestamp=datetime.now().isoformat(),
                error=None,
                debug_steps=None,
                requires_approval=True,
                request_id=request_id,
            )

        result = _execute_sql_state(result, max_retries=max_retries)

        data = None
        columns = None
        if result.get("query_result") is not None:
            import pandas as pd
            df = result["query_result"]
            if isinstance(df, pd.DataFrame) and not df.empty:
                columns = list(df.columns)
                max_rows = settings.get("max_rows", 500)
                data = df.head(max_rows).to_dict("records")

        latency = int((time.time() - start) * 1000)
        answer = result.get("final_answer", "Không thể xử lý.")
        _save_history(req.question, answer, result.get("intent") or "", result.get("generated_sql"))

        return AskResponse(
            question=req.question,
            sql=result.get("generated_sql"),
            data=data,
            columns=columns,
            answer=answer,
            intent=result.get("intent"),
            retries=result.get("retry_count", 0),
            latency_ms=latency,
            timestamp=datetime.now().isoformat(),
            error=result.get("error_message") or None,
            debug_steps=None,
            requires_approval=False,
        )
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        answer = f"❌ Lỗi server: {str(e)[:200]}"
        _save_history(req.question, answer, "server_error")
        return AskResponse(
            question=req.question,
            answer=answer,
            latency_ms=latency,
            timestamp=datetime.now().isoformat(),
            error=str(e)[:200],
            debug_steps=None,
        )


@app.post("/api/execute", response_model=AskResponse)
def execute_prepared(req: ExecuteRequest):
    _clean_pending_queries()
    payload = PENDING_QUERIES.get(req.request_id)
    if not payload:
        raise HTTPException(404, "Request ID not found or expired")

    settings = load_settings()
    apply_settings(settings)
    max_retries = int(settings.get("max_retries", 3))

    start = time.time()
    state = payload.get("state", {})
    state = _execute_sql_state(state, max_retries=max_retries)
    PENDING_QUERIES.pop(req.request_id, None)

    data = None
    columns = None
    if state.get("query_result") is not None:
        import pandas as pd
        df = state["query_result"]
        if isinstance(df, pd.DataFrame) and not df.empty:
            columns = list(df.columns)
            max_rows = settings.get("max_rows", 500)
            data = df.head(max_rows).to_dict("records")

    latency = int((time.time() - start) * 1000)
    answer = state.get("final_answer", "Không thể xử lý.")
    _save_history(state.get("question", ""), answer, state.get("intent") or "", state.get("generated_sql"))
    return AskResponse(
        question=state.get("question", ""),
        sql=state.get("generated_sql"),
        data=data,
        columns=columns,
        answer=answer,
        intent=state.get("intent"),
        retries=state.get("retry_count", 0),
        latency_ms=latency,
        timestamp=datetime.now().isoformat(),
        error=state.get("error_message") or None,
        debug_steps=None,
        requires_approval=False,
    )


@app.get("/api/history")
def get_history(limit: int = 30):
    return {"items": _load_history(limit=limit)}


@app.delete("/api/history")
def clear_history():
    _clear_history()
    return {"status": "ok"}


@app.get("/api/suggestions")
async def suggestions():
    return {
        "suggestions": [
            "Tổng doanh thu tháng này",
            "Top 10 khách hàng chi tiêu nhiều nhất",
            "Doanh thu theo khu vực tháng trước",
            "Sản phẩm bán chạy nhất",
            "Tỷ lệ đơn hàng bị hủy",
            "So sánh doanh thu Q1 với Q2",
            "Doanh thu theo danh mục sản phẩm",
            "Khách VIP ở Hồ Chí Minh",
            "Trung bình giá trị đơn hàng theo tháng",
            "Hiệu quả marketing campaign tháng này",
        ]
    }


@app.get("/api/schema")
def get_schema():
    s = load_settings()
    engine = s.get("db_engine", "duckdb")

    if engine == "duckdb":
        import duckdb
        db_path = s.get("duckdb_path", "data/kur.db")
        try:
            conn = duckdb.connect(db_path, read_only=True)
            tables = conn.execute("SHOW TABLES").fetchall()
            result = []
            for (name,) in tables:
                count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                cols = conn.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{name}' ORDER BY ordinal_position
                """).fetchall()
                result.append({
                    "name": name,
                    "row_count": count,
                    "columns": [{"name": c, "type": t} for c, t in cols]
                })
            conn.close()
            return {"tables": result}
        except Exception as e:
            raise HTTPException(500, str(e))
    if engine == "trino":
        try:
            import trino
            host = s.get("db_host", "trino")
            port = int(s.get("db_port", 8080))
            user = s.get("db_user", "analyst")
            catalog = s.get("db_name", "memory")
            schema = s.get("uc_schema", "default")

            conn = trino.dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
                schema=schema,
                http_scheme="http",
            )
            cur = conn.cursor()
            schema_safe = str(schema).replace("'", "''")
            cur.execute(
                f"""
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema_safe}'
                ORDER BY table_name, ordinal_position
                """,
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()

            tables_map = {}
            for table_name, column_name, data_type in rows:
                if table_name not in tables_map:
                    tables_map[table_name] = {
                        "name": table_name,
                        "row_count": None,
                        "columns": [],
                    }
                tables_map[table_name]["columns"].append({
                    "name": column_name,
                    "type": data_type,
                })

            return {"tables": list(tables_map.values()), "engine": "trino"}
        except Exception as e:
            raise HTTPException(500, str(e))

    return {"tables": [], "engine": engine, "message": f"{engine} schema introspection not yet implemented"}

