"""Kur API — FastAPI backend for Agentic Text-to-SQL"""
import os
import time
import uuid
import re
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

from phoenix.otel import register
from openinference.instrumentation.langchain import LangChainInstrumentor

# Initialize Phoenix tracing
tracer_provider = register(
    endpoint=os.getenv("PHOENIX_COLLECTOR_ENDPOINT", "http://phoenix:6006/v1/traces")
)
LangChainInstrumentor().instrument(tracer_provider=tracer_provider)
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

from agent.graph import build_kur_agent
from langchain_core.messages import HumanMessage
from agent.utils.llm_invoke import invoke_with_timeout
from app.core import config as cfg
from app.services import history_service, polaris_service, schema_adapter
from app.models.api_models import AskRequest, ExecuteRequest, AskResponse, HealthResponse

app = FastAPI(title="Kur API", version="2.0", description="Agentic Text-to-SQL Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Agent orchestrator (initialized on request)
# kur_graph = build_kur_graph()
PENDING_QUERIES = {}
PENDING_TTL_SECONDS = 1800
AGENT_CACHE = {"signature": None, "agent": None}


def _init_history_db():
    history_service.init_history_db()


def _save_history(question: str, answer: str, intent: str, sql_text: Optional[str] = None):
    history_service.save_history(question, answer, intent, sql_text)


def _load_history(limit: int = 30):
    return history_service.load_history(limit)


def _clear_history():
    history_service.clear_history()


def load_settings() -> dict:
    return cfg.load_settings()


def save_settings(settings: dict):
    cfg.save_settings(settings)


def apply_settings(settings: dict):
    cfg.apply_settings(settings)


def _ensure_polaris_catalog(settings: Optional[dict] = None, timeout_s: float = 4.0) -> tuple[bool, str]:
    return polaris_service.ensure_polaris_catalog(settings=settings, timeout_s=timeout_s)


def mask_key(key: str) -> str:
    return cfg.mask_key(key)


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


def _normalize_message_content(content) -> str:
    if isinstance(content, str):
        return content
    if content is None:
        return ""
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                if item.strip():
                    parts.append(item.strip())
                continue
            if isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text).strip())
                    continue
                nested = item.get("content")
                if isinstance(nested, str) and nested.strip():
                    parts.append(nested.strip())
                    continue
            rendered = str(item).strip()
            if rendered:
                parts.append(rendered)
        return "\n".join(parts).strip()
    return str(content)


def _get_or_build_agent(settings: dict):
    signature = (
        settings.get("router_provider", ""),
        settings.get("router_model", ""),
        settings.get("router_api_key", ""),
        settings.get("generator_provider", ""),
        settings.get("generator_model", ""),
        settings.get("generator_api_key", ""),
    )

    if AGENT_CACHE.get("agent") is not None and AGENT_CACHE.get("signature") == signature:
        return AGENT_CACHE["agent"]

    from agent.graph import build_kur_agent
    agent = build_kur_agent()
    AGENT_CACHE["signature"] = signature
    AGENT_CACHE["agent"] = agent
    return agent


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
    provider = s.get("generator_provider", "openai")
    model = s.get("generator_model", "")
    polaris_catalog = s.get("polaris_catalog", "kur_polaris_catalog")

    if engine == "duckdb":
        db_info = f"DuckDB ({s.get('duckdb_path', 'data/kur.db')})"
    elif engine == "trino":
        db_info = f"Trino {s.get('db_host', 'trino')}:{s.get('db_port', 8080)} / catalog={s.get('db_name', 'memory')}"
    else:
        db_info = f"{engine.upper()} {s.get('db_host', 'localhost')}:{s.get('db_port', '')} / db={s.get('db_name', '')}"

    return (
        f"Hiện tại đang dùng DB engine: {db_info}. "
        f"Catalog active: {polaris_catalog}. "
        f"LLM đang dùng: {provider}/{model}."
    )


def _build_assistant_explain_answer(question: str) -> str:
    last_answer = _find_latest_answer(limit=30)
    if not last_answer:
        return "Mình chưa có câu trả lời trước đó để diễn giải. Bạn có thể nhắc lại nội dung cần mình giải thích."

    try:
        from agent.utils.llm_factory import get_llm

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


def _format_result_value(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value:,}".replace(",", ".")
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}".replace(",", ".")
        return f"{value:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return str(value)


def _build_grounded_answer_from_result(question: str, data: Optional[list], columns: Optional[list]) -> str:
    rows = data or []
    if not rows:
        return "Truy vấn chạy thành công nhưng không có dữ liệu phù hợp."

    first = rows[0] if isinstance(rows[0], dict) else {}
    lower_map = {str(k).lower(): k for k in first.keys()}

    count_keys = ["count", "count(*)", "count_star", "count_star()", "total_count", "cnt"]
    count_key = next((lower_map[k] for k in count_keys if k in lower_map), None)
    if count_key and len(first.keys()) == 1:
        return f"Số dòng dữ liệu là {_format_result_value(first[count_key])}."

    customer_key = lower_map.get("customer_id")
    spent_key = lower_map.get("total_spent") or lower_map.get("total_consumption")
    payments_key = lower_map.get("num_orders") or lower_map.get("total_payments")
    if customer_key and spent_key and payments_key:
        return (
            f"Khách hàng có tiêu thụ cao nhất là {_format_result_value(first[customer_key])}, "
            f"tổng tiêu thụ {_format_result_value(first[spent_key])} "
            f"và số lần thanh toán {_format_result_value(first[payments_key])}."
        )

    if len(rows) == 1 and len(first.keys()) == 1:
        only_key = next(iter(first.keys()))
        return f"Kết quả {only_key}: {_format_result_value(first[only_key])}."

    return f"Truy vấn chạy thành công, trả về {len(rows)} dòng dữ liệu."


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
        from agent.utils.llm_factory import get_llm
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
        from agent.utils.llm_factory import get_llm

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


def _is_sql_explain_followup(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return False
    patterns = [
        r"tại sao.*sql",
        r"vì sao.*sql",
        r"tối ưu chưa",
        r"optimi[sz]e",
        r"giải thích.*sql",
        r"query.*trên",
        r"câu.*sql.*trên",
        r"sao lại chọn",
    ]
    return any(re.search(p, q) for p in patterns)


def _extract_inline_sql_from_text(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""

    md_match = re.search(r"```sql\s*(.*?)\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    if md_match:
        return md_match.group(1).strip()

    sql_match = re.search(r"(SELECT\s+.*?)(?:;\s|$)", raw, flags=re.DOTALL | re.IGNORECASE)
    if sql_match:
        candidate = sql_match.group(1).strip()
        if candidate:
            return candidate + (";" if not candidate.endswith(";") else "")
    return ""


def _is_schema_list_question(question: str) -> bool:
    q = (question or "").strip().lower()
    patterns = [
        r"liệt kê.*bảng",
        r"danh sách.*bảng",
        r"các bảng hiện có",
        r"list.*tables?",
        r"show.*tables?",
        r"những bảng.*hệ thống",
    ]
    return any(re.search(p, q) for p in patterns)


def _build_schema_list_answer() -> tuple[str, str]:
    try:
        tables = schema_adapter.load_schema_snapshot()
    except Exception as exc:
        return "", f"Không đọc được schema hiện tại: {str(exc)[:180]}"

    if not tables:
        return "", "Hiện chưa có bảng nào khả dụng trong schema hiện tại."

    names = sorted({meta.get("full_name") or meta.get("table") for meta in tables.values() if (meta.get("full_name") or meta.get("table"))})
    answer = "Các bảng hiện có trong hệ thống:\n- " + "\n- ".join(names)
    return "", answer


# Apply settings on startup
_startup_settings = load_settings()
apply_settings(_startup_settings)
_ensure_polaris_catalog(_startup_settings)
_init_history_db()


# ──── Settings Endpoints ────
@app.get("/api/settings")
async def get_settings():
    s = load_settings()
    router_key = s.get("router_api_key", "")
    generator_key = s.get("generator_api_key", "")
    return {
        "router_provider": s.get("router_provider", "groq"),
        "router_model": s.get("router_model", "llama-3-8b"),
        "router_api_key_masked": mask_key(router_key),
        "router_api_key_configured": bool((router_key or "").strip()),
        "generator_provider": s.get("generator_provider", "openai"),
        "generator_model": s.get("generator_model", "gpt-4o"),
        "generator_api_key_masked": mask_key(generator_key),
        "generator_api_key_configured": bool((generator_key or "").strip()),
        "ollama_url": s.get("ollama_url", "http://localhost:11434"),
        "ollama_model": s.get("ollama_model", ""),

        "db_engine": s.get("db_engine", "duckdb"),
        "duckdb_path": s.get("duckdb_path", "data/kur.db"),
        "db_host": s.get("db_host", "localhost"),
        "db_port": s.get("db_port", 5432),
        "db_name": s.get("db_name", "business_db"),
        "db_user": s.get("db_user", "analyst"),

        "polaris_url": s.get("polaris_url", "http://polaris:8181"),
        "polaris_catalog": s.get("polaris_catalog", "kur_polaris_catalog"),
        "polaris_credentials_masked": mask_key(s.get("polaris_credentials", "")),

        "max_retries": s.get("max_retries", 3),
        "query_timeout": s.get("query_timeout", 30),
        "max_rows": s.get("max_rows", 1000),
        "language": s.get("language", "auto"),
    }


@app.post("/api/settings")
async def update_settings(payload: dict):
    current = load_settings()

    # Merge — only overwrite keys that are present
    for key in ["router_provider", "router_model", "generator_provider", "generator_model", "ollama_url", "ollama_model",
                 "db_engine", "duckdb_path", "db_host", "db_port", "db_name", "db_user",
                 "polaris_url", "polaris_catalog", "polaris_credentials",
                 "max_retries", "query_timeout", "max_rows", "language"]:
        if key in payload and payload[key] is not None:
            current[key] = payload[key]

    # Sensitive keys: only update if non-empty
    if payload.get("router_api_key"):
        current["router_api_key"] = payload["router_api_key"]
    if payload.get("generator_api_key"):
        current["generator_api_key"] = payload["generator_api_key"]
    if payload.get("db_password"):
        current["db_password"] = payload["db_password"]
    if payload.get("polaris_credentials"):
        current["polaris_credentials"] = payload["polaris_credentials"]

    save_settings(current)
    apply_settings(current)

    ok, msg = _ensure_polaris_catalog(current)

    return {
        "status": "ok",
        "message": "Settings saved",
        "polaris_catalog_ready": ok,
        "polaris_message": msg,
    }


@app.post("/api/settings/reset")
async def reset_settings():
    save_settings(dict(cfg.DEFAULT_SETTINGS))
    apply_settings(cfg.DEFAULT_SETTINGS)
    return {"status": "ok", "message": "Settings reset to defaults"}


@app.post("/api/polaris/ensure")
async def ensure_polaris_catalog():
    settings = load_settings()
    ok, msg = _ensure_polaris_catalog(settings)
    return {
        "status": "ok" if ok else "warn",
        "polaris_catalog_ready": ok,
        "message": msg,
    }


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
            schema = "public"

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


@app.get("/api/history")
def get_history(limit: int = 30):
    return {"items": _load_history(limit)}


@app.get("/api/suggestions")
def get_suggestions():
    return {"suggestions": [
        "Liệt kê các bảng hiện có trong hệ thống",
        "Có bao nhiêu dòng dữ liệu trong bảng khách hàng?",
        "Tính tổng doanh thu theo từng tháng trong năm nay",
        "Hiển thị 5 bản ghi mới nhất",
    ]}


@app.post("/api/ask", response_model=AskResponse)
def ask(req: AskRequest):
    if not req.question.strip():
        raise HTTPException(400, "Question cannot be empty")

    start = time.time()
    _clean_pending_queries()
    settings = load_settings()
    apply_settings(settings)
    
    from langchain_core.messages import HumanMessage
    import re
    import uuid

    try:
        if _is_schema_list_question(req.question):
            generated_sql, answer_text = _build_schema_list_answer()
            latency = int((time.time() - start) * 1000)
            _save_history(req.question, answer_text, "clarification", generated_sql or None)
            return AskResponse(
                question=req.question,
                sql=generated_sql or None,
                data=None,
                columns=None,
                answer=answer_text,
                intent="clarification",
                retries=0,
                latency_ms=latency,
                timestamp=datetime.now().isoformat(),
                error=None,
                debug_steps=None,
                requires_approval=False,
                request_id=None,
            )

        if _is_sql_explain_followup(req.question):
            inline_sql = _extract_inline_sql_from_text(req.question)
            latest_sql = inline_sql or _find_latest_sql(limit=30)
            answer_text = _build_sql_explain_answer(req.question, latest_sql)
            latency = int((time.time() - start) * 1000)
            _save_history(req.question, answer_text, "sql_explain", latest_sql or None)
            return AskResponse(
                question=req.question,
                sql=latest_sql or None,
                data=None,
                columns=None,
                answer=answer_text,
                intent="sql_explain",
                retries=0,
                latency_ms=latency,
                timestamp=datetime.now().isoformat(),
                error=None,
                debug_steps=None,
                requires_approval=False,
                request_id=None,
            )

        quick_sql, quick_answer = schema_adapter.quick_sql_from_question(req.question)
        if quick_sql:
            generated_sql = quick_sql
            latency = int((time.time() - start) * 1000)

            if not req.auto_execute:
                request_id = str(uuid.uuid4())
                PENDING_QUERIES[request_id] = {
                    "sql": generated_sql,
                    "question": req.question,
                    "created_at": time.time(),
                }
                return AskResponse(
                    question=req.question,
                    sql=generated_sql,
                    data=None, columns=None,
                    answer="Mình đã chuẩn bị SQL để lấy kết quả chính xác từ dữ liệu. Bấm 'Allow chạy query' để chạy.",
                    intent="sql_query",
                    retries=0, latency_ms=latency,
                    timestamp=datetime.now().isoformat(),
                    error=None, debug_steps=None,
                    requires_approval=True,
                    request_id=request_id,
                )

        agent = _get_or_build_agent(settings)
        out = agent.invoke({"messages": [HumanMessage(content=req.question)]})
        raw_content = out["messages"][-1].content
        last_msg = _normalize_message_content(raw_content)
        
        sql_match = re.search(r'```sql\s*(.*?)\s*```', last_msg, flags=re.DOTALL | re.IGNORECASE)
        generated_sql = sql_match.group(1).strip() if sql_match else ""
        generated_sql = schema_adapter.rewrite_select_star(generated_sql)
        
        answer_text = re.sub(r'```sql.*?```', '', last_msg, flags=re.DOTALL | re.IGNORECASE).strip()
        if not answer_text and generated_sql:
            answer_text = "Dưới đây là câu truy vấn sinh ra:"

        latency = int((time.time() - start) * 1000)

        if generated_sql and not req.auto_execute:
            request_id = str(uuid.uuid4())
            PENDING_QUERIES[request_id] = {
                "sql": generated_sql,
                "question": req.question,
                "created_at": time.time(),
            }
            return AskResponse(
                question=req.question,
                sql=generated_sql,
                data=None, columns=None,
                answer="Mình đã chuẩn bị SQL để lấy kết quả chính xác từ dữ liệu. Bấm 'Allow chạy query' để chạy.",
                intent="sql_query",
                retries=0, latency_ms=latency,
                timestamp=datetime.now().isoformat(),
                error=None, debug_steps=None,
                requires_approval=True,
                request_id=request_id
            )
        
        if generated_sql and req.auto_execute:
            db_engine = os.getenv("DB_ENGINE", "duckdb").lower()
            data, columns, error = None, None, None
            if db_engine == "duckdb":
                import duckdb
                try:
                    conn = duckdb.connect(os.getenv("DUCKDB_PATH", "data/kur.db"), read_only=True)
                    df = conn.execute(generated_sql).fetchdf()
                    conn.close()
                    columns = list(df.columns)
                    data = df.head(500).to_dict("records")
                except Exception as e:
                    error = str(e)

            final_answer = _build_grounded_answer_from_result(req.question, data, columns) if not error else f"Query failed: {error}"
            return AskResponse(
                question=req.question,
                sql=generated_sql,
                data=data, columns=columns,
                answer=final_answer,
                intent="sql_query",
                retries=0, latency_ms=latency,
                timestamp=datetime.now().isoformat(),
                error=error, debug_steps=None,
                requires_approval=False
            )
            
        return AskResponse(
            question=req.question,
            sql=None, data=None, columns=None,
            answer=last_msg,
            intent="meta_chat",
            retries=0, latency_ms=latency,
            timestamp=datetime.now().isoformat(),
            error=None, debug_steps=None,
            requires_approval=False
        )

    except Exception as e:
        latency = int((time.time() - start) * 1000)
        message = str(e)
        if "RESOURCE_EXHAUSTED" in message or "quota" in message.lower() or "429" in message:
            message = (
                "Provider hết quota (429). Vui lòng đổi model/provider trong Settings "
                "hoặc dùng fast-path với các câu hỏi đơn giản để giảm gọi LLM."
            )
        return AskResponse(
            question=req.question,
            answer=f"❌ Lỗi server: {message[:240]}",
            latency_ms=latency,
            timestamp=datetime.now().isoformat(),
            error=message[:240]
        )


@app.post("/api/execute", response_model=AskResponse)
def execute_prepared(req: ExecuteRequest):
    _clean_pending_queries()
    payload = PENDING_QUERIES.get(req.request_id)
    if not payload:
        raise HTTPException(404, "Request ID not found or expired")

    start = time.time()
    generated_sql = payload["sql"]
    question = payload["question"]
    PENDING_QUERIES.pop(req.request_id, None)
    
    settings = load_settings()
    apply_settings(settings)

    db_engine = os.getenv("DB_ENGINE", "duckdb").lower()
    data, columns, error = None, None, None
    if db_engine == "duckdb":
        import duckdb
        try:
            conn = duckdb.connect(os.getenv("DUCKDB_PATH", "data/kur.db"), read_only=True)
            df = conn.execute(generated_sql).fetchdf()
            conn.close()
            columns = list(df.columns)
            data = df.head(1000).to_dict("records")
        except Exception as e:
            error = str(e)

    latency = int((time.time() - start) * 1000)
    final_answer = _build_grounded_answer_from_result(question, data, columns) if not error else f"Query failed: {error}"
    _save_history(question, final_answer, "sql_query", generated_sql)
    return AskResponse(
        question=question,
        sql=generated_sql,
        data=data, columns=columns,
        answer=final_answer,
        intent="sql_query",
        retries=0, latency_ms=latency,
        timestamp=datetime.now().isoformat(),
        error=error, debug_steps=None,
        requires_approval=False,
    )

