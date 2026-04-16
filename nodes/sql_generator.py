"""SQL Generator Node — Sinh SQL từ LLM"""
import os
import duckdb
from tools.skills import load_skill
from utils.llm_invoke import invoke_with_timeout


SYSTEM_PROMPT = """You are Kur, an expert SQL generator for DuckDB.
You receive a user question in Vietnamese or English, database schema, and example queries.
Generate ONLY the SQL query. No explanation. No markdown. Just pure SQL.

Rules:
- DuckDB SQL dialect (very close to PostgreSQL)
- SELECT queries only (no INSERT, UPDATE, DELETE, DROP)
- Always use explicit table aliases
- Include LIMIT 1000 if no limit specified
- Use DATE_TRUNC for date operations
- For historical snapshot datasets, prefer anchoring relative periods to latest date in data, not wall-clock CURRENT_DATE
- When aggregating, always include GROUP BY
- DuckDB supports: SAMPLE, STRUCT, LIST, PIVOT, UNPIVOT, QUALIFY
- Use EPOCH for timestamp math, not EXTRACT(EPOCH FROM ...)
"""


def _build_time_context() -> str:
    db_engine = os.getenv("DB_ENGINE", "duckdb").lower()
    if db_engine != "duckdb":
        return "No dataset date range hint available for this engine."

    db_path = os.getenv("DUCKDB_PATH", "data/kur.db")
    conn = None
    try:
        conn = duckdb.connect(db_path, read_only=True)
        max_date = None
        min_date = None

        candidate_queries = [
            "SELECT MIN(order_date), MAX(order_date) FROM fact_orders",
            "SELECT MIN(event_time::DATE), MAX(event_time::DATE) FROM fact_web_sessions",
            "SELECT MIN(created_at::DATE), MAX(created_at::DATE) FROM fact_orders",
        ]
        for query in candidate_queries:
            try:
                row = conn.execute(query).fetchone()
                if row and row[0] is not None and row[1] is not None:
                    min_date, max_date = row[0], row[1]
                    break
            except Exception:
                continue

        if min_date and max_date:
            return (
                f"Dataset date range hint: from {min_date} to {max_date}. "
                "For phrases like 'tháng này', 'tháng trước', 'Q1', 'Q2', "
                "interpret relative to this max available date unless user specifies calendar year explicitly."
            )
    except Exception:
        pass
    finally:
        if conn is not None:
            conn.close()

    return "No dataset date range hint available."


def generate_sql_node(state: dict) -> dict:
    """Generate SQL using LLM with schema context and RAG examples."""
    question = state["question"]
    schema = state["schema_context"]
    examples = state.get("rag_examples", "")
    conversation_context = state.get("conversation_context", "")
    error = state.get("error_message", "")

    # Load writing-sql skill for best practices
    skill = load_skill("writing-sql")
    time_context = _build_time_context()

    user_prompt = f"""## Database Schema
{schema}

## Similar Past Queries
{examples if examples else "No similar queries found."}

## SQL Best Practices
{skill}

## Conversation Context (recent turns)
{conversation_context if conversation_context else "No prior conversation context."}

## Dataset Time Context
{time_context}

{"## Previous Error (fix this)" + chr(10) + error if error else ""}

## User Question
{question}

## SQL Query (PostgreSQL):"""

    llm_timeout = float(os.getenv("LLM_TIMEOUT", "20"))

    from utils.llm_factory import get_llm

    def _invoke_with_model(model_type: str, prompt_text: str, timeout_s: float):
        llm = get_llm(model_type=model_type, temperature=0)

        def _invoke():
            if os.getenv("LLM_PROVIDER", "openai").lower() == "ollama":
                return llm.invoke(SYSTEM_PROMPT + "\n\n" + prompt_text)
            return llm.invoke([
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt_text},
            ])

        return invoke_with_timeout(_invoke, timeout_s)

    try:
        response_obj = _invoke_with_model("large", user_prompt, llm_timeout)
        response = response_obj.content if hasattr(response_obj, "content") else str(response_obj)
    except Exception as e1:
        try:
            compact_prompt = f"""## Database Schema
{schema[:3500]}

## User Question
{question}

## SQL Query (PostgreSQL):"""
            response_obj = _invoke_with_model("small", compact_prompt, max(15.0, llm_timeout * 1.5))
            response = response_obj.content if hasattr(response_obj, "content") else str(response_obj)
        except Exception as e2:
            state["generated_sql"] = ""
            state["error_message"] = (
                f"LLM generation timeout/error: primary={str(e1)[:80]} | fallback={str(e2)[:80]}"
            )
            return state

    # Clean SQL
    sql = response.strip()
    sql = sql.removeprefix("```sql").removeprefix("```").removesuffix("```").strip()

    state["generated_sql"] = sql
    state["error_message"] = ""
    return state
