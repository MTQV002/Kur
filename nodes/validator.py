"""SQL Validator Node — AST-based security + syntax check"""
import os
import sqlparse
import sqlglot
import re


BLOCKED_KEYWORDS = {"DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE", "EXEC"}


def _is_single_row_aggregate(sql: str) -> bool:
    sql_upper = sql.upper()
    has_agg = bool(re.search(r"\b(SUM|COUNT|AVG|MIN|MAX)\s*\(", sql_upper))
    has_group_by = "GROUP BY" in sql_upper
    has_over = " OVER " in sql_upper
    return has_agg and (not has_group_by) and (not has_over)


def validate_sql_node(state: dict) -> dict:
    """Validate SQL: syntax, security (no DDL/DML), and enforce LIMIT."""
    sql = state.get("generated_sql", "").strip()

    if not sql:
        state["validation_result"] = "FAIL"
        state["error_message"] = "Empty SQL generated"
        return state

    # 1. Security: block DDL/DML
    parsed = sqlparse.parse(sql)
    for stmt in parsed:
        stmt_type = stmt.get_type()
        if stmt_type and stmt_type.upper() in ("DDL", "DML"):
            state["validation_result"] = "FAIL"
            state["error_message"] = f"Blocked: {stmt_type} statements not allowed"
            return state

        tokens_upper = {t.ttype and t.value.upper() for t in stmt.flatten()}
        blocked_found = BLOCKED_KEYWORDS.intersection(
            t.value.upper() for t in stmt.flatten() if t.ttype is sqlparse.tokens.Keyword
            or t.ttype is sqlparse.tokens.Keyword.DDL
            or t.ttype is sqlparse.tokens.Keyword.DML
        )
        if blocked_found:
            state["validation_result"] = "FAIL"
            state["error_message"] = f"Blocked keywords: {blocked_found}"
            return state

    # 2. Syntax check via sqlglot (engine-aware dialect)
    try:
        db_engine = os.getenv("DB_ENGINE", "duckdb").lower()
        dialect = "trino" if db_engine == "trino" else "duckdb"
        sqlglot.transpile(sql, read=dialect, write=dialect)
    except sqlglot.errors.ParseError as e:
        state["validation_result"] = "FAIL"
        state["error_message"] = f"SQL syntax error: {str(e)[:200]}"
        return state

    # 3. Enforce LIMIT
    max_rows = int(os.getenv("MAX_ROWS", "1000"))
    sql_upper = sql.upper()
    if "LIMIT" not in sql_upper and not _is_single_row_aggregate(sql):
        sql = sql.rstrip(";") + f"\nLIMIT {max_rows};"
        state["generated_sql"] = sql

    state["validation_result"] = "PASS"
    state["error_message"] = ""
    return state
