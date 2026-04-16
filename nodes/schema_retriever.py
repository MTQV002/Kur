"""Schema Retriever Node — UC OSS + DuckDB fallback"""
import os
import time
import requests
import duckdb


_SCHEMA_CACHE = {
    "db_path": None,
    "schema": "",
    "ts": 0.0,
}
_SCHEMA_CACHE_TTL_SECONDS = 120


def retrieve_schema_node(state: dict) -> dict:
    """Build schema context with semantic metadata + physical execution schema."""
    db_engine = os.getenv("DB_ENGINE", "duckdb").lower()

    uc_url = os.getenv("UC_SERVER_URL", "http://uc-server:8080")
    catalog = os.getenv("UC_CATALOG", "kur_catalog")
    schema_name = os.getenv("UC_SCHEMA", "public")

    uc_context = ""

    try:
        resp = requests.get(
            f"{uc_url}/api/2.1/unity-catalog/tables",
            params={"catalog_name": catalog, "schema_name": schema_name},
            timeout=10,
        )
        resp.raise_for_status()
        tables = resp.json().get("tables", [])
        detailed_tables = []
        for table in tables[:30]:
            table_name = table.get("name")
            if not table_name:
                continue
            full_name = f"{catalog}.{schema_name}.{table_name}"
            try:
                d_resp = requests.get(
                    f"{uc_url}/api/2.1/unity-catalog/tables/{full_name}",
                    timeout=5,
                )
                d_resp.raise_for_status()
                detailed_tables.append(d_resp.json())
            except Exception:
                detailed_tables.append(table)
        if detailed_tables:
            tables = detailed_tables
        if tables:
            uc_context = _format_uc_tables(tables, catalog, schema_name)
    except Exception:
        uc_context = ""

    if db_engine == "duckdb":
        physical_context = _fallback_duckdb_schema()
        if uc_context:
            state["schema_context"] = (
                "EXECUTION ENGINE: DuckDB\n"
                "IMPORTANT: Generated SQL MUST use only physical table names listed below.\n\n"
                "[PHYSICAL TABLES - SOURCE OF TRUTH]\n"
                f"{physical_context}\n\n"
                "[UC SEMANTIC METADATA - FOR BUSINESS CONTEXT ONLY]\n"
                f"{uc_context}"
            )
        else:
            state["schema_context"] = physical_context
        return state

    if uc_context:
        state["schema_context"] = uc_context
        return state

    # Fallback: DuckDB information_schema
    state["schema_context"] = _fallback_duckdb_schema()
    return state


def _format_uc_tables(tables: list, catalog: str, schema: str) -> str:
    parts = []
    for table in tables:
        name = table.get("name", "unknown")
        comment = table.get("comment", "No description")
        columns = table.get("columns", [])
        col_lines = [
            f"    {c.get('name', '?')} {c.get('type_name', '?')}"
            f"{' -- ' + c['comment'] if c.get('comment') else ''}"
            for c in columns
        ]
        parts.append(
            f"TABLE: {name}\n  DESCRIPTION: {comment}\n  COLUMNS:\n" + "\n".join(col_lines)
        )
    return "\n\n".join(parts)


def _fallback_duckdb_schema() -> str:
    """Read schema directly from DuckDB."""
    db_path = os.getenv("DUCKDB_PATH", "data/kur.db")

    now = time.time()
    if (
        _SCHEMA_CACHE["schema"]
        and _SCHEMA_CACHE["db_path"] == db_path
        and now - _SCHEMA_CACHE["ts"] < _SCHEMA_CACHE_TTL_SECONDS
    ):
        return _SCHEMA_CACHE["schema"]

    try:
        conn = duckdb.connect(db_path, read_only=True)
        tables = conn.execute("SHOW TABLES").fetchall()
        parts = []
        for (tbl_name,) in tables:
            cols = conn.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{tbl_name}'
                ORDER BY ordinal_position
            """).fetchall()
            col_lines = [f"    {name} {dtype}" for name, dtype in cols]
            parts.append(f"TABLE: {tbl_name}\n  COLUMNS:\n" + "\n".join(col_lines))
        conn.close()
        schema_text = "\n\n".join(parts)
        _SCHEMA_CACHE["db_path"] = db_path
        _SCHEMA_CACHE["schema"] = schema_text
        _SCHEMA_CACHE["ts"] = now
        return schema_text
    except Exception as e:
        return f"[ERROR reading DuckDB schema: {e}]"
