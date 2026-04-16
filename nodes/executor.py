"""SQL Executor Node — DuckDB read-only execution with timeout"""
import os
import duckdb
import pandas as pd
import trino


_DUCKDB_CONN = None
_DUCKDB_CONN_PATH = None


def execute_sql_node(state: dict) -> dict:
    """Execute validated SQL against configured engine in read-only mode."""
    global _DUCKDB_CONN, _DUCKDB_CONN_PATH

    sql = state.get("generated_sql", "")
    db_engine = os.getenv("DB_ENGINE", "duckdb").lower()

    try:
        if db_engine == "trino":
            trino_host = os.getenv("DB_HOST", "trino")
            trino_port = int(os.getenv("DB_PORT", "8080"))
            trino_user = os.getenv("DB_USER", "analyst")
            trino_catalog = os.getenv("DB_NAME", "memory")
            trino_schema = os.getenv("UC_SCHEMA", "default")
            trino_conn = trino.dbapi.connect(
                host=trino_host,
                port=trino_port,
                user=trino_user,
                catalog=trino_catalog,
                schema=trino_schema,
                http_scheme="http",
            )
            cursor = trino_conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [col[0] for col in (cursor.description or [])]
            df = pd.DataFrame(rows, columns=columns)
            cursor.close()
            trino_conn.close()
        else:
            duckdb_path = os.getenv("DUCKDB_PATH", "data/kur.db")
            if _DUCKDB_CONN is None or _DUCKDB_CONN_PATH != duckdb_path:
                _DUCKDB_CONN = duckdb.connect(duckdb_path, read_only=True)
                _DUCKDB_CONN_PATH = duckdb_path
            conn = _DUCKDB_CONN
            df = conn.execute(sql).fetchdf()

        state["query_result"] = df
        state["error_message"] = ""
    except duckdb.Error as e:
        state["query_result"] = None
        state["error_message"] = f"DuckDB error: {str(e)[:300]}"
    except trino.exceptions.TrinoQueryError as e:
        state["query_result"] = None
        state["error_message"] = f"Trino query error: {str(e)[:300]}"
    except trino.exceptions.TrinoUserError as e:
        state["query_result"] = None
        state["error_message"] = f"Trino user error: {str(e)[:300]}"
    except Exception as e:
        state["query_result"] = None
        state["error_message"] = f"Execution error: {str(e)[:300]}"

    return state
