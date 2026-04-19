from langchain_core.tools import tool
import os
import duckdb

@tool
def get_database_schema() -> str:
    """Returns the names of all tables and columns available in the database. Use this to discover table structure before writing SQL."""
    db_engine = os.getenv("DB_ENGINE", "duckdb").lower()
    if db_engine == "duckdb":
        db_path = os.getenv("DUCKDB_PATH", "data/kur.db")
        try:
            conn = duckdb.connect(db_path, read_only=True)
            res = "Schema Information:\n"
            tables = conn.execute("SHOW TABLES").fetchall()
            for (name,) in tables:
                cols = conn.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{name}'").fetchall()
                res += f"- Table: {name} (Columns: {', '.join([c[0]+' '+c[1] for c in cols])})\n"
            conn.close()
            return res
        except Exception as e:
            return f"Error retrieving schema: {e}"
    
    # Fallback to Polaris/Trino if configured
    return "Schema tool currently implemented purely for DuckDB in this POC."

@tool
def check_sql_syntax(sql_query: str) -> str:
    """Explains an execution plan or verifies syntax of a DuckDB SQL query without returning row data. Useful for checking if query is valid."""
    db_engine = os.getenv("DB_ENGINE", "duckdb").lower()
    if db_engine != "duckdb":
        return "Not supported for this engine."
        
    db_path = os.getenv("DUCKDB_PATH", "data/kur.db")
    try:
        conn = duckdb.connect(db_path, read_only=True)
        # Explain avoids running the actual large query
        plan = conn.execute(f"EXPLAIN {sql_query}").fetchall()
        conn.close()
        return f"Query is valid. Plan:\n{plan[0]}"
    except Exception as e:
        return f"Error / Invalid SQL: {str(e)}"
