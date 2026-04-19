import re
import time

from app.core.config import load_settings

SCHEMA_CACHE = {"key": None, "loaded_at": 0.0, "tables": {}}


def extract_limit_from_question(question: str, default: int = 5, max_limit: int = 200) -> int:
    q = (question or "").lower()
    m = re.search(r"(?:top|limit|lấy|hiển thị)?\s*(\d{1,4})\s*(?:bản ghi|dòng|rows?|record)?", q)
    if not m:
        return default
    try:
        value = int(m.group(1))
        return max(1, min(value, max_limit))
    except Exception:
        return default


def _schema_cache_key() -> tuple:
    s = load_settings()
    engine = (s.get("db_engine") or "duckdb").lower()
    return (
        engine,
        s.get("duckdb_path"),
        s.get("db_host"),
        int(s.get("db_port") or 5432),
        s.get("db_name"),
    )


def load_schema_snapshot(ttl_seconds: int = 60) -> dict[str, dict]:
    now = time.time()
    cache_key = _schema_cache_key()
    if (
        SCHEMA_CACHE.get("key") == cache_key
        and (now - float(SCHEMA_CACHE.get("loaded_at", 0))) < ttl_seconds
        and SCHEMA_CACHE.get("tables")
    ):
        return SCHEMA_CACHE["tables"]

    s = load_settings()
    engine = (s.get("db_engine") or "duckdb").lower()
    rows = []

    if engine == "duckdb":
        import duckdb

        conn = duckdb.connect(s.get("duckdb_path", "data/kur.db"), read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT table_schema, table_name, column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name, ordinal_position
                """
            ).fetchall()
        finally:
            conn.close()

    elif engine == "trino":
        import trino

        conn = trino.dbapi.connect(
            host=s.get("db_host", "trino"),
            port=int(s.get("db_port", 8080)),
            user=s.get("db_user", "analyst"),
            catalog=s.get("db_name", "memory"),
            schema="information_schema",
            http_scheme="http",
        )
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT table_schema, table_name, column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_schema NOT IN ('information_schema')
                ORDER BY table_schema, table_name, ordinal_position
                """
            )
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()

    elif engine == "postgres":
        import psycopg2

        conn = psycopg2.connect(
            host=s.get("db_host", "localhost"),
            port=int(s.get("db_port", 5432)),
            dbname=s.get("db_name", "postgres"),
            user=s.get("db_user", "postgres"),
            password=s.get("db_password", ""),
            connect_timeout=5,
        )
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT table_schema, table_name, column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name, ordinal_position
                """
            )
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()

    tables: dict[str, dict] = {}
    for schema_name, table_name, column_name, data_type, ordinal_position in rows:
        schema_name = (schema_name or "").strip()
        table_name = (table_name or "").strip()
        if not table_name:
            continue

        key = f"{schema_name}.{table_name}".lower() if schema_name else table_name.lower()
        if key not in tables:
            full_name = table_name
            if schema_name and schema_name.lower() not in {"main", "public"}:
                full_name = f"{schema_name}.{table_name}"
            tables[key] = {
                "schema": schema_name,
                "table": table_name,
                "full_name": full_name,
                "columns": [],
                "columns_lc": set(),
            }

        tables[key]["columns"].append((int(ordinal_position or 0), column_name, str(data_type or "")))
        tables[key]["columns_lc"].add((column_name or "").lower())

    for meta in tables.values():
        meta["columns"].sort(key=lambda item: item[0])
        meta["column_names"] = [name for _, name, _ in meta["columns"]]

    SCHEMA_CACHE["key"] = cache_key
    SCHEMA_CACHE["loaded_at"] = now
    SCHEMA_CACHE["tables"] = tables
    return tables


def choose_projection(columns: list[str], max_cols: int = 10) -> list[str]:
    preferred = [
        "id",
        "order_id",
        "customer_id",
        "user_id",
        "product_id",
        "quantity",
        "amount",
        "discount",
        "status",
        "created_at",
        "updated_at",
        "event_time",
        "session_start",
    ]
    lower_to_orig = {c.lower(): c for c in columns}
    chosen = []
    for name in preferred:
        if name in lower_to_orig and lower_to_orig[name] not in chosen:
            chosen.append(lower_to_orig[name])
        if len(chosen) >= max_cols:
            return chosen

    for col in columns:
        if col not in chosen:
            chosen.append(col)
        if len(chosen) >= max_cols:
            break
    return chosen


def choose_order_column(columns_lc: set[str]) -> str:
    for col in ["created_at", "updated_at", "event_time", "session_start", "paid_at", "order_date", "date"]:
        if col in columns_lc:
            return col
    return ""


def quick_sql_from_question(question: str) -> tuple[str, str]:
    q = (question or "").lower()
    n = extract_limit_from_question(question, default=5)
    try:
        tables = load_schema_snapshot()
    except Exception:
        return "", ""

    if not tables:
        return "", ""

    asks_latest = bool(re.search(r"mới nhất|gần đây|latest|recent", q))
    asks_rows = bool(re.search(r"bản ghi|dòng|rows?|record", q))
    if asks_latest and asks_rows:
        candidates = []
        for meta in tables.values():
            order_col = choose_order_column(meta["columns_lc"])
            if order_col:
                score = 0
                table_lc = meta["table"].lower()
                if any(token in q for token in ["đơn hàng", "order", "orders"]) and "order" in table_lc:
                    score += 3
                if any(token in q for token in ["khách hàng", "customer", "user"]) and ("customer" in table_lc or "user" in table_lc):
                    score += 3
                if "fact" in table_lc:
                    score += 1
                candidates.append((score, meta, order_col))

        if candidates:
            candidates.sort(key=lambda item: item[0], reverse=True)
            _, picked, order_col = candidates[0]
            projection = choose_projection(picked["column_names"], max_cols=10)
            sql = (
                f"SELECT {', '.join(projection)}\n"
                f"FROM {picked['full_name']}\n"
                f"ORDER BY {order_col} DESC\n"
                f"LIMIT {n};"
            )
            return sql, f"Đây là {n} bản ghi mới nhất từ bảng {picked['full_name']} (tự suy luận từ schema hiện tại)."

    asks_max_qty_record = bool(re.search(r"bản ghi.*số lượng.*lớn nhất|quantity.*(max|lớn nhất)|số lượng mua lớn nhất", q))
    asks_user_top_qty = bool(re.search(r"(user\s*id|customer\s*id|khách hàng).*(mua nhiều nhất|số lượng mua nhiều nhất|nhiều nhất)", q))

    qty_tables = [meta for meta in tables.values() if "quantity" in meta["columns_lc"]]
    if asks_max_qty_record and not asks_user_top_qty and qty_tables:
        picked = sorted(qty_tables, key=lambda m: ("fact" in m["table"].lower(), len(m["column_names"])), reverse=True)[0]
        projection = choose_projection(picked["column_names"], max_cols=8)
        sql = (
            f"SELECT {', '.join(projection)}\n"
            f"FROM {picked['full_name']}\n"
            "ORDER BY quantity DESC\n"
            "LIMIT 1;"
        )
        return sql, f"Đây là bản ghi có quantity lớn nhất trong {picked['full_name']} (dynamic schema)."

    if asks_user_top_qty:
        same_table = [
            meta
            for meta in qty_tables
            if ("customer_id" in meta["columns_lc"] or "user_id" in meta["columns_lc"])
        ]
        if same_table:
            picked = same_table[0]
            uid_col = "customer_id" if "customer_id" in picked["columns_lc"] else "user_id"
            sql = (
                f"SELECT {uid_col}, SUM(quantity) AS total_quantity\n"
                f"FROM {picked['full_name']}\n"
                f"GROUP BY {uid_col}\n"
                "ORDER BY total_quantity DESC\n"
                "LIMIT 1;"
            )
            return sql, f"Đây là {uid_col} mua nhiều nhất từ {picked['full_name']} (dynamic schema)."

        item_candidates = [meta for meta in qty_tables if "order_id" in meta["columns_lc"]]
        order_candidates = [meta for meta in tables.values() if "id" in meta["columns_lc"] and "customer_id" in meta["columns_lc"]]
        if item_candidates and order_candidates:
            items = item_candidates[0]
            orders = order_candidates[0]
            sql = (
                "SELECT o.customer_id, SUM(i.quantity) AS total_quantity\n"
                f"FROM {items['full_name']} i\n"
                f"JOIN {orders['full_name']} o ON i.order_id = o.id\n"
                "GROUP BY o.customer_id\n"
                "ORDER BY total_quantity DESC\n"
                "LIMIT 1;"
            )
            return sql, f"Đây là customer_id mua nhiều nhất (join động {items['full_name']} với {orders['full_name']})."

    return "", ""


def rewrite_select_star(sql: str) -> str:
    if not sql:
        return sql

    m = re.search(r"SELECT\s+\*\s+FROM\s+([a-zA-Z0-9_\.\"]+)", sql, flags=re.IGNORECASE)
    if not m:
        return sql

    raw_table = m.group(1).replace('"', '')
    table_key = raw_table.lower()
    short_key = table_key.split(".")[-1]

    try:
        tables = load_schema_snapshot()
    except Exception:
        return sql

    picked = tables.get(table_key)
    if not picked:
        for meta in tables.values():
            if meta["table"].lower() == short_key:
                picked = meta
                break
    if not picked:
        return sql

    projection = choose_projection(picked["column_names"], max_cols=10)
    if not projection:
        return sql

    replacement = f"SELECT {', '.join(projection)} FROM {raw_table}"
    return re.sub(r"SELECT\s+\*\s+FROM\s+[a-zA-Z0-9_\.\"]+", replacement, sql, flags=re.IGNORECASE, count=1)
