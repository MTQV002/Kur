import sqlite3
import time
from datetime import datetime
from typing import Optional

from app.core.config import HISTORY_DB_PATH


def init_history_db():
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


def save_history(question: str, answer: str, intent: str, sql_text: Optional[str] = None):
    now = time.time()
    conn = sqlite3.connect(HISTORY_DB_PATH)
    try:
        cur = conn.execute("SELECT question, created_at FROM chat_history ORDER BY id DESC LIMIT 1")
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


def load_history(limit: int = 30):
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
        items.append(
            {
                "id": row["id"],
                "question": row["question"],
                "answer": row["answer"],
                "intent": row["intent"],
                "sql": row["sql_text"],
                "timestamp": datetime.fromtimestamp(row["created_at"]).isoformat(),
            }
        )
    return items


def clear_history():
    conn = sqlite3.connect(HISTORY_DB_PATH)
    try:
        conn.execute("DELETE FROM chat_history")
        conn.commit()
    finally:
        conn.close()
