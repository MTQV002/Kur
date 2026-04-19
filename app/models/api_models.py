from typing import Optional

from pydantic import BaseModel


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
