"""Pydantic 请求体。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class TargetConn(BaseModel):
    id: str
    name: str = ""
    dialect: str = "postgresql"
    host: str = "127.0.0.1"
    port: int = 5432
    user: str = ""
    password: str = ""
    note: str = ""
    is_default: bool = False


class TargetsPut(BaseModel):
    connections: List[TargetConn]


class TargetTestBody(BaseModel):
    db_id: Optional[str] = None


class SandboxPut(BaseModel):
    overlay: Dict[str, Any] = Field(default_factory=dict)


class AgentsPut(BaseModel):
    overlay: Dict[str, Any] = Field(default_factory=dict)
    workers: Optional[int] = None


class TaskCreate(BaseModel):
    connection_id: str
    db_id: str
    issue_sql: Any
    is_slow_sql: bool = False
    query: str = ""
    preprocess_sql: Any = None
    clean_up_sql: Any = None
    category: Optional[str] = None
    mode: Optional[str] = None
    dialect: Optional[str] = None
    instance_id: Optional[str] = None
    function: Optional[str] = None


class StartBody(BaseModel):
    ids: Optional[List[str]] = None


class ExportBody(BaseModel):
    ids: Optional[List[str]] = None
