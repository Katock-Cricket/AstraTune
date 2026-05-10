"""
目标 PostgreSQL 只读风格查询 HTTP 接口，便于诊断流程或脚本替代直连 psql。
与 BIRD evaluation 中按 db_id 选库、执行 SQL 的用法对齐：请求体指定 database。
"""
import os
import re
import time
from decimal import Decimal
from datetime import date, datetime
from typing import Any, List, Optional
import uuid

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="AstraTune Target PG Query API", version="1.0.0")

PGHOST = os.environ.get("PGHOST", "127.0.0.1")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGUSER = os.environ.get("PGUSER", "root")
PGPASSWORD = os.environ.get("PGPASSWORD", "123123")
QUERY_API_KEY = os.environ.get("QUERY_API_KEY", "").strip()


class QueryBody(BaseModel):
    database: str = Field(..., description="PostgreSQL 库名，如 BIRD 的 db_id：financial")
    sql: str = Field(..., description="可含多条语句，分号分隔")
    fetch: bool = Field(True, description="是否对 SELECT/WITH/EXPLAIN/SHOW 取行")


def _json_safe(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    return obj


def _split_statements(sql: str) -> List[str]:
    return [s.strip() for s in sql.split(";") if s.strip()]


def _is_row_fetching(stmt: str) -> bool:
    s = re.sub(r"--[^\n]*", "", stmt).strip().upper()
    return s.startswith(("SELECT", "WITH", "SHOW", "EXPLAIN"))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/v1/databases")
def list_databases(x_api_key: Optional[str] = Header(None, alias="X-Api-Key")):
    if QUERY_API_KEY and x_api_key != QUERY_API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")
    try:
        conn = psycopg2.connect(
            host=PGHOST,
            port=PGPORT,
            user=PGUSER,
            password=PGPASSWORD,
            database="postgres",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
            )
            names = [r[0] for r in cur.fetchall()]
        conn.close()
        return {"databases": names}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e)) from e


@app.post("/v1/query")
def run_query(
    body: QueryBody,
    x_api_key: Optional[str] = Header(None, alias="X-Api-Key"),
):
    if QUERY_API_KEY and x_api_key != QUERY_API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")
    if not re.match(r"^[A-Za-z0-9_]+$", body.database):
        raise HTTPException(status_code=400, detail="invalid database name")
    statements = _split_statements(body.sql)
    if not statements:
        raise HTTPException(status_code=400, detail="empty sql")
    conn = None
    try:
        conn = psycopg2.connect(
            host=PGHOST,
            port=PGPORT,
            user=PGUSER,
            password=PGPASSWORD,
            database=body.database,
        )
        conn.autocommit = False
        results: List[dict[str, Any]] = []
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for stmt in statements:
                t0 = time.time()
                cur.execute(stmt)
                elapsed = time.time() - t0
                base = {
                    "sql": stmt,
                    "execution_time": round(elapsed, 4),
                    "execution_time_ms": round(elapsed * 1000, 2),
                }
                if body.fetch and _is_row_fetching(stmt):
                    rows = [dict(r) for r in cur.fetchall()]
                    base["rows"] = _json_safe(rows)
                    base["row_count"] = len(rows)
                else:
                    base["affected_rows"] = cur.rowcount
                    base["message"] = "ok"
                results.append(base)
            conn.commit()
        conn.close()
        return {"ok": True, "database": body.database, "results": results}
    except Exception as e:
        if conn is not None and not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        raise HTTPException(status_code=400, detail=str(e)) from e
