"""WebUI SQLite：overlay + 任务 + 阶段事件。不写 config/*.json。"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.config_loader import load_all_configs
from webui.overlay import clamp_workers, seed_targets_from_target_db
from webui.task_state import STATUS_PENDING

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "webui"
DEFAULT_DB = DATA_DIR / "webui.sqlite"

_LOCK = threading.RLock()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


class Store:
    def __init__(self, db_path: Optional[Path] = None):
        self.path = Path(db_path) if db_path else DEFAULT_DB
        self._conn = _connect(self.path)
        self._init_schema()
        self._ensure_seed()

    def close(self) -> None:
        with _LOCK:
            self._conn.close()

    def _init_schema(self) -> None:
        with _LOCK:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    instance_id TEXT,
                    connection_id TEXT,
                    db_id TEXT,
                    dialect TEXT,
                    issue_sql TEXT,
                    preprocess_sql TEXT,
                    clean_up_sql TEXT,
                    query TEXT,
                    category TEXT,
                    is_slow_sql INTEGER,
                    function TEXT,
                    mode TEXT,
                    status TEXT,
                    pred_sqls TEXT,
                    diagnosis_report TEXT,
                    error TEXT,
                    elapsed_ms REAL,
                    pid INTEGER,
                    slot_index INTEGER,
                    original_schema TEXT,
                    optimization_meta TEXT,
                    queued INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    name TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
                CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, id);
                """
            )
            cols = {r[1] for r in self._conn.execute("PRAGMA table_info(tasks)").fetchall()}
            if "queued" not in cols:
                self._conn.execute("ALTER TABLE tasks ADD COLUMN queued INTEGER DEFAULT 0")
            self._conn.commit()

    def _kv_get(self, key: str, default: Any = None) -> Any:
        row = self._conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        if not row:
            return default
        return json.loads(row["value"])

    def _kv_set(self, key: str, value: Any) -> None:
        self._conn.execute(
            "INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, ensure_ascii=False)),
        )

    def _ensure_seed(self) -> None:
        with _LOCK:
            if self._kv_get("targets") is None:
                base = load_all_configs()
                self._kv_set("targets", seed_targets_from_target_db(base.get("target_db") or {}))
            if self._kv_get("sandbox_overlay") is None:
                self._kv_set("sandbox_overlay", {})
            if self._kv_get("agents_overlay") is None:
                self._kv_set("agents_overlay", {})
            if self._kv_get("workers") is None:
                self._kv_set("workers", 2)
            self._conn.commit()

    # ---- overlay ----
    def get_targets(self) -> List[Dict[str, Any]]:
        with _LOCK:
            return list(self._kv_get("targets") or [])

    def set_targets(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        with _LOCK:
            self._kv_set("targets", rows)
            self._conn.commit()
            return rows

    def get_sandbox_overlay(self) -> Dict[str, Any]:
        with _LOCK:
            return dict(self._kv_get("sandbox_overlay") or {})

    def set_sandbox_overlay(self, overlay: Dict[str, Any]) -> Dict[str, Any]:
        with _LOCK:
            self._kv_set("sandbox_overlay", overlay or {})
            self._conn.commit()
            return self.get_sandbox_overlay()

    def get_agents_overlay(self) -> Dict[str, Any]:
        with _LOCK:
            return dict(self._kv_get("agents_overlay") or {})

    def set_agents_overlay(self, overlay: Dict[str, Any]) -> Dict[str, Any]:
        with _LOCK:
            self._kv_set("agents_overlay", overlay or {})
            self._conn.commit()
            return self.get_agents_overlay()

    def get_workers(self) -> int:
        with _LOCK:
            return clamp_workers(self._kv_get("workers", 2))

    def set_workers(self, n: Any) -> int:
        v = clamp_workers(n)
        with _LOCK:
            self._kv_set("workers", v)
            self._conn.commit()
        return v

    # ---- tasks ----
    def insert_task(self, draft: Dict[str, Any]) -> Dict[str, Any]:
        tid = str(uuid.uuid4())
        now = _now()
        instance_id = draft.get("instance_id") or tid[:8]
        row = {
            "id": tid,
            "created_at": now,
            "updated_at": now,
            "instance_id": str(instance_id),
            "connection_id": draft.get("connection_id"),
            "db_id": draft.get("db_id"),
            "dialect": draft.get("dialect") or "postgresql",
            "issue_sql": draft.get("issue_sql") or [],
            "preprocess_sql": draft.get("preprocess_sql") or [],
            "clean_up_sql": draft.get("clean_up_sql") or [],
            "query": draft.get("query") or "",
            "category": draft.get("category") or "Query",
            "is_slow_sql": bool(draft.get("is_slow_sql")),
            "function": draft.get("function") or "general",
            "mode": draft.get("mode") or "diag+opt",
            "status": STATUS_PENDING,
            "pred_sqls": [],
            "diagnosis_report": "",
            "error": None,
            "elapsed_ms": None,
            "pid": None,
            "slot_index": None,
            "original_schema": draft.get("original_schema") or "",
            "optimization_meta": None,
            "queued": False,
        }
        with _LOCK:
            self._conn.execute(
                """
                INSERT INTO tasks(
                    id, created_at, updated_at, instance_id, connection_id, db_id, dialect,
                    issue_sql, preprocess_sql, clean_up_sql, query, category, is_slow_sql,
                    function, mode, status, pred_sqls, diagnosis_report, error, elapsed_ms,
                    pid, slot_index, original_schema, optimization_meta, queued
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                self._task_to_params(row),
            )
            self._conn.commit()
        return row

    def _task_to_params(self, row: Dict[str, Any]) -> tuple:
        return (
            row["id"],
            row["created_at"],
            row["updated_at"],
            row.get("instance_id"),
            row.get("connection_id"),
            row.get("db_id"),
            row.get("dialect"),
            json.dumps(row.get("issue_sql") or [], ensure_ascii=False),
            json.dumps(row.get("preprocess_sql") or [], ensure_ascii=False),
            json.dumps(row.get("clean_up_sql") or [], ensure_ascii=False),
            row.get("query") or "",
            row.get("category"),
            1 if row.get("is_slow_sql") else 0,
            row.get("function"),
            row.get("mode"),
            row.get("status"),
            json.dumps(row.get("pred_sqls") or [], ensure_ascii=False),
            row.get("diagnosis_report") or "",
            row.get("error"),
            row.get("elapsed_ms"),
            row.get("pid"),
            row.get("slot_index"),
            row.get("original_schema") or "",
            json.dumps(row.get("optimization_meta"), ensure_ascii=False)
            if row.get("optimization_meta") is not None
            else None,
            1 if row.get("queued") else 0,
        )

    def _row_to_task(self, r: sqlite3.Row) -> Dict[str, Any]:
        def _lst(raw: Any) -> list:
            if not raw:
                return []
            if isinstance(raw, list):
                return raw
            try:
                v = json.loads(raw)
                return v if isinstance(v, list) else []
            except (TypeError, json.JSONDecodeError):
                return []

        meta = r["optimization_meta"]
        try:
            meta_obj = json.loads(meta) if meta else None
        except (TypeError, json.JSONDecodeError):
            meta_obj = None
        return {
            "id": r["id"],
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
            "instance_id": r["instance_id"],
            "connection_id": r["connection_id"],
            "db_id": r["db_id"],
            "dialect": r["dialect"],
            "issue_sql": _lst(r["issue_sql"]),
            "preprocess_sql": _lst(r["preprocess_sql"]),
            "clean_up_sql": _lst(r["clean_up_sql"]),
            "query": r["query"] or "",
            "category": r["category"],
            "is_slow_sql": bool(r["is_slow_sql"]),
            "function": r["function"],
            "mode": r["mode"],
            "status": r["status"],
            "pred_sqls": _lst(r["pred_sqls"]),
            "diagnosis_report": r["diagnosis_report"] or "",
            "error": r["error"],
            "elapsed_ms": r["elapsed_ms"],
            "pid": r["pid"],
            "slot_index": r["slot_index"],
            "original_schema": r["original_schema"] or "",
            "optimization_meta": meta_obj,
            "queued": bool(r["queued"]) if "queued" in r.keys() else False,
        }

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        with _LOCK:
            r = self._conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return self._row_to_task(r) if r else None

    def list_tasks(
        self,
        *,
        status: Optional[str] = None,
        is_slow_sql: Optional[bool] = None,
        dialect: Optional[str] = None,
        q: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM tasks WHERE 1=1"
        args: list = []
        if status:
            sql += " AND status=?"
            args.append(status)
        if is_slow_sql is not None:
            sql += " AND is_slow_sql=?"
            args.append(1 if is_slow_sql else 0)
        if dialect:
            sql += " AND dialect=?"
            args.append(dialect)
        if q:
            like = f"%{q}%"
            sql += " AND (instance_id LIKE ? OR db_id LIKE ? OR query LIKE ? OR id LIKE ?)"
            args.extend([like, like, like, like])
        sql += " ORDER BY created_at DESC"
        with _LOCK:
            rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_task(r) for r in rows]

    def update_task(self, task_id: str, patch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        cur = self.get_task(task_id)
        if not cur:
            return None
        cur.update(patch)
        cur["updated_at"] = _now()
        with _LOCK:
            self._conn.execute(
                """
                UPDATE tasks SET
                    updated_at=?, instance_id=?, connection_id=?, db_id=?, dialect=?,
                    issue_sql=?, preprocess_sql=?, clean_up_sql=?, query=?, category=?,
                    is_slow_sql=?, function=?, mode=?, status=?, pred_sqls=?,
                    diagnosis_report=?, error=?, elapsed_ms=?, pid=?, slot_index=?,
                    original_schema=?, optimization_meta=?, queued=?
                WHERE id=?
                """,
                self._task_to_params(cur)[2:] + (task_id,),
            )
            self._conn.commit()
        return self.get_task(task_id)

    def delete_task(self, task_id: str) -> bool:
        with _LOCK:
            cur = self._conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
            self._conn.execute("DELETE FROM events WHERE task_id=?", (task_id,))
            self._conn.commit()
            return cur.rowcount > 0

    def add_event(self, task_id: str, name: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        ts = _now()
        body = json.dumps(payload or {}, ensure_ascii=False)
        with _LOCK:
            self._conn.execute(
                "INSERT INTO events(task_id, ts, name, payload) VALUES (?,?,?,?)",
                (task_id, ts, name, body),
            )
            self._conn.commit()
            eid = self._conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        return {"id": eid, "task_id": task_id, "ts": ts, "name": name, "payload": payload or {}}

    def list_events(self, task_id: str, after_id: int = 0) -> List[Dict[str, Any]]:
        with _LOCK:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE task_id=? AND id>? ORDER BY id ASC",
                (task_id, after_id),
            ).fetchall()
        out = []
        for r in rows:
            try:
                payload = json.loads(r["payload"] or "{}")
            except json.JSONDecodeError:
                payload = {}
            out.append(
                {
                    "id": r["id"],
                    "task_id": r["task_id"],
                    "ts": r["ts"],
                    "name": r["name"],
                    "payload": payload,
                }
            )
        return out

    def clear_events(self, task_id: str) -> None:
        with _LOCK:
            self._conn.execute("DELETE FROM events WHERE task_id=?", (task_id,))
            self._conn.commit()
