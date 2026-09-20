"""AstraTune WebUI FastAPI 装配。"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request

from utils.config_loader import load_all_configs, normalize_dialect
from webui.import_jsonl import apply_manual_create, iter_export_rows, parse_jsonl_text
from webui.overlay import deep_merge, match_connection
from webui.task_state import can_delete, can_start, prepare_start
from webui.scheduler import Scheduler
from webui.schemas import AgentsPut, ExportBody, SandboxPut, StartBody, TargetTestBody, TargetsPut, TaskCreate
from webui.store import Store

FRONTEND_DIST = Path(__file__).resolve().parent / "frontend" / "dist"
_store: Optional[Store] = None
_scheduler: Optional[Scheduler] = None


def get_store() -> Store:
    assert _store is not None
    return _store


def get_scheduler() -> Scheduler:
    assert _scheduler is not None
    return _scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _store, _scheduler
    _store = Store()
    _scheduler = Scheduler(_store)
    _scheduler.start()
    yield
    _scheduler.stop()
    _store.close()


app = FastAPI(title="AstraTune WebUI", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def http_exc_handler(_req: Request, exc: HTTPException):
    detail = exc.detail
    if isinstance(detail, dict) and "code" in detail:
        return JSONResponse(status_code=exc.status_code, content=detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": "error", "message": str(detail)},
    )


def _err(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"code": code, "message": message})


def _mask_agents(overlay: Dict[str, Any]) -> Dict[str, Any]:
    import copy

    out = copy.deepcopy(overlay or {})

    def _mask_llm(llm: Any) -> None:
        if not isinstance(llm, dict):
            return
        if llm.get("api_key"):
            llm["api_key_set"] = True
            llm["api_key"] = ""
        else:
            llm["api_key_set"] = False

    diag = out.get("diagnosis")
    if isinstance(diag, dict):
        _mask_llm(diag.get("llm"))
    opt = out.get("optimization")
    if isinstance(opt, dict):
        _mask_llm(opt.get("llm"))
        v = opt.get("verify")
        if isinstance(v, dict):
            _mask_llm(v.get("llm"))
    return out


def _effective_sandbox() -> Dict[str, Any]:
    base = load_all_configs().get("sandbox") or {}
    return deep_merge(base, get_store().get_sandbox_overlay())


def _effective_agents() -> Dict[str, Any]:
    base = load_all_configs().get("agents") or {}
    return deep_merge(base, get_store().get_agents_overlay())


@app.get("/api/config/targets")
def get_targets():
    return {"connections": get_store().get_targets()}


@app.put("/api/config/targets")
def put_targets(body: TargetsPut):
    rows = [c.model_dump() for c in body.connections]
    return {"connections": get_store().set_targets(rows)}


@app.post("/api/config/targets/{conn_id}/test")
def test_target(conn_id: str, body: TargetTestBody | None = None):
    body = body or TargetTestBody()
    conn = match_connection(get_store().get_targets(), connection_id=conn_id)
    if not conn:
        raise _err(404, "not_found", "连接不存在")
    cfg = {
        "dialect": conn.get("dialect") or "postgresql",
        "host": conn.get("host"),
        "port": conn.get("port"),
        "user": conn.get("user"),
        "password": conn.get("password"),
        "database": body.db_id or ("postgres" if str(conn.get("dialect")) != "mysql" else "mysql"),
    }
    return _test_sql_conn(cfg)


@app.get("/api/config/sandbox")
def get_sandbox():
    return {
        "base": load_all_configs().get("sandbox") or {},
        "overlay": get_store().get_sandbox_overlay(),
        "effective": _effective_sandbox(),
    }


@app.put("/api/config/sandbox")
def put_sandbox(body: SandboxPut):
    overlay = get_store().set_sandbox_overlay(body.overlay or {})
    return {
        "overlay": overlay,
        "effective": _effective_sandbox(),
    }


@app.post("/api/config/sandbox/test")
def test_sandbox(dialect: str = Query("postgresql")):
    try:
        d = normalize_dialect(dialect)
    except ValueError as e:
        raise _err(400, "bad_dialect", str(e)) from e
    sec = (_effective_sandbox().get(d) or {}) if isinstance(_effective_sandbox().get(d), dict) else {}
    cfg = dict(sec)
    cfg.setdefault("dialect", d)
    return _test_sql_conn(cfg)


@app.get("/api/config/agents")
def get_agents():
    return {
        "base": load_all_configs().get("agents") or {},
        "overlay": _mask_agents(get_store().get_agents_overlay()),
        "effective": _mask_agents(_effective_agents()),
        "workers": get_store().get_workers(),
    }


@app.put("/api/config/agents")
def put_agents(body: AgentsPut):
    incoming = dict(body.overlay or {})
    current = get_store().get_agents_overlay()

    def _keep_key(path: tuple) -> None:
        # 空 api_key 且 api_key_set 表示沿用旧值
        node_in = incoming
        node_cur = current
        for p in path[:-1]:
            node_in = (node_in or {}).get(p) if isinstance(node_in, dict) else None
            node_cur = (node_cur or {}).get(p) if isinstance(node_cur, dict) else None
        if not isinstance(node_in, dict):
            return
        if node_in.get("api_key"):
            return
        if isinstance(node_cur, dict) and node_cur.get("api_key"):
            node_in["api_key"] = node_cur["api_key"]

    _keep_key(("diagnosis", "llm"))
    _keep_key(("optimization", "llm"))
    _keep_key(("optimization", "verify", "llm"))
    overlay = get_store().set_agents_overlay(incoming)
    workers = get_store().get_workers()
    if body.workers is not None:
        workers = get_store().set_workers(body.workers)
    return {
        "overlay": _mask_agents(overlay),
        "workers": workers,
        "effective": _mask_agents(_effective_agents()),
    }


def _test_sql_conn(cfg: Dict[str, Any]) -> Dict[str, Any]:
    from sandbox.db_admin import create_executor

    ex = None
    try:
        ex = create_executor(cfg)
        ex.connect()
        return {"ok": True, "message": "连接成功"}
    except Exception as e:
        return {"ok": False, "message": str(e)}
    finally:
        if ex is not None:
            try:
                ex.close()
            except Exception:
                pass


@app.post("/api/tasks")
def create_task(body: TaskCreate):
    store = get_store()
    payload = body.model_dump()
    if payload.get("connection_id"):
        conn = match_connection(store.get_targets(), connection_id=payload["connection_id"])
        if conn:
            payload.setdefault("dialect", conn.get("dialect"))
    draft, err = apply_manual_create(payload, connections=store.get_targets())
    if err:
        raise _err(400, "bad_task", err)
    return store.insert_task(draft)


@app.post("/api/tasks/import")
async def import_tasks(file: UploadFile = File(...)):
    raw = (await file.read()).decode("utf-8", errors="replace")
    store = get_store()
    ok, failures = parse_jsonl_text(raw, connections=store.get_targets())
    created = [store.insert_task(d) for d in ok]
    return {"created": len(created), "failed": len(failures), "failures": failures, "tasks": created}


@app.get("/api/tasks")
def list_tasks(
    status: Optional[str] = None,
    is_slow_sql: Optional[bool] = None,
    dialect: Optional[str] = None,
    q: Optional[str] = None,
):
    items = get_store().list_tasks(status=status, is_slow_sql=is_slow_sql, dialect=dialect, q=q)
    conns = {c["id"]: c for c in get_store().get_targets()}
    for it in items:
        c = conns.get(it.get("connection_id") or "")
        it["connection_name"] = (c or {}).get("name") or it.get("connection_id")
    running = sum(1 for it in items if it.get("status") == "running")
    return {"items": items, "workers": get_store().get_workers(), "running": running}


@app.get("/api/tasks/{task_id}")
def get_task(task_id: str):
    t = get_store().get_task(task_id)
    if not t:
        raise _err(404, "not_found", "任务不存在")
    conns = {c["id"]: c for c in get_store().get_targets()}
    c = conns.get(t.get("connection_id") or "")
    t["connection_name"] = (c or {}).get("name") or t.get("connection_id")
    t["events"] = get_store().list_events(task_id)
    return t


@app.get("/api/tasks/{task_id}/events")
def task_events(task_id: str, after_id: int = 0):
    if not get_store().get_task(task_id):
        raise _err(404, "not_found", "任务不存在")
    return {"events": get_store().list_events(task_id, after_id=after_id)}


@app.post("/api/tasks/start")
def start_tasks(body: StartBody | None = None):
    body = body or StartBody()
    store = get_store()
    if body.ids:
        targets = [t for t in (store.get_task(i) for i in body.ids) if t]
    else:
        targets = (
            store.list_tasks(status="pending")
            + store.list_tasks(status="failed")
            + store.list_tasks(status="cancelled")
        )
    started = []
    skipped = []
    for t in targets:
        if not can_start(t["status"]):
            skipped.append({"id": t["id"], "reason": f"状态 {t['status']} 不可启动"})
            continue
        patched = prepare_start(t)
        store.update_task(t["id"], patched)
        store.clear_events(t["id"])
        store.add_event(t["id"], "queued", {})
        started.append(t["id"])
    return {"started": started, "skipped": skipped}


@app.post("/api/tasks/{task_id}/cancel")
def cancel_task(task_id: str):
    t = get_store().get_task(task_id)
    if not t:
        raise _err(404, "not_found", "任务不存在")
    if t.get("status") != "running":
        raise _err(409, "not_running", "仅执行中的任务可中断")
    out = get_scheduler().cancel(task_id)
    return out


@app.delete("/api/tasks/{task_id}")
def delete_task(task_id: str):
    t = get_store().get_task(task_id)
    if not t:
        raise _err(404, "not_found", "任务不存在")
    if not can_delete(t.get("status") or ""):
        raise _err(409, "running", "执行中的任务请先中断再删除")
    get_store().delete_task(task_id)
    return {"ok": True}


@app.post("/api/tasks/export")
def export_tasks(body: ExportBody | None = None):
    body = body or ExportBody()
    store = get_store()
    if body.ids:
        tasks = [store.get_task(i) for i in body.ids]
        tasks = [t for t in tasks if t]
    else:
        tasks = store.list_tasks()
    rows = iter_export_rows(tasks)
    text = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
    return Response(
        content=text.encode("utf-8"),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": "attachment; filename=astratune_pred.jsonl"},
    )


@app.get("/api/health")
def health():
    return {"ok": True}


def mount_frontend(application: FastAPI) -> None:
    dist = FRONTEND_DIST
    index = dist / "index.html"
    if not index.is_file():
        @application.get("/")
        def _frontend_missing():
            return JSONResponse(
                {
                    "code": "frontend_missing",
                    "message": "前端尚未构建。开发请启动 Vite（端口 5173），或 npm run build 后重启。",
                }
            )
        return

    assets = dist / "assets"
    if assets.is_dir():
        application.mount("/assets", StaticFiles(directory=str(assets)), name="assets")

    @application.get("/{full_path:path}")
    def spa(full_path: str):
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail={"code": "not_found", "message": "API 不存在"})
        file_path = dist / full_path
        if full_path and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(index)


if os.environ.get("AT_WEBUI_DEV") != "1":
    mount_frontend(app)
