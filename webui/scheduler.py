"""有界子进程调度：拉 pending、分配槽位、超时强杀、回收结果。"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from utils.config_loader import load_all_configs
from webui.overlay import merge_runtime_configs
from webui.task_state import (
    STATUS_RUNNING,
    apply_cancelled,
    apply_completed,
    apply_failed,
    apply_running,
    how_many_to_launch,
    pick_free_slot,
    select_pending,
    used_slots,
)
from webui.store import DATA_DIR, Store

ROOT = Path(__file__).resolve().parent.parent


def kill_process_tree(pid: int) -> None:
    if not pid:
        return
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return
    try:
        os.killpg(pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass


def _popen_kwargs() -> dict:
    if sys.platform == "win32":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


class Scheduler:
    def __init__(self, store: Store):
        self.store = store
        self._lock = threading.Lock()
        self._procs: Dict[str, subprocess.Popen] = {}
        self._started_at: Dict[str, float] = {}
        self._event_off: Dict[str, int] = {}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="webui-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            pids = list(self._procs.items())
        for tid, proc in pids:
            kill_process_tree(proc.pid)
        if self._thread:
            self._thread.join(timeout=3)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                pass
            self._stop.wait(1.0)

    def _timeout_s(self, configs: Dict[str, Any]) -> float:
        batch = (configs.get("agents") or {}).get("batch") or {}
        try:
            return float(batch.get("instance_timeout_s") or 2400)
        except (TypeError, ValueError):
            return 2400.0

    def _run_dir(self, task_id: str) -> Path:
        d = DATA_DIR / "runs" / task_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _ingest_events(self, task_id: str) -> None:
        path = self._run_dir(task_id) / "events.jsonl"
        if not path.is_file():
            return
        off = self._event_off.get(task_id, 0)
        raw = path.read_text(encoding="utf-8")
        lines = raw.splitlines()
        for line in lines[off:]:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            self.store.add_event(task_id, str(ev.get("name") or "event"), ev.get("payload") or {})
        self._event_off[task_id] = len(lines)

    def _launch(self, task: Dict[str, Any], slot: int, workers: int) -> None:
        tid = task["id"]
        conns = self.store.get_targets()
        conn = next((c for c in conns if c.get("id") == task.get("connection_id")), None)
        base = load_all_configs()
        merged = merge_runtime_configs(
            base,
            agents_overlay=self.store.get_agents_overlay(),
            sandbox_overlay=self.store.get_sandbox_overlay(),
            connection=conn,
            db_id=task.get("db_id"),
        )
        run_dir = self._run_dir(tid)
        payload_path = run_dir / "payload.json"
        events_path = run_dir / "events.jsonl"
        result_path = run_dir / "result.json"
        payload = {
            "task": task,
            "configs": merged,
            "agents_for_env": self.store.get_agents_overlay(),
            "slot_index": slot,
            "workers": workers,
            "events_path": str(events_path),
            "result_path": str(result_path),
        }
        payload_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        proc = subprocess.Popen(
            [sys.executable, "-m", "webui.worker", str(payload_path)],
            cwd=str(ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            **_popen_kwargs(),
        )
        self._procs[tid] = proc
        self._started_at[tid] = time.time()
        self._event_off[tid] = 0
        patched = apply_running(task, pid=proc.pid, slot_index=slot)
        self.store.update_task(tid, patched)
        self.store.add_event(tid, "queued", {"slot_index": slot})

    def _finish(self, task_id: str, proc: subprocess.Popen) -> None:
        self._ingest_events(task_id)
        task = self.store.get_task(task_id)
        if not task:
            return
        result_path = self._run_dir(task_id) / "result.json"
        data: Dict[str, Any] = {}
        if result_path.is_file():
            try:
                data = json.loads(result_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                data = {}
        pred = list(data.get("pred_sqls") or [])
        err = data.get("error")
        elapsed = data.get("elapsed_ms")
        code = proc.returncode
        if task.get("status") == "cancelled":
            return
        if pred:
            patched = apply_completed(
                task,
                pred_sqls=pred,
                diagnosis_report=str(data.get("diagnosis_report") or ""),
                elapsed_ms=elapsed,
                meta=data.get("meta"),
            )
            if err:
                patched["error"] = err
            self.store.update_task(task_id, patched)
            return
        reason = err or (f"子进程退出码 {code}" if code else "无 pred_sqls")
        if code is not None and code < 0:
            reason = "超时或被终止"
        patched = apply_failed(task, error=str(reason), elapsed_ms=elapsed, pred_sqls=pred)
        self.store.update_task(task_id, patched)

    def cancel(self, task_id: str) -> Optional[Dict[str, Any]]:
        task = self.store.get_task(task_id)
        if not task:
            return None
        with self._lock:
            proc = self._procs.pop(task_id, None)
        if proc and proc.poll() is None:
            kill_process_tree(proc.pid)
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
        patched = apply_cancelled(task)
        self.store.update_task(task_id, patched)
        self.store.add_event(task_id, "cancelled", {})
        self._started_at.pop(task_id, None)
        return self.store.get_task(task_id)

    def tick(self) -> None:
        with self._lock:
            workers = self.store.get_workers()
            tasks = self.store.list_tasks()
            running = [t for t in tasks if t.get("status") == STATUS_RUNNING]
            # 回收
            done_ids = []
            for t in running:
                tid = t["id"]
                proc = self._procs.get(tid)
                if proc is None:
                    # 进程丢失
                    self.store.update_task(
                        tid, apply_failed(t, error="调度器丢失子进程")
                    )
                    continue
                self._ingest_events(tid)
                timeout = self._timeout_s(load_all_configs())
                started = self._started_at.get(tid) or 0
                if proc.poll() is None and started and (time.time() - started) > timeout:
                    kill_process_tree(proc.pid)
                    try:
                        proc.wait(timeout=5)
                    except Exception:
                        pass
                    self.store.update_task(tid, apply_failed(t, error="实例超时"))
                    self.store.add_event(tid, "failed", {"error": "实例超时"})
                    done_ids.append(tid)
                    continue
                if proc.poll() is not None:
                    self._finish(tid, proc)
                    done_ids.append(tid)
            for tid in done_ids:
                self._procs.pop(tid, None)
                self._started_at.pop(tid, None)

            tasks = self.store.list_tasks()
            running = [t for t in tasks if t.get("status") == STATUS_RUNNING]
            n = how_many_to_launch(len(running), workers)
            pending = select_pending(tasks, n)
            used = used_slots(running)
            for t in pending:
                slot = pick_free_slot(used, workers)
                if slot is None:
                    break
                used.add(slot)
                self._launch(t, slot, workers)
