"""任务状态机与有界调度纯函数。进程管家见 webui.scheduler。"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Set

from webui.overlay import clamp_workers

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

DELETABLE = {STATUS_PENDING, STATUS_FAILED, STATUS_CANCELLED, STATUS_COMPLETED}
RERUNNABLE = {STATUS_PENDING, STATUS_FAILED, STATUS_CANCELLED, STATUS_COMPLETED}


def can_delete(status: str) -> bool:
    return str(status or "") in DELETABLE


def can_start(status: str) -> bool:
    return str(status or "") in RERUNNABLE and str(status) != STATUS_RUNNING


def prepare_start(task: Dict[str, Any]) -> Dict[str, Any]:
    """重跑：清空产物，回到 pending（由调度器再拉成 running）。"""
    out = dict(task)
    out["status"] = STATUS_PENDING
    out["pred_sqls"] = []
    out["diagnosis_report"] = ""
    out["error"] = None
    out["elapsed_ms"] = None
    out["pid"] = None
    out["slot_index"] = None
    out["optimization_meta"] = None
    out["queued"] = True
    return out


def apply_running(task: Dict[str, Any], *, pid: int, slot_index: int) -> Dict[str, Any]:
    out = dict(task)
    out["status"] = STATUS_RUNNING
    out["pid"] = int(pid)
    out["slot_index"] = int(slot_index)
    out["error"] = None
    return out


def apply_completed(task: Dict[str, Any], *, pred_sqls: List[str], diagnosis_report: str = "", elapsed_ms: Any = None, meta: Any = None) -> Dict[str, Any]:
    out = dict(task)
    out["status"] = STATUS_COMPLETED
    out["pred_sqls"] = list(pred_sqls or [])
    out["diagnosis_report"] = diagnosis_report or ""
    out["elapsed_ms"] = elapsed_ms
    out["pid"] = None
    out["optimization_meta"] = meta
    out["error"] = None
    out["queued"] = False
    return out


def apply_failed(task: Dict[str, Any], *, error: str, elapsed_ms: Any = None, pred_sqls: Optional[List[str]] = None) -> Dict[str, Any]:
    out = dict(task)
    preds = [str(x).strip() for x in (pred_sqls or []) if str(x).strip()]
    if preds:
        # 有 pred 仍标 completed（与批跑「有 pred 不清空」对齐：界面算修复完成）
        out["status"] = STATUS_COMPLETED
        out["pred_sqls"] = preds
        out["error"] = error
    else:
        out["status"] = STATUS_FAILED
        out["pred_sqls"] = []
        out["error"] = error
    out["elapsed_ms"] = elapsed_ms
    out["pid"] = None
    out["queued"] = False
    return out


def apply_cancelled(task: Dict[str, Any], *, error: str = "用户中断") -> Dict[str, Any]:
    out = dict(task)
    out["status"] = STATUS_CANCELLED
    out["error"] = error
    out["pid"] = None
    out["queued"] = False
    return out


def pick_free_slot(used: Set[int], workers: int) -> Optional[int]:
    n = clamp_workers(workers)
    for i in range(n):
        if i not in used:
            return i
    return None


def how_many_to_launch(running_count: int, workers: int) -> int:
    n = clamp_workers(workers)
    return max(0, n - int(running_count or 0))


def select_pending(tasks: Iterable[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []
    pending = [
        t
        for t in tasks
        if str(t.get("status")) == STATUS_PENDING and t.get("queued")
    ]
    pending.sort(key=lambda t: str(t.get("created_at") or t.get("id") or ""))
    return pending[:limit]


def used_slots(tasks: Iterable[Dict[str, Any]]) -> Set[int]:
    used: Set[int] = set()
    for t in tasks:
        if str(t.get("status")) != STATUS_RUNNING:
            continue
        slot = t.get("slot_index")
        if slot is None:
            continue
        try:
            used.add(int(slot))
        except (TypeError, ValueError):
            continue
    return used
