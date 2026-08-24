"""结构化耗时日志：[timing] instance_id=... stage=... elapsed_ms=...

用 grep \"[timing]\" 过滤。线程局部上下文，4 路并行时 worker/slot 可分清。
不打印 SQL 全文。
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

from utils.logger import default_logger

_tls = threading.local()

_CTX_KEYS = (
    "instance_id",
    "db_id",
    "dialect",
    "worker",
    "slot",
)


def set_timing_context(**kwargs: Any) -> None:
    ctx = dict(getattr(_tls, "ctx", None) or {})
    for k, v in kwargs.items():
        if k in _CTX_KEYS:
            ctx[k] = v
    _tls.ctx = ctx


def clear_timing_context() -> None:
    _tls.ctx = {}
    _tls.events = []


def get_timing_context() -> Dict[str, Any]:
    return dict(getattr(_tls, "ctx", None) or {})


def _events() -> list:
    ev = getattr(_tls, "events", None)
    if ev is None:
        ev = []
        _tls.events = ev
    return ev


def log_timing(stage: str, elapsed_ms: float, **extra: Any) -> None:
    ctx = get_timing_context()
    rec = {
        "stage": str(stage),
        "elapsed_ms": float(elapsed_ms),
        "instance_id": ctx.get("instance_id"),
        "db_id": ctx.get("db_id"),
        "dialect": ctx.get("dialect"),
        "worker": ctx.get("worker"),
        "slot": ctx.get("slot"),
    }
    rec.update({k: v for k, v in extra.items() if v is not None})
    _events().append(rec)
    parts = [
        "[timing]",
        f"instance_id={ctx.get('instance_id') if ctx.get('instance_id') is not None else '-'}",
        f"db_id={ctx.get('db_id') if ctx.get('db_id') is not None else '-'}",
        f"dialect={ctx.get('dialect') if ctx.get('dialect') is not None else '-'}",
        f"worker={ctx.get('worker') if ctx.get('worker') is not None else '-'}",
        f"slot={ctx.get('slot') if ctx.get('slot') is not None else '-'}",
        f"stage={stage}",
        f"elapsed_ms={float(elapsed_ms):.1f}",
    ]
    for k, v in extra.items():
        if v is None:
            continue
        s = str(v).replace("\n", " ").replace("\r", " ")
        if len(s) > 80:
            s = s[:77] + "..."
        parts.append(f"{k}={s}")
    default_logger.info(" ".join(parts))


def get_timing_events() -> list:
    return list(_events())


def summarize_timing(events: Optional[list] = None) -> Dict[str, Any]:
    """按 stage 汇总 count/sum_ms。嵌套 stage 会重叠，占比以 infer_total/run_one_total 为分母。"""
    rows = events if events is not None else get_timing_events()
    by: Dict[str, Dict[str, Any]] = {}
    for rec in rows:
        st = str(rec.get("stage") or "-")
        slot = by.setdefault(st, {"stage": st, "count": 0, "sum_ms": 0.0})
        slot["count"] += 1
        slot["sum_ms"] += float(rec.get("elapsed_ms") or 0.0)
    denom = 0.0
    for key in ("infer_total", "run_one_total", "diagnosis_total", "optimization_total"):
        if key in by:
            denom = float(by[key]["sum_ms"])
            break
    out = []
    for st, slot in by.items():
        item = {
            "stage": st,
            "count": slot["count"],
            "sum_ms": round(slot["sum_ms"], 1),
        }
        if denom > 0:
            item["pct"] = round(100.0 * slot["sum_ms"] / denom, 1)
        out.append(item)
    out.sort(key=lambda x: -x["sum_ms"])
    return {"denom_ms": round(denom, 1), "stages": out}


def log_timing_summary() -> Dict[str, Any]:
    summary = summarize_timing()
    denom = summary.get("denom_ms") or 0.0
    parts = ["[timing-summary]", f"denom_ms={denom:.1f}"]
    # 顶层互斥阶段优先，避免嵌套刷屏
    prefer = (
        "extract_tables_llm",
        "target_connect",
        "sandbox_connect",
        "sandbox_slot_ensure",
        "schema_fetch",
        "sandbox_prepare",
        "diagnosis_total",
        "optimization_total",
        "generate",
        "verify_agent",
        "verify_agent_llm",
        "verify_agent_reset_probe",
        "verify_rules",
        "verify_exec",
        "sandbox_reset",
        "sandbox_clone_from_template",
        "sandbox_clone_from_target",
        "infer_total",
        "run_one_total",
        "slot_wait",
        "sandbox_template_lock_wait",
        "llm_pool_wait",
        "llm_retry",
        "llm_retry_success",
    )
    seen = set()
    for st in prefer:
        for item in summary.get("stages") or []:
            if item["stage"] == st:
                parts.append(
                    f"{st}={item['sum_ms']:.0f}ms/{item['count']}({item.get('pct', 0)}%)"
                )
                seen.add(st)
                break
    default_logger.info(" ".join(parts))
    return summary


@contextmanager
def timed_stage(stage: str, **extra: Any) -> Iterator[Dict[str, Any]]:
    """yield 一个可写入 extra 的 dict，finally 时打点。"""
    bag: Dict[str, Any] = dict(extra)
    t0 = time.perf_counter()
    try:
        yield bag
    finally:
        elapsed = (time.perf_counter() - t0) * 1000.0
        log_timing(stage, elapsed, **bag)


def sql_kind(sql: str) -> str:
    s = (sql or "").strip().lstrip("(")
    if not s:
        return "EMPTY"
    tok = s.split(None, 1)[0].upper().rstrip(";")
    return tok[:20]
