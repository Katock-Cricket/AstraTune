"""批跑记录处理纯函数（自 scripts/run_bird_critic.py 迁出，便于单测与复用）。

依赖：仅 json/copy/pathlib（无 DB / LLM 依赖）。
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

_MAX_DIAG_CHARS = 4000
_MAX_TIMING_JSON = 4000


def load_done_ids(
    output_path: str, *, rerun_nonok: bool = False
) -> Dict[Any, Dict[str, Any]]:
    """读取已完成的 instance_id→rec。

    - 默认（resume）：inference_ok 或 有 pred 均视为完成。
    - rerun_nonok=True：仅 inference_ok 视为完成（fallback/失败/空 pred 可重跑）。
    """
    path = Path(output_path)
    if not path.is_file():
        return {}
    done: Dict[Any, Dict[str, Any]] = {}
    for rec in _load_jsonl(path):
        iid = rec.get("instance_id")
        if iid is None:
            continue
        if rerun_nonok:
            if is_inference_success(rec):
                done[iid] = rec
        elif is_inference_success(rec) or has_pred_sqls(rec.get("pred_sqls")):
            done[iid] = rec
    return done


def load_all_pred_records(output_path: str) -> Dict[Any, Dict[str, Any]]:
    """输出文件中每个 instance_id 的最后一条（含失败/未完成）。"""
    path = Path(output_path)
    if not path.is_file():
        return {}
    by_id: Dict[Any, Dict[str, Any]] = {}
    for rec in _load_jsonl(path):
        iid = rec.get("instance_id")
        if iid is not None:
            by_id[iid] = rec
    return by_id


def has_pred_sqls(pred: Any) -> bool:
    if isinstance(pred, list):
        return any(str(x).strip() for x in pred)
    return bool(str(pred or "").strip())


def is_inference_success(rec: Dict[str, Any]) -> bool:
    """推理成功：Verify 通过、无 fallback/error、pred_sqls 非空。不含 issue_sql 硬填。"""
    if rec.get("error"):
        return False
    meta = rec.get("optimization_meta") or {}
    if meta.get("fallback"):
        return False
    if not meta.get("success"):
        return False
    return has_pred_sqls(rec.get("pred_sqls"))


def slim_output_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """压缩 pred 行体积，避免 JSONL/日志拖死磁盘与终端。"""
    out = copy.deepcopy(rec)
    for key in ("original_schema", "preprocess_schema"):
        out.pop(key, None)
    dr = out.get("diagnosis_report")
    if isinstance(dr, str) and len(dr) > _MAX_DIAG_CHARS:
        out["diagnosis_report"] = dr[:_MAX_DIAG_CHARS] + "…[truncated]"
    ts = out.get("timing_summary")
    if ts is not None:
        raw = json.dumps(ts, ensure_ascii=False)
        if len(raw) > _MAX_TIMING_JSON:
            stages = (ts or {}).get("stages") or []
            out["timing_summary"] = {
                "truncated": True,
                "stages_count": len(stages),
            }
    return out


def finalize_inference_record(out: Dict[str, Any]) -> Dict[str, Any]:
    """有可用 pred 时不清空；hard_fail 仅限真空候选/异常。"""
    meta = out.setdefault("optimization_meta", {})
    nonempty = has_pred_sqls(out.get("pred_sqls"))
    if nonempty:
        meta["hard_fail"] = False
        return out
    meta["success"] = False
    meta["hard_fail"] = True
    out["pred_sqls"] = []
    if not out.get("error"):
        reasons = meta.get("failure_reasons") or []
        out["error"] = reasons[0] if reasons else "推理未通过 Verify 且无可用候选"
    return out


def append_jsonl(record: Dict[str, Any], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    slim = slim_output_record(record)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(slim, ensure_ascii=False) + "\n")


def fallback_row(
    rec: Dict[str, Any],
    err: str,
    *,
    mode: str,
    prior_pred: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """子进程超时/异常时的兜底行（原 _timeout_row，更名副其实）。

    prior_pred 有可用 pred 则继承（保留 hard_fail=False 的 fallback 语义）；
    否则 hard_fail=True。
    """
    out = copy.deepcopy(rec)
    out.pop("sol_sql", None)
    out.pop("test_cases", None)
    out["error"] = err
    if prior_pred and has_pred_sqls(prior_pred.get("pred_sqls")):
        out["pred_sqls"] = list(prior_pred.get("pred_sqls") or [])
        prev_dr = prior_pred.get("diagnosis_report")
        if prev_dr and str(prev_dr).strip():
            out["diagnosis_report"] = str(prev_dr)
        prev_meta = prior_pred.get("optimization_meta") or {}
        out["optimization_meta"] = {
            "success": False,
            "rounds": int(prev_meta.get("rounds") or 0),
            "fallback": True,
            "hard_fail": False,
            "mode": mode,
            "failure_reasons": [err],
        }
    else:
        out["pred_sqls"] = []
        out["optimization_meta"] = {
            "success": False,
            "rounds": 0,
            "fallback": False,
            "hard_fail": True,
            "mode": mode,
            "failure_reasons": [err],
        }
    return finalize_inference_record(out)


# 向后兼容别名（_timeout_row 在 run_bird_critic 内部仍用此名）
_timeout_row = fallback_row


def parse_worker_stdout(stdout: str) -> Optional[Dict[str, Any]]:
    """从 worker 子进程 stdout 中取最后一行可解析 JSON。"""
    for ln in reversed((stdout or "").splitlines()):
        line = ln.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


# 向后兼容别名
_parse_worker_stdout = parse_worker_stdout


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(rows)
    success = 0
    fallback = 0
    errors = 0
    hard_fail = 0
    inference_ok = 0
    empty_pred = 0
    elapsed = []
    for r in rows:
        meta = r.get("optimization_meta") or {}
        if meta.get("success"):
            success += 1
        if meta.get("fallback"):
            fallback += 1
        if meta.get("hard_fail"):
            hard_fail += 1
        if r.get("error"):
            errors += 1
        if is_inference_success(r):
            inference_ok += 1
        if r.get("elapsed_ms") is not None:
            elapsed.append(float(r["elapsed_ms"]))
        if not has_pred_sqls(r.get("pred_sqls")):
            empty_pred += 1
    elapsed_sorted = sorted(elapsed)
    p50 = elapsed_sorted[len(elapsed_sorted) // 2] if elapsed_sorted else None
    return {
        "total": n,
        "success": success,
        "inference_ok": inference_ok,
        "fallback": fallback,
        "hard_fail": hard_fail,
        "empty_pred": empty_pred,
        "with_error": errors,
        "elapsed_ms_mean": round(sum(elapsed) / len(elapsed), 1) if elapsed else None,
        "elapsed_ms_p50": p50,
    }


# 向后兼容别名
_summarize = summarize


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """读 JSONL（容忍空行/坏行）。"""
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if isinstance(rec, dict):
                    out.append(rec)
            except json.JSONDecodeError:
                continue
    return out
