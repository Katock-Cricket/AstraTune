"""Efficiency 采样沙箱基数外推与 QEP 结构分辅助。"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence


def compute_scale_factor(fidelity: Optional[Dict[str, Any]]) -> Optional[float]:
    """从 sandbox fidelity 估算 target/sandbox 行数比（取采样表最大比）。"""
    if not fidelity or not isinstance(fidelity, dict):
        return None
    tables = fidelity.get("tables") or []
    if not isinstance(tables, list):
        return None
    max_scale = 1.0
    found = False
    for t in tables:
        if not isinstance(t, dict):
            continue
        mode = str(t.get("mode") or "").lower()
        if mode not in ("sample", "采样", "sampled"):
            continue
        target_n = t.get("target_rows") or t.get("source_rows")
        sandbox_n = t.get("rows") or t.get("sandbox_rows")
        try:
            target_n = float(target_n)
            sandbox_n = float(sandbox_n)
        except (TypeError, ValueError):
            continue
        if sandbox_n <= 0 or target_n <= 0:
            continue
        found = True
        max_scale = max(max_scale, target_n / sandbox_n)
    return max_scale if found else None


def estimate_full_timeout_risk(
    latency_ms: Optional[float],
    scale: Optional[float],
    exec_timeout_ms: int,
    *,
    safety_ratio: float = 0.85,
) -> Optional[str]:
    """采样下执行耗时外推全量是否可能超时。"""
    if latency_ms is None or scale is None or scale <= 1.0:
        return None
    projected = float(latency_ms) * float(scale)
    threshold = float(exec_timeout_ms) * safety_ratio
    if projected >= threshold:
        return (
            f"全量外推耗时约 {projected:.0f}ms（采样×{scale:.1f}）"
            f" ≥ 阈值 {threshold:.0f}ms，存在 Timeout 风险"
        )
    return None


def qep_structure_allows_cost_regression(
    pred_sqls: Sequence[str],
    issue_sql: Sequence[str],
) -> bool:
    """结构优化（NOT IN 合并 / recursive depth 上限）允许小幅 QEP 回退。"""
    pred_blob = "\n".join(str(s) for s in (pred_sqls or []))
    issue_blob = "\n".join(str(s) for s in (issue_sql or []))

    merged_notin = bool(
        re.search(r"NOT\s+IN\s*\([\s\S]*UNION\s+ALL", pred_blob, re.I)
        and pred_blob.lower().count("not in") < issue_blob.lower().count("not in")
    )
    depth_cap = bool(
        re.search(r"depth\s*<\s*\d+|depth\s*\+\s*1", pred_blob, re.I)
        and re.search(r"recursive", pred_blob, re.I)
    )
    return merged_notin or depth_cap
