"""Effi-SQL 题面 ↔ AstraTune 推理记录适配（禁止泄漏 optimized_sql / fast_time）。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

EFFI_QUERY = (
    "在保持结果语义与集合等价的前提下，优化下列 SQL 的执行效率"
    "（降低耗时 / QEP cost；勿改变结果含义）。"
)

_AGENT_FORBIDDEN_KEYS = frozenset(
    {
        "optimized_sql",
        "fast_time",
        "optimized_explain_analyze",
        "base_explain_analyze",
        "base_time",
    }
)


def instance_id_from_effi(raw_id: Any) -> str:
    return f"EffiSQL_{raw_id}"


def adapt_effi_record(row: Dict[str, Any]) -> Dict[str, Any]:
    """将 Effi-SQL JSONL 一行转为 AstraTune 推理用记录（不含 gold 优化 SQL）。"""
    if not isinstance(row, dict):
        raise TypeError("effi record must be a dict")
    raw_id = row.get("id")
    if raw_id is None:
        raise ValueError("effi record missing id")
    base_sql = row.get("base_sql")
    if not isinstance(base_sql, str) or not base_sql.strip():
        raise ValueError(f"effi id={raw_id}: missing base_sql")
    db = str(row.get("db") or "").strip().lower()
    if not db:
        raise ValueError(f"effi id={raw_id}: missing db")

    out: Dict[str, Any] = {
        "instance_id": instance_id_from_effi(raw_id),
        "effi_id": raw_id,
        "db_id": db,
        "db": db,
        "issue_sql": [base_sql.strip()],
        "base_sql": base_sql.strip(),
        "query": EFFI_QUERY,
        "category": "Efficiency",
        "efficiency": True,
        "function": "efficiency",
        "dialect": "postgresql",
        "benchmark": "effi",
    }
    if row.get("difficulty") is not None:
        out["difficulty"] = row.get("difficulty")
    return out


def pred_sqls_to_prediction(pred_sqls: Sequence[str] | None) -> str:
    """AstraTune pred_sqls → Diff-SQL end2end 的 prediction 字符串。"""
    parts = [str(x).strip().rstrip(";") for x in (pred_sqls or []) if str(x).strip()]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return ";\n".join(parts) + ";"


def attach_prediction(record: Dict[str, Any], pred_sqls: Sequence[str] | None) -> Dict[str, Any]:
    """在评测用记录上写入 prediction（保留 base_sql 等评测字段）。"""
    out = dict(record)
    out["prediction"] = pred_sqls_to_prediction(pred_sqls)
    out["pred_sqls"] = list(pred_sqls or [])
    return out


def strip_agent_forbidden(record: Dict[str, Any]) -> Dict[str, Any]:
    """移除不得喂给 Agent 的字段（防御性）。"""
    return {k: v for k, v in record.items() if k not in _AGENT_FORBIDDEN_KEYS}


def load_effi_by_id(
    path: Any,
    effi_id: int | str,
) -> Dict[str, Any]:
    """从 JSONL 按 id 加载并 adapt；找不到则抛 ValueError。"""
    import json
    from pathlib import Path

    p = Path(path)
    want = int(effi_id)
    with p.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if int(row.get("id", -1)) == want:
                return adapt_effi_record(row)
    raise ValueError(f"Effi-SQL id={want} not found in {p}")
