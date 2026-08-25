"""从诊断结论确定性抽取 pred_sqls（消融 diag_only；不调 LLM）。"""

from __future__ import annotations

import json
import re
from typing import List, Optional, Tuple


_SQL_FENCE = re.compile(
    r"```(?:sql|postgresql|mysql)?\s*([\s\S]*?)```",
    re.IGNORECASE,
)
_PRED_LABEL = re.compile(
    r"(?:pred_sqls?|修复\s*SQL|优化\s*SQL|建议\s*SQL|改写\s*SQL)\s*[:：]\s*(.+)",
    re.IGNORECASE,
)
_JSON_PRED = re.compile(
    r"\{[^{}]*\"pred_sqls\"\s*:\s*\[[\s\S]*?\][^{}]*\}",
    re.IGNORECASE,
)


def _normalize_parts(block: str) -> List[str]:
    parts = [p.strip().rstrip(";") for p in re.split(r";\s*\n|;\s*$", block) if p.strip()]
    if not parts:
        s = block.strip().rstrip(";")
        return [s] if s else []
    return [p for p in parts if p and not p.startswith("--")]


def extract_pred_sqls_from_diagnosis(
    diagnosis_report: str,
    *,
    fallback_issue_sql: Optional[List[str]] = None,
) -> Tuple[List[str], bool]:
    """
    返回 (pred_sqls, used_fallback)。
    优先顺序：JSON {"pred_sqls":[...]} → Markdown SQL 围栏 → 「修复 SQL:」标签。
    """
    text = diagnosis_report or ""

    for m in _JSON_PRED.finditer(text):
        try:
            obj = json.loads(m.group())
        except json.JSONDecodeError:
            continue
        raw = obj.get("pred_sqls") if isinstance(obj, dict) else None
        if isinstance(raw, list):
            parts = [str(x).strip().rstrip(";") for x in raw if str(x).strip()]
            if parts:
                return parts, False
        if isinstance(raw, str) and raw.strip():
            return _normalize_parts(raw), False

    # 整段若是 JSON
    stripped = text.strip()
    if stripped.startswith("{") and "pred_sqls" in stripped:
        try:
            obj = json.loads(stripped)
            raw = obj.get("pred_sqls") if isinstance(obj, dict) else None
            if isinstance(raw, list):
                parts = [str(x).strip().rstrip(";") for x in raw if str(x).strip()]
                if parts:
                    return parts, False
        except json.JSONDecodeError:
            pass

    fences = _SQL_FENCE.findall(text)
    collected: List[str] = []
    for block in fences:
        collected.extend(_normalize_parts(block))
    if collected:
        return collected, False

    for m in _PRED_LABEL.finditer(text):
        chunk = m.group(1).strip()
        chunk = re.split(r"\n\s*\n|\n#{1,3}\s|\n【", chunk, maxsplit=1)[0].strip()
        chunk = chunk.strip("`").strip()
        if chunk:
            parts = _normalize_parts(chunk)
            if parts:
                return parts, False

    fb = [str(x).strip() for x in (fallback_issue_sql or []) if str(x).strip()]
    return fb, True
