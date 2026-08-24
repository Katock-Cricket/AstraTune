"""从诊断结论确定性抽取 pred_sqls（消融 diag_only；不调 LLM）。"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple


_SQL_FENCE = re.compile(
    r"```(?:sql|postgresql|mysql)?\s*([\s\S]*?)```",
    re.IGNORECASE,
)
_PRED_LABEL = re.compile(
    r"(?:pred_sqls?|修复\s*SQL|优化\s*SQL|建议\s*SQL)\s*[:：]\s*(.+)",
    re.IGNORECASE,
)


def extract_pred_sqls_from_diagnosis(
    diagnosis_report: str,
    *,
    fallback_issue_sql: Optional[List[str]] = None,
) -> Tuple[List[str], bool]:
    """
    返回 (pred_sqls, used_fallback)。
    优先 Markdown SQL 围栏；其次「修复 SQL:」类标签后的语句。
    """
    text = diagnosis_report or ""
    fences = _SQL_FENCE.findall(text)
    collected: List[str] = []
    for block in fences:
        parts = [p.strip().rstrip(";") for p in re.split(r";\s*\n|;\s*$", block) if p.strip()]
        # 整块也可能是多语句用分号连接
        if not parts:
            s = block.strip().rstrip(";")
            if s:
                collected.append(s)
        else:
            for p in parts:
                if p and not p.startswith("--"):
                    collected.append(p)

    if collected:
        return collected, False

    for m in _PRED_LABEL.finditer(text):
        chunk = m.group(1).strip()
        # 截到下一空行或标题
        chunk = re.split(r"\n\s*\n|\n#{1,3}\s|\n【", chunk, maxsplit=1)[0].strip()
        chunk = chunk.strip("`").strip()
        if chunk:
            parts = [p.strip().rstrip(";") for p in chunk.split(";") if p.strip()]
            if parts:
                return parts, False

    fb = [str(x).strip() for x in (fallback_issue_sql or []) if str(x).strip()]
    return fb, True
