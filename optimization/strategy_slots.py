"""Generate 候选 strategy_tag 分槽补齐（后处理，非 prompt 堆料）。"""

from __future__ import annotations

from dataclasses import replace
from typing import Dict, List, Sequence

from optimization.types import Candidate

# 每类目必需槽位（至少覆盖 rewrite + 第二槽）
_CATEGORY_SLOTS: Dict[str, List[str]] = {
    "query": ["rewrite", "mixed"],
    "personalization": ["rewrite", "represent_row"],
    "management": ["rewrite", "ddl"],
    "efficiency": ["set_equiv", "rewrite"],
}


def required_slots(category: str, efficiency: bool | None = None) -> List[str]:
    cat = (category or "").strip().lower()
    if efficiency is True or cat == "efficiency":
        return list(_CATEGORY_SLOTS["efficiency"])
    return list(_CATEGORY_SLOTS.get(cat, _CATEGORY_SLOTS["query"]))


def _normalize_tag(tag: str) -> str:
    t = (tag or "").strip().lower()
    aliases = {
        "index": "ddl",
        "param": "mixed",
        "ddl": "ddl",
        "mixed": "mixed",
        "rewrite": "rewrite",
        "represent_row": "represent_row",
        "set_equiv": "set_equiv",
    }
    return aliases.get(t, t or "rewrite")


def enforce_strategy_slots(
    candidates: Sequence[Candidate],
    *,
    category: str,
    efficiency: bool | None = None,
) -> List[Candidate]:
    """确保必需 strategy_tag 槽位存在；缺则复制首个候选并改 tag。"""
    if not candidates:
        return []
    out: List[Candidate] = [replace(c) for c in candidates]
    slots = required_slots(category, efficiency)
    present = {_normalize_tag(c.strategy_tag) for c in out}
    base = out[0]
    next_id = len(out) + 1
    for slot in slots:
        if slot in present:
            continue
        out.append(
            Candidate(
                id=f"c_slot_{next_id}",
                pred_sqls=list(base.pred_sqls),
                rationale=f"[slot-fill] strategy_tag={slot}",
                strategy_tag=slot,
                meta=dict(base.meta or {}),
            )
        )
        present.add(slot)
        next_id += 1
    # 规范化已有 tag
    for c in out:
        if c.strategy_tag:
            c.strategy_tag = _normalize_tag(c.strategy_tag)
    return out
