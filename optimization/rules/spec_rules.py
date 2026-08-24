"""Intent Spec 驱动的泛化 Verify 规则（非单题 overfit）。"""

from __future__ import annotations

import re
from typing import List, Optional, Sequence

from optimization.intent_spec import IntentSpec
from optimization.rules.base import Rule, RuleContext
from optimization.rules.category import (
    mgmt_trigger_incomplete,
    personalization_group_key_only,
)


def _spec(ctx: RuleContext) -> Optional[IntentSpec]:
    spec = getattr(ctx, "intent_spec", None)
    return spec if isinstance(spec, IntentSpec) else None


def spec_output_columns_missing(ctx: RuleContext) -> Optional[str]:
    """Spec 声明 output_columns 时，pred 投影应覆盖列名/别名。"""
    spec = _spec(ctx)
    if not spec or not spec.output_columns:
        return None
    blob = "\n".join(str(s) for s in (ctx.pred_sqls or []))
    if not blob.strip():
        return None
    missing: List[str] = []
    for col in spec.output_columns:
        name = (col.name or "").strip()
        if not name:
            continue
        pat = rf"\b{re.escape(name)}\b|AS\s+{re.escape(name)}\b"
        if not re.search(pat, blob, re.I):
            missing.append(name)
    if missing:
        return f"Intent Spec 输出契约：pred 未覆盖列 {missing[:5]}"
    return None


def spec_predicate_required(ctx: RuleContext) -> Optional[str]:
    """Spec 有 agg_filters / must_predicates 时，pred 须含条件表达式。"""
    spec = _spec(ctx)
    if not spec:
        return None
    if not spec.agg_filters and not spec.must_predicates:
        return None
    blob = "\n".join(str(s) for s in (ctx.pred_sqls or []))
    if re.search(
        r"\bFILTER\s*\(|COUNT\s*\(\s*CASE\s+WHEN|\bWHERE\b|\bHAVING\b",
        blob,
        re.I,
    ):
        return None
    return "Intent Spec：存在条件约束但 pred 缺少 WHERE/FILTER/CASE/HAVING"


def spec_unresolved_filter_risk(ctx: RuleContext) -> Optional[str]:
    """unresolved agg_filter 且无 evidence 列时，强 FILTER 视为高风险臆造。"""
    spec = _spec(ctx)
    if not spec or not spec.agg_filters:
        return None
    has_unresolved = any(f.unresolved for f in spec.agg_filters)
    if not has_unresolved:
        return None
    has_evidence_col = any(e.columns_seen for e in spec.evidence)
    blob = "\n".join(str(s) for s in (ctx.pred_sqls or []))
    if has_evidence_col:
        return None
    if re.search(r"FILTER\s*\(\s*WHERE", blob, re.I):
        return (
            "Intent Spec：agg_filter 未绑定 evidence 列，"
            "不应使用无依据的 FILTER 条件"
        )
    return None


def spec_pers_full_row(ctx: RuleContext) -> Optional[str]:
    """pers_need_full_row：代表行形态检查。"""
    spec = _spec(ctx)
    if not spec or not spec.pers_need_full_row:
        return None
    if ctx.category_lower != "personalization":
        return None
    blob = "\n".join(str(s) for s in (ctx.pred_sqls or []))
    if re.search(r"\bDISTINCT\s+ON\b", blob, re.I):
        return None
    if re.search(
        r"JOIN\s*\(\s*SELECT\s+(?:MIN|MAX)\s*\(\s*\w+\s*\)",
        blob,
        re.I,
    ):
        return None
    # 委托已有分组键否决
    return personalization_group_key_only(list(ctx.pred_sqls or []))


def spec_mgmt_artifacts(ctx: RuleContext) -> Optional[str]:
    """Spec 声明 mgmt_artifacts 时的结构完备性。"""
    spec = _spec(ctx)
    if not spec or not spec.mgmt_artifacts:
        return None
    if ctx.category_lower != "management":
        return None
    blob = "\n".join(str(s) for s in (ctx.pred_sqls or []))
    arts = {a.lower() for a in spec.mgmt_artifacts}
    if "function" in arts or "trigger" in arts:
        msg = mgmt_trigger_incomplete(ctx.query, list(ctx.pred_sqls or []))
        if msg:
            return msg
    if "unique_index" in arts:
        if re.search(r"ON\s+CONFLICT", blob, re.I) and not re.search(
            r"CREATE\s+UNIQUE\s+INDEX|ADD\s+CONSTRAINT\s+\w+\s+UNIQUE",
            blob,
            re.I,
        ):
            return "Intent Spec：UPSERT 需 matching UNIQUE 索引/约束"
    return None


SPEC_RULES: List[Rule] = [
    Rule(
        id="spec_output_columns_missing",
        categories=frozenset(),
        source="spec",
        check=spec_output_columns_missing,
    ),
    Rule(
        id="spec_predicate_required",
        categories=frozenset(),
        source="spec",
        check=spec_predicate_required,
    ),
    Rule(
        id="spec_unresolved_filter_risk",
        categories=frozenset(),
        source="spec",
        check=spec_unresolved_filter_risk,
    ),
    Rule(
        id="spec_pers_full_row",
        categories=frozenset({"personalization"}),
        source="spec",
        check=spec_pers_full_row,
    ),
    Rule(
        id="spec_mgmt_artifacts",
        categories=frozenset({"management"}),
        source="spec",
        check=spec_mgmt_artifacts,
    ),
]
