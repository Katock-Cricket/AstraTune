"""Intent Spec 驱动的泛化 Verify 规则（非单题 overfit）。"""

from __future__ import annotations

import re
from typing import List, Optional

from optimization.intent_spec import IntentSpec
from optimization.rules.base import Rule, RuleContext
from optimization.rules.category import (
    mgmt_trigger_incomplete,
    personalization_group_key_only,
)
from optimization.sql_shape import (
    is_ddl_or_dml,
    issue_sql_is_readonly_select,
    last_projection_arity,
    looks_like_group_aggregate_only,
    named_output_columns,
    pred_has_add_column,
    pred_has_alter_column_type,
    query_is_dynamic_sql_generator,
    tables_mentioned_in_sql,
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
    if is_ddl_or_dml(ctx.pred_sqls):
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
    if re.search(r"\bROW_NUMBER\s*\(", blob, re.I):
        return None
    if re.search(
        r"JOIN\s*\(\s*SELECT\s+(?:MIN|MAX)\s*\(\s*\w+\s*\)",
        blob,
        re.I,
    ):
        return None
    if looks_like_group_aggregate_only(ctx.pred_sqls):
        return (
            "Intent Spec：pers_need_full_row 需要分组代表行（DISTINCT ON / "
            "ROW_NUMBER / JOIN MIN(id)），不要 GROUP BY 后只留聚合指标"
        )
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
    if "column_type" in arts:
        if pred_has_add_column(ctx.pred_sqls) and not pred_has_alter_column_type(
            ctx.pred_sqls
        ):
            return (
                "Intent Spec：原地改列类型应 ALTER COLUMN ... TYPE，"
                "不要只 ADD COLUMN 旁路"
            )
    return None


def spec_output_arity_extra(ctx: RuleContext) -> Optional[str]:
    """题面点名 ≥2 列时，末条投影勿附加未要求列。"""
    names = named_output_columns(ctx.query or "")
    if len(names) < 2:
        return None
    arity = last_projection_arity(ctx.pred_sqls)
    if arity is None:
        return None
    if arity > len(names):
        return (
            f"输出契约：题面点名 {len(names)} 列 {names}，"
            f"pred 投影约 {arity} 列，勿附加未要求列"
        )
    return None


def spec_diagnosed_table_missing(ctx: RuleContext) -> Optional[str]:
    """诊断明确换表时，pred 须引用 preferred evidence 表。"""
    spec = _spec(ctx)
    if not spec or not spec.evidence:
        return None
    preferred = [
        e.table
        for e in spec.evidence
        if (e.sample_note or "") == "diag_preferred" and e.table
    ]
    if not preferred:
        return None
    used = tables_mentioned_in_sql(ctx.pred_sqls)
    missing = [t for t in preferred if t.lower() not in used]
    if missing:
        return f"Intent Spec：诊断建议改用表 {missing[:5]}，pred 未引用"
    return None


def spec_issue_shape_no_ddl_escalation(ctx: RuleContext) -> Optional[str]:
    """issue_sql 为只读 SELECT 时，Query pred 勿升级为 ADD COLUMN。"""
    if ctx.category_lower != "query":
        return None
    if not issue_sql_is_readonly_select(getattr(ctx, "issue_sql", None)):
        return None
    q = (ctx.query or "").lower()
    if re.search(r"add column|new column|alter table", q):
        return None
    if pred_has_add_column(ctx.pred_sqls):
        return (
            "issue_sql 是只读 SELECT：不要 ALTER TABLE ADD COLUMN；"
            "用 SELECT 表达式产出列"
        )
    return None


def spec_dynamic_sql_single_stmt(ctx: RuleContext) -> Optional[str]:
    """动态生成 SQL 字符串的题，pred 不应再执行生成结果。"""
    if not query_is_dynamic_sql_generator(ctx.query or ""):
        return None
    stmts = [str(s).strip() for s in (ctx.pred_sqls or []) if str(s).strip()]
    if len(stmts) >= 2:
        return (
            "动态生成 SQL 题应只交付生成语句本身，"
            "不要再执行生成出的 SELECT（会污染评测结果）"
        )
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
    Rule(
        id="spec_output_arity_extra",
        categories=frozenset(),
        source="spec",
        check=spec_output_arity_extra,
    ),
    Rule(
        id="spec_diagnosed_table_missing",
        categories=frozenset(),
        source="spec",
        check=spec_diagnosed_table_missing,
    ),
    Rule(
        id="spec_issue_shape_no_ddl_escalation",
        categories=frozenset({"query"}),
        source="spec",
        check=spec_issue_shape_no_ddl_escalation,
    ),
    Rule(
        id="spec_dynamic_sql_single_stmt",
        categories=frozenset(),
        source="spec",
        check=spec_dynamic_sql_single_stmt,
    ),
]
