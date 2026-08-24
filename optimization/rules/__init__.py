"""Verify 规则注册表。

三层规则（判定逻辑均自原 optimization/verify.py 逐字迁移）：
- common: 跨类目通用信号（意图表、PG 方言残留等）
- category: 类目级启发（Personalization 分组、Management 触发器等）
- flash_overfit: 针对 Flash 子集特定题面的规则（docstring 标注来源特征）

Verifier 的执行/超时/Soft EX/QEP 属测量主流程，不走本注册表。
"""

from __future__ import annotations

from typing import Dict, List

from optimization.rules.base import Rule, RuleContext
from optimization.rules.category import (
    CATEGORY_RULES,
    efficiency_merge_notin,
    mgmt_trigger_incomplete,
    on_conflict_index_mismatch,
    personalization_group_key_only,
    pers_streak_histogram_mismatch,
    returning_join_mismatch,
    upsert_operation_type_mismatch,
)
from optimization.rules.common import (
    COMMON_RULES,
    conditional_count_without_filter,
    extract_preprocess_created_tables,
    missing_intent_tables,
    missing_preprocess_created_tables,
    mysql_dialect_funcs_in_pg,
    pg_delete_update_limit,
    query_output_example_mismatch,
)
from optimization.rules.spec_rules import SPEC_RULES
from optimization.rules.flash_overfit import (
    FLASH_OVERFIT_RULES,
    circular_fk_chicken_egg,
    consecutive_date_island_count,
    next_free_date_mismatch,
    query_time_format_mismatch,
    table_count_uses_count_column,
)

__all__ = [
    "Rule",
    "RuleContext",
    "ALL_RULES",
    "rules_for",
    # 规则函数（供 verify.py 再导出与单测直接使用）
    "conditional_count_without_filter",
    "consecutive_date_island_count",
    "circular_fk_chicken_egg",
    "efficiency_merge_notin",
    "extract_preprocess_created_tables",
    "mgmt_trigger_incomplete",
    "missing_intent_tables",
    "missing_preprocess_created_tables",
    "mysql_dialect_funcs_in_pg",
    "next_free_date_mismatch",
    "on_conflict_index_mismatch",
    "personalization_group_key_only",
    "pers_streak_histogram_mismatch",
    "pg_delete_update_limit",
    "query_output_example_mismatch",
    "query_time_format_mismatch",
    "returning_join_mismatch",
    "table_count_uses_count_column",
    "upsert_operation_type_mismatch",
    "CATEGORY_RULES",
    "COMMON_RULES",
    "SPEC_RULES",
    "FLASH_OVERFIT_RULES",
]


# 注册顺序 = 原 _verify_candidates_body 内的执行顺序 + spec 规则（common 之后、flash_overfit 之前）
_BY_ID: Dict[str, Rule] = {
    r.id: r for r in COMMON_RULES + CATEGORY_RULES + SPEC_RULES + FLASH_OVERFIT_RULES
}
_EXECUTION_ORDER = [
    "missing_intent_tables",            # query / personalization
    "personalization_group_key_only",   # personalization
    "spec_output_columns_missing",      # spec（全类目，空 Spec 跳过）
    "spec_predicate_required",
    "spec_unresolved_filter_risk",
    "spec_pers_full_row",               # personalization
    "spec_mgmt_artifacts",              # management
    "query_time_format_mismatch",       # query（flash 单题）
    "conditional_count_without_filter", # query
    "table_count_uses_count_column",    # query（flash 单题）
    "consecutive_date_island_count",    # query（flash 单题）
    "query_output_example_mismatch",    # query
    "pers_streak_histogram_mismatch",   # personalization
    "upsert_operation_type_mismatch",   # management
    "missing_preprocess_created_tables",# management
    "on_conflict_index_mismatch",       # management
    "returning_join_mismatch",          # management
    "circular_fk_chicken_egg",          # management（flash 单题）
    "next_free_date_mismatch",          # management（flash 单题）
    "mgmt_trigger_incomplete",          # management
    "mysql_dialect_funcs_in_pg",        # 全类目
    "efficiency_merge_notin",           # efficiency
    "pg_delete_update_limit",           # 全类目
]

assert set(_EXECUTION_ORDER) == set(_BY_ID), "规则注册表与执行顺序清单不一致"
ALL_RULES: List[Rule] = [_BY_ID[i] for i in _EXECUTION_ORDER]

_RULES_CACHE: Dict[tuple, List[Rule]] = {}


def rules_for(category_lower: str, is_efficiency: bool) -> List[Rule]:
    """按类目预筛规则（模块级缓存，零重复计算）。"""
    key = (category_lower, bool(is_efficiency))
    hit = _RULES_CACHE.get(key)
    if hit is None:
        hit = [r for r in ALL_RULES if r.applies(category_lower, is_efficiency)]
        _RULES_CACHE[key] = hit
    return hit
