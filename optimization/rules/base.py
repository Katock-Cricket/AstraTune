"""规则数据结构（独立于各规则模块，避免循环导入）。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, FrozenSet, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from optimization.intent_spec import IntentSpec


@dataclass(frozen=True)
class RuleContext:
    """单候选规则判定上下文（一次构造，全规则复用）。"""

    dialect: str                     # 小写规范化方言
    category: str                    # 原始类目
    category_lower: str              # 小写类目
    is_efficiency: bool              # is_efficiency_case(category, efficiency)
    query: str
    schema: str
    pred_sqls: List[str]
    preprocess_sql: List[str]
    rows: Optional[List[Any]] = None  # 候选执行结果行
    column_names: Optional[List[str]] = None  # SELECT 结果列名
    intent_spec: Optional["IntentSpec"] = None


@dataclass(frozen=True)
class Rule:
    """一条否决规则：check 返回否决消息文案（None 表示通过）。"""

    id: str
    categories: FrozenSet[str]       # 空 = 不限类目
    source: str                      # common | category | flash_overfit:<来源>
    check: Callable[[RuleContext], Optional[str]]
    veto_log: Optional[str] = None   # 否决时附加的日志短语（保持历史日志文案）
    efficiency_only: bool = False    # 仅 is_efficiency=True 时启用

    def applies(self, category_lower: str, is_efficiency: bool) -> bool:
        if self.efficiency_only and not is_efficiency:
            return False
        if self.categories and category_lower not in self.categories:
            return False
        return True
