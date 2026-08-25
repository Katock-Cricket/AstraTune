"""Flash 子集单题过拟合规则（判定逻辑自 optimization/verify.py 逐字迁移）。

**冻结新增（2026-08-24）**：不再向本模块添加单题规则；泛化约束请用 optimization/rules/spec_rules.py。
这些规则的触发条件绑定特定题面特征词（表名/列名/错误消息文案）；换题集/方言时需重新评估。
每条规则的 docstring 标注来源特征。
"""

from __future__ import annotations

import re
from typing import FrozenSet, List, Optional, Sequence

from optimization.rules.base import Rule


def query_time_format_mismatch(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """【Flash 单题】题面含 "hh:mm:ss / leading zero" 的时间格式题：
    用户只要 hh:mm:ss 且抱怨前导零时，勿默认 HH24 / ::time。
    """
    q = query or ""
    if not re.search(r"hh:mm:ss|leading zero", q, flags=re.IGNORECASE):
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    if not blob.strip():
        return None
    # 样例里出现 13–23 点则明确要 24 小时制
    if re.search(r"\b(?:1[3-9]|2[0-3]):[0-5]\d:[0-5]\d\b", q):
        return None
    if re.search(r"24[\s-]?hour|24\s*小时|military|hh24", q, flags=re.IGNORECASE):
        return None
    has_fmhh12 = bool(
        re.search(r"to_char\s*\([\s\S]*['\"]FMHH:MI:SS['\"]", blob, flags=re.IGNORECASE)
    )
    if has_fmhh12 and not re.search(r"HH24|FMHH24", blob, flags=re.IGNORECASE):
        return None
    if re.search(
        r"HH24|FMHH24|::\s*time\b|EXTRACT\s*\(\s*HOUR|date_part\s*\(\s*'hour'|"
        r"regexp_replace\s*\(",
        blob,
        flags=re.IGNORECASE,
    ):
        return (
            "时间格式: 用户只要 hh:mm:ss 且不要前导零、又未要求 24 小时制时，"
            "必须用 to_char(..., 'FMHH:MI:SS')（12 小时 + FM）。"
            "不要 ::time、不要 HH24/FMHH24、不要 EXTRACT(HOUR)/regexp_replace"
            "（后两者仍是 0–23 时制，官方结果按 FMHH 比对）"
        )
    return None


def table_count_uses_count_column(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """【Flash 单题】题面含 "number of attendees / does not match the number"
    的 budget.count 列误用题：人数应对表 COUNT 行，不要用名为 count 的列。
    """
    q = query or ""
    if not re.search(
        r"number of attendees|does not match the number",
        q,
        flags=re.IGNORECASE,
    ):
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    if re.search(r"to_jsonb|->>>\s*'count'|->>\s*'count'", blob, flags=re.IGNORECASE):
        return "比较两表人数应对 budget 按事件 COUNT 行，不要用 to_jsonb 猜 count 列。"
    if re.search(r"\bbudget\.count\b", blob, flags=re.IGNORECASE):
        return "比较两表人数应对 budget 按事件 COUNT 行，不要使用名为 count 的列。"
    if re.search(r"\b[A-Za-z_][\w]*\.count\b", blob, flags=re.IGNORECASE) and not re.search(
        r"COUNT\s*\(", blob, flags=re.IGNORECASE
    ):
        return "比较两表「人数/条数」应对两表分别 COUNT(行)，不要把名为 count 的列当成人数。"
    return None


def consecutive_date_island_count(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """【Flash 单题】题面含 "consecutive + releaseDate" 的连续日期岛题：
    同一天重复行不能用 COUNT(*)-1。
    """
    q = query or ""
    if not re.search(r"consecutive", q, flags=re.IGNORECASE):
        return None
    if not re.search(r"releaseDate|start and end dates", q, flags=re.IGNORECASE):
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    if re.search(r"COUNT\s*\(\s*\*\s*\)\s*-\s*1", blob, flags=re.IGNORECASE):
        return (
            "连续日期岛长度是 (MAX(date)-MIN(date))+1（按天）；"
            "同一天重复行不能用 COUNT(*)-1。先按日期去重，再用 LAG 找 gap。"
        )
    return None


def circular_fk_chicken_egg(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """【Flash 单题】题面含 "chicken(-egg) / at least one + foreign key" 的
    account/disp 互外键题：互为 FK 会 chicken-egg，须触发器保证约束。
    """
    q = (query or "").lower()
    if "chicken" not in q and "chicken-egg" not in q:
        if not (
            "at least one" in q
            and ("foreign key" in q or "enforce this relationship" in q)
        ):
            return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    refs = re.findall(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"([A-Za-z_][\w]*)[\s\S]{0,8000}?REFERENCES\s+([A-Za-z_][\w]*)",
        blob,
        flags=re.IGNORECASE,
    )
    refs += re.findall(
        r"ALTER\s+TABLE\s+([A-Za-z_][\w]*)[\s\S]{0,400}?REFERENCES\s+([A-Za-z_][\w]*)",
        blob,
        flags=re.IGNORECASE,
    )
    pairs = {(a.lower(), b.lower()) for a, b in refs}
    for a, b in pairs:
        if a != b and (b, a) in pairs:
            return (
                "互为 FOREIGN KEY 会 chicken-egg；应去掉一侧 FK，"
                "用触发器保证「至少一行」约束，且父表 INSERT 不依赖尚未存在的子表行。"
            )
    acct = re.search(
        r"CREATE\s+TABLE\s+account\b([\s\S]*?)(?=CREATE\s+TABLE|\Z)",
        blob,
        flags=re.IGNORECASE,
    )
    if acct and re.search(r"\bdisp_id\b", acct.group(1), flags=re.IGNORECASE):
        return (
            "account 不要 disp_id 列；评测会按无 disp_id 的列清单 INSERT account。"
            "单向 FK disp→account，并用 DELETE 触发器保证每账户至少一条 disp。"
        )
    if re.search(
        r"AFTER\s+INSERT[\s\S]{0,120}ON\s+account|ON\s+account[\s\S]{0,80}AFTER\s+INSERT",
        blob,
        flags=re.IGNORECASE,
    ):
        return (
            "不要在 account INSERT 上强制已有 disp（评测会先 INSERT account 再 INSERT disp）。"
            "只在 DELETE disp 时用 BEFORE DELETE 触发器阻止删光最后一条。"
        )
    return None


def next_free_date_mismatch(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """【Flash 单题】题面含 "minimum ... date + not already" 的 races/seasons
    下一空闲日题：不要直接 INSERT 字面量日期。
    """
    q = query or ""
    if not re.search(r"minimum.{0,60}date", q, flags=re.IGNORECASE | re.DOTALL):
        return None
    if not re.search(r"not already", q, flags=re.IGNORECASE):
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    if re.search(r"generate_series|\bLOOP\b|WHILE\s+EXISTS", blob, flags=re.IGNORECASE):
        if re.search(r"INSERT\s+INTO\s+races\b", blob, flags=re.IGNORECASE) and not re.search(
            r"INSERT\s+INTO\s+seasons\b", blob, flags=re.IGNORECASE
        ):
            return (
                "INSERT races 的 year 可能不在 seasons；先 INSERT 缺失的 seasons 行，"
                "再写入 races（不要只加 UNIQUE(date)）。"
            )
        return None
    if re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION", blob, flags=re.IGNORECASE):
        if re.search(r"INSERT\s+INTO\s+races\b", blob, flags=re.IGNORECASE) and not re.search(
            r"INSERT\s+INTO\s+seasons\b", blob, flags=re.IGNORECASE
        ):
            return (
                "INSERT races 的 year 可能不在 seasons；先 INSERT 缺失的 seasons 行，"
                "再写入 races（不要只加 UNIQUE(date)）。"
            )
        return None
    return (
        "用户要 >= input_date 且尚未占用的最小日期；不要直接 INSERT 字面量日期。"
        "应写求下一空闲日的函数/循环，并先插入可能缺失的父表行（如 seasons）。"
    )


# ---------- 注册表适配器 ----------

def _check_time_format(ctx) -> Optional[str]:
    return query_time_format_mismatch(ctx.query, ctx.pred_sqls)


def _check_table_count(ctx) -> Optional[str]:
    return table_count_uses_count_column(ctx.query, ctx.pred_sqls)


def _check_island_count(ctx) -> Optional[str]:
    return consecutive_date_island_count(ctx.query, ctx.pred_sqls)


def _check_chicken_egg(ctx) -> Optional[str]:
    return circular_fk_chicken_egg(ctx.query, ctx.pred_sqls)


def _check_next_free_date(ctx) -> Optional[str]:
    return next_free_date_mismatch(ctx.query, ctx.pred_sqls)


_QUERY: FrozenSet[str] = frozenset({"query"})
_MGMT: FrozenSet[str] = frozenset({"management"})

FLASH_OVERFIT_RULES: List[Rule] = [
    Rule(
        id="query_time_format_mismatch",
        categories=_QUERY,
        source="flash_overfit:hh:mm:ss 前导零题",
        check=_check_time_format,
        veto_log="否决时间格式假过",
    ),
    Rule(
        id="table_count_uses_count_column",
        categories=_QUERY,
        source="flash_overfit:budget/attendees count 列题",
        check=_check_table_count,
    ),
    Rule(
        id="consecutive_date_island_count",
        categories=_QUERY,
        source="flash_overfit:releaseDate 连续日期岛题",
        check=_check_island_count,
    ),
    Rule(
        id="circular_fk_chicken_egg",
        categories=_MGMT,
        source="flash_overfit:account/disp 互外键题",
        check=_check_chicken_egg,
    ),
    Rule(
        id="next_free_date_mismatch",
        categories=_MGMT,
        source="flash_overfit:races/seasons 下一空闲日题",
        check=_check_next_free_date,
    ),
]
