"""跨类目通用规则：意图表、PG 方言残留、条件计数、题面输出样例。

判定逻辑自 optimization/verify.py 逐字迁移（语义不变）。
"""

from __future__ import annotations

import re
from typing import Any, FrozenSet, List, Optional, Sequence

from optimization.rules.base import Rule


def missing_intent_tables(
    query: str,
    schema: str,
    pred_sqls: Sequence[str],
) -> List[str]:
    """用户问题点名且 schema 存在的表，若 pred 未引用则视为意图偏离（Query/Pers 硬约束）。"""
    if not (query or "").strip():
        return []
    try:
        from utils.pipeline import (
            schema_tables_mentioned_in_text,
            tables_from_schema_text,
        )
    except Exception:
        return []
    required = schema_tables_mentioned_in_text(
        query, tables_from_schema_text(schema or "")
    )
    if not required:
        return []
    blob = "\n".join(str(s) for s in (pred_sqls or [])).lower()
    missing: List[str] = []
    for name in required:
        tl = name.lower()
        # 要求作为标识符出现：引号包裹，或紧跟 FROM/JOIN/INTO/UPDATE/TABLE
        pat = (
            rf'(?:"{re.escape(tl)}"|`{re.escape(tl)}`|'
            rf"\b(?:from|join|into|update|table)\s+{re.escape(tl)}\b)"
        )
        if not re.search(pat, blob, flags=re.IGNORECASE):
            missing.append(name)
    return missing


_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+"
    r'(?:ONLY\s+)?([`"]?[A-Za-z_][\w.]*[`"]?)',
    re.IGNORECASE,
)


def extract_preprocess_created_tables(preprocess_sql: Any) -> List[str]:
    """从 preprocess 中抽出 CREATE TABLE 目标名（含 CREATE TABLE AS / TEMP）。"""
    texts: List[str] = []
    if isinstance(preprocess_sql, str):
        texts = [preprocess_sql]
    elif preprocess_sql:
        texts = [str(x) for x in preprocess_sql]
    found: List[str] = []
    seen = set()
    for text in texts:
        for raw in _CREATE_TABLE_RE.findall(text or ""):
            name = str(raw).strip().strip('`"')
            if "." in name:
                name = name.split(".")[-1]
            key = name.lower()
            if not key or key in seen:
                continue
            seen.add(key)
            found.append(name)
    return found


def _ident_in_sql(name: str, blob: str) -> bool:
    tl = name.lower()
    pat = (
        rf'(?:"{re.escape(tl)}"|`{re.escape(tl)}`|'
        rf"\b(?:from|join|into|update|table)\s+{re.escape(tl)}\b|"
        rf"\b{re.escape(tl)}\b)"
    )
    return bool(re.search(pat, blob, flags=re.IGNORECASE))


def missing_preprocess_created_tables(
    preprocess_sql: Any,
    pred_sqls: Sequence[str],
    category: str = "",
) -> List[str]:
    """Management：preprocess 新建表若未出现在 pred，多为改错对象（假过）。

    Efficiency 的 helper 表（如 patients）允许不被 body 引用，故仅 Mgmt。
    """
    if (category or "").strip().lower() != "management":
        return []
    created = extract_preprocess_created_tables(preprocess_sql)
    if not created:
        return []
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    return [n for n in created if not _ident_in_sql(n, blob)]


def pg_delete_update_limit(dialect: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """PostgreSQL 顶层 DELETE/UPDATE 不能带 LIMIT（MySQL 方言残留）。"""
    d = (dialect or "").lower()
    if d not in ("postgresql", "postgres", "pg"):
        return None
    for sql in pred_sqls or []:
        s = str(sql or "").strip()
        if not s:
            continue
        head = s.lstrip("(").lstrip().upper()
        if not (head.startswith("DELETE") or head.startswith("UPDATE")):
            continue
        stripped = s
        for _ in range(8):
            nxt = re.sub(r"\([^()]*(?:\([^()]*\)[^()]*)*\)", " ", stripped)
            if nxt == stripped:
                break
            stripped = nxt
        if re.search(r"\bLIMIT\b", stripped, flags=re.IGNORECASE):
            return (
                "PostgreSQL 的 DELETE/UPDATE 不支持顶层 LIMIT；"
                "应去掉 LIMIT 并对全部匹配行生效，或改为 ctid/主键 IN (SELECT ... LIMIT n) 子查询"
            )
    return None


def mysql_dialect_funcs_in_pg(dialect: str, pred_sqls: Sequence[str]) -> Optional[str]:
    d = (dialect or "").lower()
    if d not in ("postgresql", "postgres", "pg"):
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    if re.search(r"\bcharindex\s*\(", blob, flags=re.IGNORECASE):
        return (
            "PostgreSQL 无 charindex；改用 strpos()/position()，"
            "并交付完整 CREATE FUNCTION + CREATE TRIGGER。"
        )
    return None


def conditional_count_without_filter(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    q = query or ""
    if not re.search(
        r"count[s]?.{0,100}only if|only if.{0,100}count",
        q,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    if re.search(r"\bFILTER\s*\(\s*WHERE", blob, flags=re.IGNORECASE):
        return None
    if re.search(r"COUNT\s*\(\s*CASE\s+WHEN", blob, flags=re.IGNORECASE):
        return None
    if re.search(r"SUM\s*\(\s*CASE\s+WHEN", blob, flags=re.IGNORECASE):
        return None
    return (
        "条件计数须用 COUNT(*) FILTER (WHERE ...) 或 COUNT(CASE WHEN ... THEN 1 END)，"
        "不要无条件 COUNT。"
    )


def query_output_example_mismatch(query: str, rows: Sequence[Any]) -> Optional[str]:
    """用户题面给出的输出样例（非 sol）应能在 pred 结果中对上。"""
    q = query or ""
    if not re.search(r"output like|expected result should", q, flags=re.IGNORECASE):
        return None
    examples = re.findall(
        r"\(\s*(\d+)\s*,\s*(\d{4}-\d{2}-\d{2})\s*,\s*(\d{4}-\d{2}-\d{2})\s*\)",
        q,
    )
    if not examples:
        return None
    if not rows:
        return "用户题面中的输出样例未出现（结果为空）"

    def _cells(row: Any) -> List[str]:
        if isinstance(row, dict):
            vals = list(row.values())
        elif isinstance(row, (list, tuple)):
            vals = list(row)
        else:
            vals = [row]
        out = []
        for v in vals:
            s = "" if v is None else str(v)
            s = s.replace(" 00:00:00", "")
            out.append(s)
        return out

    rendered = ["|".join(_cells(r)) for r in rows]
    blob = " ".join(rendered)
    n, a, b = examples[0]
    if n in blob and a in blob and b in blob:
        return None
    return (
        f"结果未覆盖用户给出的连续区间样例（如 {n}, {a}..{b}）；"
        "应按去重日期的岛屿统计跨度 (MAX-MIN)+1。"
    )


# ---------- 注册表适配器（包装消息文案与原 if 链逐字一致） ----------

def _check_intent_tables(ctx) -> Optional[str]:
    miss = missing_intent_tables(ctx.query, ctx.schema, ctx.pred_sqls)
    if miss:
        return f"意图表未出现在 pred_sqls: {', '.join(miss)}"
    return None


def _check_preprocess_created_tables(ctx) -> Optional[str]:
    miss = missing_preprocess_created_tables(
        ctx.preprocess_sql, ctx.pred_sqls, ctx.category
    )
    if miss:
        return (
            "preprocess 新建表未出现在 pred_sqls: "
            + ", ".join(miss)
            + "；Mgmt 评测对象往往是这些表，而非用户口头上的生产表名"
        )
    return None


def _check_pg_delete_update_limit(ctx) -> Optional[str]:
    return pg_delete_update_limit(ctx.dialect, ctx.pred_sqls)


def _check_mysql_funcs(ctx) -> Optional[str]:
    return mysql_dialect_funcs_in_pg(ctx.dialect, ctx.pred_sqls)


def _check_conditional_count(ctx) -> Optional[str]:
    return conditional_count_without_filter(ctx.query, ctx.pred_sqls)


def _check_output_example(ctx) -> Optional[str]:
    return query_output_example_mismatch(ctx.query, ctx.rows or [])


_QP: FrozenSet[str] = frozenset({"query", "personalization"})

COMMON_RULES: List[Rule] = [
    Rule(
        id="missing_intent_tables",
        categories=_QP,
        source="common",
        check=_check_intent_tables,
    ),
    Rule(
        id="missing_preprocess_created_tables",
        categories=frozenset({"management"}),
        source="common",
        check=_check_preprocess_created_tables,
        veto_log="否决漏改 preprocess 表",
    ),
    Rule(
        id="conditional_count_without_filter",
        categories=frozenset({"query"}),
        source="common",
        check=_check_conditional_count,
    ),
    Rule(
        id="query_output_example_mismatch",
        categories=frozenset({"query"}),
        source="common",
        check=_check_output_example,
    ),
    Rule(
        id="mysql_dialect_funcs_in_pg",
        categories=frozenset(),
        source="common",
        check=_check_mysql_funcs,
    ),
    Rule(
        id="pg_delete_update_limit",
        categories=frozenset(),
        source="common",
        check=_check_pg_delete_update_limit,
        veto_log="否决 PG DELETE/UPDATE LIMIT",
    ),
]
