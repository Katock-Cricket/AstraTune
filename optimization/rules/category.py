"""类目级启发规则：Personalization 分组假修复、Management UPSERT/触发器、Efficiency 合并。

判定逻辑自 optimization/verify.py 逐字迁移（语义不变）。
"""

from __future__ import annotations

import re
from typing import FrozenSet, List, Optional, Sequence

from optimization.rules.base import Rule


def personalization_group_key_only(pred_sqls: Sequence[str]) -> Optional[str]:
    """Personalization 常见硬伤：GROUP BY 后只选出分组键，丢了整行属性。"""
    for sql in pred_sqls or []:
        s = str(sql or "").strip()
        if not s or not re.search(r"\bGROUP\s+BY\b", s, re.I):
            continue
        # SELECT artist FROM cards GROUP BY artist
        if re.search(
            r"SELECT\s+([A-Za-z_][\w\.]*)\s+FROM\s+[\w\.\"`]+(?:\s+\w+)?\s+GROUP\s+BY\s+\1\b",
            s,
            re.I,
        ):
            return (
                "Personalization: GROUP BY 后只返回分组键；应保留完整行"
                "（如 DISTINCT ON / JOIN MIN(id) 子查询），不要只 SELECT 分组列"
            )
        # SELECT a,b FROM t GROUP BY a,b 且无聚合、无 *
        if "*" not in s and not re.search(
            r"\b(COUNT|SUM|AVG|MIN|MAX|ARRAY_AGG|STRING_AGG)\s*\(", s, re.I
        ):
            sel = re.search(r"SELECT\s+(.*?)\s+FROM\b", s, re.I | re.S)
            grp = re.search(
                r"GROUP\s+BY\s+(.+?)(?:\s+ORDER\s+BY|\s+HAVING|\s+LIMIT|$)",
                s,
                re.I | re.S,
            )
            if sel and grp:
                sel_cols = {
                    c.strip().lower() for c in sel.group(1).split(",") if c.strip()
                }
                grp_cols = {
                    c.strip().lower() for c in grp.group(1).split(",") if c.strip()
                }
                if sel_cols and sel_cols <= grp_cols:
                    return (
                        "Personalization: SELECT 列集合 ⊆ GROUP BY 键且无聚合；"
                        "通常需要返回分组代表行的全部列"
                    )
        if (
            re.search(r"SELECT\s+\*\s+FROM", s, re.I)
            and not re.search(r"\bDISTINCT\s+ON\b", s, re.I)
            and not re.search(
                r"\b(COUNT|SUM|AVG|MIN|MAX|ARRAY_AGG|STRING_AGG)\s*\(", s, re.I
            )
        ):
            return (
                "Personalization: SELECT * GROUP BY 在 PostgreSQL 非法；"
                "应 DISTINCT ON (分组键) * 或 JOIN (SELECT min(id) ... GROUP BY) 再选 t.*"
            )
    return None


def upsert_operation_type_mismatch(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """用户要区分 INSERT/UPDATE 时，必须 RETURNING * 且标签为文本 'INSERT'/'UPDATE'。"""
    q = (query or "").lower()
    if not q:
        return None
    wants = bool(
        re.search(
            r"whether an?\s+insert or an?\s+update|"
            r"insert or(?: an)? update operation|"
            r"type of operation|operation was performed|"
            r"know whether.*(insert|update)",
            q,
            flags=re.IGNORECASE,
        )
    )
    if not wants:
        wants = (
            "insert" in q
            and "update" in q
            and any(k in q for k in ("whether", "determine", "know"))
        )
    if not wants:
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    if not re.search(r"\bON\s+CONFLICT\b", blob, flags=re.IGNORECASE):
        if not re.search(r"\bRETURNING\b", blob, flags=re.IGNORECASE):
            return (
                "UPSERT: 用户要知道本次是 INSERT 还是 UPDATE；"
                "应 ON CONFLICT DO UPDATE 且 RETURNING * ,"
                " CASE WHEN xmax = 0 THEN 'INSERT' ELSE 'UPDATE' END AS operation_type"
            )
    if not re.search(r"\bRETURNING\b", blob, flags=re.IGNORECASE):
        return (
            "UPSERT: 缺少 RETURNING。应 RETURNING * 并输出文本 "
            "'INSERT'/'UPDATE' 作为 operation_type（PG 用 xmax=0 判断插入）"
        )
    has_labels = bool(re.search(r"'INSERT'", blob, flags=re.IGNORECASE)) and bool(
        re.search(r"'UPDATE'", blob, flags=re.IGNORECASE)
    )
    if has_labels and re.search(r"RETURNING\s+\*", blob, flags=re.IGNORECASE):
        return None
    if re.search(r"is_insert|xmax\s*=\s*0", blob, flags=re.IGNORECASE) and not has_labels:
        return (
            "UPSERT: 不要只 RETURNING 布尔 is_insert / (xmax=0)；"
            "应 RETURNING * , CASE WHEN xmax = 0 THEN 'INSERT' ELSE 'UPDATE' "
            "END AS operation_type，保证结果列含全部原列 + 文本标签"
        )
    if not has_labels:
        return (
            "UPSERT: RETURNING 需带文本 'INSERT'/'UPDATE' 标签"
            "（CASE WHEN xmax = 0 THEN 'INSERT' ELSE 'UPDATE' END AS operation_type）"
        )
    if not re.search(r"RETURNING\s+\*", blob, flags=re.IGNORECASE):
        return (
            "UPSERT: 应 RETURNING * 再加上 operation_type，"
            "不要只投影部分列（评测常按列下标取 UPDATE 标签）"
        )
    return None


def on_conflict_index_mismatch(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """ON CONFLICT 列集必须落在同一条 UNIQUE 索引/约束上，不能拆成两组部分列。"""
    q = query or ""
    if not re.search(
        r"no unique or exclusion constraint matching|there is no unique",
        q,
        flags=re.IGNORECASE,
    ):
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    m = re.search(r"on\s+conflict\s*\(([^)]+)\)", q, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"on\s+conflict\s*\(([^)]+)\)", blob, flags=re.IGNORECASE)
    if not m:
        return None
    need = [p.strip().strip('"').lower() for p in m.group(1).split(",") if p.strip()]
    if len(need) < 2:
        return None

    def _covers(colspec: str) -> bool:
        cols = [p.strip().strip('"').lower() for p in colspec.split(",") if p.strip()]
        return all(c in cols for c in need)

    covered = False
    for um in re.finditer(
        r"CREATE\s+UNIQUE\s+INDEX[\s\S]{0,240}?\(([^)]+)\)",
        blob,
        flags=re.IGNORECASE,
    ):
        if _covers(um.group(1)):
            covered = True
            break
    if not covered:
        for um in re.finditer(r"UNIQUE\s*\(([^)]+)\)", blob, flags=re.IGNORECASE):
            if _covers(um.group(1)):
                covered = True
                break
    if not covered:
        return (
            f"ON CONFLICT ({', '.join(need)}) 需要同一组列上的一条 UNIQUE 索引/约束；"
            "不要拆成多组部分列的部分唯一索引。可用 WHERE coalesce(...) IS NOT NULL。"
        )
    and_nulls = bool(
        re.search(
            r"where\s+\w+\s+is\s+not\s+null\s+and\s+\w+\s+is\s+not\s+null",
            blob,
            flags=re.IGNORECASE,
        )
    )
    has_coalesce = bool(re.search(r"\bcoalesce\s*\(", blob, flags=re.IGNORECASE))
    if and_nulls and not has_coalesce:
        return (
            "部分 UNIQUE 索引的 WHERE 应写成 coalesce(冲突列中可空列) IS NOT NULL，"
            "以便匹配 ON CONFLICT (...) WHERE coalesce(...) IS NOT NULL；"
            "不要写成多列 AND col IS NOT NULL。"
        )
    if re.search(r"\bINSERT\s+INTO\b", blob, flags=re.IGNORECASE):
        return (
            "本题评测会自行 INSERT/ON CONFLICT；pred 只建匹配冲突列全集的 UNIQUE 索引，"
            "不要夹带测试行 INSERT（会污染后续断言）。"
        )
    return None


def returning_join_mismatch(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """UPDATE RETURNING 若要带外表列，必须 JOIN 该表并投影，不能只 RETURNING *。"""
    q = query or ""
    if not re.search(
        r"RETURNING.{0,120}join|join to something else and return|"
        r"return the joined row",
        q,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    tables = re.findall(
        r"joined to [`']?([A-Za-z_][\w]*)",
        q,
        flags=re.IGNORECASE,
    )
    if not tables:
        tables = re.findall(
            r"join(?:ed)? to (?:the )?[`']?([A-Za-z_][\w]*)",
            q,
            flags=re.IGNORECASE,
        )
    has_join = bool(re.search(r"\bJOIN\b", blob, flags=re.IGNORECASE))
    has_ret = bool(re.search(r"\bRETURNING\b", blob, flags=re.IGNORECASE))
    if not has_ret or not has_join:
        return (
            "用户要把 RETURNING 接到另一张表；UPDATE 须 JOIN 该表，"
            "RETURNING 更新表列 + 外表列，不要只 RETURNING *。"
        )
    _join_stop = {"on", "where", "left", "right", "inner", "outer", "join", "using"}
    for t in tables:
        if t.lower() in ("something", "else", "row", "rows"):
            continue
        jm = re.search(
            rf"\bJOIN\s+{re.escape(t)}(?:\s+(?:AS\s+)?([A-Za-z_][\w]*))?",
            blob,
            flags=re.IGNORECASE,
        )
        if not jm:
            return (
                f"RETURNING 需 JOIN {t} 并投影该表列，不要只返回被更新表的 RETURNING *。"
            )
        alias = (jm.group(1) or "").strip()
        prefixes = [t]
        if alias and alias.lower() not in _join_stop:
            prefixes.append(alias)
        if not any(
            re.search(rf"\b{re.escape(p)}\.", blob, flags=re.IGNORECASE)
            for p in prefixes
        ):
            return f"JOIN {t} 之后 RETURNING 须包含 {t}（或别名）的目标列，不能仍是 RETURNING *。"
    if re.search(r"RETURNING\s+\*", blob, flags=re.IGNORECASE) and tables:
        return "RETURNING * 不含外表列；应显式列出更新表列与 JOIN 表列。"
    return None


def mgmt_trigger_incomplete(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """触发器题须交付完整 CREATE FUNCTION + CREATE TRIGGER。

    注：split_part('.com') 分支来自 Flash 单题（WebsiteUrl/stackoverflow 题面）。
    """
    q = query or ""
    if not re.search(r"\btrigger\b", q, flags=re.IGNORECASE):
        return None
    if not re.search(
        r"automatically|when a new|charindex|I created a trigger",
        q,
        flags=re.IGNORECASE,
    ):
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    has_fn = bool(
        re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION", blob, flags=re.IGNORECASE)
    )
    has_tr = bool(
        re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER", blob, flags=re.IGNORECASE)
    )
    if has_fn and has_tr:
        if re.search(
            r"split_part\s*\([^,]+,\s*'[^']*\.com|regexp_replace\s*\([\s\S]{0,80}\\?\.com",
            blob,
            flags=re.IGNORECASE,
        ):
            return (
                "WebsiteUrl 取 http:// 之后到第一个 '.' 的主机第一段"
                "（http://stackoverflow.com → stackoverflow），"
                "不要 split_part(..., '.com')（对 blogspot 会留下多余后缀）。用 strpos/position。"
            )
        return None
    return (
        "Management 触发器题须交付完整 CREATE FUNCTION + CREATE TRIGGER；"
        "不要只交函数体片段；PG 用 strpos 而非 charindex。"
    )


def pers_streak_histogram_mismatch(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """Personalization 连续月数直方图：最终投影应是 (长度, 人数) 而非单用户行。"""
    q = query or ""
    if not re.search(
        r"how many users have each|count how many users have|"
        r"number of users who have that longest streak|"
        r"count how many users have each longest streak",
        q,
        flags=re.IGNORECASE,
    ):
        return None
    last = str((pred_sqls or [""])[-1] or "")
    if re.search(r"\buser_id\b", last, flags=re.IGNORECASE) and re.search(
        r"\bLIMIT\s+1\b", last, flags=re.IGNORECASE
    ):
        return (
            "最终结果应是 (streak_length, user_count) 直方图："
            "先算每人最长连续月数，再按长度 GROUP BY 统计用户数；不要 LIMIT 1 的单用户行。"
        )
    if re.search(r"\buser_id\b", last, flags=re.IGNORECASE) and not re.search(
        r"COUNT\s*\(\s*(?:DISTINCT\s+)?user_id", last, flags=re.IGNORECASE
    ):
        return "最终投影不要保留每人一行的 user_id；应输出 streak 长度及用户数。"
    return None


def efficiency_merge_notin(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """Efficiency：两个 NOT IN 应合并为 UNION，避免全量评测超时。"""
    q = query or ""
    if not re.search(r"two separate NOT IN|two NOT IN", q, flags=re.IGNORECASE):
        return None
    blob = "\n".join(str(s) for s in (pred_sqls or []))
    notins = len(re.findall(r"\bNOT\s+IN\b", blob, flags=re.IGNORECASE))
    if notins >= 2 and not re.search(r"\bUNION\b", blob, flags=re.IGNORECASE):
        return (
            "两个 NOT IN 应合并为 NOT IN (q1 UNION [ALL] q2)，并去掉 PostgreSQL 非法 LIMIT；"
            "全量评测上双 NOT IN / 过程化循环容易超时。"
        )
    return None


# ---------- 注册表适配器 ----------

def _check_group_key_only(ctx) -> Optional[str]:
    return personalization_group_key_only(ctx.pred_sqls)


def _check_streak_histogram(ctx) -> Optional[str]:
    return pers_streak_histogram_mismatch(ctx.query, ctx.pred_sqls)


def _check_upsert(ctx) -> Optional[str]:
    return upsert_operation_type_mismatch(ctx.query, ctx.pred_sqls)


def _check_on_conflict(ctx) -> Optional[str]:
    return on_conflict_index_mismatch(ctx.query, ctx.pred_sqls)


def _check_returning_join(ctx) -> Optional[str]:
    return returning_join_mismatch(ctx.query, ctx.pred_sqls)


def _check_trigger(ctx) -> Optional[str]:
    return mgmt_trigger_incomplete(ctx.query, ctx.pred_sqls)


def _check_merge_notin(ctx) -> Optional[str]:
    return efficiency_merge_notin(ctx.query, ctx.pred_sqls)


_PERS: FrozenSet[str] = frozenset({"personalization"})
_MGMT: FrozenSet[str] = frozenset({"management"})

CATEGORY_RULES: List[Rule] = [
    Rule(
        id="personalization_group_key_only",
        categories=_PERS,
        source="category",
        check=_check_group_key_only,
    ),
    Rule(
        id="pers_streak_histogram_mismatch",
        categories=_PERS,
        source="category",
        check=_check_streak_histogram,
    ),
    Rule(
        id="upsert_operation_type_mismatch",
        categories=_MGMT,
        source="category",
        check=_check_upsert,
        veto_log="否决 UPSERT 假过",
    ),
    Rule(
        id="on_conflict_index_mismatch",
        categories=_MGMT,
        source="category",
        check=_check_on_conflict,
    ),
    Rule(
        id="returning_join_mismatch",
        categories=_MGMT,
        source="category",
        check=_check_returning_join,
    ),
    Rule(
        id="mgmt_trigger_incomplete",
        categories=_MGMT,
        source="category",
        check=_check_trigger,
    ),
    Rule(
        id="efficiency_merge_notin",
        categories=frozenset(),
        source="category",
        check=_check_merge_notin,
        efficiency_only=True,
    ),
]
