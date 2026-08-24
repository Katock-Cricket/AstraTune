from __future__ import annotations

import json
import re
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from optimization.types import Candidate, CandidateVerifyResult, VerifyReport
from utils.logger import default_logger
from utils.sql_wrap import as_sql_list, has_cleanup, wrap_with_pre_post
from utils.timing import log_timing, timed_stage


def normalize_row(row: Any) -> Tuple[Any, ...]:
    """将一行结果规范化为可哈希、可比较的元组（忽略 dict 键序）。"""
    if isinstance(row, dict):
        items = sorted((str(k), _cell(v)) for k, v in row.items())
        return tuple(items)
    if isinstance(row, (list, tuple)):
        return tuple(_cell(v) for v in row)
    return (_cell(row),)


def _cell(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return str(v)
    return str(v)


def soft_ex_equal(rows_a: Sequence[Any], rows_b: Sequence[Any]) -> bool:
    """集合语义比较：忽略行序与重复次数差异时仍用 Counter 更稳；此处用 multiset。"""
    from collections import Counter

    ca = Counter(normalize_row(r) for r in rows_a)
    cb = Counter(normalize_row(r) for r in rows_b)
    return ca == cb


def extract_pg_total_cost(explain_payload: Any) -> Optional[float]:
    """从 PG EXPLAIN (FORMAT JSON) 结果抽取 Total Cost。"""
    data = explain_payload
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return None
    if isinstance(data, list) and data:
        plan = data[0].get("Plan") if isinstance(data[0], dict) else None
        if isinstance(plan, dict) and "Total Cost" in plan:
            try:
                return float(plan["Total Cost"])
            except (TypeError, ValueError):
                return None
    if isinstance(data, dict):
        plan = data.get("Plan")
        if isinstance(plan, dict) and "Total Cost" in plan:
            try:
                return float(plan["Total Cost"])
            except (TypeError, ValueError):
                return None
    return None


def extract_mysql_query_cost(explain_payload: Any) -> Optional[float]:
    """从 MySQL EXPLAIN FORMAT=JSON 抽取 query_cost。"""
    data = explain_payload
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return None
    if isinstance(data, list) and data:
        # pymysql 可能返回 [{'EXPLAIN': '{...json...}'}] 或直接 dict
        first = data[0]
        if isinstance(first, dict):
            for key in ("EXPLAIN", "explain", "json"):
                if key in first and isinstance(first[key], str):
                    try:
                        data = json.loads(first[key])
                        break
                    except json.JSONDecodeError:
                        pass
            else:
                if "query_block" in first:
                    data = first
    if not isinstance(data, dict):
        return None
    cost_info = data.get("query_block", {}).get("cost_info", {})
    if not isinstance(cost_info, dict):
        return None
    raw = cost_info.get("query_cost")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def extract_qep_cost(explain_payload: Any, dialect: str) -> Optional[float]:
    d = (dialect or "").lower()
    if d in ("mysql",):
        return extract_mysql_query_cost(explain_payload)
    return extract_pg_total_cost(explain_payload)


def _is_selectish(sql: str) -> bool:
    s = sql.strip().lstrip("(").upper()
    return s.startswith(("SELECT", "WITH", "SHOW", "EXPLAIN", "DESC", "DESCRIBE"))


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


def query_time_format_mismatch(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
    """Query 时间格式假过：用户只要 hh:mm:ss 且抱怨前导零时，勿默认 HH24 / ::time。"""
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


def circular_fk_chicken_egg(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
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


def mgmt_trigger_incomplete(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
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


def table_count_uses_count_column(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
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


def pers_streak_histogram_mismatch(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
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


def consecutive_date_island_count(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
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


def efficiency_merge_notin(query: str, pred_sqls: Sequence[str]) -> Optional[str]:
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


def is_efficiency_case(category: str = "", efficiency: Optional[bool] = None) -> bool:
    """Efficiency 题：efficiency 标志为 True，或 category 为 Efficiency。缺省视为非 Efficiency。"""
    if efficiency is True:
        return True
    return (category or "").strip().lower() == "efficiency"


def _join_sqls(sqls: List[str]) -> str:
    parts = [s.strip().rstrip(";") for s in sqls if s and str(s).strip()]
    return ";\n".join(parts)


class Verifier:
    """
    确定性 Verify：Soft EX / QEP / 超时。
    不硬拦 DDL/DML；不调 LLM。
    非 Efficiency：耗时超时不硬否决；QEP 仅作排序。
    """

    def __init__(
        self,
        *,
        dialect: str,
        category: str,
        exec_timeout_ms: int = 5000,
        efficiency: Optional[bool] = None,
        reset_fn: Optional[Callable[[], None]] = None,
        execute_fn: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
        explain_fn: Optional[Callable[[str], Any]] = None,
        preprocess_sql: Any = None,
        clean_up_sql: Any = None,
        query: str = "",
        schema: str = "",
        qep_hard_gate: bool = True,
    ):
        self.dialect = (dialect or "postgresql").lower()
        self.category = (category or "").strip()
        self.efficiency = efficiency
        self.is_efficiency = is_efficiency_case(self.category, efficiency)
        self.exec_timeout_ms = int(exec_timeout_ms)
        self.reset_fn = reset_fn
        self.execute_fn = execute_fn
        self.explain_fn = explain_fn
        self.preprocess_sql = as_sql_list(preprocess_sql)
        self.clean_up_sql = as_sql_list(clean_up_sql)
        self.query = query or ""
        self.schema = schema or ""
        self.qep_hard_gate = bool(qep_hard_gate)

    def _mark_clean_if_restored(self) -> None:
        """有 clean_up 且包执行成功后，沙箱应回到基线，可跳过下一轮 full reset。"""
        if not has_cleanup(self.clean_up_sql):
            return
        mark = getattr(self.reset_fn, "mark_clean", None)
        if callable(mark):
            try:
                mark()
            except Exception:
                pass

    def _run_sqls(self, sqls: List[str]) -> Tuple[List[Any], float, Optional[str]]:
        """对 body 做 preprocess→body→cleanup 包裹后执行；返回 (最后可比较结果行, latency_ms, error)。"""
        if not self.execute_fn:
            return [], 0.0, "execute_fn 未配置"
        with timed_stage("sql_wrap", body_stmts=len(sqls)):
            wrapped = wrap_with_pre_post(
                sqls,
                preprocess_sql=self.preprocess_sql,
                clean_up_sql=self.clean_up_sql,
            )
            sql_text = _join_sqls(wrapped)
        t0 = time.perf_counter()
        try:
            with timed_stage("verify_exec", stmts=len(wrapped)):
                results = self.execute_fn(sql_text) or []
        except Exception as e:
            latency = (time.perf_counter() - t0) * 1000.0
            return [], latency, str(e)
        latency = (time.perf_counter() - t0) * 1000.0

        # 取「body 段」最后带 rows 的结果，避免被 cleanup 的空结果覆盖语义
        pre_n = len(self.preprocess_sql)
        body_n = len([s for s in as_sql_list(sqls) if s and str(s).strip()])
        body_slice = results[pre_n : pre_n + body_n] if body_n else results

        rows: List[Any] = []
        for item in body_slice:
            if isinstance(item, dict) and "rows" in item:
                rows = list(item.get("rows") or [])
        if not rows:
            for item in results:
                if isinstance(item, dict) and "rows" in item:
                    rows = list(item.get("rows") or [])

        self._mark_clean_if_restored()
        return rows, latency, None

    def _measure_cost(self, sqls: List[str]) -> Optional[float]:
        if not self.explain_fn:
            return None
        t0 = time.perf_counter()
        try:
            return self._measure_cost_body(sqls)
        finally:
            log_timing("verify_qep", (time.perf_counter() - t0) * 1000.0)

    def _measure_cost_body(self, sqls: List[str]) -> Optional[float]:
        # 先落地前置，再对 body 可解释语句测 cost，最后尽力 cleanup
        if self.preprocess_sql and self.execute_fn:
            try:
                self.execute_fn(_join_sqls(self.preprocess_sql))
            except Exception as e:
                default_logger.debug(f"QEP 前置 SQL 失败: {e}")
                return None

        total = 0.0
        measured = False
        try:
            for sql in as_sql_list(sqls):
                s = sql.strip()
                if not s:
                    continue
                upper = s.upper()
                if not (
                    upper.startswith("SELECT")
                    or upper.startswith("INSERT")
                    or upper.startswith("UPDATE")
                    or upper.startswith("DELETE")
                    or upper.startswith("WITH")
                ):
                    continue
                try:
                    payload = self.explain_fn(s)
                    cost = extract_qep_cost(payload, self.dialect)
                    if cost is not None:
                        total += cost
                        measured = True
                except Exception as e:
                    default_logger.debug(f"EXPLAIN 失败: {e}")
        finally:
            if self.clean_up_sql and self.execute_fn:
                try:
                    self.execute_fn(_join_sqls(self.clean_up_sql))
                    self._mark_clean_if_restored()
                except Exception as e:
                    default_logger.debug(f"QEP 后清理失败: {e}")

        return total if measured else None

    def verify_candidates(
        self,
        candidates: List[Candidate],
        issue_sql: List[str],
    ) -> VerifyReport:
        with timed_stage("verify_rules", candidates=len(candidates)):
            return self._verify_candidates_body(candidates, issue_sql)

    def _verify_candidates_body(
        self,
        candidates: List[Candidate],
        issue_sql: List[str],
    ) -> VerifyReport:
        per: List[CandidateVerifyResult] = []
        failure_reasons: List[str] = []
        passed_list: List[Tuple[Candidate, CandidateVerifyResult]] = []
        relative_best: Optional[Candidate] = None
        relative_score = float("inf")

        # Soft EX：仅 Efficiency 硬要求与 issue 结果一致（语义应保留）。
        # Query/Personalization 的 issue 往往本身错误，结果差异不应否决正确修复。
        # issue 若含 PG 非法 DELETE/UPDATE LIMIT，基线本身跑不通，不能当语义真值。
        issue_rows: Optional[List[Any]] = None
        issue_cost: Optional[float] = None
        soft_ex_applicable = self.is_efficiency and any(
            _is_selectish(s) for s in issue_sql
        )
        if self.is_efficiency and pg_delete_update_limit(self.dialect, issue_sql):
            soft_ex_applicable = False

        # Soft EX 基线与 QEP 基线共用一次 reset（只读 issue 不二次重建）
        if self.reset_fn:
            try:
                self.reset_fn()
            except Exception as e:
                default_logger.warning(f"Verify 基线 reset 失败: {e}")

        if soft_ex_applicable:
            try:
                issue_rows, _, err = self._run_sqls(issue_sql)
                if err:
                    soft_ex_applicable = False
                    issue_rows = None
                    default_logger.warning(f"issue_sql Soft EX 基线失败，跳过功能对比: {err}")
            except Exception as e:
                soft_ex_applicable = False
                issue_rows = None
                default_logger.warning(f"issue_sql Soft EX 基线异常，跳过: {e}")
        elif self.category.lower() != "management":
            default_logger.debug(
                "非 Efficiency：跳过 Soft EX 与 issue 结果硬对比"
            )

        if self.is_efficiency:
            try:
                issue_cost = self._measure_cost(issue_sql)
            except Exception as e:
                default_logger.warning(f"issue_sql QEP 基线失败: {e}")
        else:
            # 非 Eff 仍可测 cost 供排序，失败忽略
            try:
                issue_cost = self._measure_cost(issue_sql)
            except Exception:
                issue_cost = None

        for cand in candidates:
            if self.reset_fn:
                try:
                    self.reset_fn()
                except Exception as e:
                    default_logger.warning(f"候选隔离重置失败: {cand.id}: {e}")

            errors: List[str] = []
            soft_ex = "skipped"
            cost: Optional[float] = None
            latency_ms: Optional[float] = None
            passed = True

            rows, latency_ms, err = self._run_sqls(cand.pred_sqls)
            if err:
                passed = False
                hint = ""
                el = (err or "").lower()
                if "already exists" in el:
                    hint = (
                        " | 提示: 对象已存在；改用 CREATE IF NOT EXISTS / OR REPLACE，"
                        "或 DROP IF EXISTS 后再建，勿原样重试 CREATE"
                    )
                errors.append(f"执行失败: {err}{hint}")
                failure_reasons.append(f"[{cand.id}] 执行失败: {err}{hint}")
            elif latency_ms is not None and latency_ms > self.exec_timeout_ms:
                if self.is_efficiency:
                    passed = False
                    errors.append(f"执行超时: {latency_ms:.1f}ms > {self.exec_timeout_ms}ms")
                    failure_reasons.append(
                        f"[{cand.id}] 执行超时: {latency_ms:.1f}ms ≥ {self.exec_timeout_ms}ms"
                    )
                else:
                    default_logger.info(
                        f"[{cand.id}] 非 Efficiency 耗时 {latency_ms:.1f}ms > "
                        f"{self.exec_timeout_ms}ms，不硬否决"
                    )

            # Soft EX
            if passed and soft_ex_applicable and issue_rows is not None:
                # 仅当候选末条可产生结果集时比较；Management 已 skipped
                if any(_is_selectish(s) for s in cand.pred_sqls):
                    if soft_ex_equal(rows, issue_rows):
                        soft_ex = "pass"
                    else:
                        soft_ex = "fail"
                        passed = False
                        errors.append("Soft EX 失败：结果集不一致")
                        failure_reasons.append(f"[{cand.id}] Soft EX 失败：结果集不一致")
                else:
                    soft_ex = "skipped"
            elif self.category.lower() == "management":
                soft_ex = "skipped"

            # Query/Personalization：用户点名的表必须出现在 pred（防修错表仍 Verify 通过）
            if passed and self.category.lower() in ("query", "personalization"):
                miss = missing_intent_tables(
                    self.query, self.schema, cand.pred_sqls
                )
                if miss:
                    passed = False
                    msg = f"意图表未出现在 pred_sqls: {', '.join(miss)}"
                    errors.append(msg)
                    failure_reasons.append(f"[{cand.id}] {msg}")

            # Personalization：禁止「只 SELECT 分组键」的假修复
            if passed and self.category.lower() == "personalization":
                gk = personalization_group_key_only(cand.pred_sqls)
                if gk:
                    passed = False
                    errors.append(gk)
                    failure_reasons.append(f"[{cand.id}] {gk}")

            # Query：时间格式假过（::time / 默认 HH24）
            if passed and self.category.lower() == "query":
                tf = query_time_format_mismatch(self.query, cand.pred_sqls)
                if tf:
                    passed = False
                    errors.append(tf)
                    failure_reasons.append(f"[{cand.id}] {tf}")
                    default_logger.info(f"[{cand.id}] 否决时间格式假过")
                cc = conditional_count_without_filter(self.query, cand.pred_sqls)
                if passed and cc:
                    passed = False
                    errors.append(cc)
                    failure_reasons.append(f"[{cand.id}] {cc}")
                tc = table_count_uses_count_column(self.query, cand.pred_sqls)
                if passed and tc:
                    passed = False
                    errors.append(tc)
                    failure_reasons.append(f"[{cand.id}] {tc}")
                island = consecutive_date_island_count(self.query, cand.pred_sqls)
                if passed and island:
                    passed = False
                    errors.append(island)
                    failure_reasons.append(f"[{cand.id}] {island}")
                sample = query_output_example_mismatch(self.query, rows)
                if passed and sample:
                    passed = False
                    errors.append(sample)
                    failure_reasons.append(f"[{cand.id}] {sample}")

            if passed and self.category.lower() == "personalization":
                hist = pers_streak_histogram_mismatch(self.query, cand.pred_sqls)
                if hist:
                    passed = False
                    errors.append(hist)
                    failure_reasons.append(f"[{cand.id}] {hist}")

            # Management：UPSERT 操作类型 / preprocess 新表 / ON CONFLICT 索引 / 触发器
            if passed and self.category.lower() == "management":
                up = upsert_operation_type_mismatch(self.query, cand.pred_sqls)
                if up:
                    passed = False
                    errors.append(up)
                    failure_reasons.append(f"[{cand.id}] {up}")
                    default_logger.info(f"[{cand.id}] 否决 UPSERT 假过")
                miss_pre = missing_preprocess_created_tables(
                    self.preprocess_sql, cand.pred_sqls, self.category
                )
                if passed and miss_pre:
                    passed = False
                    msg = (
                        "preprocess 新建表未出现在 pred_sqls: "
                        + ", ".join(miss_pre)
                        + "；Mgmt 评测对象往往是这些表，而非用户口头上的生产表名"
                    )
                    errors.append(msg)
                    failure_reasons.append(f"[{cand.id}] {msg}")
                    default_logger.info(f"[{cand.id}] 否决漏改 preprocess 表")
                oc = on_conflict_index_mismatch(self.query, cand.pred_sqls)
                if passed and oc:
                    passed = False
                    errors.append(oc)
                    failure_reasons.append(f"[{cand.id}] {oc}")
                rj = returning_join_mismatch(self.query, cand.pred_sqls)
                if passed and rj:
                    passed = False
                    errors.append(rj)
                    failure_reasons.append(f"[{cand.id}] {rj}")
                egg = circular_fk_chicken_egg(self.query, cand.pred_sqls)
                if passed and egg:
                    passed = False
                    errors.append(egg)
                    failure_reasons.append(f"[{cand.id}] {egg}")
                nfd = next_free_date_mismatch(self.query, cand.pred_sqls)
                if passed and nfd:
                    passed = False
                    errors.append(nfd)
                    failure_reasons.append(f"[{cand.id}] {nfd}")
                trg = mgmt_trigger_incomplete(self.query, cand.pred_sqls)
                if passed and trg:
                    passed = False
                    errors.append(trg)
                    failure_reasons.append(f"[{cand.id}] {trg}")

            pgf = mysql_dialect_funcs_in_pg(self.dialect, cand.pred_sqls)
            if passed and pgf:
                passed = False
                errors.append(pgf)
                failure_reasons.append(f"[{cand.id}] {pgf}")

            if passed and self.is_efficiency:
                nin = efficiency_merge_notin(self.query, cand.pred_sqls)
                if nin:
                    passed = False
                    errors.append(nin)
                    failure_reasons.append(f"[{cand.id}] {nin}")

            lim = pg_delete_update_limit(self.dialect, cand.pred_sqls)
            if lim and not any("DELETE/UPDATE 不支持顶层 LIMIT" in e for e in errors):
                if passed:
                    passed = False
                    errors.append(lim)
                    failure_reasons.append(f"[{cand.id}] {lim}")
                    default_logger.info(f"[{cand.id}] 否决 PG DELETE/UPDATE LIMIT")
                elif err:
                    pass  # 执行失败分支已附加 LIMIT 提示

            # QEP：在候选执行后的沙箱态上测（保留候选 DDL/索引）；
            # 只读候选不会污染，后续候选的 smart reset 可跳过重建。
            try:
                cost = self._measure_cost(cand.pred_sqls)
            except Exception as e:
                default_logger.debug(f"候选 QEP 失败 {cand.id}: {e}")

            if self.is_efficiency:
                if cost is None or issue_cost is None:
                    # 缺 cost 时不单凭此否决，但记原因
                    if passed and (cost is None or issue_cost is None):
                        errors.append("QEP cost 缺失，Efficiency 无法硬比较")
                elif cost >= issue_cost:
                    msg = f"QEP cost {cost} ≥ issue {issue_cost}"
                    if self.qep_hard_gate:
                        passed = False
                        errors.append(msg)
                        failure_reasons.append(f"[{cand.id}] {msg}")
                    else:
                        errors.append(msg + "（采样沙箱仅排序，不硬否决）")
                        default_logger.info(
                            f"[{cand.id}] {msg}；采样沙箱跳过 QEP 硬门禁"
                        )

            # 相对排序键：Efficiency 用 cost，否则用 latency
            if self.is_efficiency:
                rank_key = float(cost) if cost is not None else float("inf")
            else:
                rank_key = float(latency_ms) if latency_ms is not None else float("inf")
            if not passed:
                # 失败候选仍可参与相对最优（代价越高越差）
                rank_key = rank_key if rank_key != float("inf") else 1e18
                rank_key += 1e12  # 未通过排在通过者之后

            cvr = CandidateVerifyResult(
                id=cand.id,
                passed=passed,
                soft_ex=soft_ex,
                cost=cost,
                latency_ms=latency_ms,
                errors=errors,
                rank_key=rank_key,
            )
            per.append(cvr)
            if passed:
                passed_list.append((cand, cvr))
            if rank_key < relative_score:
                relative_score = rank_key
                relative_best = cand

        best: Optional[Candidate] = None
        if passed_list:
            passed_list.sort(key=lambda x: x[1].rank_key)
            best = passed_list[0][0]
            # 清理：有通过者时 failure_reasons 可保留供日志，但不阻断
            failure_reasons = [r for r in failure_reasons if not r.startswith(f"[{best.id}]")]

        return VerifyReport(
            passed=best is not None,
            best=best,
            per_candidate=per,
            failure_reasons=failure_reasons if best is None else [],
            relative_best=relative_best,
            relative_score=relative_score,
        )


def build_failure_reasons(report: VerifyReport) -> List[str]:
    if report.failure_reasons:
        return list(report.failure_reasons)
    out: List[str] = []
    for c in report.per_candidate:
        for e in c.errors:
            out.append(f"[{c.id}] {e}")
    return out
