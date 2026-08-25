"""SQL / 题面形态纯函数：输出列契约、DDL 升温、表名抽取（非单题规则）。"""

from __future__ import annotations

import re
from typing import List, Optional, Sequence, Set

_IDENT = r"[A-Za-z_][\w]*"
_FROM_TBL = re.compile(
    rf"\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+(?:ONLY\s+)?(?:[\w]+\.)?[`\"]?({_IDENT})[`\"]?",
    re.I,
)
_DDL_DML = re.compile(
    r"\b(?:CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|TRUNCATE|GRANT|REVOKE)\b",
    re.I,
)
_SELECTISH = re.compile(r"^\s*(?:WITH|SELECT|SHOW|EXPLAIN)\b", re.I)
_ADD_COLUMN = re.compile(r"\bALTER\s+TABLE\b[\s\S]{0,80}\bADD\s+COLUMN\b", re.I)
_ALTER_TYPE = re.compile(
    r"\bALTER\s+(?:TABLE\s+\S+\s+)?(?:ALTER\s+COLUMN\s+\w+\s+)?TYPE\b",
    re.I,
)


def _blob(sqls: Sequence[str] | str | None) -> str:
    if sqls is None:
        return ""
    if isinstance(sqls, str):
        return sqls
    return "\n".join(str(s) for s in sqls)


def tables_mentioned_in_sql(sqls: Sequence[str] | str | None) -> Set[str]:
    found: Set[str] = set()
    for m in _FROM_TBL.finditer(_blob(sqls)):
        name = (m.group(1) or "").lower()
        if name and name not in {"select", "lateral", "only"}:
            found.add(name)
    return found


def schema_table_names(schema: str) -> Set[str]:
    names: Set[str] = set()
    for m in re.finditer(
        r"--\s*表:\s*(\w+)|CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\w]+\.)?[`\"]?(\w+)",
        schema or "",
        re.I,
    ):
        t = m.group(1) or m.group(2)
        if t:
            names.add(t.lower())
    return names


def preferred_tables_from_diagnosis(
    diagnosis: str,
    issue_sql: Sequence[str] | str | None,
    schema: str,
) -> List[str]:
    """诊断建议的表 − issue_sql 已用表，且必须在 schema 中。"""
    allowed = schema_table_names(schema)
    if not allowed:
        return []
    issue_t = tables_mentioned_in_sql(issue_sql)
    diag_t = tables_mentioned_in_sql(diagnosis)
    extra = [t for t in sorted(diag_t) if t not in issue_t and t in allowed]
    return extra[:8]


def named_output_columns(query: str) -> List[str]:
    """从题面抽取用户点名的输出列（get both `a` and `b` / 反引号列）。"""
    q = query or ""
    out: List[str] = []
    seen: Set[str] = set()

    def _add(raw: str) -> None:
        name = (raw or "").strip().strip("`\"")
        if "." in name:
            name = name.split(".")[-1]
        key = name.lower()
        if not name or key in seen or key in {"table", "column", "sql"}:
            return
        seen.add(key)
        out.append(name)

    for m in re.finditer(
        r"(?:get|return|returning)\s+both\s+`([^`]+)`\s+and\s+`([^`]+)`",
        q,
        re.I,
    ):
        _add(m.group(1))
        _add(m.group(2))
    if len(out) >= 2:
        return out[:8]

    if re.search(r"\b(?:return|returning|get both|output)\b", q, re.I):
        for m in re.finditer(r"`([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)`", q):
            _add(m.group(1))
    return out[:8]


def _split_top_level_commas(expr: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    i = 0
    while i < len(expr):
        ch = expr[i]
        if ch == "'":
            buf.append(ch)
            i += 1
            while i < len(expr):
                buf.append(expr[i])
                if expr[i] == "'" and (i + 1 >= len(expr) or expr[i + 1] != "'"):
                    i += 1
                    break
                if expr[i] == "'" and i + 1 < len(expr) and expr[i + 1] == "'":
                    buf.append(expr[i + 1])
                    i += 2
                    continue
                i += 1
            continue
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _last_projection_clause(sql: str) -> Optional[str]:
    s = (sql or "").strip().rstrip(";")
    if not s:
        return None
    m_ret = None
    for m in re.finditer(r"\bRETURNING\b", s, re.I):
        m_ret = m
    if m_ret and not re.search(r"\bSELECT\b", s[m_ret.end() :], re.I):
        rest = s[m_ret.end() :].strip()
        rest = re.split(r"\b(?:ORDER|LIMIT|OFFSET|ON\s+CONFLICT)\b", rest, maxsplit=1, flags=re.I)[0]
        return rest.strip()
    m_sel = None
    for m in re.finditer(r"\bSELECT\b", s, re.I):
        m_sel = m
    if not m_sel:
        return None
    rest = s[m_sel.end() :].strip()
    rest = re.split(r"\bFROM\b", rest, maxsplit=1, flags=re.I)[0]
    return rest.strip()


def last_projection_arity(pred_sqls: Sequence[str] | None) -> Optional[int]:
    """末条可产生结果集语句的投影列数；SELECT * / RETURNING * 返回 None。"""
    stmts = [str(s).strip() for s in (pred_sqls or []) if str(s).strip()]
    if not stmts:
        return None
    clause = _last_projection_clause(stmts[-1])
    if not clause:
        return None
    if clause.strip() == "*" or re.match(r"(?:\w+\.)?\*$", clause.strip()):
        return None
    parts = _split_top_level_commas(clause)
    parts = [p for p in parts if p and not p.upper().startswith("DISTINCT")]
    if not parts:
        return None
    # DISTINCT ON (...) col1, col2 — 上面可能把 ON 段混进第一列，仍计段数
    return len(parts)


def is_ddl_or_dml(sqls: Sequence[str] | str | None) -> bool:
    return bool(_DDL_DML.search(_blob(sqls)))


def issue_sql_is_readonly_select(issue_sql: Sequence[str] | str | None) -> bool:
    blob = _blob(issue_sql).strip()
    if not blob:
        return False
    if is_ddl_or_dml(blob):
        return False
    return bool(_SELECTISH.search(blob.lstrip("(")))


def pred_has_add_column(pred_sqls: Sequence[str] | None) -> bool:
    return bool(_ADD_COLUMN.search(_blob(pred_sqls)))


def pred_has_alter_column_type(pred_sqls: Sequence[str] | None) -> bool:
    blob = _blob(pred_sqls)
    return bool(
        re.search(r"\bALTER\s+COLUMN\s+\w+\s+TYPE\b", blob, re.I)
        or re.search(r"\bALTER\s+TABLE\s+\S+\s+ALTER\s+COLUMN\b[\s\S]{0,80}\bTYPE\b", blob, re.I)
    )


def query_wants_inplace_type_change(query: str) -> bool:
    q = query or ""
    if not re.search(r"\b(?:convert|cast|change|update)\b", q, re.I):
        return False
    if not re.search(r"\bcolumn\b", q, re.I):
        return False
    return bool(re.search(r"\b(?:timestamp|timestamptz|date|type)\b", q, re.I))


def query_is_dynamic_sql_generator(query: str) -> bool:
    q = query or ""
    return bool(
        re.search(
            r"dynamically generate|generate a SQL|output SQL was incorrect|sqlstmt",
            q,
            re.I,
        )
    )


def infer_mgmt_artifacts(query: str, issue_sql: Sequence[str] | str | None = None) -> List[str]:
    q = (query or "") + "\n" + _blob(issue_sql)
    arts: List[str] = []
    if re.search(r"\btrigger\b", q, re.I):
        arts.append("trigger")
    if re.search(r"\b(?:function|procedure)\b", q, re.I):
        arts.append("function")
    if re.search(r"\b(?:unique|on conflict|upsert)\b", q, re.I):
        arts.append("unique_index")
    if query_wants_inplace_type_change(query or ""):
        arts.append("column_type")
    # 去重保序
    seen: Set[str] = set()
    out: List[str] = []
    for a in arts:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out


def looks_like_group_aggregate_only(pred_sqls: Sequence[str] | None) -> bool:
    """GROUP BY + 聚合、无 DISTINCT ON / ROW_NUMBER / JOIN MIN(id) 代表行。"""
    blob = _blob(pred_sqls)
    if not re.search(r"\bGROUP\s+BY\b", blob, re.I):
        return False
    if re.search(r"\bDISTINCT\s+ON\b", blob, re.I):
        return False
    if re.search(r"\bROW_NUMBER\s*\(", blob, re.I):
        return False
    if re.search(r"JOIN\s*\(\s*SELECT\s+(?:MIN|MAX)\s*\(\s*\w+\s*\)", blob, re.I):
        return False
    return bool(re.search(r"\b(?:COUNT|SUM|AVG|MIN|MAX|ARRAY_AGG|STRING_AGG)\s*\(", blob, re.I))
