"""SQL 是否可能改写沙箱数据/结构（用于智能 reset）。"""

from __future__ import annotations

import re

# 去掉字面量与注释后再判，降低误伤（列名含 update 等仍可能误判，偏保守即可）
_STR_RE = re.compile(
    r"('([^'\\]|\\.)*'|\"([^\"\\]|\\.)*\"|\$\$.*?\$\$)",
    re.DOTALL,
)
_LINE_COMMENT = re.compile(r"--.*?$", re.MULTILINE)
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)

# 数据或 schema 变更；含 SELECT ... FOR UPDATE
_MUTATE_RE = re.compile(
    r"""(?ix)
    \b(
        INSERT|UPDATE|DELETE|MERGE|UPSERT|REPLACE|
        CREATE|DROP|ALTER|TRUNCATE|RENAME|
        COPY|CALL|DO|EXECUTE|
        GRANT|REVOKE|
        VACUUM|ANALYZE|CLUSTER|REINDEX|REFRESH|
        LOCK
    )\b
    |
    \bFOR\s+UPDATE\b
    """,
)


def sql_may_mutate(sql: str | None) -> bool:
    """True = 可能改变表数据/结构/锁，下一候选前需要重建沙箱。"""
    if not sql or not str(sql).strip():
        return False
    text = str(sql)
    text = _BLOCK_COMMENT.sub(" ", text)
    text = _LINE_COMMENT.sub(" ", text)
    text = _STR_RE.sub("''", text)
    return _MUTATE_RE.search(text) is not None


def sqls_may_mutate(sqls) -> bool:
    if sqls is None:
        return False
    if isinstance(sqls, str):
        return sql_may_mutate(sqls)
    return any(sql_may_mutate(s) for s in sqls)
