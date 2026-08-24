"""前后置 SQL 列表规范化与包裹（对齐 BIRD 评测：preprocess → body → clean_up）。"""

from __future__ import annotations

import re
from typing import Any, List, Optional, Sequence


def is_effectively_empty_sql(sql: str) -> bool:
    """去掉注释与空白后是否无可执行内容（psycopg 会对纯注释报 empty query）。"""
    if not sql or not str(sql).strip():
        return True
    s = str(sql)
    s = re.sub(r"/\*[\s\S]*?\*/", "", s)
    s = re.sub(r"--[^\n]*", "", s)
    return not s.strip().strip(";")


def split_sql_statements(sql: str) -> List[str]:
    """按分号拆分 SQL，尊重单引号 / 双引号 / PostgreSQL dollar-quote / 注释。

    避免把 ``CREATE FUNCTION ... $tag$ ...; ... $tag$`` 内部的分号当成语句边界。
    """
    if not sql or not str(sql).strip():
        return []

    text = str(sql)
    out: List[str] = []
    buf: List[str] = []
    i = 0
    n = len(text)

    def flush() -> None:
        s = "".join(buf).strip()
        buf.clear()
        if s:
            out.append(s)

    while i < n:
        ch = text[i]

        # line comment
        if ch == "-" and i + 1 < n and text[i + 1] == "-":
            while i < n and text[i] != "\n":
                buf.append(text[i])
                i += 1
            continue

        # block comment
        if ch == "/" and i + 1 < n and text[i + 1] == "*":
            buf.append(text[i])
            buf.append(text[i + 1])
            i += 2
            while i < n - 1 and not (text[i] == "*" and text[i + 1] == "/"):
                buf.append(text[i])
                i += 1
            if i < n:
                buf.append(text[i])
                i += 1
            if i < n:
                buf.append(text[i])
                i += 1
            continue

        # single-quoted string (SQL standard; '' escape)
        if ch == "'":
            buf.append(ch)
            i += 1
            while i < n:
                buf.append(text[i])
                if text[i] == "'":
                    if i + 1 < n and text[i + 1] == "'":
                        buf.append(text[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue

        # double-quoted identifier
        if ch == '"':
            buf.append(ch)
            i += 1
            while i < n:
                buf.append(text[i])
                if text[i] == '"':
                    if i + 1 < n and text[i + 1] == '"':
                        buf.append(text[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue

        # dollar-quote: $tag$ ... $tag$
        if ch == "$":
            j = i + 1
            while j < n and (text[j].isalnum() or text[j] == "_"):
                j += 1
            if j < n and text[j] == "$":
                tag = text[i : j + 1]
                buf.append(tag)
                i = j + 1
                while i < n:
                    if text.startswith(tag, i):
                        buf.append(tag)
                        i += len(tag)
                        break
                    buf.append(text[i])
                    i += 1
                continue

        if ch == ";":
            flush()
            i += 1
            continue

        buf.append(ch)
        i += 1

    flush()
    return [s for s in out if not is_effectively_empty_sql(s)]


def as_sql_list(sqls: Any) -> List[str]:
    if sqls is None:
        return []
    if isinstance(sqls, str):
        s = sqls.strip()
        if not s:
            return []
        # 题面偶发用 ;; 分隔 —— 交给 quote-aware splitter 自然产生空段并丢弃
        return split_sql_statements(s.replace(";;", ";")) or [s]
    if isinstance(sqls, (list, tuple)):
        out: List[str] = []
        for item in sqls:
            out.extend(as_sql_list(item))
        return out
    s = str(sqls).strip()
    return [s] if s else []


def wrap_with_pre_post(
    body: Sequence[str] | str | None,
    *,
    preprocess_sql: Any = None,
    clean_up_sql: Any = None,
) -> List[str]:
    """返回 preprocess + body + clean_up 的顺序执行列表。"""
    pre = as_sql_list(preprocess_sql)
    mid = as_sql_list(body)
    post = as_sql_list(clean_up_sql)
    return [*pre, *mid, *post]


def has_cleanup(clean_up_sql: Any) -> bool:
    return bool(as_sql_list(clean_up_sql))
