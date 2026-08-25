"""PG：jsonb 列误用 ARRAY 字面量时的可重试改写（泛化，非单题）。"""

from __future__ import annotations

import re

_JSONB_ARRAY_TYPE_ERR = re.compile(
    r"is of type jsonb but expression is of type \w+\[\]",
    re.I,
)


def is_jsonb_array_type_error(message: str) -> bool:
    return bool(_JSONB_ARRAY_TYPE_ERR.search(message or ""))


def _find_array_spans(sql: str) -> list[tuple[int, int]]:
    """返回 ARRAY[...] 字面量的 [start, end) 下标（含 ARRAY 与闭括号）。"""
    spans: list[tuple[int, int]] = []
    i = 0
    n = len(sql)
    upper = sql.upper()
    while True:
        j = upper.find("ARRAY[", i)
        if j < 0:
            break
        k = j + 6
        depth = 1
        in_str = False
        closed = False
        while k < n:
            ch = sql[k]
            if in_str:
                if ch == "'" and k + 1 < n and sql[k + 1] == "'":
                    k += 2
                    continue
                if ch == "'":
                    in_str = False
                k += 1
                continue
            if ch == "'":
                in_str = True
                k += 1
                continue
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    spans.append((j, k + 1))
                    i = k + 1
                    closed = True
                    break
            k += 1
        if not closed:
            break
    return spans


def wrap_array_literals_as_jsonb(sql: str) -> str:
    """把未包裹的 ARRAY[...] 改为 to_jsonb(ARRAY[...])。"""
    if not sql or "ARRAY[" not in sql.upper():
        return sql
    spans = _find_array_spans(sql)
    if not spans:
        return sql
    chunks: list[str] = []
    last = 0
    changed = False
    for start, end in spans:
        before = sql[:start].rstrip()
        if before.lower().endswith("to_jsonb("):
            continue
        chunks.append(sql[last:start])
        chunks.append("to_jsonb(")
        chunks.append(sql[start:end])
        chunks.append(")")
        last = end
        changed = True
    if not changed:
        return sql
    chunks.append(sql[last:])
    return "".join(chunks)
