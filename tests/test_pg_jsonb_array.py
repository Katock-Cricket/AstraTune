"""ARRAY 字面量改写为 to_jsonb 的纯函数测试。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.pg_jsonb_array import is_jsonb_array_type_error, wrap_array_literals_as_jsonb


def test_is_jsonb_array_type_error():
    msg = 'column "gens" is of type jsonb but expression is of type text[]'
    assert is_jsonb_array_type_error(msg)
    assert is_jsonb_array_type_error(
        'column "feat_arts" is of type jsonb but expression is of type integer[]'
    )
    assert not is_jsonb_array_type_error("syntax error")


def test_wrap_array_literals():
    sql = "INSERT INTO t(gens) VALUES (ARRAY['a', 'b'])"
    out = wrap_array_literals_as_jsonb(sql)
    assert "to_jsonb(ARRAY['a', 'b'])" in out


def test_wrap_idempotent():
    sql = "INSERT INTO t(gens) VALUES (to_jsonb(ARRAY['a']))"
    assert wrap_array_literals_as_jsonb(sql) == sql


def test_wrap_nested_brackets():
    sql = "VALUES (ARRAY[1, 2], ARRAY[3])"
    out = wrap_array_literals_as_jsonb(sql)
    assert out.count("to_jsonb(") == 2
