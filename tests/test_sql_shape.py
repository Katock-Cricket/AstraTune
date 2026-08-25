"""sql_shape 纯函数：输出列契约、诊断换表、DDL 升温。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from optimization.sql_shape import (
    infer_mgmt_artifacts,
    issue_sql_is_readonly_select,
    last_projection_arity,
    looks_like_group_aggregate_only,
    named_output_columns,
    preferred_tables_from_diagnosis,
    query_wants_inplace_type_change,
    tables_mentioned_in_sql,
)


def test_named_output_columns_both():
    q = (
        "I'd like it to return that row joined to customers and "
        "get both `transactions_1k.transactionid` and `customers.Segment` columns."
    )
    cols = named_output_columns(q)
    assert [c.lower() for c in cols] == ["transactionid", "segment"]


def test_last_projection_arity_select():
    n = last_projection_arity(
        [
            "WITH u AS (UPDATE t SET a=1 RETURNING a, b, c) "
            "SELECT u.a, u.b, x.s, x.c FROM u JOIN x ON true"
        ]
    )
    assert n == 4


def test_last_projection_star_unknown():
    assert last_projection_arity(["SELECT * FROM t"]) is None


def test_preferred_tables_from_diagnosis():
    schema = "-- 表: loan\nCREATE TABLE loan(id int);\n-- 表: trans\nCREATE TABLE trans(id int);"
    diag = "应查询交易明细：SELECT account_id FROM trans GROUP BY account_id"
    issue = ["SELECT account_id FROM loan GROUP BY account_id"]
    got = preferred_tables_from_diagnosis(diag, issue, schema)
    assert "trans" in got
    assert "loan" not in got


def test_issue_sql_readonly_select():
    assert issue_sql_is_readonly_select(["SELECT 1 FROM t"])
    assert not issue_sql_is_readonly_select(["ALTER TABLE t ADD COLUMN x int"])


def test_infer_mgmt_column_type():
    arts = infer_mgmt_artifacts(
        "convert the BIGINT column to a TIMESTAMP type",
        ["UPDATE account SET date__timestamp = date::timestamp"],
    )
    assert "column_type" in arts


def test_query_wants_inplace_type_change():
    assert query_wants_inplace_type_change(
        "The goal is to update this column to a TIMESTAMP type"
    )


def test_group_aggregate_only():
    assert looks_like_group_aggregate_only(
        ["SELECT pure_cardid, MIN(type) FROM t GROUP BY 1"]
    )
    assert not looks_like_group_aggregate_only(
        ["SELECT DISTINCT ON (k) * FROM t ORDER BY k, id"]
    )


def test_tables_mentioned():
    assert "budget" in tables_mentioned_in_sql(
        "SELECT b.link_to_event FROM budget b JOIN att ON true"
    )
