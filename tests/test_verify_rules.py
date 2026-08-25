"""Verify 规则函数特征测试：锁定每条规则的触发/不触发行为与消息文案锚点。

重构（规则注册表化、迁移 optimization/rules/）前后必须保持全等。
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from optimization.verify import (
    circular_fk_chicken_egg,
    conditional_count_without_filter,
    consecutive_date_island_count,
    efficiency_merge_notin,
    extract_preprocess_created_tables,
    missing_intent_tables,
    missing_preprocess_created_tables,
    mgmt_trigger_incomplete,
    mysql_dialect_funcs_in_pg,
    next_free_date_mismatch,
    on_conflict_index_mismatch,
    personalization_group_key_only,
    pers_streak_histogram_mismatch,
    pg_delete_update_limit,
    query_output_example_mismatch,
    query_time_format_mismatch,
    returning_join_mismatch,
    table_count_uses_count_column,
    upsert_operation_type_mismatch,
)
from optimization.rules.base import RuleContext
from optimization.rules.spec_rules import (
    spec_output_columns_missing,
    spec_predicate_required,
    spec_unresolved_filter_risk,
)
from optimization.intent_spec import AggFilter, IntentSpec, OutputColumn

SCHEMA = """
-- 表: customers
CREATE TABLE customers (id int primary key, name text);
-- 表: orders
CREATE TABLE orders (id int primary key, customer_id int);
"""


# ---------- 通用规则 ----------

def test_missing_intent_tables_triggered():
    q = "Show data from the customers table for last week"
    got = missing_intent_tables(q, SCHEMA, ["SELECT 1"])
    assert got == ["customers"]


def test_missing_intent_tables_pass():
    q = "Show data from the customers table for last week"
    assert missing_intent_tables(q, SCHEMA, ["SELECT * FROM customers"]) == []


def test_missing_intent_tables_empty_query():
    assert missing_intent_tables("", SCHEMA, ["SELECT 1"]) == []


def test_missing_preprocess_created_tables_mgmt():
    pre = "CREATE TABLE metrics_daily AS SELECT 1; CREATE TEMP TABLE helper(x int)"
    got = missing_preprocess_created_tables(pre, ["INSERT INTO metrics_daily SELECT 1"], "Management")
    assert got == ["helper"]


def test_missing_preprocess_created_tables_only_mgmt():
    pre = "CREATE TABLE helper(x int)"
    # Efficiency 的 helper 表允许不被 body 引用
    assert missing_preprocess_created_tables(pre, ["SELECT 1"], "Efficiency") == []


def test_extract_preprocess_created_tables():
    pre = "CREATE TABLE foo(a int); CREATE TABLE IF NOT EXISTS bar.baz(b int); CREATE TEMPORARY TABLE tmp(c int)"
    assert extract_preprocess_created_tables(pre) == ["foo", "baz", "tmp"]
    assert extract_preprocess_created_tables(None) == []


def test_pg_delete_update_limit_triggered():
    msg = pg_delete_update_limit("postgresql", ["DELETE FROM t WHERE id IN (SELECT id FROM x LIMIT 3) LIMIT 5"])
    assert msg and "LIMIT" in msg


def test_pg_delete_update_limit_subquery_ok():
    assert pg_delete_update_limit("postgresql", ["DELETE FROM t WHERE id IN (SELECT id FROM x LIMIT 3)"]) is None
    assert pg_delete_update_limit("mysql", ["DELETE FROM t LIMIT 5"]) is None
    assert pg_delete_update_limit("postgresql", ["SELECT * FROM t LIMIT 5"]) is None


def test_mysql_dialect_funcs_in_pg():
    assert mysql_dialect_funcs_in_pg("postgresql", ["SELECT charindex('a', b) FROM t"]) is not None
    assert mysql_dialect_funcs_in_pg("postgresql", ["SELECT strpos('a', b) FROM t"]) is None
    assert mysql_dialect_funcs_in_pg("mysql", ["SELECT charindex('a', b) FROM t"]) is None


def test_conditional_count_without_filter():
    q = "count attendees only if they attended both days"
    assert conditional_count_without_filter(q, ["SELECT COUNT(*) FROM t"]) is not None
    assert conditional_count_without_filter(q, ["SELECT COUNT(*) FILTER (WHERE ok) FROM t"]) is None
    assert conditional_count_without_filter(q, ["SELECT COUNT(CASE WHEN ok THEN 1 END) FROM t"]) is None


# ---------- Query 单题规则 ----------

def test_query_time_format_mismatch_triggered():
    q = "show the time as hh:mm:ss without leading zero"
    assert query_time_format_mismatch(q, ["SELECT event_time::time FROM t"]) is not None


def test_query_time_format_mismatch_fmhh_pass():
    q = "show the time as hh:mm:ss without leading zero"
    assert query_time_format_mismatch(q, ["SELECT to_char(event_time, 'FMHH:MI:SS') FROM t"]) is None


def test_query_time_format_mismatch_24h_pass():
    q = "show the time as hh:mm:ss in 24-hour clock"
    assert query_time_format_mismatch(q, ["SELECT event_time::time FROM t"]) is None


def test_table_count_uses_count_column():
    q = "the number of attendees does not match the number of seats"
    assert table_count_uses_count_column(q, ["SELECT b.count FROM budget b"]) is not None
    assert table_count_uses_count_column(q, ["SELECT COUNT(*) FROM budget GROUP BY event_id"]) is None


def test_consecutive_date_island_count():
    q = "count consecutive releaseDate runs per title"
    assert consecutive_date_island_count(q, ["SELECT COUNT(*) - 1 FROM dates"]) is not None
    assert consecutive_date_island_count(q, ["SELECT MAX(d) - MIN(d) + 1 FROM dates"]) is None
    assert consecutive_date_island_count("count consecutive rows", ["SELECT COUNT(*) - 1 FROM t"]) is None


def test_query_output_example_mismatch():
    q = "expected result should be output like (3, 2023-01-01, 2023-01-05)"
    assert query_output_example_mismatch(q, []) is not None
    rows = [{"n": 3, "a": "2023-01-01", "b": "2023-01-05"}]
    assert query_output_example_mismatch(q, rows) is None
    assert query_output_example_mismatch("plain query", []) is None


# ---------- Personalization 规则 ----------

def test_personalization_group_key_only_key_projection():
    assert personalization_group_key_only(["SELECT artist FROM cards GROUP BY artist"]) is not None
    assert personalization_group_key_only(["SELECT a, b FROM t GROUP BY a, b"]) is not None
    assert personalization_group_key_only(["SELECT * FROM t GROUP BY a"]) is not None


def test_personalization_group_key_only_aggregate_ok():
    assert personalization_group_key_only(["SELECT a, MAX(b) FROM t GROUP BY a"]) is None
    assert personalization_group_key_only(["SELECT DISTINCT ON (a) * FROM t ORDER BY a, id"]) is None


def test_pers_streak_histogram_mismatch():
    q = "count how many users have each longest streak"
    bad = ["WITH s AS (SELECT user_id, streak FROM u) SELECT * FROM s ORDER BY streak LIMIT 1"]
    assert pers_streak_histogram_mismatch(q, bad) is not None
    good = ["SELECT streak_length, COUNT(*) FROM s GROUP BY 1"]
    assert pers_streak_histogram_mismatch(q, good) is None


# ---------- Management 规则 ----------

def test_upsert_operation_type_mismatch_triggered():
    q = "I want to know whether an insert or an update operation was performed"
    assert upsert_operation_type_mismatch(q, ["INSERT INTO t VALUES (1)"]) is not None


def test_upsert_operation_type_mismatch_returning_labels_pass():
    q = "I want to know whether an insert or an update operation was performed"
    pred = [
        "INSERT INTO t VALUES (1) ON CONFLICT (a) DO UPDATE SET b = 1 "
        "RETURNING *, CASE WHEN xmax = 0 THEN 'INSERT' ELSE 'UPDATE' END AS operation_type"
    ]
    assert upsert_operation_type_mismatch(q, pred) is None


def test_on_conflict_index_mismatch_triggered():
    q = "ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification"
    pred = ["INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a, b) DO UPDATE SET b = 2"]
    assert on_conflict_index_mismatch(q, pred) is not None


def test_on_conflict_index_mismatch_covered_pass():
    q = "ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification"
    pred = [
        "CREATE UNIQUE INDEX ON t (a, b);",
        "SELECT 1;",
    ]
    assert on_conflict_index_mismatch(q, pred) is None


def test_returning_join_mismatch_triggered():
    q = "update the row and return the joined row"
    assert returning_join_mismatch(q, ["UPDATE t SET a = 1 RETURNING *"]) is not None


def test_returning_join_mismatch_join_projected_pass():
    q = "update the row and return the joined row"
    pred = ["UPDATE t SET a = 1 FROM u JOIN u2 ON u.id = u2.id RETURNING t.a, u.b"]
    assert returning_join_mismatch(q, pred) is None


def test_circular_fk_chicken_egg_triggered():
    q = "solve the chicken-egg foreign key problem"
    pred = [
        "CREATE TABLE a(id int REFERENCES b(id));",
        "CREATE TABLE b(id int REFERENCES a(id));",
    ]
    assert circular_fk_chicken_egg(q, pred) is not None


def test_circular_fk_chicken_egg_single_direction_pass():
    q = "solve the chicken-egg foreign key problem"
    pred = ["CREATE TABLE a(id int);", "CREATE TABLE b(id int REFERENCES a(id));"]
    assert circular_fk_chicken_egg(q, pred) is None


def test_next_free_date_mismatch_triggered():
    q = "find the minimum available date not already taken"
    assert next_free_date_mismatch(q, ["INSERT INTO races VALUES (1, '2023-01-01')"]) is not None


def test_next_free_date_mismatch_function_pass():
    q = "find the minimum available date not already taken"
    pred = [
        "CREATE FUNCTION next_date(d date) RETURNS date AS $$ SELECT d $$ LANGUAGE sql;",
        "INSERT INTO seasons VALUES (2023);",
        "INSERT INTO races VALUES (1, '2023-01-01');",
    ]
    assert next_free_date_mismatch(q, pred) is None


def test_mgmt_trigger_incomplete():
    q = "create a trigger that fires automatically when a new row is inserted"
    assert mgmt_trigger_incomplete(q, ["CREATE FUNCTION f() RETURNS trigger AS $$ BEGIN END $$ LANGUAGE plpgsql;"]) is not None


def test_mgmt_trigger_complete_pass():
    q = "create a trigger that fires automatically when a new row is inserted"
    pred = [
        "CREATE FUNCTION f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;",
        "CREATE TRIGGER t AFTER INSERT ON events FOR EACH ROW EXECUTE FUNCTION f();",
    ]
    assert mgmt_trigger_incomplete(q, pred) is None


# ---------- Efficiency 规则 ----------

def test_efficiency_merge_notin():
    q = "merge the two separate NOT IN subqueries"
    bad = ["SELECT * FROM a WHERE x NOT IN (SELECT x FROM b) AND y NOT IN (SELECT y FROM c)"]
    assert efficiency_merge_notin(q, bad) is not None
    good = ["SELECT * FROM a WHERE x NOT IN (SELECT x FROM b UNION SELECT y FROM c)"]
    assert efficiency_merge_notin(q, good) is None


# ---------- Spec 规则 ----------

def _ctx(**kw):
    base = dict(
        dialect="postgresql",
        category="Query",
        category_lower="query",
        is_efficiency=False,
        query="",
        schema="",
        pred_sqls=[],
        preprocess_sql=[],
        rows=None,
        intent_spec=None,
    )
    base.update(kw)
    return RuleContext(**base)


def test_spec_output_columns_missing():
    spec = IntentSpec(output_columns=[OutputColumn(name="answered")])
    msg = spec_output_columns_missing(_ctx(pred_sqls=["SELECT id FROM users"], intent_spec=spec))
    assert msg and "answered" in msg


def test_spec_output_columns_pass():
    spec = IntentSpec(output_columns=[OutputColumn(name="answered")])
    assert spec_output_columns_missing(
        _ctx(pred_sqls=["SELECT answered FROM users"], intent_spec=spec)
    ) is None


def test_spec_predicate_required():
    spec = IntentSpec(must_predicates=["only if premium"])
    msg = spec_predicate_required(_ctx(pred_sqls=["SELECT COUNT(*) FROM users"], intent_spec=spec))
    assert msg and (("WHERE" in msg) or ("FILTER" in msg) or ("条件" in msg))


def test_spec_mgmt_artifacts_trigger():
    from optimization.rules.spec_rules import spec_mgmt_artifacts
    spec = IntentSpec(goal="mgmt", mgmt_artifacts=["function", "trigger"])
    ctx = _ctx(
        category="Management",
        category_lower="management",
        query="create a trigger that fires automatically when a new row is inserted",
        pred_sqls=["CREATE FUNCTION f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;"],
        intent_spec=spec,
    )
    msg = spec_mgmt_artifacts(ctx)
    assert msg and "TRIGGER" in msg.upper()


def test_spec_unresolved_filter_risk():
    spec = IntentSpec(agg_filters=[AggFilter(text="premium", unresolved=True)])
    msg = spec_unresolved_filter_risk(
        _ctx(pred_sqls=["SELECT COUNT(*) FILTER (WHERE x=1) FROM users"], intent_spec=spec)
    )
    assert msg and (("evidence" in msg.lower()) or ("FILTER" in msg))


def test_spec_predicate_skips_dml():
    spec = IntentSpec(must_predicates=["segment unique"])
    msg = spec_predicate_required(
        _ctx(
            pred_sqls=[
                "INSERT INTO customers (segment, currency) VALUES ('SME','EUR') "
                "ON CONFLICT (segment, currency) DO UPDATE SET currency = EXCLUDED.currency "
                "RETURNING customerid"
            ],
            intent_spec=spec,
        )
    )
    assert msg is None


def test_spec_output_arity_extra():
    from optimization.rules.spec_rules import spec_output_arity_extra

    q = "get both `transactionid` and `Segment` columns"
    msg = spec_output_arity_extra(
        _ctx(
            query=q,
            pred_sqls=["SELECT transactionid, amount, segment, currency FROM t"],
        )
    )
    assert msg and "4" in msg


def test_spec_output_arity_pass():
    from optimization.rules.spec_rules import spec_output_arity_extra

    q = "get both `transactionid` and `Segment` columns"
    assert (
        spec_output_arity_extra(
            _ctx(query=q, pred_sqls=["SELECT transactionid, segment FROM t"])
        )
        is None
    )


def test_spec_pers_full_row_rejects_agg_only():
    from optimization.rules.spec_rules import spec_pers_full_row

    spec = IntentSpec(pers_need_full_row=True)
    msg = spec_pers_full_row(
        _ctx(
            category="Personalization",
            category_lower="personalization",
            pred_sqls=["SELECT k, MIN(type) AS min_type FROM t GROUP BY k"],
            intent_spec=spec,
        )
    )
    assert msg and "代表行" in msg


def test_spec_pers_full_row_distinct_on_pass():
    from optimization.rules.spec_rules import spec_pers_full_row

    spec = IntentSpec(pers_need_full_row=True)
    assert (
        spec_pers_full_row(
            _ctx(
                category="Personalization",
                category_lower="personalization",
                pred_sqls=["SELECT DISTINCT ON (k) * FROM t ORDER BY k, id"],
                intent_spec=spec,
            )
        )
        is None
    )


def test_spec_diagnosed_table_missing():
    from optimization.intent_spec import Evidence
    from optimization.rules.spec_rules import spec_diagnosed_table_missing

    spec = IntentSpec(
        evidence=[Evidence(table="trans", sample_note="diag_preferred")]
    )
    msg = spec_diagnosed_table_missing(
        _ctx(pred_sqls=["SELECT account_id FROM loan"], intent_spec=spec)
    )
    assert msg and "trans" in msg


def test_spec_issue_shape_no_ddl_escalation():
    from optimization.rules.spec_rules import spec_issue_shape_no_ddl_escalation

    msg = spec_issue_shape_no_ddl_escalation(
        _ctx(
            category="Query",
            category_lower="query",
            query="window function for next bank",
            pred_sqls=["ALTER TABLE trans ADD COLUMN next_bank text", "SELECT 1"],
            issue_sql=["SELECT first_value(bank) OVER (PARTITION BY account_id) FROM trans"],
        )
    )
    assert msg and "ADD COLUMN" in msg


def test_spec_dynamic_sql_single_stmt():
    from optimization.rules.spec_rules import spec_dynamic_sql_single_stmt

    msg = spec_dynamic_sql_single_stmt(
        _ctx(
            query="I tried to dynamically generate a SQL query",
            pred_sqls=[
                "SELECT 'SELECT o.a FROM account AS o' AS sqlstmt",
                "SELECT o.district_id FROM account AS o",
            ],
        )
    )
    assert msg and "动态" in msg


def test_spec_mgmt_column_type():
    from optimization.rules.spec_rules import spec_mgmt_artifacts

    spec = IntentSpec(mgmt_artifacts=["column_type"])
    msg = spec_mgmt_artifacts(
        _ctx(
            category="Management",
            category_lower="management",
            query="convert column to timestamp type",
            pred_sqls=["ALTER TABLE account ADD COLUMN date__timestamp timestamp"],
            intent_spec=spec,
        )
    )
    assert msg and "ALTER COLUMN" in msg


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
    print("all verify rules tests passed")
