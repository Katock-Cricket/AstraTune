"""Verify 确定性函数单测（无 LLM）。"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from optimization.types import Candidate
from optimization.verify import (
    Verifier,
    extract_mysql_query_cost,
    extract_pg_total_cost,
    is_efficiency_case,
    soft_ex_equal,
)


def test_soft_ex_equal_ignores_order():
    a = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
    b = [{"id": 2, "v": "b"}, {"id": 1, "v": "a"}]
    assert soft_ex_equal(a, b)
    assert not soft_ex_equal(a, [{"id": 1, "v": "a"}])


def test_extract_pg_total_cost():
    payload = [{"Plan": {"Total Cost": 42.5, "Node Type": "Seq Scan"}}]
    assert extract_pg_total_cost(payload) == 42.5
    assert extract_pg_total_cost('[{"Plan": {"Total Cost": 10}}]') == 10.0


def test_extract_mysql_query_cost():
    payload = {"query_block": {"cost_info": {"query_cost": "7.99"}}}
    assert extract_mysql_query_cost(payload) == 7.99
    wrapped = [{"EXPLAIN": '{"query_block":{"cost_info":{"query_cost":"3.5"}}}'}]
    assert extract_mysql_query_cost(wrapped) == 3.5


def test_is_efficiency_case():
    assert is_efficiency_case("Efficiency")
    assert is_efficiency_case("Query", efficiency=True)
    assert not is_efficiency_case("Query")
    assert not is_efficiency_case("", efficiency=False)


def _slow_execute(sql: str):
    import time

    time.sleep(0.05)
    return [{"sql": sql, "rows": [{"x": 1}], "row_count": 1}]


def test_timeout_hard_fail_on_efficiency():
    v = Verifier(
        dialect="postgresql",
        category="Efficiency",
        exec_timeout_ms=1,
        execute_fn=_slow_execute,
        explain_fn=lambda sql: [{"Plan": {"Total Cost": 1.0}}],
        reset_fn=lambda: None,
    )
    report = v.verify_candidates(
        [Candidate(id="c1", pred_sqls=["SELECT 1"])],
        ["SELECT 1"],
    )
    assert not report.passed
    assert any("超时" in r for r in report.failure_reasons)


def test_timeout_no_veto_on_non_efficiency():
    v = Verifier(
        dialect="postgresql",
        category="Query",
        exec_timeout_ms=1,
        execute_fn=_slow_execute,
        explain_fn=lambda sql: [{"Plan": {"Total Cost": 1.0}}],
        reset_fn=lambda: None,
    )
    report = v.verify_candidates(
        [Candidate(id="c1", pred_sqls=["SELECT 1"])],
        ["SELECT 1"],
    )
    assert report.passed
    assert report.best is not None
    assert not any("超时" in r for r in report.failure_reasons)


def test_soft_ex_hard_fail_on_efficiency():
    def execute_fn(sql: str):
        if "issue" in sql or sql.strip() == "SELECT 1":
            return [{"sql": sql, "rows": [{"x": 1}], "row_count": 1}]
        return [{"sql": sql, "rows": [{"x": 2}], "row_count": 1}]

    v = Verifier(
        dialect="postgresql",
        category="Efficiency",
        exec_timeout_ms=5000,
        execute_fn=execute_fn,
        explain_fn=lambda sql: [{"Plan": {"Total Cost": 1.0}}],
        reset_fn=lambda: None,
        efficiency=True,
    )
    report = v.verify_candidates(
        [Candidate(id="c1", pred_sqls=["SELECT 2 AS x"])],
        ["SELECT 1"],
    )
    assert not report.passed
    assert any("Soft EX" in r for r in report.failure_reasons)


def test_soft_ex_no_veto_on_query_when_differs():
    def execute_fn(sql: str):
        if sql.strip() == "SELECT 1":
            return [{"sql": sql, "rows": [{"x": 1}], "row_count": 1}]
        return [{"sql": sql, "rows": [{"x": 2}], "row_count": 1}]

    v = Verifier(
        dialect="postgresql",
        category="Query",
        exec_timeout_ms=5000,
        execute_fn=execute_fn,
        explain_fn=lambda sql: [{"Plan": {"Total Cost": 1.0}}],
        reset_fn=lambda: None,
    )
    report = v.verify_candidates(
        [Candidate(id="c1", pred_sqls=["SELECT 2 AS x"])],
        ["SELECT 1"],
    )
    assert report.passed
    assert report.best is not None
    assert not any("Soft EX" in r for r in report.failure_reasons)


def test_query_time_format_hh24_veto():
    from optimization.verify import query_time_format_mismatch

    q = (
        "extract only the hh:mm:ss part. desired: 0:00:00. "
        "current query includes leading zeros, which I don't want."
    )
    assert query_time_format_mismatch(
        q, ["SELECT to_char(CreationDate, 'FMHH24:MI:SS') FROM comments"]
    )
    assert query_time_format_mismatch(q, ["SELECT CreationDate::time FROM comments"])
    assert query_time_format_mismatch(
        q,
        [
            "SELECT format('%s:%s', EXTRACT(HOUR FROM creationdate), "
            "to_char(creationdate, 'MI:SS')) FROM comments"
        ],
    )
    assert (
        query_time_format_mismatch(
            q, ["SELECT to_char(CreationDate, 'FMHH:MI:SS') FROM comments"]
        )
        is None
    )
    q24 = q + " I need 24-hour clock like 13:00:00"
    assert (
        query_time_format_mismatch(
            q24, ["SELECT to_char(CreationDate, 'FMHH24:MI:SS') FROM comments"]
        )
        is None
    )


def test_upsert_operation_type_mismatch():
    from optimization.verify import upsert_operation_type_mismatch

    q = (
        "I want to know whether an INSERT or an UPDATE operation was performed. "
        "I attempted ON CONFLICT DO UPDATE."
    )
    bad = (
        "INSERT INTO account (account_id, district_id, frequency, date) "
        "VALUES (1, 18, 'POPLATEK MESICNE', CURRENT_DATE) "
        "ON CONFLICT (account_id) DO UPDATE SET date = CURRENT_DATE "
        "RETURNING account_id, (xmax = 0) AS is_insert"
    )
    good = (
        "INSERT INTO account (account_id, district_id, frequency, date) "
        "VALUES (1, 18, 'POPLATEK MESICNE', CURRENT_DATE) "
        "ON CONFLICT (account_id) DO UPDATE SET date = CURRENT_DATE "
        "RETURNING *, CASE WHEN xmax = 0 THEN 'INSERT' ELSE 'UPDATE' END AS operation_type"
    )
    assert upsert_operation_type_mismatch(q, [bad])
    assert upsert_operation_type_mismatch(q, [good]) is None
    # 无「是否 INSERT/UPDATE」诉求时不否决
    assert (
        upsert_operation_type_mismatch(
            "upsert attendance on conflict do update",
            ["INSERT INTO attendance VALUES (1) ON CONFLICT DO UPDATE SET attend = 1"],
        )
        is None
    )
    q_idx = (
        "I attempt to insert a new record using an UPSERT operation using the query "
        "on conflict (event_name, location, event_date) do update, "
        "I encounter an error stating that there is no unique or exclusion constraint"
    )
    assert (
        upsert_operation_type_mismatch(
            q_idx,
            [
                "CREATE UNIQUE INDEX unique_name_location ON event"
                "(event_name, location, event_date) "
                "WHERE coalesce(event_name, location) is not null;"
            ],
        )
        is None
    )


def test_missing_preprocess_created_tables_mgmt_only():
    from optimization.verify import missing_preprocess_created_tables

    pre = ["CREATE TABLE latest_trans AS SELECT DISTINCT account_id FROM trans"]
    pred = [
        "UPDATE account a SET date = t.date FROM trans t WHERE a.account_id = t.account_id"
    ]
    assert missing_preprocess_created_tables(pre, pred, "Management") == ["latest_trans"]
    assert missing_preprocess_created_tables(pre, pred, "Efficiency") == []
    ok = ["UPDATE latest_trans a SET (amount, trans_id) = (SELECT 1, 2)"]
    assert missing_preprocess_created_tables(pre, ok, "Management") == []


def test_sandbox_signals_look_broken():
    from optimization.verify_agent import sandbox_signals_look_broken

    ok = json.dumps(
        [{"id": "c1", "rules_passed": True, "errors": [], "soft_ex": "pass"}]
    )
    assert sandbox_signals_look_broken(ok) is False
    broken = json.dumps(
        [
            {
                "id": "c1",
                "rules_passed": False,
                "errors": ["执行失败: relation account does not exist"],
            }
        ]
    )
    assert sandbox_signals_look_broken(broken) is True
    syntax_only = json.dumps(
        [{"id": "c1", "errors": ["执行失败: syntax error at or near LIMIT"]}]
    )
    assert sandbox_signals_look_broken(syntax_only) is False


def test_pg_delete_update_limit():
    from optimization.verify import pg_delete_update_limit

    sql = (
        "DELETE FROM cards WHERE (uuid, setCode) NOT IN "
        "(SELECT uuid, setCode FROM norm1) LIMIT 1000"
    )
    assert pg_delete_update_limit("postgresql", [sql])
    nested = "DELETE FROM cards WHERE id IN (SELECT id FROM cards LIMIT 10)"
    assert pg_delete_update_limit("postgresql", [nested]) is None
    assert pg_delete_update_limit("mysql", [sql]) is None


def test_on_conflict_index_mismatch():
    from optimization.verify import on_conflict_index_mismatch

    q = (
        "I encounter an error stating that there is no unique or exclusion "
        "constraint matching the ON CONFLICT specification. "
        "insert into event values(...) on conflict (event_name, location, event_date) do update"
    )
    split = [
        "CREATE UNIQUE INDEX unique_name ON event(event_name, event_date) "
        "where event_name is not null; "
        "CREATE UNIQUE INDEX unique_location ON event(location, event_date) "
        "where location is not null;"
    ]
    good = [
        "CREATE UNIQUE INDEX unique_name_location ON event"
        "(event_name, location, event_date) WHERE coalesce(event_name, location) is not null;"
    ]
    assert on_conflict_index_mismatch(q, split)
    assert on_conflict_index_mismatch(q, good) is None
    assert on_conflict_index_mismatch("plain upsert on conflict do update", split) is None
    with_insert = good + [
        "INSERT INTO event (event_id, event_name, location, event_date) "
        "VALUES ('test1', 'test_name', 'test_location', 'test_date')"
    ]
    assert on_conflict_index_mismatch(q, with_insert)


def test_returning_join_mismatch():
    from optimization.verify import returning_join_mismatch

    q = (
        "how could I make the RETURNING clause join to something else and "
        "return the joined row(s)? joined to `customers` tables and get both "
        "transactions_1k.transactionid and customers.Segment"
    )
    bad = ["UPDATE transactions_1k SET Amount = 100 RETURNING *;"]
    good = [
        "UPDATE transactions_1k SET Amount = 100 "
        "FROM (SELECT customerid, transactionid FROM transactions_1k) sub "
        "JOIN customers c ON c.customerid = sub.customerid "
        "WHERE transactions_1k.transactionid = sub.transactionid "
        "RETURNING transactions_1k.transactionid, c.Segment;"
    ]
    assert returning_join_mismatch(q, bad)
    assert returning_join_mismatch(q, good) is None


def test_circular_fk_and_charindex_and_trigger():
    from optimization.verify import (
        circular_fk_chicken_egg,
        mysql_dialect_funcs_in_pg,
        mgmt_trigger_incomplete,
    )

    q = (
        "each account must have at least one associated disposition. "
        "the SQL query fails due to a chicken-egg problem."
    )
    bad = [
        "CREATE TABLE account (account_id INT PRIMARY KEY, disp_id INT "
        "REFERENCES disp(disp_id)); "
        "CREATE TABLE disp (disp_id INT PRIMARY KEY, account_id INT "
        "REFERENCES account(account_id));"
    ]
    good = [
        "CREATE TABLE account (account_id INT PRIMARY KEY); "
        "CREATE TABLE disp (disp_id INT PRIMARY KEY, account_id INT "
        "REFERENCES account(account_id)); "
        "CREATE FUNCTION check_account_disp() RETURNS TRIGGER AS $$ BEGIN "
        "RETURN OLD; END; $$ LANGUAGE plpgsql;"
    ]
    assert circular_fk_chicken_egg(q, bad)
    assert circular_fk_chicken_egg(q, good) is None
    disp_col = [
        "DROP TABLE IF EXISTS disp, account; "
        "CREATE TABLE account (account_id INT PRIMARY KEY, disp_id INT NOT NULL); "
        "CREATE TABLE disp (disp_id INT PRIMARY KEY, account_id INT);"
    ]
    assert circular_fk_chicken_egg(q, disp_col)
    assert mysql_dialect_funcs_in_pg(
        "postgresql", ["select charindex('@', x)"]
    )
    assert mysql_dialect_funcs_in_pg("postgresql", ["select strpos(x, '.')"]) is None
    tq = "I created a trigger on the users table with the following function"
    assert mgmt_trigger_incomplete(tq, ["begin insert into profiles values (1); end;"])
    assert (
        mgmt_trigger_incomplete(
            tq,
            [
                "CREATE FUNCTION profileTrigger() RETURNS TRIGGER AS $$ BEGIN "
                "RETURN NEW; END; $$ LANGUAGE plpgsql; "
                "CREATE TRIGGER profile_table AFTER INSERT ON users "
                "FOR EACH ROW EXECUTE FUNCTION profileTrigger();"
            ],
        )
        is None
    )


def test_query_pers_eff_case_heuristics():
    from optimization.verify import (
        conditional_count_without_filter,
        consecutive_date_island_count,
        efficiency_merge_notin,
        pers_streak_histogram_mismatch,
        personalization_group_key_only,
        query_output_example_mismatch,
        table_count_uses_count_column,
    )

    q_cnt = "I want to count a referral only if the referred user has activated premium"
    assert conditional_count_without_filter(q_cnt, ["SELECT COUNT(posts.Id) FROM posts"])
    assert (
        conditional_count_without_filter(
            q_cnt, ["SELECT COUNT(*) FILTER (WHERE AnswerCount > 0) FROM posts"]
        )
        is None
    )
    q_att = (
        "compare the number of attendees for each event between attendance and budget "
        "where the number of attendees does not match the number of attendees recorded"
    )
    assert table_count_uses_count_column(
        q_att, ["SELECT CTE.count AS newCount, budget.count AS oldCount FROM budget"]
    )
    q_isl = (
        "longest consecutive release periods along with their start and end dates "
        "releaseDate. An output like this: (6, 2019-12-31, 2020-01-05)"
    )
    assert consecutive_date_island_count(
        q_isl, ["SELECT COUNT(*) -1 AS count, MAX(releaseDate), MIN(releaseDate)"]
    )
    assert (
        query_output_example_mismatch(
            q_isl, [{"c": 6, "a": "2019-12-31", "b": "2020-01-05"}]
        )
        is None
    )
    assert query_output_example_mismatch(q_isl, [{"c": 2, "a": "2020-01-22", "b": "2020-01-22"}])
    q_st = (
        "count how many users have each longest streak length. "
        "the number of users who have that longest streak length"
    )
    assert pers_streak_histogram_mismatch(
        q_st, ["SELECT user_id, COUNT(*) FROM purchase GROUP BY user_id LIMIT 1"]
    )
    assert personalization_group_key_only(["SELECT * FROM cards GROUP BY artist"])
    q_eff = "rewrite this query using two separate NOT IN clauses which is inefficient"
    assert efficiency_merge_notin(
        q_eff,
        [
            "DELETE FROM cards WHERE (uuid) NOT IN (SELECT uuid FROM norm1) "
            "AND (uuid) NOT IN (SELECT uuid FROM norm2)"
        ],
    )
    assert (
        efficiency_merge_notin(
            q_eff,
            [
                "DELETE FROM cards WHERE (uuid) NOT IN ("
                "SELECT uuid FROM norm1 UNION ALL SELECT uuid FROM norm2)"
            ],
        )
        is None
    )


def test_qep_hard_gate_on_efficiency():
    def execute_fn(sql: str):
        return [{"sql": sql, "rows": [{"x": 1}], "row_count": 1}]

    v = Verifier(
        dialect="postgresql",
        category="Efficiency",
        exec_timeout_ms=5000,
        execute_fn=execute_fn,
        explain_fn=lambda sql: [
            {"Plan": {"Total Cost": 99.0 if "cand" in sql else 1.0}}
        ],
        reset_fn=lambda: None,
        efficiency=True,
        qep_hard_gate=True,
    )
    report = v.verify_candidates(
        [Candidate(id="c1", pred_sqls=["SELECT 1 -- cand"])],
        ["SELECT 1"],
    )
    assert not report.passed
    assert any("QEP cost" in r for r in report.failure_reasons)


def test_qep_sampled_skips_hard_gate():
    def execute_fn(sql: str):
        return [{"sql": sql, "rows": [{"x": 1}], "row_count": 1}]

    v = Verifier(
        dialect="postgresql",
        category="Efficiency",
        exec_timeout_ms=5000,
        execute_fn=execute_fn,
        explain_fn=lambda sql: [
            {"Plan": {"Total Cost": 99.0 if "cand" in sql else 1.0}}
        ],
        reset_fn=lambda: None,
        efficiency=True,
        qep_hard_gate=False,
    )
    report = v.verify_candidates(
        [Candidate(id="c1", pred_sqls=["SELECT 1 -- cand"])],
        ["SELECT 1"],
    )
    assert report.passed
    assert report.best is not None
    assert not any("QEP cost" in r and "≥" in r for r in report.failure_reasons)


if __name__ == "__main__":
    test_soft_ex_equal_ignores_order()
    test_extract_pg_total_cost()
    test_extract_mysql_query_cost()
    test_is_efficiency_case()
    test_timeout_hard_fail_on_efficiency()
    test_timeout_no_veto_on_non_efficiency()
    test_soft_ex_hard_fail_on_efficiency()
    test_soft_ex_no_veto_on_query_when_differs()
    test_query_time_format_hh24_veto()
    test_upsert_operation_type_mismatch()
    test_missing_preprocess_created_tables_mgmt_only()
    test_sandbox_signals_look_broken()
    test_pg_delete_update_limit()
    test_on_conflict_index_mismatch()
    test_returning_join_mismatch()
    test_circular_fk_and_charindex_and_trigger()
    test_query_pers_eff_case_heuristics()
    test_qep_hard_gate_on_efficiency()
    test_qep_sampled_skips_hard_gate()
    print("all verify tests passed")
