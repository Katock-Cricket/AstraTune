"""前后置包裹与 Verify 执行顺序单测。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from optimization.types import Candidate
from optimization.verify import Verifier
from utils.sql_wrap import as_sql_list, split_sql_statements, wrap_with_pre_post


def test_wrap_order():
    wrapped = wrap_with_pre_post(
        ["SELECT 1"],
        preprocess_sql=["CREATE TABLE t(a int)", "INSERT INTO t VALUES (1)"],
        clean_up_sql=["DROP TABLE t"],
    )
    assert wrapped == [
        "CREATE TABLE t(a int)",
        "INSERT INTO t VALUES (1)",
        "SELECT 1",
        "DROP TABLE t",
    ]


def test_as_sql_list_double_semicolon():
    assert as_sql_list("A;; B;") == ["A", "B"]


def test_split_preserves_dollar_quote_body():
    sql = (
        "CREATE OR REPLACE FUNCTION f() RETURNS trigger AS $fn$ "
        "BEGIN INSERT INTO t VALUES (1); RETURN NEW; END; "
        "$fn$ LANGUAGE plpgsql;\n"
        "CREATE TRIGGER trg AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION f();"
    )
    parts = split_sql_statements(sql)
    assert len(parts) == 2
    assert "INSERT INTO t VALUES (1);" in parts[0]
    assert parts[0].startswith("CREATE OR REPLACE FUNCTION")
    assert parts[1].startswith("CREATE TRIGGER")


def test_as_sql_list_does_not_shatter_function_element():
    body = [
        "CREATE OR REPLACE FUNCTION f() RETURNS trigger AS $fn$ "
        "BEGIN INSERT INTO t VALUES (1); RETURN NEW; END; $fn$ LANGUAGE plpgsql;"
    ]
    parts = as_sql_list(body)
    assert len(parts) == 1
    assert "INSERT INTO t VALUES (1);" in parts[0]


def test_verifier_wraps_and_mark_clean():
    calls: list[str] = []

    def execute_fn(sql: str):
        calls.append(sql)
        parts = split_sql_statements(sql)
        out = []
        for p in parts:
            if p.upper().startswith("SELECT"):
                out.append({"sql": p, "rows": [{"x": 1}], "row_count": 1})
            else:
                out.append({"sql": p, "affected_rows": 0, "message": "ok"})
        return out

    state = {"dirty": True}

    def reset_fn():
        state["dirty"] = False
        return "full"

    def mark_clean():
        state["dirty"] = False

    reset_fn.mark_clean = mark_clean  # type: ignore[attr-defined]

    v = Verifier(
        dialect="postgresql",
        category="Query",
        execute_fn=execute_fn,
        explain_fn=lambda sql: [{"Plan": {"Total Cost": 1.0}}],
        reset_fn=reset_fn,
        preprocess_sql=["CREATE TABLE t(a int)"],
        clean_up_sql=["DROP TABLE t"],
    )
    report = v.verify_candidates(
        [Candidate(id="c1", pred_sqls=["SELECT 1 AS x"])],
        ["SELECT 1 AS x"],
    )
    assert report.passed
    assert any("CREATE TABLE" in c and "SELECT" in c and "DROP TABLE" in c for c in calls)
    assert state["dirty"] is False


if __name__ == "__main__":
    test_wrap_order()
    test_as_sql_list_double_semicolon()
    test_split_preserves_dollar_quote_body()
    test_as_sql_list_does_not_shatter_function_element()
    test_verifier_wraps_and_mark_clean()
    print("all sql_wrap/verify wrap tests passed")
