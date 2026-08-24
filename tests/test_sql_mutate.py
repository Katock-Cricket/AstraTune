"""sql_may_mutate / smart reset 单测。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.sql_mutate import sql_may_mutate, sqls_may_mutate


def test_select_readonly():
    assert not sql_may_mutate("SELECT * FROM t WHERE x=1")
    assert not sql_may_mutate("WITH c AS (SELECT 1) SELECT * FROM c")
    assert not sql_may_mutate("EXPLAIN SELECT 1")


def test_mutate_dml_ddl():
    assert sql_may_mutate("INSERT INTO t VALUES (1)")
    assert sql_may_mutate("UPDATE t SET a=1")
    assert sql_may_mutate("DELETE FROM t")
    assert sql_may_mutate("CREATE INDEX ON t(a)")
    assert sql_may_mutate("DROP TABLE t")
    assert sql_may_mutate("ALTER TABLE t ADD COLUMN c int")
    assert sql_may_mutate("TRUNCATE t")
    assert sql_may_mutate("SELECT * FROM t FOR UPDATE")


def test_string_literal_not_mutate():
    # 字面量里的 INSERT 不应触发
    assert not sql_may_mutate("SELECT 'INSERT INTO x' AS s")


def test_sqls_list():
    assert not sqls_may_mutate(["SELECT 1", "SELECT 2"])
    assert sqls_may_mutate(["SELECT 1", "UPDATE t SET a=1"])


def test_smart_reset_skip(monkeypatch=None):
    calls = {"cleanup": 0, "setup": 0}

    class FakeMgr:
        def cleanup_sandbox(self):
            calls["cleanup"] += 1

        def setup_sandbox(self, tables):
            calls["setup"] += 1

    from utils.pipeline import make_reset_fn

    reset_fn, mark_sql = make_reset_fn(
        FakeMgr(), ["t"], smart=True, initially_dirty=True
    )
    assert reset_fn() == "full"
    assert calls["cleanup"] == 1
    # 只读后再次 reset 应跳过
    mark_sql("SELECT 1")
    assert reset_fn() == "skipped"
    assert calls["cleanup"] == 1
    mark_sql("UPDATE t SET a=1")
    assert reset_fn() == "full"
    assert calls["cleanup"] == 2


if __name__ == "__main__":
    test_select_readonly()
    test_mutate_dml_ddl()
    test_string_literal_not_mutate()
    test_sqls_list()
    test_smart_reset_skip()
    print("all sql_mutate/smart_reset tests passed")
