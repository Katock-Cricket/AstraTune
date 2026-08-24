"""工具层：空 SQL / psql 元命令改写。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.sandbox_tool import SandboxTool
from utils.sql_wrap import is_effectively_empty_sql, split_sql_statements


class _DummyEx:
    connection = None

    def execute(self, sql, fetch=True):
        return [{"sql": sql, "rows": [{"ok": 1}], "row_count": 1}]


def test_empty_and_comment():
    assert is_effectively_empty_sql("")
    assert is_effectively_empty_sql("-- only comment")
    assert is_effectively_empty_sql("/* x */ ;")
    assert not is_effectively_empty_sql("SELECT 1")
    assert split_sql_statements("SELECT 1; -- c\n; SELECT 2") == ["SELECT 1", "SELECT 2"]


def test_normalize_psql_and_sqlite():
    t = SandboxTool(_DummyEx())
    out = t._normalize_agent_sql(r"\d transactions_1k")
    assert "information_schema.columns" in out
    assert "transactions_1k" in out.lower()
    out2 = t._normalize_agent_sql(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='event';"
    )
    assert "pg_indexes" in out2


if __name__ == "__main__":
    test_empty_and_comment()
    test_normalize_psql_and_sqlite()
    print("ok")
