"""WebUI 导入映射、剥 gold、导出。"""

from __future__ import annotations

import json

from webui.import_jsonl import iter_export_rows, map_import_row, parse_jsonl_text
from webui.overlay import seed_targets_from_target_db


def _conns():
    return seed_targets_from_target_db(
        {
            "postgresql": {"host": "127.0.0.1", "port": 5432, "user": "root", "password": "x"},
            "mysql": {"host": "127.0.0.1", "port": 3306, "user": "root", "password": "x"},
        }
    )


def test_efficiency_maps_to_slow_and_strips_gold():
    row = {
        "instance_id": "37",
        "db_id": "financial",
        "dialect": "PostgreSQL",
        "category": "Efficiency",
        "issue_sql": ["SELECT 1"],
        "query": "slow",
        "sol_sql": ["SELECT gold"],
        "test_cases": ["assert 1"],
        "gold": "nope",
    }
    draft, err = map_import_row(row, connections=_conns())
    assert err is None
    assert draft["is_slow_sql"] is True
    assert draft["function"] == "efficiency"
    assert draft["mode"] == "diag+opt"
    assert draft["connection_id"] == "postgresql-default"
    assert "sol_sql" not in draft
    assert "test_cases" not in draft
    assert "gold" not in draft


def test_general_default_and_missing_issue_sql():
    conns = _conns()
    ok, err = map_import_row(
        {"instance_id": "a", "db_id": "financial", "issue_sql": "SELECT 1 FROM t", "query": "q"},
        connections=conns,
    )
    assert err is None
    assert ok["is_slow_sql"] is False
    assert ok["function"] == "general"
    assert ok["category"] == "Query"

    bad, err2 = map_import_row({"db_id": "financial", "query": "x"}, connections=conns)
    assert bad is None
    assert "issue_sql" in err2


def test_parse_jsonl_reports_failures_without_stopping():
    text = "\n".join(
        [
            json.dumps({"db_id": "financial", "issue_sql": ["SELECT 1"], "instance_id": "ok1"}),
            "not-json",
            json.dumps({"instance_id": "no-sql", "db_id": "financial"}),
            json.dumps(
                {
                    "instance_id": "slow1",
                    "db_id": "financial",
                    "issue_sql": ["SELECT 2"],
                    "is_slow_sql": True,
                }
            ),
        ]
    )
    ok, failures = parse_jsonl_text(text, connections=_conns())
    assert len(ok) == 2
    assert ok[1]["function"] == "efficiency"
    assert len(failures) == 2
    reasons = " ".join(f["reason"] for f in failures)
    assert "JSON" in reasons or "无效" in reasons
    assert "issue_sql" in reasons


def test_no_matching_connection_is_failure():
    draft, err = map_import_row(
        {"db_id": "x", "issue_sql": ["SELECT 1"], "dialect": "postgresql"},
        connections=[],
    )
    assert draft is None
    assert "连接" in err


def test_export_only_nonempty_pred():
    rows = iter_export_rows(
        [
            {"instance_id": "a", "pred_sqls": ["SELECT 1"], "status": "completed", "db_id": "financial"},
            {"instance_id": "b", "pred_sqls": [], "status": "completed"},
            {"instance_id": "c", "pred_sqls": ["  "], "status": "failed"},
        ]
    )
    assert [r["instance_id"] for r in rows] == ["a"]
    assert rows[0]["pred_sqls"] == ["SELECT 1"]
