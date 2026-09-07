"""Effi 批跑辅助：fallback 行保留 prediction 字段。"""

from scripts.run_effi_sql import _effi_timeout_row


def test_effi_timeout_row_has_prediction_field():
    rec = {
        "instance_id": "EffiSQL_9",
        "db_id": "solar_panel",
        "issue_sql": ["SELECT 1"],
    }
    raw = {"id": 9, "db": "solar_panel", "base_sql": "SELECT 1"}
    row = _effi_timeout_row(rec, raw, "boom", mode="diag+opt")
    assert row["instance_id"] == "EffiSQL_9"
    assert row["prediction"] == ""
    assert row["pred_sqls"] == []
    assert row["base_sql"] == "SELECT 1"
    assert row["optimization_meta"]["hard_fail"] is True
