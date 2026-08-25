"""Effi adapter 单元测试。"""

from utils.effi_adapter import (
    EFFI_QUERY,
    adapt_effi_record,
    attach_prediction,
    pred_sqls_to_prediction,
    strip_agent_forbidden,
)


def test_adapt_maps_fields_and_strips_gold():
    row = {
        "id": 7,
        "db": "solar_panel",
        "base_sql": "SELECT 1",
        "optimized_sql": "SELECT 2",
        "fast_time": 1.0,
        "base_time": 9.0,
    }
    out = adapt_effi_record(row)
    assert out["instance_id"] == "EffiSQL_7"
    assert out["db_id"] == "solar_panel"
    assert out["issue_sql"] == ["SELECT 1"]
    assert out["category"] == "Efficiency"
    assert out["efficiency"] is True
    assert out["function"] == "efficiency"
    assert out["query"] == EFFI_QUERY
    assert "optimized_sql" not in out
    assert "fast_time" not in out


def test_pred_sqls_to_prediction():
    assert pred_sqls_to_prediction(["SELECT 1"]) == "SELECT 1"
    assert "SELECT 1" in pred_sqls_to_prediction(["SELECT 1", "SELECT 2"])
    assert pred_sqls_to_prediction([]) == ""


def test_attach_prediction_keeps_base():
    raw = {"id": 1, "db": "tpch", "base_sql": "SELECT 1", "optimized_sql": "X"}
    out = attach_prediction(raw, ["SELECT 1 AS x"])
    assert out["prediction"] == "SELECT 1 AS x"
    assert out["base_sql"] == "SELECT 1"
    assert out["optimized_sql"] == "X"  # 评测记录可保留；Agent 侧用 strip


def test_strip_agent_forbidden():
    cleaned = strip_agent_forbidden(
        {"base_sql": "A", "optimized_sql": "B", "fast_time": 1, "id": 1}
    )
    assert cleaned == {"base_sql": "A", "id": 1}
