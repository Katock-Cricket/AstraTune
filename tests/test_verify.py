"""Verify 确定性信号与 Verifier 集成（无 LLM）。规则单测见 test_verify_rules.py。"""

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


def test_eff_risk_scale_factor_vetoes_efficiency():
    """采样墙钟 × scale 外推超阈值 → Efficiency 硬否决。"""
    import time

    def execute_fn(sql: str):
        time.sleep(0.05)
        return [{"sql": sql, "rows": [{"x": 1}], "row_count": 1}]

    v = Verifier(
        dialect="postgresql",
        category="Efficiency",
        exec_timeout_ms=5000,
        execute_fn=execute_fn,
        explain_fn=lambda sql: [{"Plan": {"Total Cost": 1.0}}],
        reset_fn=lambda: None,
        efficiency=True,
        scale_factor=100.0,  # ~50ms × 100 ≥ 0.85×5000
        qep_hard_gate=False,
    )
    report = v.verify_candidates(
        [Candidate(id="c1", pred_sqls=["SELECT 1"])],
        ["SELECT 1"],
    )
    assert not report.passed
    assert any("外推" in r or "Timeout" in r for r in report.failure_reasons)
