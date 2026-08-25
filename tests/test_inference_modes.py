"""推理二维模式：function × assembly 分支契约（假组件，无 DB/LLM）。"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from utils.inference_runner import VALID_FUNCTIONS, VALID_MODES, resolve_function, run_single_instance


def test_valid_modes_include_llm_only():
    assert "llm_only" in VALID_MODES
    assert set(VALID_FUNCTIONS) == {"general", "efficiency"}


def test_resolve_function_from_category_and_flag():
    assert resolve_function(category="Query") == "general"
    assert resolve_function(category="Efficiency") == "efficiency"
    assert resolve_function(efficiency=True, category="Query") == "efficiency"
    assert resolve_function(function="efficiency", category="Query") == "efficiency"
    assert resolve_function(function="general", category="Efficiency") == "general"


def _fake_setup(*args, **kwargs):
    target = MagicMock()
    sandbox = MagicMock()
    agent_cfg = {"llm": {}, "max_iter": 2, "enable_test": False}
    opt_cfg = {"llm": {}, "max_rounds": 1, "num_candidates": 2, "exec_timeout_ms": 1000}
    target_cfg = {"database": "financial", "host": "127.0.0.1", "port": 5432}
    sandbox_cfg = {"database": "astratune_sandbox"}
    return target, sandbox, agent_cfg, opt_cfg, target_cfg, sandbox_cfg


@patch("utils.inference_runner.setup_executors", side_effect=_fake_setup)
@patch("utils.inference_runner.extract_involved_tables", return_value=["t1"])
@patch("utils.inference_runner.resolve_and_validate_tables", return_value=["t1"])
@patch("utils.inference_runner.fetch_schema_ddl", return_value="-- 表: t1\nCREATE TABLE t1(id int);")
@patch("utils.inference_runner.prepare_sandbox")
@patch("utils.inference_runner.run_llm_only", return_value=["SELECT 1"])
def test_llm_only_skips_diag_and_opt(
    mock_llm_only, mock_prepare, *_mocks
):
    mock_prepare.return_value = {
        "manager": MagicMock(),
        "info": {"fidelity": {}, "sampled_tables": []},
    }
    r = run_single_instance(
        sql="SELECT 1 FROM t1",
        query="q",
        mode="llm_only",
        function="general",
        stream=False,
        instance_id="t_llm",
    )
    assert r.pred_sqls == ["SELECT 1"]
    assert r.meta.get("mode") == "llm_only"
    assert r.diagnosis_report == ""
    mock_llm_only.assert_called_once()


@patch("utils.inference_runner.setup_executors", side_effect=_fake_setup)
@patch("utils.inference_runner.extract_involved_tables", return_value=["t1"])
@patch("utils.inference_runner.resolve_and_validate_tables", return_value=["t1"])
@patch("utils.inference_runner.fetch_schema_ddl", return_value="-- 表: t1\nCREATE TABLE t1(id int);")
@patch("utils.inference_runner.prepare_sandbox")
@patch("utils.inference_runner.extract_intent_spec")
@patch("utils.inference_runner.run_optimization")
@patch("utils.inference_runner.DiagnosisAgent")
def test_opt_only_empty_diagnosis(
    mock_diag_cls, mock_opt, mock_spec, mock_prepare, *_mocks
):
    mock_prepare.return_value = {
        "manager": MagicMock(),
        "info": {"fidelity": {}, "sampled_tables": []},
    }
    spec = MagicMock()
    spec.summary.return_value = {"empty": True}
    mock_spec.return_value = spec
    opt_out = MagicMock()
    opt_out.pred_sqls = ["SELECT 2"]
    opt_out.success = True
    opt_out.rounds = 1
    opt_out.fallback = False
    opt_out.failure_reasons = []
    opt_out.error = None
    mock_opt.return_value = opt_out

    r = run_single_instance(
        sql="SELECT 1 FROM t1",
        query="用户问题",
        mode="opt_only",
        function="general",
        stream=False,
        instance_id="t_opt",
    )
    mock_diag_cls.assert_not_called()
    assert r.diagnosis_report == ""
    assert mock_opt.call_args.kwargs["diagnosis_report"] == ""
    assert r.pred_sqls == ["SELECT 2"]


@patch("utils.inference_runner.setup_executors", side_effect=_fake_setup)
@patch("utils.inference_runner.extract_involved_tables", return_value=["t1"])
@patch("utils.inference_runner.resolve_and_validate_tables", return_value=["t1"])
@patch("utils.inference_runner.fetch_schema_ddl", return_value="-- 表: t1\nCREATE TABLE t1(id int);")
@patch("utils.inference_runner.prepare_sandbox")
@patch("utils.inference_runner.extract_intent_spec")
@patch("utils.inference_runner.extract_pred_sqls_from_diagnosis")
@patch("utils.inference_runner.DiagnosisAgent")
def test_diag_only_extracts_pred(
    mock_diag_cls, mock_extract, mock_spec, mock_prepare, *_mocks
):
    mock_prepare.return_value = {
        "manager": MagicMock(),
        "info": {"fidelity": {}, "sampled_tables": []},
    }
    agent = MagicMock()
    agent.diagnose.return_value = "【诊断结论】\n```sql\nSELECT 9\n```"
    mock_diag_cls.return_value = agent
    spec = MagicMock()
    spec.summary.return_value = {}
    mock_spec.return_value = spec
    mock_extract.return_value = (["SELECT 9"], False)

    r = run_single_instance(
        sql="SELECT 1 FROM t1",
        mode="diag_only",
        function="efficiency",
        category="Efficiency",
        stream=False,
        instance_id="t_diag",
    )
    assert r.pred_sqls == ["SELECT 9"]
    assert r.meta.get("function") == "efficiency"
    assert r.meta.get("fallback") is False
    agent.diagnose.assert_called_once()
    assert agent.diagnose.call_args.kwargs.get("assembly") == "diag_only"
