"""批跑记录处理纯函数特征测试（slim / finalize / done 判定 / fallback 行 / worker stdout 解析 / 汇总）。"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.batch_records import (
    fallback_row,
    finalize_inference_record,
    has_pred_sqls,
    is_inference_success,
    load_done_ids,
    parse_worker_stdout,
    slim_output_record,
    summarize,
)


def test_has_pred_sqls():
    assert has_pred_sqls(["SELECT 1"]) is True
    assert has_pred_sqls(["  "]) is False
    assert has_pred_sqls([]) is False
    assert has_pred_sqls("SELECT 1") is True
    assert has_pred_sqls(None) is False


def test_is_inference_success():
    ok = {
        "error": None,
        "optimization_meta": {"success": True, "fallback": False},
        "pred_sqls": ["SELECT 1"],
    }
    assert is_inference_success(ok) is True
    fb = {
        "error": None,
        "optimization_meta": {"success": False, "fallback": True},
        "pred_sqls": ["SELECT 1"],
    }
    assert is_inference_success(fb) is False
    err = {
        "error": "boom",
        "optimization_meta": {"success": True},
        "pred_sqls": ["SELECT 1"],
    }
    assert is_inference_success(err) is False
    empty = {
        "error": None,
        "optimization_meta": {"success": True},
        "pred_sqls": [],
    }
    assert is_inference_success(empty) is False


def test_finalize_keeps_pred():
    out = {"pred_sqls": ["SELECT 1"], "optimization_meta": {"success": True}}
    got = finalize_inference_record(out)
    assert got["pred_sqls"] == ["SELECT 1"]
    assert got["optimization_meta"]["hard_fail"] is False


def test_finalize_hard_fail():
    out = {
        "pred_sqls": [],
        "optimization_meta": {"success": True, "failure_reasons": ["规则否决: x"]},
        "error": None,
    }
    got = finalize_inference_record(out)
    assert got["pred_sqls"] == []
    assert got["optimization_meta"]["success"] is False
    assert got["optimization_meta"]["hard_fail"] is True
    assert got["error"] == "规则否决: x"


def test_finalize_hard_fail_default_error():
    got = finalize_inference_record({"pred_sqls": [], "optimization_meta": {}})
    assert got["error"] == "推理未通过 Verify 且无可用候选"


def test_slim_drops_schema_and_truncates_report():
    rec = {
        "instance_id": "x",
        "original_schema": "CREATE TABLE t(...)",
        "preprocess_schema": "CREATE TABLE t(...)",
        "diagnosis_report": "r" * 5000,
        "timing_summary": {"stages": []},
    }
    out = slim_output_record(rec)
    assert "original_schema" not in out
    assert "preprocess_schema" not in out
    assert out["diagnosis_report"].endswith("…[truncated]")
    assert len(out["diagnosis_report"]) <= 4012
    assert len(rec["diagnosis_report"]) == 5000


def test_slim_truncates_huge_timing():
    stages = [{"name": f"s{i}", "data": "x" * 200} for i in range(30)]
    rec = {"instance_id": "x", "timing_summary": {"stages": stages}}
    out = slim_output_record(rec)
    assert out["timing_summary"] == {"truncated": True, "stages_count": 30}


def test_load_done_ids_resume_semantics(tmp_path):
    p = tmp_path / "out.jsonl"
    rows = [
        {
            "instance_id": "ok",
            "optimization_meta": {"success": True},
            "pred_sqls": ["SELECT 1"],
        },
        {
            "instance_id": "fb",
            "optimization_meta": {"success": False, "fallback": True},
            "pred_sqls": ["SELECT 2"],
        },
        {"instance_id": "fail", "error": "timeout", "pred_sqls": []},
    ]
    p.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8"
    )

    done = load_done_ids(str(p))
    assert set(done) == {"ok", "fb"}

    done2 = load_done_ids(str(p), rerun_nonok=True)
    assert set(done2) == {"ok"}


def test_load_done_ids_missing_file(tmp_path):
    assert load_done_ids(str(tmp_path / "nope.jsonl")) == {}


def test_fallback_row_with_prior_pred():
    rec = {"instance_id": "i1", "sol_sql": "SECRET", "test_cases": [1]}
    prior = {
        "pred_sqls": ["SELECT prior"],
        "diagnosis_report": "old report",
        "optimization_meta": {"rounds": 2},
    }
    out = fallback_row(rec, "worker timeout", mode="diag+opt", prior_pred=prior)
    assert out["pred_sqls"] == ["SELECT prior"]
    assert "sol_sql" not in out and "test_cases" not in out
    assert out["error"] == "worker timeout"
    assert out["optimization_meta"]["fallback"] is True
    assert out["optimization_meta"]["hard_fail"] is False
    assert out["optimization_meta"]["rounds"] == 2
    assert out["diagnosis_report"] == "old report"


def test_fallback_row_hard_fail():
    rec = {"instance_id": "i1"}
    out = fallback_row(rec, "worker crash", mode="opt_only")
    assert out["pred_sqls"] == []
    assert out["optimization_meta"]["hard_fail"] is True
    assert out["optimization_meta"]["fallback"] is False
    assert out["error"] == "worker crash"


def test_parse_worker_stdout_last_json():
    text = 'log line\n{"instance_id": "a"}\nnoise\n{"instance_id": "b"}\n'
    got = parse_worker_stdout(text)
    assert got == {"instance_id": "b"}


def test_parse_worker_stdout_tolerant():
    assert parse_worker_stdout("not json at all") is None
    assert parse_worker_stdout("") is None
    assert parse_worker_stdout(None) is None


def test_summarize_counts():
    rows = [
        {
            "optimization_meta": {"success": True, "fallback": False},
            "pred_sqls": ["SELECT 1"],
            "elapsed_ms": 100.0,
        },
        {
            "optimization_meta": {
                "success": False,
                "fallback": True,
                "hard_fail": False,
            },
            "pred_sqls": ["SELECT 2"],
            "elapsed_ms": 300.0,
        },
        {
            "error": "timeout",
            "optimization_meta": {"hard_fail": True},
            "pred_sqls": [],
        },
    ]
    s = summarize(rows)
    assert s["total"] == 3
    assert s["success"] == 1
    assert s["fallback"] == 1
    assert s["hard_fail"] == 1
    assert s["with_error"] == 1
    assert s["inference_ok"] == 1
    assert s["empty_pred"] == 1
    assert s["elapsed_ms_mean"] == 200.0
