"""推理交付与 JSONL IO：fallback 不清空 pred；hard_fail 仅真空候选。"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.batch_records import (
    fallback_row,
    finalize_inference_record,
    has_pred_sqls,
    is_inference_success,
)
from utils.pipeline import dedupe_jsonl_by_id, dump_jsonl, load_jsonl


def test_fallback_keeps_pred():
    out = {
        "pred_sqls": ["SELECT 1"],
        "optimization_meta": {
            "success": False,
            "fallback": True,
            "failure_reasons": ["轮次耗尽"],
        },
        "error": "hard_fail: 轮次耗尽且无可用候选",
    }
    finalize_inference_record(out)
    assert out["pred_sqls"] == ["SELECT 1"]
    assert out["optimization_meta"]["hard_fail"] is False
    assert not is_inference_success(out)
    assert has_pred_sqls(out["pred_sqls"])


def test_empty_pred_is_hard_fail():
    out = {
        "pred_sqls": [],
        "optimization_meta": {
            "success": False,
            "fallback": False,
            "failure_reasons": [],
        },
    }
    finalize_inference_record(out)
    assert out["pred_sqls"] == []
    assert out["optimization_meta"]["hard_fail"] is True
    assert out.get("error")


def test_verify_pass_not_hard_fail():
    out = {
        "pred_sqls": ["SELECT 1"],
        "optimization_meta": {"success": True, "fallback": False},
    }
    finalize_inference_record(out)
    assert is_inference_success(out)
    assert out["optimization_meta"]["hard_fail"] is False


def test_dump_jsonl_atomic_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "pred.jsonl"
        rows = [{"instance_id": 1, "pred_sqls": ["A"]}, {"instance_id": 2}]
        dump_jsonl(rows, path)
        assert not path.with_name(path.name + ".tmp").exists()
        got = load_jsonl(path)
        assert got[0]["pred_sqls"] == ["A"]
        assert (
            json.loads(path.read_text(encoding="utf-8").splitlines()[1])["instance_id"]
            == 2
        )


def test_timeout_row_keeps_prior_pred():
    rec = {"instance_id": 9, "db_id": "x", "issue_sql": ["SELECT 1"]}
    prior = {
        "pred_sqls": ["SELECT 2"],
        "diagnosis_report": "diag",
        "optimization_meta": {"rounds": 2},
    }
    out = fallback_row(
        rec, "instance_timeout_2400s", mode="diag+opt", prior_pred=prior
    )
    assert out["pred_sqls"] == ["SELECT 2"]
    assert out["optimization_meta"]["fallback"] is True
    assert out["optimization_meta"]["hard_fail"] is False


def test_dedupe_jsonl_by_id():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "pred.jsonl"
        with path.open("w", encoding="utf-8") as f:
            f.write('{"instance_id":1,"pred_sqls":["a"]}\n')
            f.write('{"instance_id":1,"pred_sqls":["b"]}\n')
            f.write('{"instance_id":2,"pred_sqls":["c"]}\n')
        n = dedupe_jsonl_by_id(path)
        assert n == 2
        got = load_jsonl(path)
        by = {r["instance_id"]: r for r in got}
        assert by[1]["pred_sqls"] == ["b"]
        assert by[2]["pred_sqls"] == ["c"]
