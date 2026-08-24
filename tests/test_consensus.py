"""多候选共识分簇与早停门禁测试。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from optimization.consensus import (
    detect_result_split,
    row_fingerprint,
    should_block_early_stop,
)
from optimization.intent_spec import empty_intent_spec
from optimization.types import Candidate, CandidateVerifyResult, VerifyReport


def test_row_fingerprint_same():
    rows = [{"a": 1}, {"b": 2}]
    assert row_fingerprint(rows) == row_fingerprint(list(reversed(rows)))


def test_row_fingerprint_diff():
    assert row_fingerprint([{"a": 1}]) != row_fingerprint([{"a": 2}])


def test_detect_result_split():
    c1 = Candidate(id="c1", pred_sqls=["SELECT 1"])
    report = VerifyReport(
        passed=True,
        best=c1,
        per_candidate=[
            CandidateVerifyResult(id="c1", passed=True, rows=[(1,)]),
            CandidateVerifyResult(id="c2", passed=True, rows=[(2,)]),
        ],
    )
    split, msg = detect_result_split(report)
    assert split is True
    assert "互斥" in msg


def test_should_block_early_stop_split():
    c1 = Candidate(id="c1", pred_sqls=["SELECT 1"])
    report = VerifyReport(
        passed=True,
        best=c1,
        per_candidate=[
            CandidateVerifyResult(id="c1", passed=True, rows=[(1,)]),
            CandidateVerifyResult(id="c2", passed=True, rows=[(9,)]),
        ],
    )
    block, _ = should_block_early_stop(
        round_num=1,
        intent_spec=empty_intent_spec(),
        report=report,
        num_candidates=2,
    )
    assert block is True


def test_should_block_early_stop_weak_spec_round1():
    c1 = Candidate(id="c1", pred_sqls=["SELECT 1"])
    report = VerifyReport(
        passed=True,
        best=c1,
        per_candidate=[CandidateVerifyResult(id="c1", passed=True, rows=[(1,)])],
    )
    block, msg = should_block_early_stop(
        round_num=1,
        intent_spec=empty_intent_spec(),
        report=report,
        num_candidates=3,
    )
    assert block is True
    assert "Intent Spec" in msg


def test_should_not_block_round2():
    c1 = Candidate(id="c1", pred_sqls=["SELECT 1"])
    report = VerifyReport(
        passed=True,
        best=c1,
        per_candidate=[CandidateVerifyResult(id="c1", passed=True, rows=[(1,)])],
    )
    block, _ = should_block_early_stop(
        round_num=2,
        intent_spec=empty_intent_spec(),
        report=report,
        num_candidates=3,
    )
    assert block is False
