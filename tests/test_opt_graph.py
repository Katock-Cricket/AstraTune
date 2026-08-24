"""优化图路由与 fallback 特征测试：注入假 Generate/Verifier/VerifyAgent。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from optimization.graph import create_optimization_graph
from optimization.intent_spec import IntentSpec, OutputColumn
from optimization.types import Candidate, CandidateVerifyResult, VerifyReport


def _cand(cid, sql):
    return Candidate(id=cid, pred_sqls=[sql], rationale="")


def _cvr(cid, passed, errors=None, rows=None):
    return CandidateVerifyResult(
        id=cid, passed=passed, soft_ex="skipped", cost=None,
        latency_ms=None, errors=list(errors or []), rank_key=0.0, rows=rows,
    )


class FakeGenerate:
    def __init__(self, cands):
        self.cands = cands
        self.calls = 0

    def generate(self, **kwargs):
        self.calls += 1
        return list(self.cands)


class FakeVerifier:
    def __init__(self, reports):
        self.reports = list(reports)
        self.calls = 0

    def verify_candidates(self, cands, issue_sql):
        self.calls += 1
        return self.reports[min(self.calls - 1, len(self.reports) - 1)]


class FakeVerifyAgent:
    def __init__(self, verdicts=None, final=None):
        self.verdicts = verdicts or {}
        self.final = final or []
        self.select_calls = 0

    def review(self, **kwargs):
        return dict(self.verdicts)

    def select_final(self, **kwargs):
        self.select_calls += 1
        return list(self.final)


def _init(**extra):
    base = {
        "round": 0, "candidates": [], "verify_report": None,
        "failure_reasons": [], "round_history": [], "pred_sqls": [],
        "diagnosis_report": "d", "issue_sql": ["SELECT 1"], "query": "q",
        "schema": "s", "category": "Query", "dialect": "postgresql",
        "success": False, "fallback": False, "error": None,
        "intent_spec": IntentSpec(goal="query", output_columns=[OutputColumn(name="x")]),
    }
    base.update(extra)
    return base


def test_first_round_pass():
    c1 = _cand("c1", "SELECT 1")
    report = VerifyReport(
        passed=True, best=c1, relative_best=c1, per_candidate=[_cvr("c1", True)],
    )
    gen = FakeGenerate([c1])
    ver = FakeVerifier([report])
    g = create_optimization_graph(
        generate_agent=gen, verifier=ver, max_rounds=3,
        verify_agent=None, verify_mode="rules_only",
    )
    final = g.invoke(_init())
    assert final["success"] is True
    assert final["fallback"] is False
    assert final["pred_sqls"] == ["SELECT 1"]
    assert gen.calls == 1 and ver.calls == 1


def test_rounds_exhausted_verify_agent_select_final():
    c1 = _cand("c1", "SELECT bad")
    report = VerifyReport(
        passed=False, best=None, relative_best=c1,
        per_candidate=[_cvr("c1", False, errors=["执行失败: syntax"])],
    )
    gen = FakeGenerate([c1])
    ver = FakeVerifier([report])
    va = FakeVerifyAgent(verdicts={}, final=["SELECT rescued"])
    g = create_optimization_graph(
        generate_agent=gen, verifier=ver, max_rounds=1,
        verify_agent=va, verify_mode="agent+rules",
    )
    final = g.invoke(_init())
    assert final["success"] is False
    assert final["fallback"] is True
    assert final["pred_sqls"] == ["SELECT rescued"]
    assert va.select_calls == 1


def test_rounds_exhausted_relative_best_no_exec_error():
    c1 = _cand("c1", "SELECT soft-fail")
    report = VerifyReport(
        passed=False, best=None, relative_best=c1,
        per_candidate=[_cvr("c1", False, errors=["规则否决: x"])],
    )
    g = create_optimization_graph(
        generate_agent=FakeGenerate([c1]), verifier=FakeVerifier([report]),
        max_rounds=1, verify_agent=None, verify_mode="rules_only",
    )
    final = g.invoke(_init())
    assert final["fallback"] is True
    assert final["pred_sqls"] == ["SELECT soft-fail"]


def test_agent_vetoes_false_pass():
    c1 = _cand("c1", "SELECT 1")
    report = VerifyReport(
        passed=True, best=c1, relative_best=c1, per_candidate=[_cvr("c1", True)],
    )
    va = FakeVerifyAgent(verdicts={"c1": {"pass": False, "feedback": "投影错误"}})
    g = create_optimization_graph(
        generate_agent=FakeGenerate([c1]), verifier=FakeVerifier([report]),
        max_rounds=1, verify_agent=va, verify_mode="agent+rules",
    )
    final = g.invoke(_init())
    # 假过被否决：passed 变 False
    assert final["verify_report"].per_candidate[0].passed is False
    assert final["verify_report"].best is None
    assert final["verify_report"].passed is False
    # 轮次耗尽 → select_final（返回空）→ relative_best 无执行错误 → 回退
    assert final["fallback"] is True
    assert final["pred_sqls"] == ["SELECT 1"]


def test_agent_cannot_flip_exec_failure():
    c1 = _cand("c1", "SELECT bad")
    report = VerifyReport(
        passed=False, best=None, relative_best=c1,
        per_candidate=[_cvr("c1", False, errors=["执行失败: boom"])],
    )
    va = FakeVerifyAgent(
        verdicts={"c1": {"pass": True, "feedback": "看起来可以"}},
        final=["SELECT salvaged"],
    )
    g = create_optimization_graph(
        generate_agent=FakeGenerate([c1]), verifier=FakeVerifier([report]),
        max_rounds=1, verify_agent=va, verify_mode="agent+rules",
    )
    final = g.invoke(_init())
    # 执行失败不可被 agent 翻案
    assert final["verify_report"].per_candidate[0].passed is False
    assert final["pred_sqls"] == ["SELECT salvaged"]


def test_hard_fail_when_no_usable_candidate():
    c1 = _cand("c1", "SELECT bad")
    report = VerifyReport(
        passed=False, best=None, relative_best=c1,
        per_candidate=[_cvr("c1", False, errors=["执行失败: boom"])],
    )
    g = create_optimization_graph(
        generate_agent=FakeGenerate([c1]), verifier=FakeVerifier([report]),
        max_rounds=1, verify_agent=None, verify_mode="rules_only",
    )
    final = g.invoke(_init())
    assert final["pred_sqls"] == []
    assert final["fallback"] is False
    assert final["error"] == "hard_fail: 轮次耗尽且无可用候选"


def test_retry_next_round_on_veto():
    # 第一轮否决 → 第二轮通过，验证 conditional edges 的 generate 重入
    c1 = _cand("c1", "SELECT v1")
    c2 = _cand("c2", "SELECT v2")
    r1 = VerifyReport(passed=False, best=None, relative_best=c1, per_candidate=[_cvr("c1", False)])
    r2 = VerifyReport(passed=True, best=c2, relative_best=c2, per_candidate=[_cvr("c2", True)])
    gen = FakeGenerate([c1])
    ver = FakeVerifier([r1, r2])
    g = create_optimization_graph(
        generate_agent=gen, verifier=ver, max_rounds=2,
        verify_agent=None, verify_mode="rules_only",
    )
    final = g.invoke(_init())
    assert final["success"] is True
    assert final["pred_sqls"] == ["SELECT v2"]
    assert ver.calls == 2
    assert len(final["round_history"]) == 2


def test_consensus_blocks_first_round_pass():
    """结果分裂时共识门禁拦截早停，触发第二轮 generate。"""
    c1 = _cand("c1", "SELECT 1")
    c2 = _cand("c2", "SELECT 2")
    r1 = VerifyReport(
        passed=True,
        best=c1,
        relative_best=c1,
        per_candidate=[
            _cvr("c1", True, rows=[(1,)]),
            _cvr("c2", True, rows=[(2,)]),
        ],
    )
    r2 = VerifyReport(
        passed=True,
        best=c2,
        relative_best=c2,
        per_candidate=[_cvr("c2", True, rows=[(2,)])],
    )
    gen = FakeGenerate([c1, c2])
    ver = FakeVerifier([r1, r2])
    g = create_optimization_graph(
        generate_agent=gen,
        verifier=ver,
        max_rounds=2,
        verify_agent=None,
        verify_mode="rules_only",
    )
    spec = IntentSpec(goal="query", output_columns=[OutputColumn(name="v")])
    final = g.invoke(_init(intent_spec=spec))
    assert final["success"] is True
    assert ver.calls == 2
    assert gen.calls == 2


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
    print("all opt graph tests passed")
