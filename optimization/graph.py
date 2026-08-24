from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, TypedDict

from langgraph.graph import END, StateGraph

from optimization.generate_agent import GenerateAgent
from optimization.types import Candidate, OptimizationResult, VerifyReport
from optimization.verify import Verifier, build_failure_reasons
from utils.logger import default_logger
from utils.sql_wrap import as_sql_list as _as_sql_list
from utils.timing import timed_stage


class OptState(TypedDict, total=False):
    round: int
    candidates: List[Candidate]
    verify_report: Optional[VerifyReport]
    failure_reasons: List[str]
    round_history: List[Dict[str, Any]]
    pred_sqls: List[str]
    diagnosis_report: str
    issue_sql: List[str]
    query: str
    schema: str
    category: str
    dialect: str
    efficiency: Optional[bool]
    preprocess_sql: List[str]
    clean_up_sql: List[str]
    sandbox_fidelity: str
    success: bool
    fallback: bool
    error: Optional[str]


def _candidate_has_exec_error(report: VerifyReport, cand: Candidate) -> bool:
    for cvr in report.per_candidate or []:
        if cvr.id != cand.id:
            continue
        for e in cvr.errors or []:
            if "执行失败" in e:
                return True
    return False


def _summarize_round(
    rnd: int,
    cands: List[Candidate],
    report: VerifyReport,
    agent_verdicts: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """压缩一轮 Generate–Verify 供下轮 Generate / 最终仲裁使用。"""
    by_id = {c.id: c for c in cands}
    entries = []
    for cvr in report.per_candidate or []:
        cand = by_id.get(cvr.id)
        v = (agent_verdicts or {}).get(cvr.id) or {}
        entries.append(
            {
                "id": cvr.id,
                "pred_sqls": list(cand.pred_sqls) if cand else [],
                "rationale": (cand.rationale[:240] if cand else ""),
                "rules_passed": cvr.passed,
                "soft_ex": cvr.soft_ex,
                "cost": cvr.cost,
                "latency_ms": cvr.latency_ms,
                "errors": list(cvr.errors or [])[:8],
                "agent_pass": v.get("pass"),
                "agent_feedback": (v.get("feedback") or "")[:400],
            }
        )
    return {
        "round": rnd,
        "passed_any": bool(report.passed),
        "best_id": report.best.id if report.best else None,
        "candidates": entries,
        "failure_reasons": list(report.failure_reasons or [])[:20],
    }


def create_optimization_graph(
    *,
    generate_agent: GenerateAgent,
    verifier: Verifier,
    max_rounds: int = 3,
    verify_agent=None,
    verify_mode: str = "agent+rules",
):
    max_rounds = max(1, int(max_rounds))
    mode = (verify_mode or "agent+rules").strip().lower()

    def generate_node(state: OptState) -> Dict[str, Any]:
        rnd = int(state.get("round") or 0) + 1
        default_logger.info(f"[Generate–Verify] Generate round={rnd}")
        try:
            with timed_stage("generate", round=rnd):
                cands = generate_agent.generate(
                diagnosis_report=state.get("diagnosis_report") or "",
                issue_sql=state.get("issue_sql") or [],
                query=state.get("query") or "",
                schema=state.get("schema") or "",
                category=state.get("category") or "",
                dialect=state.get("dialect") or "postgresql",
                failure_reasons=state.get("failure_reasons") or [],
                round_history=state.get("round_history") or [],
                efficiency=state.get("efficiency"),
                preprocess_sql=state.get("preprocess_sql") or [],
                clean_up_sql=state.get("clean_up_sql") or [],
                sandbox_fidelity=state.get("sandbox_fidelity") or "",
            )
            return {
                "round": rnd,
                "candidates": cands,
                "error": None,
            }
        except Exception as e:
            default_logger.error(f"Generate 失败: {e}")
            reasons = list(state.get("failure_reasons") or [])
            reasons.append(f"Generate 失败: {e}")
            return {
                "round": rnd,
                "candidates": [],
                "failure_reasons": reasons,
                "error": str(e),
            }

    def verify_node(state: OptState) -> Dict[str, Any]:
        cands = state.get("candidates") or []
        issue_sql = state.get("issue_sql") or []
        default_logger.info(
            f"[Generate–Verify] Verify round={state.get('round')} candidates={len(cands)}"
        )
        if not cands:
            report = VerifyReport(
                passed=False,
                failure_reasons=list(state.get("failure_reasons") or ["无候选可验证"]),
            )
            return {
                "verify_report": report,
                "failure_reasons": build_failure_reasons(report),
                "success": False,
            }

        report = verifier.verify_candidates(cands, issue_sql)

        # Verify Agent：规则作信号，可否决假过；不可把执行失败改成通过
        agent_verdicts: Dict[str, Dict[str, Any]] = {}
        if mode in ("agent+rules", "agent_rules", "agent") and verify_agent is not None:
            with timed_stage("verify_agent", round=int(state.get("round") or 0)):
                verdicts = verify_agent.review(
                candidates=cands,
                per_candidate=report.per_candidate,
                query=state.get("query") or "",
                category=state.get("category") or "",
                dialect=state.get("dialect") or "postgresql",
                issue_sql=issue_sql,
                sandbox_fidelity=state.get("sandbox_fidelity") or "",
                efficiency=state.get("efficiency"),
                round_history=state.get("round_history") or [],
            )
            agent_verdicts = verdicts or {}
            if verdicts:
                by_cand = {c.id: c for c in cands}
                agent_reasons: List[str] = []
                for cvr in report.per_candidate:
                    v = verdicts.get(cvr.id)
                    if not v:
                        continue
                    fb = v.get("feedback") or ""
                    if cvr.passed and not v.get("pass"):
                        cvr.passed = False
                        msg = fb or "VerifyAgent 否决（疑似假过）"
                        cvr.errors.append(f"VerifyAgent: {msg}")
                        agent_reasons.append(f"[{cvr.id}] VerifyAgent: {msg}")
                        # 打压排序
                        cvr.rank_key = (cvr.rank_key or 0) + 1e12
                    elif (not cvr.passed) and v.get("pass"):
                        # 不允许覆盖执行失败
                        if any("执行失败" in e for e in (cvr.errors or [])):
                            if fb:
                                agent_reasons.append(
                                    f"[{cvr.id}] VerifyAgent(仍失败): {fb}"
                                )
                        else:
                            # 其它规则失败：仍保持否决，但把反馈留给 Generate
                            if fb:
                                agent_reasons.append(
                                    f"[{cvr.id}] VerifyAgent: {fb}"
                                )
                    elif fb and not cvr.passed:
                        agent_reasons.append(f"[{cvr.id}] VerifyAgent: {fb}")

                # 重选 best
                passed_list = [
                    (by_cand[cvr.id], cvr)
                    for cvr in report.per_candidate
                    if cvr.passed and cvr.id in by_cand
                ]
                if passed_list:
                    passed_list.sort(key=lambda x: x[1].rank_key)
                    report.best = passed_list[0][0]
                    report.passed = True
                    report.failure_reasons = []
                else:
                    report.best = None
                    report.passed = False
                    base = build_failure_reasons(report)
                    report.failure_reasons = list(dict.fromkeys(base + agent_reasons))

        hist = list(state.get("round_history") or [])
        hist.append(
            _summarize_round(
                int(state.get("round") or 0),
                cands,
                report,
                agent_verdicts=agent_verdicts,
            )
        )

        reasons = build_failure_reasons(report)
        if report.passed and report.best:
            default_logger.info(
                f"[Generate–Verify] 通过候选={report.best.id} "
                f"failure_reasons 清空"
            )
            return {
                "verify_report": report,
                "failure_reasons": [],
                "round_history": hist,
                "pred_sqls": list(report.best.pred_sqls),
                "success": True,
                "fallback": False,
            }

        default_logger.info(
            f"[Generate–Verify] 本轮全否决，failure_reasons={len(reasons)}"
        )
        return {
            "verify_report": report,
            "failure_reasons": reasons,
            "round_history": hist,
            "success": False,
        }

    def route_after_verify(state: OptState) -> str:
        if state.get("success"):
            return "end"
        rnd = int(state.get("round") or 0)
        if rnd < max_rounds:
            return "generate"
        return "fallback"

    def fallback_node(state: OptState) -> Dict[str, Any]:
        report = state.get("verify_report")
        issue_sql = list(state.get("issue_sql") or [])
        round_history = list(state.get("round_history") or [])

        if verify_agent is not None and round_history:
            final_pred = verify_agent.select_final(
                round_history=round_history,
                issue_sql=issue_sql,
                query=state.get("query") or "",
                category=state.get("category") or "",
                dialect=state.get("dialect") or "postgresql",
                sandbox_fidelity=state.get("sandbox_fidelity") or "",
                efficiency=state.get("efficiency"),
            )
            if final_pred:
                default_logger.warning(
                    "[Generate–Verify] 轮次耗尽，VerifyAgent 综合仲裁最终 pred_sqls"
                )
                return {
                    "pred_sqls": list(final_pred),
                    "success": False,
                    "fallback": True,
                }

        # 启发式回退：优先相对最优且无执行失败的候选
        if report and report.relative_best:
            rb = report.relative_best
            if not _candidate_has_exec_error(report, rb):
                default_logger.warning(
                    f"[Generate–Verify] 轮次耗尽，使用相对最优候选 {rb.id}"
                )
                return {
                    "pred_sqls": list(rb.pred_sqls),
                    "success": False,
                    "fallback": True,
                }
            # 再找其它无执行失败者
            by_id = {c.id: c for c in (state.get("candidates") or [])}
            ranked = sorted(report.per_candidate or [], key=lambda x: x.rank_key)
            for cvr in ranked:
                cand = by_id.get(cvr.id)
                if not cand:
                    continue
                if not _candidate_has_exec_error(report, cand):
                    default_logger.warning(
                        f"[Generate–Verify] 轮次耗尽，改用无执行失败候选 {cand.id}"
                    )
                    return {
                        "pred_sqls": list(cand.pred_sqls),
                        "success": False,
                        "fallback": True,
                    }
        default_logger.error("[Generate–Verify] 轮次耗尽且无可用候选，硬失败（不回退 issue_sql）")
        return {
            "pred_sqls": [],
            "success": False,
            "fallback": False,
            "error": "hard_fail: 轮次耗尽且无可用候选",
        }

    g = StateGraph(OptState)
    g.add_node("generate", generate_node)
    g.add_node("verify", verify_node)
    g.add_node("fallback", fallback_node)
    g.set_entry_point("generate")
    g.add_edge("generate", "verify")
    g.add_conditional_edges(
        "verify",
        route_after_verify,
        {"generate": "generate", "fallback": "fallback", "end": END},
    )
    g.add_edge("fallback", END)
    return g.compile()


def run_optimization(
    *,
    diagnosis_report: str,
    issue_sql: Any,
    query: str,
    schema: str,
    category: str,
    dialect: str,
    opt_config: Dict[str, Any],
    efficiency: Optional[bool] = None,
    preprocess_sql: Any = None,
    clean_up_sql: Any = None,
    sandbox_fidelity: str = "",
    sampled_tables: Optional[List[str]] = None,
    reset_fn: Optional[Callable[[], None]] = None,
    execute_fn: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
    explain_fn: Optional[Callable[[str], Any]] = None,
) -> OptimizationResult:
    cfg = opt_config or {}
    max_rounds = int(cfg.get("max_rounds", 3))
    timeout_ms = int(cfg.get("exec_timeout_ms", 5000))
    verify_mode = str(cfg.get("verify_mode") or "agent+rules")
    sampled = bool(sampled_tables) or (
        "mode=采样" in (sandbox_fidelity or "") or "mode=采样(" in (sandbox_fidelity or "")
    )
    qep_hard_gate = not sampled

    generate_agent = GenerateAgent(cfg)
    verifier = Verifier(
        dialect=dialect,
        category=category,
        exec_timeout_ms=timeout_ms,
        efficiency=efficiency,
        reset_fn=reset_fn,
        execute_fn=execute_fn,
        explain_fn=explain_fn,
        preprocess_sql=preprocess_sql,
        clean_up_sql=clean_up_sql,
        query=query or "",
        schema=schema or "",
        qep_hard_gate=qep_hard_gate,
    )

    verify_agent = None
    if verify_mode.strip().lower() not in ("rules_only", "rules", "rule"):
        try:
            from optimization.verify_agent import VerifyAgent

            verify_agent = VerifyAgent(cfg, reset_fn=reset_fn)
        except Exception as e:
            default_logger.warning(f"VerifyAgent 初始化失败，降级 rules_only: {e}")
            verify_mode = "rules_only"

    graph = create_optimization_graph(
        generate_agent=generate_agent,
        verifier=verifier,
        max_rounds=max_rounds,
        verify_agent=verify_agent,
        verify_mode=verify_mode,
    )

    issue_list = _as_sql_list(issue_sql)
    pre_list = _as_sql_list(preprocess_sql)
    cu_list = _as_sql_list(clean_up_sql)
    init: OptState = {
        "round": 0,
        "candidates": [],
        "verify_report": None,
        "failure_reasons": [],
        "round_history": [],
        "pred_sqls": [],
        "diagnosis_report": diagnosis_report or "",
        "issue_sql": issue_list,
        "query": query or "",
        "schema": schema or "",
        "category": category or "",
        "dialect": dialect or "postgresql",
        "efficiency": efficiency,
        "preprocess_sql": pre_list,
        "clean_up_sql": cu_list,
        "sandbox_fidelity": sandbox_fidelity or "",
        "success": False,
        "fallback": False,
        "error": None,
    }

    with timed_stage("optimization_total"):
        final = graph.invoke(init)
    with timed_stage("pred_assemble"):
        pred = list(final.get("pred_sqls") or [])
    return OptimizationResult(
        pred_sqls=pred,
        success=bool(final.get("success")),
        rounds=int(final.get("round") or 0),
        verify_report=final.get("verify_report"),
        failure_reasons=list(final.get("failure_reasons") or []),
        fallback=bool(final.get("fallback")),
        error=final.get("error"),
    )
