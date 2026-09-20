"""统一单条实例推理链路：抽表 → 沙箱 → 装配模式 → pred_sqls。

入口：scripts/infer.py；批跑 run_bird_critic / run_effi_sql / worker。

二维模式：
- function: general | efficiency
- assembly (mode): diag+opt | diag_only | opt_only | llm_only
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union

from diagnosis.agent import DiagnosisAgent, extract_involved_tables
from optimization.graph import run_optimization
from optimization.intent_spec import extract_intent_spec
from tools.sandbox_tool import SandboxTool
from utils.config_loader import is_effi_db_id, load_all_configs
from utils.diag_extract import extract_pred_sqls_from_diagnosis
from utils.llm_only import run_llm_only
from utils.logger import default_logger
from utils.pipeline import (
    fetch_schema_ddl,
    make_direct_verify_fns,
    make_verify_fns,
    normalize_tables_for_dialect,
    prepare_sandbox,
    resolve_and_validate_tables,
    setup_executors,
    sql_list_to_text,
)
from utils.sandbox_fidelity import format_fidelity_text, mark_preprocess_applied
from utils.stream_handler import StreamHandler
from utils.timing import (
    clear_timing_context,
    log_timing,
    log_timing_summary,
    set_timing_context,
)

VALID_MODES = ("diag+opt", "diag_only", "opt_only", "llm_only")
VALID_FUNCTIONS = ("general", "efficiency")

PhaseCallback = Optional[Callable[[str, Dict[str, Any]], None]]


def _emit_phase(on_phase: PhaseCallback, name: str, payload: Optional[Dict[str, Any]] = None) -> None:
    if on_phase is None:
        return
    try:
        on_phase(name, payload or {})
    except Exception:
        pass


@dataclass
class InferenceResult:
    """单条实例推理结果（不含 instance 元数据，由调用方合并）。"""

    diagnosis_report: str = ""
    pred_sqls: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    elapsed_ms: float = 0.0
    timing_summary: Dict[str, Any] = field(default_factory=dict)
    tables: List[str] = field(default_factory=list)
    schema: str = ""
    fidelity_text: str = ""
    sandbox_info: Dict[str, Any] = field(default_factory=dict)


def resolve_function(
    *,
    function: Optional[str] = None,
    category: str = "Query",
    efficiency: Optional[bool] = None,
) -> str:
    """BIRD 默认 general；category=Efficiency / efficiency=True / 显式 efficiency → efficiency。"""
    fn = (function or "").strip().lower()
    if fn in VALID_FUNCTIONS:
        return fn
    if efficiency is True or str(category or "").strip().lower() == "efficiency":
        return "efficiency"
    return "general"


def run_single_instance(
    *,
    sql: Union[str, List[str]],
    query: str = "",
    category: str = "Query",
    dialect: str = "postgresql",
    db_id: Optional[str] = None,
    preprocess_sql: Optional[str] = None,
    clean_up_sql: Optional[str] = None,
    exec_log: Optional[str] = None,
    original_schema: str = "",
    efficiency: Optional[bool] = None,
    mode: str = "diag+opt",
    function: Optional[str] = None,
    prior_diagnosis: Optional[str] = None,
    stream: bool = False,
    tables_override: Optional[List[str]] = None,
    slot_index: Optional[int] = None,
    workers: int = 1,
    instance_id: str = "-",
    configs: Optional[Dict[str, Any]] = None,
    logger=None,
    benchmark: Optional[str] = None,
    direct_target: Optional[bool] = None,
    on_phase: PhaseCallback = None,
) -> InferenceResult:
    """跑通一条实例；返回结构化结果，异常吞入 error/meta。

    mode (assembly):
      - diag+opt: 诊断 → Intent Spec → Generate–Verify
      - diag_only: 诊断须产出可执行 SQL，再抽取 pred_sqls
      - opt_only: 无诊断（空串），直接 Generate–Verify
      - llm_only: 单次 LLM → pred_sqls
    function:
      - general | efficiency（影响提示与 Verify 侧重）
    """
    log = logger or default_logger
    mode = str(mode or "diag+opt").strip().lower()
    if mode not in VALID_MODES:
        raise ValueError(f"未知推理模式: {mode}（可选 {VALID_MODES}）")
    fn = resolve_function(function=function, category=category, efficiency=efficiency)
    if efficiency is None and fn == "efficiency":
        efficiency = True
    if fn == "efficiency" and str(category or "").lower() != "efficiency":
        # Effi / 显式 efficiency：对齐类目，便于 Soft EX / Intent Spec
        if str(benchmark or "").lower() == "effi" or efficiency is True:
            category = "Efficiency"

    all_configs = configs or load_all_configs()
    use_direct = direct_target
    if use_direct is None:
        use_direct = str(benchmark or "").lower() == "effi" or is_effi_db_id(
            all_configs, db_id
        )

    result = InferenceResult()
    target_ex = sandbox_ex = None
    manager = None
    sandbox_cfg: Dict[str, Any] = {}
    t0 = time.perf_counter()

    def _finalize_meta_success(
        success: bool, rounds: int, fallback: bool, failure_reasons: List[str]
    ) -> None:
        result.pred_sqls = list(result.pred_sqls or [])
        spec_summary = result.meta.get("intent_spec")
        result.meta = {
            "success": success,
            "rounds": rounds,
            "fallback": fallback,
            "mode": mode,
            "function": fn,
            "failure_reasons": list(failure_reasons or []),
        }
        if spec_summary is not None:
            result.meta["intent_spec"] = spec_summary

    try:
        if isinstance(sql, list):
            issue_sql = [str(x).strip() for x in sql if str(x).strip()]
        else:
            issue_sql = [s.strip() for s in str(sql).split(";") if s.strip()]
        sql_text = sql_list_to_text(issue_sql)
        if not issue_sql:
            raise RuntimeError("issue SQL 为空")

        db_name = db_id or ""
        set_timing_context(
            instance_id=str(instance_id),
            db_id=str(db_name or "-"),
            dialect=dialect,
            worker=slot_index if slot_index is not None else 0,
            slot=slot_index,
        )
        target_ex, sandbox_ex, agent_cfg, opt_cfg, target_cfg, sandbox_cfg = (
            setup_executors(
                dialect=dialect,
                db_id=db_id,
                slot_index=slot_index,
                workers=workers,
                configs=all_configs,
                direct_target=bool(use_direct),
            )
        )
        if not db_name:
            db_name = str(target_cfg.get("database") or "financial")
        set_timing_context(slot=sandbox_cfg.get("database"))
        llm_cfg = agent_cfg.get("llm", {})

        # ---- 抽表 + 校验 ----
        if tables_override:
            tables = list(tables_override)
        else:
            tables = extract_involved_tables(
                sql_text, llm_cfg, user_prompt=str(query or "")
            )
        tables = normalize_tables_for_dialect(tables, dialect)
        tables = resolve_and_validate_tables(
            tables,
            target_ex,
            dialect=dialect,
            original_schema=str(original_schema or ""),
            hint_text=f"{query}\n{sql_text}",
        )
        if not tables:
            raise RuntimeError("未能从 issue_sql/用户问题提取表名")
        result.tables = tables
        _emit_phase(on_phase, "extract_tables", {"tables": tables})

        schema = fetch_schema_ddl(target_ex, tables)
        if original_schema and not schema.strip():
            schema = str(original_schema)
        result.schema = schema

        # ---- 沙箱 / 直连 Effi ----
        if use_direct:
            fidelity = {
                "mode": "direct_target",
                "note": "Effi-SQL: Soft EX / Agent 直连评测库，无 AstraTune 采样拷贝",
                "database": target_cfg.get("database"),
                "host": target_cfg.get("host"),
                "port": target_cfg.get("port"),
            }
            sandbox_info = {
                "fidelity": fidelity,
                "sampled_tables": [],
                "direct_target": True,
            }
            manager = None
        else:
            prepared = prepare_sandbox(
                target_ex,
                sandbox_ex,
                sandbox_cfg,
                tables,
                db_id=db_name,
                dialect=dialect,
            )
            manager = prepared["manager"]
            sandbox_info = prepared["info"]
            fidelity = sandbox_info.get("fidelity") or {}

        if preprocess_sql:
            fidelity = mark_preprocess_applied(fidelity, True)
            sandbox_info["fidelity"] = fidelity
        fidelity_text = format_fidelity_text(fidelity)
        result.fidelity_text = fidelity_text
        result.sandbox_info = sandbox_info
        _emit_phase(
            on_phase,
            "sandbox_setup",
            {
                "direct_target": bool(use_direct),
                "fidelity": (fidelity_text or "")[:800],
            },
        )

        # ---- llm_only：跳过诊断与 Verify ----
        if mode == "llm_only":
            result.diagnosis_report = ""
            pred = run_llm_only(
                issue_sql=issue_sql,
                query=query or "",
                schema=schema,
                dialect=dialect,
                category=category,
                function=fn,
                llm_config=(opt_cfg.get("llm") or llm_cfg),
            )
            result.pred_sqls = pred
            _finalize_meta_success(
                success=True, rounds=0, fallback=False, failure_reasons=[]
            )
            return result

        # ---- 诊断 ----
        diagnosis_report = ""
        if mode != "opt_only":
            _emit_phase(on_phase, "diagnosis", {"stage": "start"})
            tool = SandboxTool(sandbox_ex)

            def _on_reset(info: Dict[str, Any]) -> None:
                nonlocal fidelity, fidelity_text, sandbox_info
                if use_direct:
                    return
                sandbox_info = {**sandbox_info, **(info or {})}
                fidelity = sandbox_info.get("fidelity") or fidelity
                if preprocess_sql:
                    fidelity = mark_preprocess_applied(fidelity, True)
                    sandbox_info["fidelity"] = fidelity
                fidelity_text = format_fidelity_text(fidelity)
                result.fidelity_text = fidelity_text
                result.sandbox_info = sandbox_info

            if manager is not None:
                tool.bind_sandbox_reset(manager, tables, on_full_reset=_on_reset)
            if preprocess_sql:
                tool.register_preprocess_sql(preprocess_sql)
                tool.apply_preprocess_once()
            if clean_up_sql:
                tool.register_clean_up_sql(clean_up_sql)
            agent = DiagnosisAgent(agent_cfg, tool)
            common = dict(
                ori_sql=sql_text,
                schema=schema,
                exec_log=exec_log,
                sampled_tables=sandbox_info.get("sampled_tables"),
                preprocess_sql=preprocess_sql or None,
                clean_up_sql=clean_up_sql or None,
                user_prompt=query or None,
                sandbox_fidelity=fidelity_text,
                function=fn,
                assembly=mode,
            )
            if stream:
                stream_handler = StreamHandler(mode="rich")
                diagnosis_report = asyncio.run(
                    agent.diagnose_stream(stream_handler=stream_handler, **common)
                )
            else:
                diagnosis_report = agent.diagnose(**common)
        else:
            # opt_only：诊断为空；仅当显式传入 prior 时复用（续跑）
            prev_dr = (prior_diagnosis or "").strip()
            if prev_dr:
                diagnosis_report = prev_dr
                log.info("opt_only: 复用已有 diagnosis_report")
            else:
                diagnosis_report = ""
                log.info("opt_only: 无诊断，Generate 仅依题面+schema")
        result.diagnosis_report = diagnosis_report
        if mode != "opt_only":
            _emit_phase(
                on_phase,
                "diagnosis",
                {"stage": "end", "chars": len(diagnosis_report or "")},
            )

        intent_spec = extract_intent_spec(
            query=query or "",
            category=category,
            diagnosis_report=diagnosis_report,
            schema=schema,
            dialect=dialect,
            efficiency=efficiency if isinstance(efficiency, bool) else (fn == "efficiency"),
            llm_config=(opt_cfg.get("llm") or {}),
            use_llm=mode != "diag_only",
            issue_sql=issue_sql,
        )
        result.meta["intent_spec"] = intent_spec.summary()
        _emit_phase(on_phase, "intent_spec", {"summary": intent_spec.summary()})

        if mode == "diag_only":
            pred, used_fb = extract_pred_sqls_from_diagnosis(
                diagnosis_report, fallback_issue_sql=issue_sql
            )
            result.pred_sqls = pred
            _finalize_meta_success(
                success=not used_fb, rounds=0, fallback=used_fb, failure_reasons=[]
            )
            if used_fb:
                result.error = "diag_only: 未能从诊断抽取 SQL，回退 issue_sql"
            return result

        # ---- Generate–Verify ----
        if use_direct:
            reset_fn, execute_fn, explain_fn = make_direct_verify_fns(sandbox_ex)
        else:
            smart = str(opt_cfg.get("verify_isolation", "smart_reset")).lower() != "full_reset"
            initially_dirty = mode != "opt_only"
            reset_fn, execute_fn, explain_fn = make_verify_fns(
                manager,
                tables,
                sandbox_ex,
                dialect,
                smart_reset=smart,
                initially_dirty=initially_dirty,
            )

        opt = run_optimization(
            diagnosis_report=diagnosis_report,
            issue_sql=issue_sql,
            query=query,
            schema=schema,
            category=category,
            dialect=dialect,
            efficiency=efficiency if isinstance(efficiency, bool) else (fn == "efficiency"),
            function=fn,
            preprocess_sql=preprocess_sql or None,
            clean_up_sql=clean_up_sql or None,
            sandbox_fidelity=fidelity_text,
            sampled_tables=list(sandbox_info.get("sampled_tables") or []),
            intent_spec=intent_spec,
            sandbox_fidelity_dict=fidelity if isinstance(fidelity, dict) else None,
            opt_config=opt_cfg,
            reset_fn=reset_fn,
            execute_fn=execute_fn,
            explain_fn=explain_fn,
            on_phase=on_phase,
        )

        result.pred_sqls = list(opt.pred_sqls)
        _emit_phase(
            on_phase,
            "select_final",
            {
                "success": bool(opt.success),
                "fallback": bool(opt.fallback),
                "n_pred": len(result.pred_sqls),
            },
        )
        _finalize_meta_success(
            success=opt.success,
            rounds=opt.rounds,
            fallback=opt.fallback,
            failure_reasons=opt.failure_reasons,
        )
        if opt.error:
            result.error = opt.error
        return result
    except Exception as e:
        log.error(f"{instance_id} 失败: {e}", exc_info=True)
        if not any(str(x).strip() for x in (result.pred_sqls or [])):
            result.pred_sqls = []
        result.error = str(e)
        has_pred = any(str(x).strip() for x in (result.pred_sqls or []))
        _emit_phase(on_phase, "failed", {"error": str(e)})
        result.meta = {
            "success": False,
            "rounds": 0,
            "fallback": False,
            "hard_fail": not has_pred,
            "mode": mode,
            "function": fn,
            "failure_reasons": [str(e)],
        }
        return result
    finally:
        result.elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 1)
        log_timing(
            "run_one_total",
            result.elapsed_ms,
            slot_db=sandbox_cfg.get("database") if sandbox_cfg else None,
        )
        try:
            result.timing_summary = log_timing_summary()
        except Exception:
            result.timing_summary = {"stages": []}
        clear_timing_context()
        _release_resources(manager, target_ex, sandbox_ex, log)


def _release_resources(manager, target_ex, sandbox_ex, log=None) -> None:
    """统一释放：模板绑定 → 沙箱 → 目标库连接 → 沙箱连接（顺序不可变）。"""
    try:
        if manager is not None:
            try:
                from sandbox.template_cache import release_template_binding

                release_template_binding(manager)
            except Exception:
                pass
            manager.cleanup_sandbox()
    except Exception:
        pass
    try:
        if target_ex is not None:
            target_ex.close()
    except Exception:
        pass
    try:
        if sandbox_ex is not None:
            sandbox_ex.close()
    except Exception:
        pass
