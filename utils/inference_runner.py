"""统一单条实例推理链路：抽表 → 沙箱 → 诊断 → Generate–Verify → pred_sqls。

四个入口共用本模块：
- scripts/diag.py（mode=diag_only，流式诊断）
- scripts/opt.py（mode=opt_only，prior_diagnosis=CLI 传入）
- scripts/infer.py（mode=diag+opt / opt_only）
- scripts/run_bird_critic.py::run_one（mode 由批跑参数决定，prior_pred 续跑）

资源（沙箱模板绑定 / 沙箱数据 / 目标库与沙箱连接）在 finally 中统一释放；
异常不外抛，结构化写入 InferenceResult.error / meta。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from diagnosis.agent import DiagnosisAgent, extract_involved_tables
from optimization.graph import run_optimization
from optimization.intent_spec import extract_intent_spec
from tools.sandbox_tool import SandboxTool
from utils.config_loader import load_all_configs
from utils.diag_extract import extract_pred_sqls_from_diagnosis
from utils.logger import default_logger
from utils.pipeline import (
    fetch_schema_ddl,
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

VALID_MODES = ("diag+opt", "diag_only", "opt_only")


@dataclass
class InferenceResult:
    """单条实例推理结果（不含 instance 元数据，由调用方合并）。"""

    diagnosis_report: str = ""
    pred_sqls: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)  # optimization_meta
    error: Optional[str] = None
    elapsed_ms: float = 0.0
    timing_summary: Dict[str, Any] = field(default_factory=dict)
    tables: List[str] = field(default_factory=list)
    schema: str = ""
    fidelity_text: str = ""
    sandbox_info: Dict[str, Any] = field(default_factory=dict)


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
    prior_diagnosis: Optional[str] = None,
    stream: bool = False,
    tables_override: Optional[List[str]] = None,
    slot_index: Optional[int] = None,
    workers: int = 1,
    instance_id: str = "-",
    configs: Optional[Dict[str, Any]] = None,
    logger=None,
) -> InferenceResult:
    """跑通一条实例；返回结构化结果，异常吞入 error/meta。

    mode:
      - diag+opt: 完整链路（诊断 → 优化）
      - diag_only: 仅诊断，并从报告抽取 pred_sqls（回退 issue_sql）
      - opt_only: 跳过诊断（复用 prior_diagnosis 或占位文案），直接优化
    """
    log = logger or default_logger
    mode = str(mode or "diag+opt").strip().lower()
    if mode not in VALID_MODES:
        raise ValueError(f"未知推理模式: {mode}（可选 {VALID_MODES}）")

    result = InferenceResult()
    target_ex = sandbox_ex = None
    manager = None
    sandbox_cfg: Dict[str, Any] = {}
    t0 = time.perf_counter()
    def _finalize_meta_success(success: bool, rounds: int, fallback: bool,
                                failure_reasons: List[str]) -> None:
        result.pred_sqls = list(result.pred_sqls or [])
        spec_summary = result.meta.get("intent_spec")
        result.meta = {
            "success": success,
            "rounds": rounds,
            "fallback": fallback,
            "mode": mode,
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
                configs=configs or load_all_configs(),
            )
        )
        if not db_name:
            db_name = str(target_cfg.get("database") or "financial")
        set_timing_context(slot=sandbox_cfg.get("database"))
        llm_cfg = agent_cfg.get("llm", {})

        # ---- 抽表 + 校验回退（LLM 抽表 → original_schema → 全库基表）----
        if tables_override:
            tables = list(tables_override)
        else:
            tables = extract_involved_tables(sql_text, llm_cfg, user_prompt=str(query or ""))
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

        schema = fetch_schema_ddl(target_ex, tables)
        if original_schema and not schema.strip():
            schema = str(original_schema)
        result.schema = schema

        # ---- 沙箱（优先模板缓存）----
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

        # ---- 诊断 ----
        diagnosis_report = ""
        if mode != "opt_only":
            tool = SandboxTool(sandbox_ex)

            def _on_reset(info: Dict[str, Any]) -> None:
                nonlocal fidelity, fidelity_text, sandbox_info
                sandbox_info = {**sandbox_info, **(info or {})}
                fidelity = sandbox_info.get("fidelity") or fidelity
                if preprocess_sql:
                    fidelity = mark_preprocess_applied(fidelity, True)
                    sandbox_info["fidelity"] = fidelity
                fidelity_text = format_fidelity_text(fidelity)
                result.fidelity_text = fidelity_text
                result.sandbox_info = sandbox_info

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
            )
            if stream:
                stream_handler = StreamHandler(mode="rich")
                diagnosis_report = asyncio.run(
                    agent.diagnose_stream(stream_handler=stream_handler, **common)
                )
            else:
                diagnosis_report = agent.diagnose(**common)
        else:
            prev_dr = (prior_diagnosis or "").strip()
            if prev_dr:
                diagnosis_report = prev_dr
                log.info("opt_only: 复用已有 diagnosis_report")
            else:
                diagnosis_report = f"跳过诊断。用户问题: {query or '(无)'}"
        result.diagnosis_report = diagnosis_report

        intent_spec = extract_intent_spec(
            query=query or "",
            category=category,
            diagnosis_report=diagnosis_report,
            schema=schema,
            dialect=dialect,
            efficiency=efficiency if isinstance(efficiency, bool) else None,
            llm_config=(opt_cfg.get("llm") or {}),
            use_llm=mode != "diag_only",
            issue_sql=issue_sql,
        )
        result.meta["intent_spec"] = intent_spec.summary()

        # ---- 仅诊断：从报告抽取 pred_sqls ----
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
        smart = str(opt_cfg.get("verify_isolation", "smart_reset")).lower() != "full_reset"
        # opt_only 未跑诊断，沙箱刚建好，无需保守视为脏
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
            efficiency=efficiency if isinstance(efficiency, bool) else None,
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
        )

        result.pred_sqls = list(opt.pred_sqls)
        _finalize_meta_success(
            success=opt.success,
            rounds=opt.rounds,
            fallback=opt.fallback,
            failure_reasons=opt.failure_reasons,
        )
        if opt.error:
            result.error = opt.error
        # hard_fail 判定（有 pred 不清空）由调用方 finalize_inference_record 统一处理
        return result
    except Exception as e:
        log.error(f"{instance_id} 失败: {e}", exc_info=True)
        if not any(str(x).strip() for x in (result.pred_sqls or [])):
            result.pred_sqls = []
        result.error = str(e)
        has_pred = any(str(x).strip() for x in (result.pred_sqls or []))
        result.meta = {
            "success": False,
            "rounds": 0,
            "fallback": False,
            "hard_fail": not has_pred,
            "mode": mode,
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
