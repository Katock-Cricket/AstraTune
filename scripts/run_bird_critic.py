#!/usr/bin/env python3
"""BIRD-CRITIC 批量推理：诊断 → Generate–Verify → pred_sqls（不读 sol）。"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

sys.path.append(str(Path(__file__).resolve().parent.parent))

from diagnosis.agent import DiagnosisAgent, extract_involved_tables
from optimization.graph import run_optimization
from tools.sandbox_tool import SandboxTool
from utils.config_loader import get_batch_config, load_all_configs, normalize_dialect
from utils.diag_extract import extract_pred_sqls_from_diagnosis
from utils.batch_preflight import preflight_batch_inference, release_batch_lock
from utils.logger import setup_logger
from utils.llm import batch_llm_pool_options, format_pool_status, init_llm_pool
from utils.timing import clear_timing_context, log_timing, log_timing_summary, set_timing_context
from utils.pipeline import (
    dedupe_jsonl_by_id,
    fetch_schema_ddl,
    filter_by_dialect,
    instance_issue_sql,
    load_jsonl,
    make_verify_fns,
    merge_schema_and_input,
    normalize_tables_for_dialect,
    prepare_sandbox,
    resolve_and_validate_tables,
    setup_executors,
    sql_list_to_text,
)

_WORKER_SCRIPT = Path(__file__).resolve().parent / "run_bird_worker.py"
_MAX_DIAG_CHARS = 4000
_MAX_TIMING_JSON = 2000


def parse_args():
    p = argparse.ArgumentParser(description="AstraTune BIRD-CRITIC 批量推理")
    p.add_argument("--schema", type=str, default=None, help="schema JSONL（可与 --input 合并）")
    p.add_argument("--input", type=str, default=None, help="完整或题面 JSONL")
    p.add_argument("--output", type=str, required=True, help="输出带 pred_sqls 的 JSONL")
    p.add_argument(
        "--dialect",
        type=str,
        default="postgresql",
        choices=["postgresql", "mysql", "pg", "postgres"],
    )
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--offset", type=int, default=0, help="跳过前 N 条（过滤后）")
    p.add_argument(
        "--ids",
        type=str,
        default=None,
        help="仅跑这些 instance_id（逗号分隔）",
    )
    p.add_argument(
        "--mode",
        type=str,
        default="diag+opt",
        choices=["diag+opt", "diag_only", "opt_only"],
        help="消融模式",
    )
    p.add_argument(
        "--skip-diagnosis",
        action="store_true",
        help="等价于 --mode opt_only（兼容旧参数）",
    )
    p.add_argument("--resume", action="store_true", help="跳过输出中 Verify 已通过的 id")
    p.add_argument(
        "--rerun-nonok",
        action="store_true",
        help="与 --resume 联用：仅跳过 inference_ok，重跑 fallback/失败/空 pred",
    )
    p.add_argument(
        "--force-batch",
        action="store_true",
        help="忽略批次锁/残留进程警告（磁盘不足仍会中止）",
    )
    p.add_argument(
        "--instance-timeout",
        type=int,
        default=None,
        help="单题墙钟超时秒数（默认 config batch.instance_timeout_s，0 表示关闭）",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=None,
        help="沙箱槽位/题目并发数（默认 config batch.default_workers，与 LLM 工作池独立）",
    )
    p.add_argument(
        "--llm-workers",
        type=int,
        default=None,
        help="LLM 工作池大小（默认=OPENAI_API_KEYS 条数，且不会超过 key 数）",
    )
    p.add_argument("--log-file", type=str, default=None)
    p.add_argument(
        "--isolate",
        type=str,
        default="subprocess",
        choices=["subprocess", "inproc"],
        help="subprocess=单题子进程+超时强杀（默认）；inproc=线程内直连（易假死）",
    )
    return p.parse_args()


def resolve_mode(args) -> str:
    if args.skip_diagnosis:
        return "opt_only"
    return args.mode


def load_records(args, dialect: str) -> List[Dict[str, Any]]:
    schema_recs: List[Dict[str, Any]] = []
    input_recs: List[Dict[str, Any]] = []

    if args.schema:
        schema_recs = load_jsonl(args.schema)
    if args.input:
        input_recs = load_jsonl(args.input)

    if input_recs and schema_recs:
        records = merge_schema_and_input(schema_recs, input_recs)
    elif input_recs:
        records = input_recs
    elif schema_recs:
        records = schema_recs
    else:
        raise SystemExit("请提供 --input 和/或 --schema")

    records = filter_by_dialect(records, dialect)
    usable = [r for r in records if instance_issue_sql(r)]
    if not usable:
        raise SystemExit(
            "过滤后无可用记录（需要 issue_sql）。"
            "Flash 请使用 --schema flash_schema.jsonl "
            "--input flash/flash-00000-of-00001.jsonl"
        )

    if args.ids:
        want: Set[str] = {x.strip() for x in args.ids.split(",") if x.strip()}
        usable = [
            r
            for r in usable
            if str(r.get("instance_id")) in want or r.get("instance_id") in want
        ]

    offset = max(0, int(args.offset or 0))
    if offset:
        usable = usable[offset:]
    if args.limit is not None:
        usable = usable[: max(0, args.limit)]
    return usable


def load_done_ids(
    output_path: str, *, rerun_nonok: bool = False
) -> Dict[Any, Dict[str, Any]]:
    path = Path(output_path)
    if not path.is_file():
        return {}
    done: Dict[Any, Dict[str, Any]] = {}
    for rec in load_jsonl(path):
        iid = rec.get("instance_id")
        if iid is None:
            continue
        if rerun_nonok:
            # 仅 Verify 通过视为完成；fallback / 失败 / 空 pred 可重跑
            if is_inference_success(rec):
                done[iid] = rec
        elif is_inference_success(rec) or has_pred_sqls(rec.get("pred_sqls")):
            done[iid] = rec
    return done


def load_all_pred_records(output_path: str) -> Dict[Any, Dict[str, Any]]:
    """输出文件中每个 instance_id 的最后一条（含失败/未完成）。"""
    path = Path(output_path)
    if not path.is_file():
        return {}
    by_id: Dict[Any, Dict[str, Any]] = {}
    for rec in load_jsonl(path):
        iid = rec.get("instance_id")
        if iid is not None:
            by_id[iid] = rec
    return by_id


def has_pred_sqls(pred: Any) -> bool:
    if isinstance(pred, list):
        return any(str(x).strip() for x in pred)
    return bool(str(pred or "").strip())


def is_inference_success(rec: Dict[str, Any]) -> bool:
    """推理成功：Verify 通过、无 fallback/error、pred_sqls 非空。不含 issue_sql 硬填。"""
    if rec.get("error"):
        return False
    meta = rec.get("optimization_meta") or {}
    if meta.get("fallback"):
        return False
    if not meta.get("success"):
        return False
    return has_pred_sqls(rec.get("pred_sqls"))


def slim_output_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """压缩 pred 行体积，避免 JSONL/日志拖死磁盘与终端。"""
    out = copy.deepcopy(rec)
    for key in ("original_schema", "preprocess_schema"):
        out.pop(key, None)
    dr = out.get("diagnosis_report")
    if isinstance(dr, str) and len(dr) > _MAX_DIAG_CHARS:
        out["diagnosis_report"] = dr[:_MAX_DIAG_CHARS] + "…[truncated]"
    ts = out.get("timing_summary")
    if ts is not None:
        raw = json.dumps(ts, ensure_ascii=False)
        if len(raw) > _MAX_TIMING_JSON:
            stages = (ts or {}).get("stages") or []
            out["timing_summary"] = {
                "truncated": True,
                "stages_count": len(stages),
            }
    return out


def finalize_inference_record(out: Dict[str, Any]) -> Dict[str, Any]:
    """有可用 pred 时不清空；hard_fail 仅限真空候选/异常。"""
    meta = out.setdefault("optimization_meta", {})
    nonempty = has_pred_sqls(out.get("pred_sqls"))
    if nonempty:
        meta["hard_fail"] = False
        return out
    meta["success"] = False
    meta["hard_fail"] = True
    out["pred_sqls"] = []
    if not out.get("error"):
        reasons = meta.get("failure_reasons") or []
        out["error"] = reasons[0] if reasons else "推理未通过 Verify 且无可用候选"
    return out


def append_jsonl(record: Dict[str, Any], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    slim = slim_output_record(record)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(slim, ensure_ascii=False) + "\n")


def _timeout_row(
    rec: Dict[str, Any],
    err: str,
    *,
    mode: str,
    prior_pred: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = copy.deepcopy(rec)
    out.pop("sol_sql", None)
    out.pop("test_cases", None)
    out["error"] = err
    if prior_pred and has_pred_sqls(prior_pred.get("pred_sqls")):
        out["pred_sqls"] = list(prior_pred.get("pred_sqls") or [])
        prev_dr = prior_pred.get("diagnosis_report")
        if prev_dr and str(prev_dr).strip():
            out["diagnosis_report"] = str(prev_dr)
        prev_meta = prior_pred.get("optimization_meta") or {}
        out["optimization_meta"] = {
            "success": False,
            "rounds": int(prev_meta.get("rounds") or 0),
            "fallback": True,
            "hard_fail": False,
            "mode": mode,
            "failure_reasons": [err],
        }
    else:
        out["pred_sqls"] = []
        out["optimization_meta"] = {
            "success": False,
            "rounds": 0,
            "fallback": False,
            "hard_fail": True,
            "mode": mode,
            "failure_reasons": [err],
        }
    return finalize_inference_record(out)


def _parse_worker_stdout(stdout: str) -> Optional[Dict[str, Any]]:
    for ln in reversed((stdout or "").splitlines()):
        line = ln.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


def _kill_proc_tree(proc: subprocess.Popen) -> None:
    try:
        proc.kill()
    except OSError:
        pass
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pass


def _run_one_subprocess(
    record: Dict[str, Any],
    *,
    dialect: str,
    mode: str,
    logger,
    slot_index: Optional[int],
    workers: int,
    timeout_s: int,
    prior_pred: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    iid = record.get("instance_id")
    payload = {
        "record": record,
        "dialect": dialect,
        "mode": mode,
        "slot_index": slot_index,
        "workers": workers,
        "prior_pred": prior_pred,
    }
    fd, path = tempfile.mkstemp(suffix=".json", prefix="bird_worker_")
    os.close(fd)
    try:
        Path(path).write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )
        cmd = [sys.executable, str(_WORKER_SCRIPT), "--payload", path]
        run_timeout = timeout_s if timeout_s > 0 else None
        try:
            cp = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=run_timeout,
            )
        except subprocess.TimeoutExpired as te:
            logger.error(f"{iid}: 子进程超时 {timeout_s}s，已强杀")
            if te.process is not None:
                _kill_proc_tree(te.process)
            return _timeout_row(
                record,
                f"instance_timeout_{timeout_s}s",
                mode=mode,
                prior_pred=prior_pred,
            )

        if cp.returncode != 0:
            tail = (cp.stderr or cp.stdout or "").strip()[-500:]
            logger.error(f"{iid}: worker exit {cp.returncode}: {tail}")
            parsed = _parse_worker_stdout(cp.stdout or "")
            if parsed is not None and has_pred_sqls(parsed.get("pred_sqls")):
                parsed.setdefault("error", tail or f"worker_exit_{cp.returncode}")
                return finalize_inference_record(parsed)
            return _timeout_row(
                record,
                f"worker_exit_{cp.returncode}",
                mode=mode,
                prior_pred=prior_pred,
            )

        parsed = _parse_worker_stdout(cp.stdout or "")
        if parsed is None:
            logger.error(f"{iid}: worker 无有效 JSON 输出")
            return _timeout_row(
                record,
                "worker_empty_output",
                mode=mode,
                prior_pred=prior_pred,
            )
        return finalize_inference_record(parsed)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def run_one(
    record: Dict[str, Any],
    *,
    dialect: str,
    mode: str,
    logger,
    slot_index: Optional[int] = None,
    workers: int = 1,
    prior_pred: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = copy.deepcopy(record)
    out.pop("sol_sql", None)
    out.pop("test_cases", None)

    issue_sql = instance_issue_sql(record)
    db_id = record.get("db_id") or "financial"
    category = record.get("category") or (
        "Efficiency" if record.get("efficiency") else "Query"
    )
    efficiency = record.get("efficiency")
    if efficiency is None and str(category).lower() == "efficiency":
        efficiency = True
    query = record.get("query") or ""
    preprocess = sql_list_to_text(record.get("preprocess_sql"))
    cleanup = sql_list_to_text(record.get("clean_up_sql"))
    schema_from_record = (
        record.get("original_schema") or record.get("preprocess_schema") or ""
    )

    target_ex = sandbox_ex = None
    manager = None
    sandbox_cfg: Dict[str, Any] = {}
    t0 = time.perf_counter()
    try:
        set_timing_context(
            instance_id=str(record.get("instance_id") or "-"),
            db_id=str(db_id),
            dialect=dialect,
            worker=slot_index if slot_index is not None else 0,
            slot=slot_index,
        )
        target_ex, sandbox_ex, agent_cfg, opt_cfg, _t, sandbox_cfg = setup_executors(
            dialect=dialect,
            db_id=db_id,
            slot_index=slot_index,
            workers=workers,
        )
        set_timing_context(slot=sandbox_cfg.get("database"))
        llm_cfg = agent_cfg.get("llm", {})
        sql_text = sql_list_to_text(issue_sql)

        tables = extract_involved_tables(
            sql_text, llm_cfg, user_prompt=str(query or "")
        )
        tables = normalize_tables_for_dialect(tables, dialect)
        tables = resolve_and_validate_tables(
            tables,
            target_ex,
            dialect=dialect,
            original_schema=str(schema_from_record or ""),
            hint_text=f"{query}\n{sql_text}",
        )
        if not tables:
            raise RuntimeError("未能从 issue_sql/用户问题提取表名")

        schema = fetch_schema_ddl(target_ex, tables)
        if schema_from_record and not schema.strip():
            schema = str(schema_from_record)

        prepared = prepare_sandbox(
            target_ex,
            sandbox_ex,
            sandbox_cfg,
            tables,
            db_id=db_id,
            dialect=dialect,
        )
        manager = prepared["manager"]
        sandbox_info = prepared["info"]
        from utils.sandbox_fidelity import format_fidelity_text, mark_preprocess_applied

        fidelity = sandbox_info.get("fidelity") or {}
        if preprocess:
            fidelity = mark_preprocess_applied(fidelity, True)
            sandbox_info["fidelity"] = fidelity
        fidelity_text = format_fidelity_text(fidelity)

        diagnosis_report = ""
        skip_diag = mode == "opt_only"
        if not skip_diag:
            tool = SandboxTool(sandbox_ex)

            def _on_reset(info: Dict[str, Any]) -> None:
                nonlocal fidelity, fidelity_text, sandbox_info
                sandbox_info = {**sandbox_info, **(info or {})}
                fidelity = sandbox_info.get("fidelity") or fidelity
                if preprocess:
                    fidelity = mark_preprocess_applied(fidelity, True)
                    sandbox_info["fidelity"] = fidelity
                fidelity_text = format_fidelity_text(fidelity)

            tool.bind_sandbox_reset(manager, tables, on_full_reset=_on_reset)
            if preprocess:
                tool.register_preprocess_sql(preprocess)
                tool.apply_preprocess_once()
            if cleanup:
                tool.register_clean_up_sql(cleanup)
            agent = DiagnosisAgent(agent_cfg, tool)
            diagnosis_report = agent.diagnose(
                ori_sql=sql_text,
                schema=schema,
                sampled_tables=sandbox_info.get("sampled_tables"),
                preprocess_sql=preprocess or None,
                clean_up_sql=cleanup or None,
                user_prompt=query or None,
                sandbox_fidelity=fidelity_text,
            )
        else:
            prev_dr = (prior_pred or {}).get("diagnosis_report")
            if prev_dr and str(prev_dr).strip():
                diagnosis_report = str(prev_dr).strip()
                logger.info("opt_only: 复用已有 diagnosis_report")
            else:
                diagnosis_report = f"跳过诊断。用户问题: {query}"

        if mode == "diag_only":
            pred, used_fb = extract_pred_sqls_from_diagnosis(
                diagnosis_report, fallback_issue_sql=issue_sql
            )
            out["pred_sqls"] = pred
            out["diagnosis_report"] = diagnosis_report
            out["optimization_meta"] = {
                "success": not used_fb,
                "rounds": 0,
                "fallback": used_fb,
                "mode": mode,
                "failure_reasons": [],
            }
            if used_fb:
                out["error"] = "diag_only: 未能从诊断抽取 SQL，回退 issue_sql"
            logger.info(
                f"{record.get('instance_id')}: diag_only pred={len(pred)} fallback={used_fb}"
            )
            return out

        smart = str(opt_cfg.get("verify_isolation", "smart_reset")).lower() != "full_reset"
        reset_fn, execute_fn, explain_fn = make_verify_fns(
            manager,
            tables,
            sandbox_ex,
            dialect,
            preprocess_sql=preprocess or None,
            smart_reset=smart,
        )

        opt = run_optimization(
            diagnosis_report=diagnosis_report,
            issue_sql=issue_sql,
            query=query,
            schema=schema,
            category=category,
            dialect=dialect,
            efficiency=efficiency if isinstance(efficiency, bool) else None,
            preprocess_sql=preprocess or None,
            clean_up_sql=cleanup or None,
            sandbox_fidelity=fidelity_text,
            sampled_tables=list(sandbox_info.get("sampled_tables") or []),
            opt_config=opt_cfg,
            reset_fn=reset_fn,
            execute_fn=execute_fn,
            explain_fn=explain_fn,
        )

        out["pred_sqls"] = opt.pred_sqls
        out["diagnosis_report"] = diagnosis_report
        out["optimization_meta"] = {
            "success": opt.success,
            "rounds": opt.rounds,
            "fallback": opt.fallback,
            "mode": mode,
            "failure_reasons": opt.failure_reasons,
        }
        if opt.error:
            out["error"] = opt.error
        finalize_inference_record(out)
        logger.info(
            f"{record.get('instance_id')}: pred_sqls={len(out.get('pred_sqls') or [])} "
            f"success={opt.success} rounds={opt.rounds} fallback={opt.fallback} "
            f"hard_fail={out.get('optimization_meta', {}).get('hard_fail')}"
        )
        return out
    except Exception as e:
        logger.error(f"{record.get('instance_id')} 失败: {e}", exc_info=True)
        if not has_pred_sqls(out.get("pred_sqls")):
            out["pred_sqls"] = []
        out["error"] = str(e)
        out["optimization_meta"] = {
            "success": False,
            "rounds": 0,
            "fallback": False,
            "hard_fail": not has_pred_sqls(out.get("pred_sqls")),
            "mode": mode,
            "failure_reasons": [str(e)],
        }
        return out
    finally:
        out["elapsed_ms"] = round((time.perf_counter() - t0) * 1000.0, 1)
        log_timing(
            "run_one_total",
            out["elapsed_ms"],
            slot_db=sandbox_cfg.get("database") if sandbox_cfg else None,
        )
        try:
            summary = log_timing_summary()
            out["timing_summary"] = summary
        except Exception:
            out["timing_summary"] = {"stages": []}
        clear_timing_context()
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


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(rows)
    success = 0
    fallback = 0
    errors = 0
    hard_fail = 0
    inference_ok = 0
    empty_pred = 0
    elapsed = []
    for r in rows:
        meta = r.get("optimization_meta") or {}
        if meta.get("success"):
            success += 1
        if meta.get("fallback"):
            fallback += 1
        if meta.get("hard_fail"):
            hard_fail += 1
        if r.get("error"):
            errors += 1
        if is_inference_success(r):
            inference_ok += 1
        if r.get("elapsed_ms") is not None:
            elapsed.append(float(r["elapsed_ms"]))
        if not has_pred_sqls(r.get("pred_sqls")):
            empty_pred += 1
    elapsed_sorted = sorted(elapsed)
    p50 = elapsed_sorted[len(elapsed_sorted) // 2] if elapsed_sorted else None
    return {
        "total": n,
        "success": success,
        "inference_ok": inference_ok,
        "fallback": fallback,
        "hard_fail": hard_fail,
        "empty_pred": empty_pred,
        "with_error": errors,
        "elapsed_ms_mean": round(sum(elapsed) / len(elapsed), 1) if elapsed else None,
        "elapsed_ms_p50": p50,
    }


def main():
    args = parse_args()
    logger = setup_logger(log_file=args.log_file)
    lock_path = Path(args.output).with_suffix(".batch.lock")
    preflight_batch_inference(
        lock_path=lock_path,
        force_batch=bool(args.force_batch),
    )
    dialect = normalize_dialect(args.dialect)
    mode = resolve_mode(args)
    configs = load_all_configs()
    batch_cfg = get_batch_config(configs)

    workers = int(args.workers if args.workers is not None else batch_cfg.get("default_workers", 4))
    max_w = int(batch_cfg.get("max_workers", 16))
    workers = max(1, min(workers, max_w))
    pool_opts = batch_llm_pool_options(batch_cfg, args.llm_workers)
    if args.isolate == "inproc":
        llm_workers = init_llm_pool(**pool_opts)
    else:
        llm_workers = int(pool_opts.get("max_workers") or batch_cfg.get("llm_workers") or 4)
        logger.info(
            f"isolate=subprocess：主进程不初始化 LLM 池（子进程各 1 槽，配置 llm_workers={llm_workers}）"
        )

    records = load_records(args, dialect)
    done_map: Dict[Any, Dict[str, Any]] = {}
    if args.resume:
        out_path = Path(args.output)
        if out_path.is_file():
            n = dedupe_jsonl_by_id(args.output)
            logger.info(f"resume: 去重输出文件 → {n} 条唯一 id")
        done_map = load_done_ids(args.output, rerun_nonok=bool(args.rerun_nonok))
        before = len(records)
        records = [r for r in records if r.get("instance_id") not in done_map]
        tag = "inference_ok" if args.rerun_nonok else "已完成"
        logger.info(f"resume: 跳过{tag} {before - len(records)} 条，剩余 {len(records)}")

    logger.info(
        f"待推理 {len(records)} 条，dialect={dialect} mode={mode} isolate={args.isolate} "
        f"sandbox_workers={workers} llm_workers={llm_workers}"
    )
    if args.isolate == "inproc":
        logger.info(format_pool_status())

    try:
        pred_map = load_all_pred_records(args.output) if args.resume else {}
        _run_batch(
            args, logger, dialect, mode, configs, batch_cfg, workers, records, done_map, pred_map
        )
    finally:
        release_batch_lock(lock_path)


def _run_batch(args, logger, dialect, mode, configs, batch_cfg, workers, records, done_map, pred_map):

    if args.resume and done_map and not Path(args.output).is_file():
        # 不应发生
        pass
    if not args.resume:
        # 覆盖写：先清空
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text("", encoding="utf-8")
    elif not Path(args.output).is_file():
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text("", encoding="utf-8")

    timeout_s = args.instance_timeout
    if timeout_s is None:
        timeout_s = int(batch_cfg.get("instance_timeout_s") or 0)
    timeout_s = int(timeout_s or 0)
    isolate = str(getattr(args, "isolate", "subprocess") or "subprocess")
    logger.info(
        f"instance_timeout_s={timeout_s} sandbox_workers={workers} isolate={isolate}"
    )

    outputs: List[Dict[str, Any]] = list(done_map.values()) if args.resume else []
    write_lock = __import__("threading").Lock()
    slot_q: Optional[Any] = None
    if workers > 1:
        from queue import Queue

        slot_q = Queue()
        for s in range(workers):
            slot_q.put(s)

    def _job(idx_rec):
        i, rec = idx_rec
        slot = None
        if slot_q is not None:
            set_timing_context(
                instance_id=str(rec.get("instance_id") or "-"),
                db_id=str(rec.get("db_id") or "-"),
                dialect=dialect,
            )
            wait_t0 = time.perf_counter()
            slot = slot_q.get()
            log_timing("slot_wait", (time.perf_counter() - wait_t0) * 1000.0, slot=slot)
        logger.info(
            f"==== [{i}/{len(records)}] {rec.get('instance_id')} "
            f"db={rec.get('db_id')} slot={slot} ===="
        )
        prior = pred_map.get(rec.get("instance_id"))

        try:
            if isolate == "subprocess":
                return _run_one_subprocess(
                    rec,
                    dialect=dialect,
                    mode=mode,
                    logger=logger,
                    slot_index=slot,
                    workers=workers,
                    timeout_s=timeout_s,
                    prior_pred=prior,
                )
            return run_one(
                rec,
                dialect=dialect,
                mode=mode,
                logger=logger,
                slot_index=slot,
                workers=workers,
                prior_pred=prior,
            )
        finally:
            if slot_q is not None and slot is not None:
                slot_q.put(slot)

    indexed = list(enumerate(records, 1))
    if workers <= 1:
        for item in indexed:
            row = _job(item)
            with write_lock:
                append_jsonl(row, args.output)
            outputs.append(row)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            it = iter(indexed)
            pending: Dict[Any, Any] = {}
            started: Dict[Any, float] = {}
            completed = 0
            total = len(indexed)

            def _submit_next() -> bool:
                try:
                    item = next(it)
                except StopIteration:
                    return False
                fut = pool.submit(_job, item)
                pending[fut] = item[1].get("instance_id")
                started[fut] = time.time()
                return True

            for _ in range(min(workers, total)):
                _submit_next()

            while pending:
                done, _ = wait(pending, timeout=30, return_when=FIRST_COMPLETED)
                if not done:
                    oldest = sorted(pending, key=lambda f: started[f])[:workers]
                    inflight = [
                        f"{pending[f]}({int(time.time() - started[f])}s)" for f in oldest
                    ]
                    stuck = [
                        f"{pending[f]}({int(time.time() - started[f])}s)"
                        for f in oldest
                        if timeout_s > 0 and (time.time() - started[f]) > timeout_s
                    ]
                    logger.info(
                        f"心跳: 已完成 {completed}/{total}；进行中 {inflight}"
                    )
                    if stuck:
                        logger.error(
                            f"单题超过 {timeout_s}s 仍未返回：{stuck}"
                        )
                        if isolate == "inproc":
                            logger.error(
                                "inproc 模式无法强杀线程；建议 --isolate subprocess 或降低 --workers"
                            )
                    continue
                for fut in done:
                    iid = pending.pop(fut)
                    started.pop(fut, None)
                    row = fut.result()
                    with write_lock:
                        append_jsonl(row, args.output)
                    outputs.append(row)
                    completed += 1
                    logger.info(f"进度: {completed}/{total} 刚完成 id={iid}")
                    _submit_next()

    # resume 时文件含追加重复行；流式去重后汇总
    unique_count = dedupe_jsonl_by_id(args.output) if Path(args.output).is_file() else 0
    final_rows = load_jsonl(args.output) if unique_count else outputs
    summary = _summarize(final_rows)
    logger.info(f"已写出 {unique_count or len(final_rows)} 条 → {args.output}")
    logger.info(f"汇总: {json.dumps(summary, ensure_ascii=False)}")
    print(json.dumps({"output": args.output, **summary}, ensure_ascii=False))


if __name__ == "__main__":
    main()
