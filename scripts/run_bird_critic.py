#!/usr/bin/env python3
"""BIRD-CRITIC 批量推理：诊断 → Generate–Verify → pred_sqls（不读 sol）。"""

from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.batch_records import (
    _parse_worker_stdout,
    _summarize,
    _timeout_row,
    append_jsonl,
    finalize_inference_record,
    has_pred_sqls,
    is_inference_success,
    load_all_pred_records,
    load_done_ids,
    slim_output_record,
)
from utils.batch_preflight import preflight_batch_inference, release_batch_lock
from utils.config_loader import get_batch_config, load_all_configs, normalize_dialect
from utils.inference_runner import run_single_instance
from utils.logger import setup_logger
from utils.llm import batch_llm_pool_options, format_pool_status, init_llm_pool
from utils.pipeline import (
    dedupe_jsonl_by_id,
    filter_by_dialect,
    instance_issue_sql,
    load_jsonl,
    merge_schema_and_input,
    sql_list_to_text,
)
from utils.timing import log_timing, set_timing_context

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
        choices=["diag+opt", "diag_only", "opt_only", "llm_only"],
        help="装配模式（四种均产出 pred_sqls）",
    )
    p.add_argument(
        "--function",
        type=str,
        default=None,
        choices=["general", "efficiency"],
        help="职能模式；默认由 category / efficiency 字段推断",
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
            "Flash 请使用 --schema benchmark_data/schema/flash_schema.jsonl "
            "--input benchmark_data/input/flash/flash-00000-of-00001.jsonl"
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
    function: Optional[str] = None,
) -> Dict[str, Any]:
    iid = record.get("instance_id")
    payload = {
        "record": record,
        "dialect": dialect,
        "mode": mode,
        "slot_index": slot_index,
        "workers": workers,
        "function": function,
    }
    if prior_pred:
        payload["prior_pred"] = {
            "pred_sqls": prior_pred.get("pred_sqls") or [],
            "diagnosis_report": prior_pred.get("diagnosis_report") or "",
            "optimization_meta": prior_pred.get("optimization_meta") or {},
        }
    blob = json.dumps(payload, ensure_ascii=False)

    worker_path = str(Path(__file__).resolve().parent / "run_bird_worker.py")
    payload_arg = blob
    payload_file: Optional[str] = None
    # Windows 命令行长度上限约 8k；大题面（如 card_games）走临时文件
    if len(blob) > 6000:
        fd, payload_file = tempfile.mkstemp(suffix=".json", prefix="bird_worker_")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(blob)
        payload_arg = f"@{payload_file}"
    try:
        try:
            proc = subprocess.Popen(
                [sys.executable, worker_path, payload_arg],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                cwd=str(Path(__file__).resolve().parent.parent),
            )
        except Exception as e:
            logger.error(f"启动 worker 失败 {iid}: {e}")
            return _timeout_row(record, f"worker spawn 失败: {e}", mode=mode, prior_pred=prior_pred)

        try:
            out, err = proc.communicate(timeout=timeout_s if timeout_s > 0 else None)
        except subprocess.TimeoutExpired:
            _kill_proc_tree(proc)
            logger.warning(f"子进程超时 {iid} ({timeout_s}s)，强杀")
            return _timeout_row(record, f"子进程超时 ({timeout_s}s)", mode=mode, prior_pred=prior_pred)
    finally:
        if payload_file:
            try:
                os.unlink(payload_file)
            except OSError:
                pass

    if proc.returncode != 0:
        tail = (err or "")[-1500:]
        logger.warning(
            f"worker exit code={proc.returncode} {iid}: {tail.strip()[:400]}"
        )
        if "worker_exit" in (err or "") or "Traceback" in (err or ""):
            row = _timeout_row(
                record, f"worker_exit({proc.returncode})", mode=mode, prior_pred=prior_pred
            )
            row["error"] = (row.get("error") or "") + f" | stderr_tail={tail[:600]}"
            return row
    parsed = _parse_worker_stdout(out or "")
    if not parsed:
        logger.warning(f"worker 无 stdout JSON {iid}; out_tail={(out or '')[-400:]}")
        if (out or "").strip() == "":
            return _timeout_row(record, "empty_output", mode=mode, prior_pred=prior_pred)
        return _timeout_row(
            record, "worker 无 stdout JSON", mode=mode, prior_pred=prior_pred
        )
    return parsed


def run_one(
    record: Dict[str, Any],
    *,
    dialect: str,
    mode: str,
    logger,
    slot_index: Optional[int] = None,
    workers: int = 1,
    prior_pred: Optional[Dict[str, Any]] = None,
    function: Optional[str] = None,
) -> Dict[str, Any]:
    """单题推理（进程内）。核心链路见 utils/inference_runner.run_single_instance。"""
    out = copy.deepcopy(record)
    out.pop("sol_sql", None)
    out.pop("test_cases", None)

    category = record.get("category") or (
        "Efficiency" if record.get("efficiency") else "Query"
    )
    efficiency = record.get("efficiency")
    if efficiency is None and str(category).lower() == "efficiency":
        efficiency = True
    schema_from_record = (
        record.get("original_schema") or record.get("preprocess_schema") or ""
    )
    fn = function or record.get("function")
    benchmark = record.get("benchmark")

    result = run_single_instance(
        sql=instance_issue_sql(record),
        query=record.get("query") or "",
        category=category,
        dialect=dialect,
        db_id=record.get("db_id") or "financial",
        preprocess_sql=sql_list_to_text(record.get("preprocess_sql")) or None,
        clean_up_sql=sql_list_to_text(record.get("clean_up_sql")) or None,
        original_schema=str(schema_from_record or ""),
        efficiency=efficiency,
        mode=mode,
        function=fn,
        prior_diagnosis=(prior_pred or {}).get("diagnosis_report"),
        slot_index=slot_index,
        workers=workers,
        instance_id=str(record.get("instance_id") or "-"),
        logger=logger,
        benchmark=str(benchmark) if benchmark else None,
    )

    out["pred_sqls"] = result.pred_sqls
    out["diagnosis_report"] = result.diagnosis_report
    out["optimization_meta"] = result.meta
    if result.error:
        out["error"] = result.error
    out["elapsed_ms"] = result.elapsed_ms
    out["timing_summary"] = result.timing_summary
    finalize_inference_record(out)
    logger.info(
        f"{record.get('instance_id')}: pred_sqls={len(out.get('pred_sqls') or [])} "
        f"success={out['optimization_meta'].get('success')} "
        f"rounds={out['optimization_meta'].get('rounds')} "
        f"fallback={out['optimization_meta'].get('fallback')} "
        f"hard_fail={out['optimization_meta'].get('hard_fail')} "
        f"function={out['optimization_meta'].get('function')}"
    )
    return out


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
    write_lock = threading.Lock()
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
            fn = getattr(args, "function", None)
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
                    function=fn,
                )
            return run_one(
                rec,
                dialect=dialect,
                mode=mode,
                logger=logger,
                slot_index=slot,
                workers=workers,
                prior_pred=prior,
                function=fn,
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
