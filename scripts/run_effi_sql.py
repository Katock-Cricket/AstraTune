#!/usr/bin/env python3
"""Effi-SQL 批量推理：读 benchmark_data/effi/input → pred + prediction JSONL。

对齐 BIRD 批跑踩坑（见 docs/过程文档.md）：
- 默认 --isolate subprocess：单题子进程 + 超时强杀，避免线程假死 / 内存堆积
- 主进程 subprocess 模式不建 LLM 池；子进程各 1 槽
- 有界在飞（slot 队列）+ 30s 心跳；勿一次 submit 全部
- 同 db_id 串行（共享评测库 Soft EX）；跨库可并行
- 写出 slim；--resume / --rerun-nonok 语义与 run_bird_critic 一致
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from queue import Queue
from typing import Any, Dict, List, Optional, Set

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.batch_preflight import (
    find_running_batch_pids,
    preflight_batch_inference,
    release_batch_lock,
)
from utils.batch_records import (
    append_jsonl,
    fallback_row,
    finalize_inference_record,
    has_pred_sqls,
    load_all_pred_records,
    load_done_ids,
    parse_worker_stdout,
    slim_output_record,
    summarize,
)
from utils.benchmark_data import EFFI_INPUT
from utils.config_loader import get_batch_config, load_all_configs
from utils.effi_adapter import adapt_effi_record, attach_prediction
from utils.inference_runner import VALID_MODES, run_single_instance
from utils.llm import batch_llm_pool_options, format_pool_status, init_llm_pool
from utils.logger import setup_logger
from utils.pipeline import dedupe_jsonl_by_id, load_jsonl
from utils.timing import log_timing, set_timing_context

_WORKER_SCRIPT = Path(__file__).resolve().parent / "run_effi_worker.py"


def parse_args():
    p = argparse.ArgumentParser(description="Effi-SQL 批量推理（end2end prediction）")
    p.add_argument("--input", type=str, default=str(EFFI_INPUT))
    p.add_argument("--output", type=str, default="outputs/effi_pred.jsonl")
    p.add_argument("--mode", type=str, default="diag+opt", choices=list(VALID_MODES))
    p.add_argument("--function", type=str, default="efficiency", choices=["general", "efficiency"])
    p.add_argument("--ids", type=str, default=None, help="逗号分隔 Effi 数字 id")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--resume", action="store_true", help="跳过输出中已完成的 id")
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
        help="单题墙钟超时秒数（默认 config batch.instance_timeout_s，0 关闭）",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=None,
        help="在飞子进程数（同库串行、跨库并行；默认 min(4, max_workers)）",
    )
    p.add_argument(
        "--llm-workers",
        type=int,
        default=None,
        help="仅 isolate=inproc 时生效；subprocess 下每子进程固定 1 槽",
    )
    p.add_argument("--log-file", type=str, default=None)
    p.add_argument(
        "--isolate",
        type=str,
        default="subprocess",
        choices=["subprocess", "inproc"],
        help="subprocess=单题子进程+超时强杀（默认）；inproc=线程内直连（易 OOM）",
    )
    return p.parse_args()


def load_effi_records(args) -> List[Dict[str, Any]]:
    id_filter: Optional[Set[int]] = None
    if args.ids:
        id_filter = {int(x.strip()) for x in args.ids.split(",") if x.strip()}

    records: List[Dict[str, Any]] = []
    with Path(args.input).open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            if id_filter is not None and int(row.get("id", -1)) not in id_filter:
                continue
            adapted = adapt_effi_record(row)
            adapted["_raw"] = row
            records.append(adapted)
    if args.limit is not None:
        records = records[: max(0, args.limit)]
    return records


def run_one_effi(
    record: Dict[str, Any],
    *,
    raw: Dict[str, Any],
    mode: str,
    function: str,
    logger,
    prior_pred: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """单题推理（进程内）。核心链路见 utils/inference_runner.run_single_instance。"""
    result = run_single_instance(
        sql=record["issue_sql"],
        query=record.get("query") or "",
        category=record.get("category") or "Efficiency",
        dialect="postgresql",
        db_id=record.get("db_id"),
        efficiency=True,
        mode=mode,
        function=function,
        prior_diagnosis=(prior_pred or {}).get("diagnosis_report"),
        instance_id=str(record.get("instance_id") or "-"),
        logger=logger,
        benchmark="effi",
        direct_target=True,
        stream=False,
    )
    base = attach_prediction(raw, result.pred_sqls)
    out = dict(base)
    out["instance_id"] = record.get("instance_id")
    out["db_id"] = record.get("db_id")
    out["effi_id"] = record.get("effi_id")
    out["pred_sqls"] = result.pred_sqls
    out["diagnosis_report"] = result.diagnosis_report
    out["optimization_meta"] = result.meta
    if result.error:
        out["error"] = result.error
    out["elapsed_ms"] = result.elapsed_ms
    out["timing_summary"] = result.timing_summary
    finalize_inference_record(out)
    # 空 pred 时仍补 prediction 空串，便于 Diff-SQL 字段对齐
    if "prediction" not in out:
        out["prediction"] = ""
    elif not has_pred_sqls(out.get("pred_sqls")):
        out["prediction"] = ""
    logger.info(
        f"{record.get('instance_id')}: pred_n={len(out.get('pred_sqls') or [])} "
        f"success={out['optimization_meta'].get('success')} "
        f"rounds={out['optimization_meta'].get('rounds')} "
        f"fallback={out['optimization_meta'].get('fallback')} "
        f"hard_fail={out['optimization_meta'].get('hard_fail')}"
    )
    return out


def _kill_proc_tree(proc: subprocess.Popen) -> None:
    try:
        proc.kill()
    except OSError:
        pass
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pass


def _effi_timeout_row(
    record: Dict[str, Any],
    raw: Dict[str, Any],
    err: str,
    *,
    mode: str,
    prior_pred: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    row = fallback_row(record, err, mode=mode, prior_pred=prior_pred)
    # 评测字段：合并 raw + prediction
    merged = attach_prediction(raw, row.get("pred_sqls") or [])
    for k, v in row.items():
        if k not in ("base_sql", "id", "db", "optimized_sql", "fast_time"):
            merged[k] = v
    merged["instance_id"] = record.get("instance_id")
    merged["db_id"] = record.get("db_id")
    return finalize_inference_record(merged)


def _slim_effi_raw(raw: Dict[str, Any]) -> Dict[str, Any]:
    """worker payload 只保留评测必需字段，勿塞 explain_analyze 等大字段（防 argv/内存膨胀）。"""
    keep = ("id", "db", "base_sql", "difficulty")
    return {k: raw[k] for k in keep if k in raw}


def _run_one_subprocess(
    record: Dict[str, Any],
    *,
    raw: Dict[str, Any],
    mode: str,
    function: str,
    logger,
    timeout_s: int,
    prior_pred: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    iid = record.get("instance_id")
    # 勿把 _raw / 超大字段塞进 argv
    rec = {k: v for k, v in record.items() if k != "_raw"}
    payload = {
        "record": rec,
        "raw": _slim_effi_raw(raw),
        "mode": mode,
        "function": function,
    }
    if prior_pred:
        dr = str(prior_pred.get("diagnosis_report") or "")
        if len(dr) > 4000:
            dr = dr[:4000] + "…[truncated]"
        payload["prior_pred"] = {
            "pred_sqls": prior_pred.get("pred_sqls") or [],
            "diagnosis_report": dr,
            "optimization_meta": prior_pred.get("optimization_meta") or {},
        }
    blob = json.dumps(payload, ensure_ascii=False)
    worker_path = str(_WORKER_SCRIPT)
    payload_arg = blob
    payload_file: Optional[str] = None
    if len(blob) > 6000:
        fd, payload_file = tempfile.mkstemp(suffix=".json", prefix="effi_worker_")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(blob)
        payload_arg = f"@{payload_file}"

    proc: Optional[subprocess.Popen] = None
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
            return _effi_timeout_row(
                record, raw, f"worker spawn 失败: {e}", mode=mode, prior_pred=prior_pred
            )

        try:
            out, err = proc.communicate(timeout=timeout_s if timeout_s > 0 else None)
        except subprocess.TimeoutExpired:
            _kill_proc_tree(proc)
            logger.warning(f"子进程超时 {iid} ({timeout_s}s)，强杀")
            return _effi_timeout_row(
                record, raw, f"子进程超时 ({timeout_s}s)", mode=mode, prior_pred=prior_pred
            )
    finally:
        if payload_file:
            try:
                os.unlink(payload_file)
            except OSError:
                pass

    assert proc is not None
    # 子进程 stderr 只记短尾，避免父日志膨胀
    if err and err.strip():
        tail = err.strip().replace("\n", " | ")
        if len(tail) > 500:
            tail = "…" + tail[-500:]
        logger.info(f"worker_stderr {iid}: {tail}")

    if proc.returncode != 0:
        tail = (err or "")[-1500:]
        logger.warning(
            f"worker exit code={proc.returncode} {iid}: {tail.strip()[:400]}"
        )
        if "Traceback" in (err or "") or proc.returncode != 0:
            row = _effi_timeout_row(
                record,
                raw,
                f"worker_exit({proc.returncode})",
                mode=mode,
                prior_pred=prior_pred,
            )
            row["error"] = (row.get("error") or "") + f" | stderr_tail={tail[:600]}"
            return row

    parsed = parse_worker_stdout(out or "")
    if not parsed:
        logger.warning(f"worker 无 stdout JSON {iid}; out_tail={(out or '')[-400:]}")
        return _effi_timeout_row(
            record,
            raw,
            "empty_output" if not (out or "").strip() else "worker 无 stdout JSON",
            mode=mode,
            prior_pred=prior_pred,
        )
    return parsed


def _preflight_effi(lock_path: Path, *, force_batch: bool) -> None:
    # 查 Effi 残留；顺带沿用磁盘/LLM 探测（不强制 AstraTune 5433）
    others = [
        p
        for p in find_running_batch_pids("run_effi_sql.py")
        if p != os.getpid()
    ]
    if others and not force_batch:
        raise SystemExit(
            f"检测到其它 run_effi_sql 进程: {others}。"
            f"请先结束残留进程或使用 --force-batch（慎用）。"
        )
    preflight_batch_inference(
        lock_path=lock_path,
        force_batch=force_batch,
        check_docker=False,
        min_free_gb=20.0,
    )


def main():
    args = parse_args()
    logger = setup_logger(log_file=args.log_file)
    lock_path = Path(args.output).with_suffix(".batch.lock")
    _preflight_effi(lock_path, force_batch=bool(args.force_batch))

    configs = load_all_configs()
    batch_cfg = get_batch_config(configs)
    max_w = int(batch_cfg.get("max_workers", 16))
    default_w = min(4, max_w)
    workers = int(args.workers if args.workers is not None else default_w)
    workers = max(1, min(workers, max_w))

    pool_opts = batch_llm_pool_options(batch_cfg, args.llm_workers)
    if args.isolate == "inproc":
        llm_workers = init_llm_pool(**pool_opts)
        logger.warning("isolate=inproc：多线程共享进程，易 OOM；生产请用 subprocess")
    else:
        llm_workers = 1
        logger.info(
            f"isolate=subprocess：主进程不初始化 LLM 池"
            f"（子进程各 1 槽；在飞 workers={workers}）"
        )

    records = load_effi_records(args)
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

    db_counts: Dict[str, int] = {}
    for rec in records:
        db = str(rec.get("db_id") or "?")
        db_counts[db] = db_counts.get(db, 0) + 1
    logger.info(
        f"待推理 {len(records)} 条 mode={args.mode} function={args.function} "
        f"isolate={args.isolate} workers={workers} llm_workers={llm_workers} dbs={db_counts}"
    )
    if args.isolate == "inproc":
        logger.info(format_pool_status())

    try:
        pred_map = load_all_pred_records(args.output) if args.resume else {}
        _run_batch(args, logger, batch_cfg, workers, records, done_map, pred_map)
    finally:
        release_batch_lock(lock_path)


def _run_batch(args, logger, batch_cfg, workers, records, done_map, pred_map):
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not args.resume:
        out_path.write_text("", encoding="utf-8")
    elif not out_path.is_file():
        out_path.write_text("", encoding="utf-8")

    timeout_s = args.instance_timeout
    if timeout_s is None:
        timeout_s = int(batch_cfg.get("instance_timeout_s") or 0)
    timeout_s = int(timeout_s or 0)
    isolate = str(args.isolate or "subprocess")
    logger.info(f"instance_timeout_s={timeout_s} workers={workers} isolate={isolate}")

    write_lock = threading.Lock()
    db_locks: Dict[str, threading.Lock] = {}
    locks_guard = threading.Lock()
    slot_q: Optional[Queue] = None
    if workers > 1:
        slot_q = Queue()
        for s in range(workers):
            slot_q.put(s)

    def _db_lock(db_id: str) -> threading.Lock:
        with locks_guard:
            return db_locks.setdefault(db_id, threading.Lock())

    def _job(idx_rec):
        i, rec = idx_rec
        raw = rec.get("_raw") or {}
        work = {k: v for k, v in rec.items() if k != "_raw"}
        db_id = str(work.get("db_id") or "")
        prior = pred_map.get(work.get("instance_id"))
        lock = _db_lock(db_id)
        # 先锁库再占槽：同库排队不空占 worker 槽，跨库才能真正并行
        with lock:
            slot = None
            if slot_q is not None:
                set_timing_context(
                    instance_id=str(work.get("instance_id") or "-"),
                    db_id=db_id,
                    dialect="postgresql",
                )
                wait_t0 = time.perf_counter()
                slot = slot_q.get()
                log_timing("slot_wait", (time.perf_counter() - wait_t0) * 1000.0, slot=slot)
            logger.info(
                f"==== [{i}/{len(records)}] {work.get('instance_id')} "
                f"db={db_id} slot={slot} ===="
            )
            try:
                if isolate == "subprocess":
                    return _run_one_subprocess(
                        work,
                        raw=raw,
                        mode=args.mode,
                        function=args.function,
                        logger=logger,
                        timeout_s=timeout_s,
                        prior_pred=prior,
                    )
                return run_one_effi(
                    work,
                    raw=raw,
                    mode=args.mode,
                    function=args.function,
                    logger=logger,
                    prior_pred=prior,
                )
            finally:
                if slot_q is not None and slot is not None:
                    slot_q.put(slot)

    indexed = list(enumerate(records, 1))
    if workers <= 1:
        for item in indexed:
            row = slim_output_record(_job(item))
            with write_lock:
                append_jsonl(row, args.output)
    else:
        from collections import defaultdict, deque

        # 按库分队列：全局 workers 上限 + 每库至多 1 路在飞（共享评测库）
        by_db: Dict[str, deque] = defaultdict(deque)
        for item in indexed:
            by_db[str(item[1].get("db_id") or "?")].append(item)
        db_inflight: Dict[str, int] = {db: 0 for db in by_db}
        db_order = list(by_db.keys())
        rr = 0

        with ThreadPoolExecutor(max_workers=workers) as pool:
            pending: Dict[Any, Any] = {}
            started: Dict[Any, float] = {}
            completed = 0
            total = len(indexed)

            def _submit_next() -> bool:
                nonlocal rr
                if len(pending) >= workers:
                    return False
                n_db = len(db_order)
                for _ in range(n_db):
                    db = db_order[rr % n_db]
                    rr += 1
                    if db_inflight.get(db, 0) >= 1:
                        continue
                    q = by_db.get(db)
                    if not q:
                        continue
                    item = q.popleft()
                    fut = pool.submit(_job, item)
                    pending[fut] = (item[1].get("instance_id"), db)
                    started[fut] = time.time()
                    db_inflight[db] = db_inflight.get(db, 0) + 1
                    return True
                return False

            while len(pending) < workers and _submit_next():
                pass

            while pending:
                done, _ = wait(pending, timeout=30, return_when=FIRST_COMPLETED)
                if not done:
                    oldest = sorted(pending, key=lambda f: started[f])[:workers]
                    inflight = [
                        f"{pending[f][0]}@{pending[f][1]}({int(time.time() - started[f])}s)"
                        for f in oldest
                    ]
                    stuck = [
                        f"{pending[f][0]}({int(time.time() - started[f])}s)"
                        for f in oldest
                        if timeout_s > 0 and (time.time() - started[f]) > timeout_s
                    ]
                    logger.info(f"心跳: 已完成 {completed}/{total}；进行中 {inflight}")
                    if stuck:
                        logger.error(f"单题超过 {timeout_s}s 仍未返回：{stuck}")
                        if isolate == "inproc":
                            logger.error(
                                "inproc 无法强杀线程；请改 --isolate subprocess 或降 --workers"
                            )
                    continue
                for fut in done:
                    iid, db = pending.pop(fut)
                    started.pop(fut, None)
                    db_inflight[db] = max(0, db_inflight.get(db, 1) - 1)
                    try:
                        row = slim_output_record(fut.result())
                    except Exception as e:
                        logger.exception(f"{iid} 失败: {e}")
                        rec0 = next(
                            (r for r in records if r.get("instance_id") == iid),
                            {"instance_id": iid},
                        )
                        raw0 = rec0.get("_raw") or {}
                        row = _effi_timeout_row(
                            {k: v for k, v in rec0.items() if k != "_raw"},
                            raw0,
                            str(e),
                            mode=args.mode,
                        )
                    with write_lock:
                        append_jsonl(row, args.output)
                    completed += 1
                    meta = row.get("optimization_meta") or {}
                    logger.info(
                        f"进度: {completed}/{total} 刚完成 id={iid} db={db} "
                        f"pred_n={len(row.get('pred_sqls') or [])} "
                        f"ok={meta.get('success')} fallback={meta.get('fallback')}"
                    )
                    while _submit_next():
                        pass

    unique_count = dedupe_jsonl_by_id(args.output) if Path(args.output).is_file() else 0
    final_rows = load_jsonl(args.output) if unique_count else []
    summary = summarize(final_rows)
    logger.info(f"已写出 {unique_count or len(final_rows)} 条 → {args.output}")
    logger.info(f"汇总: {json.dumps(summary, ensure_ascii=False)}")
    print(json.dumps({"output": args.output, **summary}, ensure_ascii=False))


if __name__ == "__main__":
    main()
