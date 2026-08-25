#!/usr/bin/env python3
"""Effi-SQL 批量推理：读 benchmark_data/effi/input → pred + prediction JSONL。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.batch_records import append_jsonl, dedupe_jsonl_by_id, finalize_inference_record
from utils.benchmark_data import EFFI_INPUT
from utils.config_loader import get_batch_config, load_all_configs
from utils.effi_adapter import adapt_effi_record, attach_prediction
from utils.inference_runner import VALID_MODES, run_single_instance
from utils.llm import batch_llm_pool_options, init_llm_pool
from utils.logger import setup_logger


def parse_args():
    p = argparse.ArgumentParser(description="Effi-SQL 批量推理（end2end prediction）")
    p.add_argument("--input", type=str, default=str(EFFI_INPUT))
    p.add_argument("--output", type=str, default="outputs/effi_pred.jsonl")
    p.add_argument("--mode", type=str, default="diag+opt", choices=list(VALID_MODES))
    p.add_argument("--function", type=str, default="efficiency", choices=["general", "efficiency"])
    p.add_argument("--ids", type=str, default=None, help="逗号分隔 Effi 数字 id")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--workers", type=int, default=1, help="Effi 建议 1（共享评测库）")
    p.add_argument("--log-file", type=str, default=None)
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logger(log_file=args.log_file)
    configs = load_all_configs()
    batch_cfg = get_batch_config(configs)
    pool_opts = batch_llm_pool_options(batch_cfg, cli_llm_workers=1)
    init_llm_pool(**pool_opts)

    id_filter = None
    if args.ids:
        id_filter = {int(x.strip()) for x in args.ids.split(",") if x.strip()}

    records = []
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

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not args.resume:
        out_path.write_text("", encoding="utf-8")
    done = set()
    if args.resume and out_path.is_file():
        dedupe_jsonl_by_id(str(out_path))
        with out_path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                iid = r.get("instance_id")
                if iid and r.get("prediction"):
                    done.add(iid)
        records = [r for r in records if r.get("instance_id") not in done]
        logger.info(f"resume: 剩余 {len(records)} 条")

    logger.info(f"Effi 推理 {len(records)} 条 mode={args.mode} function={args.function}")
    for i, rec in enumerate(records, 1):
        raw = rec.pop("_raw", {})
        logger.info(f"==== [{i}/{len(records)}] {rec.get('instance_id')} db={rec.get('db_id')} ====")
        result = run_single_instance(
            sql=rec["issue_sql"],
            query=rec["query"],
            category=rec["category"],
            dialect="postgresql",
            db_id=rec["db_id"],
            efficiency=True,
            mode=args.mode,
            function=args.function,
            instance_id=str(rec["instance_id"]),
            logger=logger,
            benchmark="effi",
            direct_target=True,
            stream=False,
        )
        out = attach_prediction(raw, result.pred_sqls)
        out["instance_id"] = rec["instance_id"]
        out["db_id"] = rec["db_id"]
        out["pred_sqls"] = result.pred_sqls
        out["diagnosis_report"] = result.diagnosis_report
        out["optimization_meta"] = result.meta
        if result.error:
            out["error"] = result.error
        out["elapsed_ms"] = result.elapsed_ms
        finalize_inference_record(out)
        append_jsonl(out, str(out_path))
        logger.info(
            f"{rec['instance_id']}: pred_n={len(result.pred_sqls)} "
            f"success={result.meta.get('success')} fallback={result.meta.get('fallback')}"
        )

    n = dedupe_jsonl_by_id(str(out_path))
    logger.info(f"完成，输出 {out_path} unique={n}")


if __name__ == "__main__":
    main()
