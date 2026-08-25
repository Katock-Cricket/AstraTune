#!/usr/bin/env python3
"""Effi-SQL 官方评测薄封装：调用 Diff-SQL end2end runner（不重写评测逻辑）。

前置：
  1. benchmark_data dumps 已就位（compose 已挂载）
  2. 在 Diff-SQL 目录执行过 bash scripts/db_up.sh
  3. 推理产物含 prediction 字段（run_effi_sql / infer --benchmark effi）
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.benchmark_data import DIFF_SQL_ROOT, EFFI_INPUT


def parse_args():
    p = argparse.ArgumentParser(description="Effi-SQL Diff-SQL end2end 评测封装")
    p.add_argument(
        "--pred",
        type=str,
        required=True,
        help="含 prediction 的 JSONL（通常 outputs/effi_pred.jsonl）",
    )
    p.add_argument(
        "--workdir",
        type=str,
        default=str(DIFF_SQL_ROOT),
        help="Diff-SQL 根目录",
    )
    p.add_argument(
        "--copy-name",
        type=str,
        default="astratune_effi_pred.jsonl",
        help="复制到 Diff-SQL data/benchmark/effi-sql/ 下的文件名",
    )
    p.add_argument("--skip-aggregate", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    root = Path(args.workdir)
    if not (root / "scripts" / "run_eval.sh").is_file():
        raise SystemExit(f"Diff-SQL 不完整: {root}")

    pred = Path(args.pred)
    if not pred.is_file():
        raise SystemExit(f"pred 不存在: {pred}")

    dest_dir = root / "data" / "benchmark" / "effi-sql"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / args.copy_name
    shutil.copy2(pred, dest)
    print(f"已复制 pred → {dest}")

    env = os.environ.copy()
    env["EVAL_SQL_MODE"] = "end2end"
    env["EVAL_RESPONSE_FIELD"] = "prediction"
    env["EVAL_INPUT_FILE"] = args.copy_name
    env["BENCHMARK_SCALE"] = env.get("BENCHMARK_SCALE", "scale")
    env["DIALECT"] = env.get("DIALECT", "postgres")

    # Git Bash / WSL：优先 bash
    bash = shutil.which("bash") or shutil.which("bash.exe")
    if not bash:
        raise SystemExit("需要 bash 以运行 Diff-SQL/scripts/run_eval.sh（Git Bash / WSL）")

    print("运行 Diff-SQL run_eval.sh …")
    r = subprocess.run(
        [bash, "scripts/run_eval.sh"],
        cwd=str(root),
        env=env,
    )
    if r.returncode != 0:
        raise SystemExit(r.returncode)

    if not args.skip_aggregate:
        print("运行 aggregate_eval.sh …")
        r2 = subprocess.run([bash, "scripts/aggregate_eval.sh"], cwd=str(root), env=env)
        if r2.returncode != 0:
            raise SystemExit(r2.returncode)

    print("Effi 评测完成。输出见 Diff-SQL/outputs/postgres/")
    print(f"题面参考: {EFFI_INPUT}")


if __name__ == "__main__":
    main()
