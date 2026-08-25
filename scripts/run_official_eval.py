#!/usr/bin/env python3
"""薄封装：在 AstraTune target（127.0.0.1:5432/3306）上跑官方 wrapper。"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVAL_SRC = ROOT / "benchmark" / "evaluation" / "src"
DUMP_MYSQL = ROOT / "benchmark_data" / "dumps" / "mysql"
DUMP_PG = ROOT / "benchmark_data" / "dumps" / "postgresql"


def _count_jsonl_rows(path: Path) -> int:
    n = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                n += 1
    return n


def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Run BIRD-CRITIC official evaluation. "
            "--limit 默认=JSONL 全部行；官方 wrapper 若省略 limit 也是全量，"
            "但本脚本旧默认曾是 1，极易只评 1 条——务必显式确认。"
        )
    )
    p.add_argument("--dialect", required=True, choices=["postgresql", "mysql", "pg", "postgres"])
    p.add_argument("--jsonl", required=True, help="eval_ready JSONL（含 pred_sqls + sol）")
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "评测实例条数上限。"
            "默认不传 / 省略 = 评测 JSONL 全部非空行；"
            "传 N = 只评前 N 条（冒烟用）。"
            "禁止假设「默认会跑全量」之外的其它隐式行为。"
        ),
    )
    p.add_argument("--mode", choices=["pred", "gold"], default="pred")
    p.add_argument("--num_threads", type=int, default=1)
    p.add_argument("--logging", default="false")
    return p.parse_args()


def main():
    args = parse_args()
    dialect = "postgresql" if args.dialect in ("pg", "postgres", "postgresql") else "mysql"
    jsonl = Path(args.jsonl).resolve()
    if not jsonl.is_file():
        raise SystemExit(f"JSONL 不存在: {jsonl}")

    total = _count_jsonl_rows(jsonl)
    if total <= 0:
        raise SystemExit(f"JSONL 无有效行: {jsonl}")

    if args.limit is None:
        limit = total
        limit_note = f"全量 ({total})"
    else:
        limit = max(0, int(args.limit))
        if limit <= 0:
            raise SystemExit("--limit 必须为正整数，或省略以评全量")
        if limit < total:
            print(
                f"WARNING: --limit={limit} < JSONL 行数 {total}，"
                f"只会评前 {limit} 条（不是全量准确率）",
                file=sys.stderr,
            )
        limit_note = f"{min(limit, total)}/{total}"

    print(f"Official eval scope: {limit_note} from {jsonl}")

    env = os.environ.copy()
    if dialect == "postgresql":
        env.setdefault("BIRD_PG_HOST", "127.0.0.1")
        env.setdefault("BIRD_PG_PORT", "5432")
        env.setdefault("BIRD_DB_HOST", "127.0.0.1")
        env.setdefault("BIRD_DB_PORT", "5432")
        env.setdefault("BIRD_PG_DUMP_DIR", str(DUMP_PG))
        script = EVAL_SRC / "wrapper_evaluation_postgresql.py"
    else:
        env.setdefault("BIRD_MYSQL_HOST", "127.0.0.1")
        env.setdefault("BIRD_MYSQL_PORT", "3306")
        env.setdefault("BIRD_DB_HOST", "127.0.0.1")
        env.setdefault("BIRD_DB_PORT", "3306")
        env.setdefault("BIRD_MYSQL_DUMP_DIR", str(DUMP_MYSQL))
        script = EVAL_SRC / "wrapper_evaluation_mysql.py"

    cmd = [
        sys.executable,
        str(script),
        "--jsonl_file",
        str(jsonl),
        "--mode",
        args.mode,
        "--limit",
        str(limit),
        "--num_threads",
        str(args.num_threads),
        "--logging",
        args.logging,
    ]
    if dialect == "mysql":
        cmd.extend(["--mysql_password", "123123"])

    print("Running:", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(EVAL_SRC), env=env)
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
