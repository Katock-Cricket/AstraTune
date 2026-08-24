#!/usr/bin/env python3
"""一次性压缩/去重 pred JSONL（resume 前跑，防 read_text 卡死）。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.run_bird_critic import slim_output_record
from utils.pipeline import dedupe_jsonl_by_id


def main() -> None:
    p = argparse.ArgumentParser(description="去重 + slim pred JSONL")
    p.add_argument("--path", type=str, required=True)
    args = p.parse_args()
    path = Path(args.path)
    if not path.is_file():
        raise SystemExit(f"not found: {path}")
    n = dedupe_jsonl_by_id(path)
    tmp = path.with_name(path.name + ".slim.tmp")
    count = 0
    with path.open(encoding="utf-8") as fin, tmp.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            slim = slim_output_record(obj)
            fout.write(json.dumps(slim, ensure_ascii=False) + "\n")
            count += 1
    tmp.replace(path)
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"done unique={n} lines={count} size_mb={size_mb:.2f} → {path}")


if __name__ == "__main__":
    main()
