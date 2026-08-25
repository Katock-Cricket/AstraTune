#!/usr/bin/env python3
"""将推理产出的 pred_sqls 与 gold sol（含 test_cases）按 instance_id 合并为评测输入。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.pipeline import dump_jsonl, load_jsonl


def parse_args():
    p = argparse.ArgumentParser(description="merge pred_sqls with sol JSONL")
    p.add_argument("--pred", required=True, help="含 pred_sqls 的推理输出 JSONL")
    p.add_argument(
        "--sol",
        required=True,
        help="gold sol JSONL（如 benchmark_data/sol/pg_sol/pg_sol.jsonl）",
    )
    p.add_argument("--output", required=True, help="评测用 eval_ready JSONL")
    p.add_argument(
        "--require-pred",
        action="store_true",
        help="若 pred 缺失则跳过该条（默认用空 pred_sqls 仍写出）",
    )
    p.add_argument(
        "--id-map",
        type=str,
        default=None,
        help="可选 JSON：pred_instance_id → sol_instance_id",
    )
    return p.parse_args()


def _load_id_map(path: Optional[str]) -> Dict[Any, Any]:
    if not path:
        return {}
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit("--id-map 须为 JSON 对象")
    return obj


def main():
    args = parse_args()
    preds = load_jsonl(args.pred)
    sols = load_jsonl(args.sol)
    id_map = _load_id_map(args.id_map)

    pred_map: Dict[Any, Dict[str, Any]] = {r.get("instance_id"): r for r in preds}
    sol_map: Dict[Any, Dict[str, Any]] = {r.get("instance_id"): r for r in sols}
    merged: List[Dict[str, Any]] = []
    missing = 0
    mapped = 0

    for sol in sols:
        sol_iid = sol.get("instance_id")
        # 反查：哪些 pred_id 映射到此 sol_id
        pred = pred_map.get(sol_iid)
        if pred is None and id_map:
            for pred_id, mapped_sol_id in id_map.items():
                # JSON 键常为 str
                if mapped_sol_id == sol_iid or str(mapped_sol_id) == str(sol_iid):
                    key = pred_id
                    if key not in pred_map:
                        try:
                            key = int(pred_id)
                        except (TypeError, ValueError):
                            pass
                    pred = pred_map.get(key) or pred_map.get(pred_id)
                    if pred is not None:
                        mapped += 1
                        break

        if pred is None:
            missing += 1
            if args.require_pred:
                continue
            row = dict(sol)
            row.setdefault("pred_sqls", [])
            merged.append(row)
            continue

        row = {**sol, **{k: v for k, v in pred.items() if k not in ("sol_sql", "test_cases")}}
        if "sol_sql" in sol:
            row["sol_sql"] = sol["sol_sql"]
        if "test_cases" in sol:
            row["test_cases"] = sol["test_cases"]
        if "pred_sqls" not in row:
            row["pred_sqls"] = []
        # 官方评测要求 instance_id 与 sol 一致
        row["instance_id"] = sol_iid
        merged.append(row)

    dump_jsonl(merged, args.output)
    print(
        json.dumps(
            {
                "merged": len(merged),
                "pred_count": len(preds),
                "sol_count": len(sols),
                "pred_missing_for_sol": missing,
                "id_map_hits": mapped,
                "output": args.output,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
