#!/usr/bin/env python3
"""校验题面/schema 与 gold sol 的 instance_id 对齐（评测门禁）。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.pipeline import load_jsonl, merge_schema_and_input


def parse_args():
    p = argparse.ArgumentParser(description="Check schema/input ↔ sol alignment")
    p.add_argument("--schema", type=str, default=None)
    p.add_argument("--input", type=str, default=None, help="题面 JSONL（可与 schema 合并）")
    p.add_argument("--sol", required=True, help="gold sol JSONL")
    p.add_argument("--report", type=str, default=None, help="写出 JSON 报告路径")
    p.add_argument(
        "--require-pred-fields",
        action="store_true",
        help="要求题面侧含 db_id 与 issue_sql（评测必需）",
    )
    return p.parse_args()


def _load_problems(args) -> List[Dict[str, Any]]:
    schema_recs: List[Dict[str, Any]] = []
    input_recs: List[Dict[str, Any]] = []
    if args.schema:
        schema_recs = load_jsonl(args.schema)
    if args.input:
        input_recs = load_jsonl(args.input)
    if input_recs and schema_recs:
        return merge_schema_and_input(schema_recs, input_recs)
    if input_recs:
        return input_recs
    if schema_recs:
        return schema_recs
    raise SystemExit("请提供 --schema 和/或 --input")


def main():
    args = parse_args()
    problems = _load_problems(args)
    sols = load_jsonl(args.sol)

    prob_map = {r.get("instance_id"): r for r in problems}
    sol_map = {r.get("instance_id"): r for r in sols}
    prob_ids: Set[Any] = set(prob_map)
    sol_ids: Set[Any] = set(sol_map)

    only_prob = sorted(prob_ids - sol_ids, key=lambda x: str(x))
    only_sol = sorted(sol_ids - prob_ids, key=lambda x: str(x))
    common = prob_ids & sol_ids

    conflicts: List[Dict[str, Any]] = []
    missing_fields: List[Dict[str, Any]] = []
    for iid in sorted(common, key=lambda x: str(x)):
        p = prob_map[iid]
        s = sol_map[iid]
        for key in ("db_id", "dialect"):
            pv, sv = p.get(key), s.get(key)
            if pv is not None and sv is not None and str(pv) != str(sv):
                conflicts.append(
                    {"instance_id": iid, "field": key, "problem": pv, "sol": sv}
                )
        if args.require_pred_fields:
            if not p.get("db_id"):
                missing_fields.append({"instance_id": iid, "missing": "db_id"})
            iss = p.get("issue_sql")
            if not iss or (isinstance(iss, list) and not any(str(x).strip() for x in iss)):
                missing_fields.append({"instance_id": iid, "missing": "issue_sql"})
        if "sol_sql" not in s or s.get("sol_sql") in (None, [], ""):
            missing_fields.append({"instance_id": iid, "missing": "sol_sql"})
        if "test_cases" not in s:
            missing_fields.append({"instance_id": iid, "missing": "test_cases"})

    ok = not only_prob and not only_sol and not conflicts and not missing_fields
    report = {
        "ok": ok,
        "problem_count": len(prob_ids),
        "sol_count": len(sol_ids),
        "common_count": len(common),
        "only_in_problem": only_prob,
        "only_in_sol": only_sol,
        "conflicts": conflicts,
        "missing_fields": missing_fields,
    }

    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(text, encoding="utf-8")
        print(f"report → {args.report}")
    print(text)
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
