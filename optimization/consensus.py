"""多候选结果共识：分簇与早停门禁。"""

from __future__ import annotations

import hashlib
import json
from typing import Any, List, Optional, Sequence, Tuple

from optimization.intent_spec import IntentSpec
from optimization.types import CandidateVerifyResult, VerifyReport


def _normalize_row(row: Any) -> str:
    if isinstance(row, dict):
        items = sorted((str(k), repr(v)) for k, v in row.items())
        return json.dumps(items, ensure_ascii=False, sort_keys=True)
    if isinstance(row, (list, tuple)):
        return json.dumps([repr(x) for x in row], ensure_ascii=False)
    return repr(row)


def row_fingerprint(rows: Optional[Sequence[Any]]) -> str:
    """只读查询结果指纹；None 表示不可比。"""
    if rows is None:
        return ""
    normalized = [_normalize_row(r) for r in rows]
    normalized.sort()
    payload = "\n".join(normalized)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def detect_result_split(report: VerifyReport) -> Tuple[bool, str]:
    """已通过且含 rows 的候选是否分裂为 ≥2 互斥簇。"""
    fps: List[str] = []
    for cvr in report.per_candidate or []:
        if not cvr.passed:
            continue
        fp = row_fingerprint(getattr(cvr, "rows", None))
        if fp:
            fps.append(fp)
    unique = set(fps)
    if len(unique) >= 2:
        return True, "多候选结果集分裂为多个互斥簇，禁止本轮早停"
    return False, ""


def count_passed_with_rows(report: VerifyReport) -> int:
    n = 0
    for cvr in report.per_candidate or []:
        if cvr.passed and getattr(cvr, "rows", None) is not None:
            n += 1
    return n


def spec_contract_weak(intent_spec: Optional[IntentSpec], report: VerifyReport) -> bool:
    """Spec 为空或仅单一候选通过（首轮置信不足）。"""
    if intent_spec is None or intent_spec.is_empty():
        return True
    passed_n = sum(1 for c in (report.per_candidate or []) if c.passed)
    return passed_n <= 1


def should_block_early_stop(
    *,
    round_num: int,
    intent_spec: Optional[IntentSpec],
    report: VerifyReport,
    num_candidates: int,
) -> Tuple[bool, str]:
    split, split_msg = detect_result_split(report)
    if split:
        return True, split_msg
    if round_num == 1 and num_candidates >= 2 and spec_contract_weak(intent_spec, report):
        return True, "首轮 Intent Spec 为空或仅单一候选通过，需再生成以提升置信"
    return False, ""


def consensus_failure_reason(report: VerifyReport, candidates_meta: int) -> Optional[str]:
    blocked, msg = should_block_early_stop(
        round_num=1,
        intent_spec=None,
        report=report,
        num_candidates=candidates_meta,
    )
    return msg if blocked else None
