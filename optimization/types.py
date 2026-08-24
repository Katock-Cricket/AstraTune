from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Candidate:
    id: str
    pred_sqls: List[str]
    rationale: str = ""
    strategy_tag: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CandidateVerifyResult:
    id: str
    passed: bool
    soft_ex: str = "skipped"  # pass | fail | skipped
    cost: Optional[float] = None
    latency_ms: Optional[float] = None
    errors: List[str] = field(default_factory=list)
    rank_key: float = float("inf")


@dataclass
class VerifyReport:
    passed: bool
    best: Optional[Candidate] = None
    per_candidate: List[CandidateVerifyResult] = field(default_factory=list)
    failure_reasons: List[str] = field(default_factory=list)
    relative_best: Optional[Candidate] = None
    relative_score: float = float("inf")


@dataclass
class OptimizationResult:
    pred_sqls: List[str]
    success: bool
    rounds: int
    verify_report: Optional[VerifyReport] = None
    failure_reasons: List[str] = field(default_factory=list)
    fallback: bool = False
    error: Optional[str] = None
