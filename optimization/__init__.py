"""Generate–Verify 优化包（无 Reflect）。"""

from optimization.types import Candidate, OptimizationResult, VerifyReport

__all__ = [
    "Candidate",
    "VerifyReport",
    "OptimizationResult",
    "run_optimization",
]


def __getattr__(name: str):
    if name == "run_optimization":
        from optimization.graph import run_optimization

        return run_optimization
    raise AttributeError(name)
