"""Efficiency 基数外推与 QEP 结构分测试。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from optimization.eff_risk import (
    compute_scale_factor,
    estimate_full_timeout_risk,
    qep_structure_allows_cost_regression,
)


def test_compute_scale_factor_sampled():
    fid = {
        "tables": [
            {"mode": "sample", "target_rows": 10000, "rows": 1000},
            {"mode": "copy", "target_rows": 50, "rows": 50},
        ]
    }
    assert compute_scale_factor(fid) == 10.0


def test_compute_scale_factor_empty():
    assert compute_scale_factor(None) is None
    assert compute_scale_factor({}) is None


def test_estimate_full_timeout_risk():
    msg = estimate_full_timeout_risk(600.0, 10.0, 5000)
    assert msg and "外推" in msg


def test_estimate_no_risk_small_scale():
    assert estimate_full_timeout_risk(100.0, 1.5, 5000) is None


def test_qep_structure_notin_merge():
    issue = ["DELETE FROM t WHERE x NOT IN (SELECT a FROM n1) AND x NOT IN (SELECT b FROM n2)"]
    pred = ["DELETE FROM t WHERE x NOT IN (SELECT a FROM n1 UNION ALL SELECT b FROM n2)"]
    assert qep_structure_allows_cost_regression(pred, issue) is True


def test_qep_structure_recursive_depth():
    pred = ["WITH RECURSIVE r AS (SELECT 1, 0 AS depth UNION ALL SELECT id, depth+1 FROM t WHERE depth < 100) SELECT * FROM r"]
    assert qep_structure_allows_cost_regression(pred, ["WITH RECURSIVE r AS (SELECT 1 FROM t) SELECT *"]) is True
