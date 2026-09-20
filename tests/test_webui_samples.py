"""样例 JSONL 契约。"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GEN = ROOT / "data" / "webui" / "sample_general.jsonl"
SLOW = ROOT / "data" / "webui" / "sample_slow.jsonl"
GOLD = {"sol_sql", "test_cases", "gold_sql", "gold", "sol"}


def _load(path: Path) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def test_sample_jsonl_shape():
    assert GEN.is_file() and SLOW.is_file()
    general = _load(GEN)
    slow = _load(SLOW)
    assert len(general) == 20
    assert len(slow) == 20
    for r in general:
        assert r.get("is_slow_sql") is False
        assert r.get("issue_sql")
        assert r.get("db_id")
        assert not GOLD.intersection(r.keys())
        assert str(r.get("category") or "").lower() != "efficiency"
    for r in slow:
        assert r.get("is_slow_sql") is True
        assert r.get("issue_sql")
        assert r.get("db_id")
        assert not GOLD.intersection(r.keys())
        assert str(r.get("category") or "").lower() == "efficiency"
