"""IntentSpec 解析与启发式抽取测试。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from optimization.intent_spec import (
    AggFilter,
    IntentSpec,
    OutputColumn,
    empty_intent_spec,
    extract_intent_spec_heuristic,
    parse_intent_spec,
)


def test_parse_intent_spec_full():
    raw = {
        "goal": "query",
        "output_columns": [{"name": "answered", "role": "metric"}],
        "must_predicates": ["only activated premium"],
        "agg_filters": [{"text": "premium active", "unresolved": True}],
        "pers_need_full_row": False,
    }
    spec = parse_intent_spec(raw)
    assert spec.goal == "query"
    assert spec.output_columns[0].name == "answered"
    assert spec.agg_filters[0].unresolved is True


def test_parse_intent_spec_fail_open():
    assert parse_intent_spec("not json").is_empty()
    assert parse_intent_spec(None).is_empty()


def test_render_for_prompt():
    spec = IntentSpec(goal="pers", pers_need_full_row=True)
    text = spec.render_for_prompt()
    assert "pers_need_full_row" in text
    assert "true" in text.lower()


def test_heuristic_premium_filter():
    spec = extract_intent_spec_heuristic(
        query="count referral only if premium activated",
        category="Query",
        diagnosis_report="",
        schema="-- 表: users\nCREATE TABLE users(id int);",
    )
    assert spec.goal == "query"
    assert spec.agg_filters
    assert spec.agg_filters[0].unresolved is True


def test_heuristic_pers_full_row():
    spec = extract_intent_spec_heuristic(
        query="each artist one row from cards",
        category="Personalization",
        diagnosis_report="",
        schema="",
    )
    assert spec.pers_need_full_row is True


def test_empty_intent_spec():
    assert empty_intent_spec().is_empty()
    spec = IntentSpec(output_columns=[OutputColumn(name="x")])
    assert not spec.is_empty()
