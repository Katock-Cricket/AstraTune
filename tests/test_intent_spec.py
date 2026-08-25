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
    assert spec.goal == "pers"


def test_heuristic_goal_from_category():
    spec = extract_intent_spec_heuristic(
        query="merge users by ref",
        category="Personalization",
        diagnosis_report="",
        schema="",
    )
    assert spec.goal == "pers"


def test_heuristic_mgmt_column_type():
    spec = extract_intent_spec_heuristic(
        query="convert the date column to a TIMESTAMP type",
        category="Management",
        diagnosis_report="",
        schema="",
        issue_sql=["UPDATE account SET date__timestamp = date::timestamp"],
    )
    assert spec.goal == "mgmt"
    assert "column_type" in spec.mgmt_artifacts


def test_heuristic_diag_preferred_table():
    spec = extract_intent_spec_heuristic(
        query="accounts with transaction variability",
        category="Query",
        diagnosis_report="应改用 trans：SELECT account_id FROM trans GROUP BY account_id",
        schema="-- 表: loan\nCREATE TABLE loan(id int);\n-- 表: trans\nCREATE TABLE trans(id int);",
        issue_sql=["SELECT account_id FROM loan GROUP BY account_id"],
    )
    notes = {e.table: e.sample_note for e in spec.evidence}
    assert notes.get("trans") == "diag_preferred"


def test_heuristic_named_output_columns():
    spec = extract_intent_spec_heuristic(
        query="get both `transactionid` and `Segment` columns",
        category="Management",
        diagnosis_report="",
        schema="",
    )
    names = [c.name.lower() for c in spec.output_columns]
    assert "transactionid" in names and "segment" in names


def test_empty_intent_spec():
    assert empty_intent_spec().is_empty()
    spec = IntentSpec(output_columns=[OutputColumn(name="x")])
    assert not spec.is_empty()


def test_extract_intent_spec_goal_from_category_overrides_llm(monkeypatch):
    """LLM 把 Pers 写成 query 时，合并必须以类目启发式 goal 为准。"""
    from optimization import intent_spec as mod

    def fake_llm(**_kwargs):
        return IntentSpec(
            goal="query",
            output_columns=[OutputColumn(name="artist")],
        )

    monkeypatch.setattr(mod, "extract_intent_spec_llm", fake_llm)
    spec = mod.extract_intent_spec(
        query="each artist one representative card row",
        category="Personalization",
        diagnosis_report="",
        schema="",
        llm_config={"model": "x", "base_url": "http://x"},
        use_llm=True,
    )
    assert spec.goal == "pers"
    assert spec.pers_need_full_row is True
