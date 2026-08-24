"""Verify：用户意图表必须出现在 pred。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from optimization.verify import missing_intent_tables


def test_missing_order_table():
    q = "we have a table named 'order' that records details about orders"
    schema = 'CREATE TABLE "loan" (a int); CREATE TABLE "order" (b int);'
    assert missing_intent_tables(q, schema, ["SELECT * FROM loan"]) == ["order"]
    assert missing_intent_tables(q, schema, ['SELECT account_id FROM "order"']) == []
    assert missing_intent_tables(q, schema, ["SELECT * FROM order"]) == []


def test_no_false_positive_on_english_orders():
    q = "count how many orders each user placed"  # 无引号点名表
    schema = 'CREATE TABLE "order" (b int); CREATE TABLE "users" (id int);'
    # 未以 table named / from 等形式点名 order 表时不应强加
    assert missing_intent_tables(q, schema, ["SELECT 1"]) == []


def test_personalization_group_key_only():
    from optimization.verify import personalization_group_key_only

    assert personalization_group_key_only(
        ["SELECT artist FROM cards GROUP BY artist"]
    )
    assert (
        personalization_group_key_only(
            [
                "SELECT t.* FROM cards t JOIN (SELECT min(id) AS id FROM cards GROUP BY artist) x ON t.id = x.id"
            ]
        )
        is None
    )


if __name__ == "__main__":
    test_missing_order_table()
    test_no_false_positive_on_english_orders()
    test_personalization_group_key_only()
    print("ok")
