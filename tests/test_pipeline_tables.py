"""pipeline 表解析/抽表校验回退链/沙箱库名特征测试。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.pipeline import (
    normalize_tables_for_dialect,
    resolve_and_validate_tables,
    sandbox_db_name_for_slot,
    schema_tables_mentioned_in_text,
    tables_from_schema_text,
)


class FakeTargetExecutor:
    def __init__(self, existing, base_tables=None):
        self.existing = set(existing)
        self.base = list(base_tables if base_tables is not None else existing)

    def resolve_table_name(self, name):
        return name if name in self.existing else None

    def table_exists(self, name):
        return name in self.existing

    def list_base_tables(self):
        return list(self.base)


# ---------- normalize_tables_for_dialect ----------

def test_normalize_strips_prefix_and_dedupes():
    assert normalize_tables_for_dialect(
        ["public.orders", "orders", "`o`", '"orders"', " pg_tables "], "postgresql"
    ) == ["orders", "o"]


def test_normalize_filters_system_tables():
    got = normalize_tables_for_dialect(
        ["pg_stat_activity", "information_schema", "dual", "customers"], "postgresql"
    )
    assert got == ["customers"]


# ---------- tables_from_schema_text ----------

def test_tables_from_schema_text():
    schema = """
    CREATE TABLE public.orders (id int);
    CREATE TABLE IF NOT EXISTS "Order Items" (id int);
    CREATE TABLE `weird name` (id int);
    CREATE TABLE customers (id int);
    CREATE INDEX idx ON orders(id);
    """
    # 现有行为：不带引号的 schema 前缀名（public）也会被捕获为表名，
    # 由后续 normalize + resolve 过滤。重构若改进前缀解析需同步更新此断言。
    assert tables_from_schema_text(schema) == ["public", "Order Items", "weird name", "customers"]
    assert tables_from_schema_text("") == []
    assert tables_from_schema_text(None) == []


# ---------- schema_tables_mentioned_in_text ----------

def test_mentioned_quoted():
    assert schema_tables_mentioned_in_text("check 'customers' table", ["customers"]) == ["customers"]


def test_mentioned_from_join():
    assert schema_tables_mentioned_in_text("select from customers", ["customers"]) == ["customers"]
    assert schema_tables_mentioned_in_text("join orders on ...", ["orders"]) == ["orders"]


def test_mentioned_table_suffix():
    assert schema_tables_mentioned_in_text("the customers table", ["customers"]) == ["customers"]


def test_mentioned_no_false_positive():
    # 无邻近关键词且非引号包裹时不应命中
    assert schema_tables_mentioned_in_text("customers are important", ["customers"]) == []


# ---------- resolve_and_validate_tables ----------

def test_resolve_first_level_llm_tables():
    ex = FakeTargetExecutor(["customers", "orders"])
    got = resolve_and_validate_tables(["customers"], ex, dialect="postgresql")
    assert got == ["customers"]


def test_resolve_falls_back_to_schema():
    # LLM 抽表全 miss → original_schema 回退
    ex = FakeTargetExecutor(["orders"])
    schema = "CREATE TABLE orders (id int);"
    got = resolve_and_validate_tables(["nonexistent"], ex, dialect="postgresql", original_schema=schema)
    assert got == ["orders"]


def test_resolve_falls_back_to_all_base_tables():
    ex = FakeTargetExecutor(["a", "b", "c"])
    got = resolve_and_validate_tables(["nope"], ex, dialect="postgresql", original_schema="")
    assert got == ["a", "b", "c"]


def test_resolve_all_miss_big_db_returns_empty():
    ex = FakeTargetExecutor([f"t{i}" for i in range(41)])
    got = resolve_and_validate_tables(["nope"], ex, dialect="postgresql", original_schema="")
    assert got == []


def test_resolve_hint_text_mentions_force_include():
    ex = FakeTargetExecutor(["customers", "orders"])
    schema = "CREATE TABLE customers (id int); CREATE TABLE orders (id int);"
    got = resolve_and_validate_tables(
        [], ex, dialect="postgresql", original_schema=schema,
        hint_text="show data from the customers table",
    )
    assert got == ["customers"]


def test_resolve_llm_miss_but_hint_hit():
    # LLM 抽错表 + hint 点名正确表 → 命中表胜出
    ex = FakeTargetExecutor(["orders"])
    schema = "CREATE TABLE orders (id int);"
    got = resolve_and_validate_tables(
        ["wrong_table"], ex, dialect="postgresql", original_schema=schema,
        hint_text="query the orders table",
    )
    assert got == ["orders"]


# ---------- sandbox_db_name_for_slot ----------

def test_sandbox_db_name_single_worker():
    assert sandbox_db_name_for_slot(
        dialect="postgresql", slot_index=0, workers=1, base_database="astratune_sandbox"
    ) == "astratune_sandbox"


def test_sandbox_db_name_slot():
    assert sandbox_db_name_for_slot(
        dialect="postgresql", slot_index=2, workers=4, base_database="astratune_sandbox"
    ) == "astratune_sandbox_2"


def test_sandbox_db_name_custom_prefix():
    got = sandbox_db_name_for_slot(
        dialect="postgresql", slot_index=1, workers=2,
        batch_cfg={"sandbox_db_prefix": "sb_"}, base_database="astratune_sandbox",
    )
    assert got == "sb_1"


def test_sandbox_db_name_none_slot():
    assert sandbox_db_name_for_slot(
        dialect="mysql", slot_index=None, workers=4, base_database="mydb"
    ) == "mydb"


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
    print("all pipeline table tests passed")
