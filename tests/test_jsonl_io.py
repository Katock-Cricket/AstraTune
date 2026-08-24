"""JSONL IO 纯函数特征测试。"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.pipeline import dedupe_jsonl_by_id, filter_by_dialect, load_jsonl, merge_schema_and_input


def test_dedupe_keeps_last(tmp_path):
    p = tmp_path / "d.jsonl"
    p.write_text(
        '{"instance_id": "a", "v": 1}\n'
        '{"instance_id": "b", "v": 1}\n'
        '{"instance_id": "a", "v": 2}\n',
        encoding="utf-8",
    )
    n = dedupe_jsonl_by_id(p)
    assert n == 2
    recs = load_jsonl(p)
    by_id = {r["instance_id"]: r for r in recs}
    assert by_id["a"]["v"] == 2
    assert by_id["b"]["v"] == 1


def test_dedupe_tolerates_bad_lines(tmp_path):
    p = tmp_path / "d.jsonl"
    p.write_text(
        'not json\n{"instance_id": "a", "v": 1}\n{"novel": 1}\n',
        encoding="utf-8",
    )
    n = dedupe_jsonl_by_id(p)
    assert n == 1


def test_dedupe_missing_file(tmp_path):
    assert dedupe_jsonl_by_id(tmp_path / "nope.jsonl") == 0


def test_merge_schema_and_input_input_wins():
    schema = [
        {"instance_id": "a", "original_schema": "CREATE TABLE a", "dialect": "PostgreSQL"},
        {"instance_id": "b", "original_schema": "CREATE TABLE b"},
    ]
    inputs = [
        {"instance_id": "a", "query": "qa"},
        {"instance_id": "c", "query": "qc"},
    ]
    got = merge_schema_and_input(schema, inputs)
    assert len(got) == 2
    by_id = {r["instance_id"]: r for r in got}
    assert by_id["a"]["original_schema"] == "CREATE TABLE a"
    assert by_id["a"]["query"] == "qa"
    assert "original_schema" not in by_id["c"]


def test_filter_by_dialect():
    recs = [
        {"instance_id": "1", "dialect": "PostgreSQL"},
        {"instance_id": "2", "dialect": "mysql"},
        {"instance_id": "3"},  # 缺 dialect 放行
        {"instance_id": "4", "dialect": "oracle"},  # 不匹配剔除
    ]
    got = filter_by_dialect(recs, "postgresql")
    assert [r["instance_id"] for r in got] == ["1", "3"]


def test_load_jsonl_skips_blank_lines(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text('\n{"a": 1}\n\n{"b": 2}\n', encoding="utf-8")
    assert load_jsonl(p) == [{"a": 1}, {"b": 2}]


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        _tmp = Path(d)
        for name, fn in sorted(list(globals().items())):
            if name.startswith("test_") and callable(fn):
                try:
                    fn(_tmp)
                except TypeError:
                    fn()
    print("all jsonl io tests passed")
