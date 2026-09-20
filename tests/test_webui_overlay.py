"""WebUI overlay 合并与 workers 钳制。"""

from __future__ import annotations

import copy
from pathlib import Path

from webui.overlay import (
    clamp_workers,
    deep_merge,
    match_connection,
    merge_runtime_configs,
    sanitize_agents_for_inference,
    seed_targets_from_target_db,
)


def test_clamp_workers():
    assert clamp_workers(0) == 1
    assert clamp_workers(-3) == 1
    assert clamp_workers(2) == 2
    assert clamp_workers(4) == 4
    assert clamp_workers(16) == 4
    assert clamp_workers("3") == 3
    assert clamp_workers("x") == 2


def test_deep_merge_nested_and_does_not_mutate():
    base = {"a": {"x": 1, "y": 2}, "b": 0}
    overlay = {"a": {"y": 9, "z": 3}}
    snapshot = copy.deepcopy(base)
    got = deep_merge(base, overlay)
    assert got == {"a": {"x": 1, "y": 9, "z": 3}, "b": 0}
    assert base == snapshot


def test_seed_and_match_connections():
    target_db = {
        "postgresql": {"host": "127.0.0.1", "port": 5432, "user": "root", "password": "p"},
        "mysql": {"host": "127.0.0.1", "port": 3306, "user": "root", "password": "p"},
    }
    conns = seed_targets_from_target_db(target_db)
    assert [c["id"] for c in conns] == ["postgresql-default", "mysql-default"]
    pg = match_connection(conns, dialect="pg")
    assert pg and pg["id"] == "postgresql-default"
    by_id = match_connection(conns, dialect="mysql", connection_id="postgresql-default")
    assert by_id and by_id["dialect"] == "postgresql"
    assert match_connection(conns, dialect="postgresql", connection_id="missing") is None


def test_merge_runtime_does_not_write_config(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "agents.json").write_text("{}", encoding="utf-8")
    base = {
        "agents": {"diagnosis": {"llm": {"model": "base-model", "timeout": 10}, "max_iter": 8}},
        "sandbox": {"postgresql": {"host": "s", "copy_thr": 20000}},
        "target_db": {"postgresql": {"host": "t", "port": 5432, "user": "u", "password": "pw", "database": "financial"}},
        "effi_db": {"keep": True},
    }
    overlay = {"diagnosis": {"llm": {"model": "overlay-model"}, "max_iter": 12}}
    conn = {
        "id": "postgresql-default",
        "dialect": "postgresql",
        "host": "10.0.0.1",
        "port": 15432,
        "user": "web",
        "password": "secret",
    }
    merged = merge_runtime_configs(
        base,
        agents_overlay=overlay,
        sandbox_overlay={"postgresql": {"copy_thr": 100}},
        connection=conn,
        db_id="card_games",
    )
    assert merged["agents"]["diagnosis"]["llm"]["model"] == "overlay-model"
    assert merged["agents"]["diagnosis"]["llm"]["timeout"] == 10
    assert merged["agents"]["diagnosis"]["max_iter"] == 12
    assert merged["sandbox"]["postgresql"]["copy_thr"] == 100
    assert merged["sandbox"]["postgresql"]["host"] == "s"
    assert merged["target_db"]["postgresql"]["host"] == "10.0.0.1"
    assert merged["target_db"]["postgresql"]["database"] == "card_games"
    assert merged["effi_db"]["keep"] is True
    assert (config_dir / "agents.json").read_text(encoding="utf-8") == "{}"
    # 确认没有把仓库 config 路径写脏
    for p in Path(tmp_path).rglob("*.json"):
        if p.name == "agents.json":
            assert p.read_text(encoding="utf-8") == "{}"


def test_sanitize_direct_key_kept_env_key_dropped():
    agents = {
        "diagnosis": {
            "llm": {
                "model": "m",
                "api_key_source": "direct",
                "api_key_env": "OPENAI_API_KEY",
                "api_key": "sk-test",
            }
        }
    }
    got = sanitize_agents_for_inference(agents)
    assert got["diagnosis"]["llm"]["api_key"] == "sk-test"
    assert "api_key_source" not in got["diagnosis"]["llm"]

    agents2 = {
        "diagnosis": {
            "llm": {
                "model": "m",
                "api_key_source": "env",
                "api_key_env": "MY_KEY",
                "api_key": "should-drop",
            }
        }
    }
    got2 = sanitize_agents_for_inference(agents2)
    assert "api_key" not in got2["diagnosis"]["llm"]
