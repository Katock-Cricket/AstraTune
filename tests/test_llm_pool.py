"""LLM 工作池：key 解析、并发上限、同步/异步共享、重入。"""

from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from utils.llm import (
    PooledChatOpenAI,
    format_pool_status,
    get_llm_pool_stats,
    make_chat_openai,
    parse_api_keys,
    reset_llm_pool,
)
from utils.timing import clear_timing_context, get_timing_events, set_timing_context


def test_parse_api_keys_semicolon(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEYS", " ka; kb ; ;kc ")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert parse_api_keys() == ["ka", "kb", "kc"]


def test_parse_api_keys_fallback_single(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEYS", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "only")
    assert parse_api_keys() == ["only"]


def test_parse_api_keys_cfg_last(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEYS", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert parse_api_keys(cfg_key="from-cfg") == ["from-cfg"]


def test_keys_override_single(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEYS", "a;b")
    monkeypatch.setenv("OPENAI_API_KEY", "ignored")
    assert parse_api_keys() == ["a", "b"]


def test_pool_size_follows_keys():
    n = reset_llm_pool(["k1", "k2", "k3"])
    assert n == 3
    stats = get_llm_pool_stats()
    assert stats["workers"] == 3
    assert stats["unique_keys"] == 3
    assert stats["in_use"] == 0
    assert "workers=3" in format_pool_status()


def test_max_workers_caps_but_not_above_keys():
    assert reset_llm_pool(["a", "b", "c", "d"], max_workers=2) == 2
    assert get_llm_pool_stats()["workers"] == 2
    assert reset_llm_pool(["a", "b"], max_workers=8) == 2


def test_single_key_expands_to_max_workers():
    assert reset_llm_pool(["solo"], max_workers=4) == 4
    stats = get_llm_pool_stats()
    assert stats["workers"] == 4
    assert stats["unique_keys"] == 1


def test_parse_api_keys_single_mode(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEYS", "stale;a;b")
    monkeypatch.setenv("OPENAI_API_KEY", "fresh")
    assert parse_api_keys(key_mode="single") == ["fresh"]


def test_pool_caps_inflight():
    reset_llm_pool(["k1", "k2"])
    barrier = threading.Barrier(4)
    hold = threading.Event()

    def worker():
        barrier.wait(timeout=5)
        from utils.llm import borrow_llm_worker

        with borrow_llm_worker():
            hold.wait(timeout=2)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    deadline = time.time() + 1.0
    stats = get_llm_pool_stats()
    while time.time() < deadline:
        stats = get_llm_pool_stats()
        if stats["in_use"] == 2:
            break
        time.sleep(0.02)
    assert stats["in_use"] == 2
    assert stats["peak"] == 2
    hold.set()
    for t in threads:
        t.join(timeout=3)
        assert not t.is_alive()
    assert get_llm_pool_stats()["in_use"] == 0


def test_concurrent_workers_use_distinct_keys():
    reset_llm_pool(["alpha", "beta"])
    barrier = threading.Barrier(2)
    hold = threading.Event()
    seen = []
    lock = threading.Lock()

    class FakeLLM:
        def __init__(self, **kwargs):
            self.api_key = kwargs.get("api_key")

        def invoke(self, *_a, **_k):
            barrier.wait(timeout=5)
            with lock:
                seen.append(self.api_key)
            hold.wait(timeout=2)
            return AIMessage(content="ok")

        def bind_tools(self, tools, **kwargs):
            return self

    def worker():
        llm = make_chat_openai(
            {
                "base_url": "http://127.0.0.1:9/v1",
                "model": "dummy",
                "timeout": 1,
                "max_attempts": 1,
                "first_token_timeout": 1,
                "response_timeout": 1,
            }
        )
        llm.invoke([HumanMessage(content="hi")])

    with patch("utils.llm.ChatOpenAI", FakeLLM):
        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        deadline = time.time() + 1.0
        while time.time() < deadline and len(seen) < 2:
            time.sleep(0.02)
        hold.set()
        for t in threads:
            t.join(timeout=3)
    assert set(seen) == {"alpha", "beta"}


def test_pool_wait_is_timed():
    reset_llm_pool(["only"])
    clear_timing_context()
    set_timing_context(instance_id="pool", dialect="postgresql")
    blocker = threading.Event()
    entered = threading.Event()

    def hold_slot():
        from utils.llm import borrow_llm_worker

        with borrow_llm_worker():
            entered.set()
            blocker.wait(timeout=3)

    t = threading.Thread(target=hold_slot)
    t.start()
    assert entered.wait(timeout=2)
    threading.Timer(0.15, blocker.set).start()
    from utils.llm import borrow_llm_worker

    with borrow_llm_worker():
        pass
    t.join(timeout=3)
    waits = [e for e in get_timing_events() if e["stage"] == "llm_pool_wait"]
    assert waits, "主线程应记录 llm_pool_wait"
    assert max(e["elapsed_ms"] for e in waits) >= 50.0
    clear_timing_context()


def test_async_and_sync_share_one_pool():
    reset_llm_pool(["solo"])
    hold = threading.Event()
    entered = threading.Event()

    def sync_hold():
        from utils.llm import borrow_llm_worker

        with borrow_llm_worker():
            entered.set()
            hold.wait(timeout=3)

    t = threading.Thread(target=sync_hold)
    t.start()
    assert entered.wait(timeout=2)

    async def waiter():
        from utils.llm import aborrow_llm_worker

        t0 = time.perf_counter()
        async with aborrow_llm_worker():
            return (time.perf_counter() - t0) * 1000.0

    async def run():
        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.12)
        hold.set()
        return await task

    waited = asyncio.run(run())
    t.join(timeout=3)
    assert waited >= 50.0


def test_reentrant_borrow_same_thread():
    reset_llm_pool(["one"])
    from utils.llm import borrow_llm_worker

    with borrow_llm_worker() as (i1, k1):
        with borrow_llm_worker() as (i2, k2):
            assert (i1, k1) == (i2, k2)
            assert get_llm_pool_stats()["in_use"] == 1
    assert get_llm_pool_stats()["in_use"] == 0


def test_make_chat_openai_is_pooled():
    reset_llm_pool(["sk-test"])
    llm = make_chat_openai(
        {
            "base_url": "http://127.0.0.1:9/v1",
            "model": "dummy",
            "timeout": 1,
            "max_attempts": 1,
            "first_token_timeout": 1,
            "response_timeout": 1,
        }
    )
    assert isinstance(llm, PooledChatOpenAI)
    bound = llm.bind_tools([])
    assert isinstance(bound, PooledChatOpenAI)


def test_bind_tools_invoke_uses_pool():
    reset_llm_pool(["tk"])
    calls = []

    class FakeLLM:
        def __init__(self, **kwargs):
            self.api_key = kwargs.get("api_key")

        def bind_tools(self, tools, **kwargs):
            calls.append(("bind", self.api_key, len(tools)))
            return self

        def invoke(self, *_a, **_k):
            calls.append(("invoke", self.api_key))
            return AIMessage(content="ok", tool_calls=[])

    with patch("utils.llm.ChatOpenAI", FakeLLM):
        llm = make_chat_openai(
            {
                "base_url": "http://127.0.0.1:9/v1",
                "model": "dummy",
                "timeout": 1,
                "max_attempts": 1,
                "first_token_timeout": 1,
                "response_timeout": 1,
            }
        )
        bound = llm.bind_tools([SimpleNamespace(name="t")])
        resp = bound.invoke([SystemMessage(content="s")])
    assert resp.content == "ok"
    assert ("bind", "tk", 1) in calls
    assert ("invoke", "tk") in calls


def test_llm_retry_on_response_timeout():
    reset_llm_pool(["rk"])
    attempts = {"n": 0}

    class SlowLLM:
        def __init__(self, **kwargs):
            pass

        def invoke(self, *_a, **_k):
            attempts["n"] += 1
            n = attempts["n"]
            if n == 1:
                time.sleep(0.05)
            return AIMessage(content="ok")

    with patch("utils.llm.ChatOpenAI", SlowLLM):
        llm = make_chat_openai(
            {
                "base_url": "http://127.0.0.1:9/v1",
                "model": "dummy",
                "timeout": 5,
                "first_token_timeout": 0.02,
                "response_timeout": 0.02,
                "max_attempts": 3,
            }
        )
        resp = llm.invoke([HumanMessage(content="hi")])
    assert resp.content == "ok"
    assert attempts["n"] == 2
    retries = [e for e in get_timing_events() if e["stage"] == "llm_retry"]
    assert retries and retries[0]["reason"] == "response_timeout"
    clear_timing_context()


def test_llm_retry_exhausted_raises():
    reset_llm_pool(["rk"])

    class AlwaysSlowLLM:
        def __init__(self, **kwargs):
            pass

        def invoke(self, *_a, **_k):
            time.sleep(0.05)
            return AIMessage(content="never")

    with patch("utils.llm.ChatOpenAI", AlwaysSlowLLM):
        llm = make_chat_openai(
            {
                "base_url": "http://127.0.0.1:9/v1",
                "model": "dummy",
                "timeout": 5,
                "first_token_timeout": 0.01,
                "response_timeout": 0.01,
                "max_attempts": 2,
            }
        )
        try:
            llm.invoke([HumanMessage(content="hi")])
            assert False, "should raise"
        except Exception as exc:
            assert "response_timeout" in str(exc) or "LlmRetryableError" in type(exc).__name__


def test_tool_calls_only_is_usable():
    from utils.llm import _is_usable_llm_result

    only_tools = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "execute_sql",
                "args": {"sql": "SELECT 1"},
                "id": "1",
                "type": "tool_call",
            }
        ],
    )
    assert _is_usable_llm_result(only_tools) is True
    assert _is_usable_llm_result(AIMessage(content="")) is False
    assert _is_usable_llm_result(AIMessage(content="ok")) is True


def test_client_kwargs_disable_streaming():
    from utils.llm import _client_kwargs_from_cfg

    kwargs, _ = _client_kwargs_from_cfg(
        {"base_url": "http://x", "model": "m"},
        temperature=0.2,
        default_temperature=0.2,
    )
    assert kwargs.get("disable_streaming") is True


class _MonkeyPatch:
    def __init__(self):
        self._undo = []

    def setenv(self, key: str, value: str) -> None:
        import os

        existed = key in os.environ
        old = os.environ.get(key)
        self._undo.append((key, old, existed))
        os.environ[key] = value

    def delenv(self, key: str, raising: bool = True) -> None:
        import os

        if key not in os.environ:
            if raising:
                raise KeyError(key)
            return
        self._undo.append((key, os.environ.get(key), True))
        del os.environ[key]

    def undo(self) -> None:
        import os

        for key, old, existed in reversed(self._undo):
            if existed and old is not None:
                os.environ[key] = old
            else:
                os.environ.pop(key, None)
        self._undo.clear()


if __name__ == "__main__":
    ns = {k: v for k, v in globals().items() if k.startswith("test_") and callable(v)}
    failed = 0
    for name, fn in ns.items():
        mp = _MonkeyPatch()
        try:
            if "monkeypatch" in fn.__code__.co_varnames:
                fn(mp)
            else:
                fn()
            print(f"ok  {name}")
        except Exception as e:
            failed += 1
            print(f"FAIL {name}: {e}")
            raise
        finally:
            mp.undo()
    if failed:
        raise SystemExit(1)
    print(f"passed {len(ns)}")

