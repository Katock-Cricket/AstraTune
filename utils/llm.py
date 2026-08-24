"""LLM 工作池：沙箱并行度与网关并发度拆开；LLM worker 数由 batch.llm_workers 配置。

环境变量：
- OPENAI_API_KEY：单 key（`llm_key_mode=single` 时仅用此项，忽略 OPENAI_API_KEYS）
- OPENAI_API_KEYS：多 key，英文分号 `;` 分隔（`llm_key_mode=auto` 时优先）

单 KEY 时可配置 `llm_workers=N`，同一 key 占 N 个 worker 槽以并发多路请求。
多 KEY 时 worker 数 ≤ key 数（每个 key 一槽，不重复）。
同一线程/任务重入时复用已借到的 worker，避免嵌套调用死锁。

单次请求重试（PooledChatOpenAI）：
- 同步 invoke：走 ChatOpenAI 同步 HTTP（禁止 asyncio.run(ainvoke)）
- 异步 ainvoke：非流式；disable_streaming=True，避免 astream_events 逼出空 chunk
- 仅有 tool_calls、正文为空（ReAct 首轮）视为有效响应
- 完整响应超过 response_timeout（默认 120s）→ 重试
- 空 content 且无 tool_calls / 超时 / event-loop-closed → 重试
- 最多 max_attempts 次（默认 10）；LangChain 内置 max_retries 置 0
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import os
import queue
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional, Sequence, Tuple

from langchain_core.runnables import Runnable
from langchain_openai import ChatOpenAI

from utils.logger import default_logger
from utils.timing import log_timing

_KEYS_ENV = "OPENAI_API_KEYS"
_KEY_ENV = "OPENAI_API_KEY"

_pool_lock = threading.Lock()
_pool: Optional["LlmWorkerPool"] = None
_client_cache: Dict[Tuple[Any, ...], ChatOpenAI] = {}
_cache_lock = threading.Lock()
_held_cv: contextvars.ContextVar[Optional[Tuple[int, str]]] = contextvars.ContextVar(
    "llm_worker_held", default=None
)


class LlmRetryableError(Exception):
    """可重试的 LLM 调用失败（首 token / 完整响应超时等）。"""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def parse_api_keys(
    *, cfg_key: Optional[str] = None, key_mode: str = "auto"
) -> List[str]:
    """解析 key 列表。`key_mode=single` 仅用 OPENAI_API_KEY / 配置 api_key。"""
    if key_mode == "single":
        single = (os.environ.get(_KEY_ENV) or "").strip()
        if single:
            return [single]
        extra = (cfg_key or "").strip()
        if extra:
            return [extra]
        return []
    raw = os.environ.get(_KEYS_ENV) or ""
    keys = [p.strip() for p in raw.split(";") if p.strip()]
    if keys:
        return keys
    single = (os.environ.get(_KEY_ENV) or "").strip()
    if single:
        return [single]
    extra = (cfg_key or "").strip()
    if extra:
        return [extra]
    return []


def batch_llm_pool_options(
    batch_cfg: Dict[str, Any], cli_llm_workers: Optional[int] = None
) -> Dict[str, Any]:
    """从 batch 配置解析 init_llm_pool 参数。"""
    key_mode = str(batch_cfg.get("llm_key_mode") or "auto")
    llm_cap = cli_llm_workers
    if llm_cap is None:
        raw = batch_cfg.get("llm_workers", "from_keys")
        if isinstance(raw, int):
            llm_cap = raw
        elif isinstance(raw, str) and raw.isdigit():
            llm_cap = int(raw)
    return {"max_workers": llm_cap, "key_mode": key_mode}


def _expand_pool_keys(keys: List[str], max_workers: Optional[int]) -> List[str]:
    if not keys or max_workers is None:
        return keys
    n = int(max_workers)
    if n < 1:
        raise ValueError("llm_workers 必须 >= 1")
    if len(set(keys)) == 1:
        return [keys[0]] * n
    if n < len(keys):
        return keys[:n]
    return keys


class LlmWorkerPool:
    """按 key 分槽的借还队列。"""

    def __init__(self, keys: Sequence[str]):
        if not keys:
            raise ValueError("LLM 工作池至少需要 1 个 API key")
        self._keys = list(keys)
        self._q: queue.Queue[int] = queue.Queue()
        for i in range(len(self._keys)):
            self._q.put(i)
        self._lock = threading.Lock()
        self.in_use = 0
        self.peak = 0

    @property
    def workers(self) -> int:
        return len(self._keys)

    @property
    def unique_keys(self) -> int:
        return len(set(self._keys))

    def key_at(self, idx: int) -> str:
        return self._keys[idx]


def get_llm_pool() -> Optional[LlmWorkerPool]:
    return _pool


def get_llm_pool_stats() -> Dict[str, Any]:
    p = _pool
    if p is None:
        return {
            "workers": 0,
            "unique_keys": 0,
            "in_use": 0,
            "peak": 0,
            "available": 0,
            "ready": False,
        }
    return {
        "workers": p.workers,
        "unique_keys": p.unique_keys,
        "in_use": p.in_use,
        "peak": p.peak,
        "available": p._q.qsize(),
        "ready": True,
    }


def format_pool_status() -> str:
    s = get_llm_pool_stats()
    if not s.get("ready"):
        return "LLM 工作池未初始化"
    mode_note = (
        "单 KEY 多 worker"
        if s["unique_keys"] == 1 and s["workers"] > 1
        else "多 KEY"
        if s["unique_keys"] > 1
        else "单 KEY"
    )
    return (
        f"LLM 工作池: workers={s['workers']} unique_keys={s['unique_keys']} "
        f"({mode_note})；沙箱并行度由 --workers 独立配置"
    )


def init_llm_pool(
    keys: Optional[Sequence[str]] = None,
    *,
    max_workers: Optional[int] = None,
    cfg_key: Optional[str] = None,
    key_mode: Optional[str] = None,
) -> int:
    """初始化（或重建）全局工作池。单 KEY 时 max_workers 可大于 1。返回 worker 数。"""
    global _pool
    if keys is None and max_workers is None and key_mode is None:
        try:
            from utils.config_loader import load_all_configs, get_batch_config

            opts = batch_llm_pool_options(get_batch_config(load_all_configs()))
            max_workers = opts["max_workers"]
            key_mode = opts["key_mode"]
        except Exception:
            key_mode = "auto"
    resolved_mode = key_mode or "auto"
    resolved = (
        list(keys)
        if keys is not None
        else parse_api_keys(cfg_key=cfg_key, key_mode=resolved_mode)
    )
    if not resolved:
        hint = (
            "请设置 OPENAI_API_KEY"
            if resolved_mode == "single"
            else "请设置 OPENAI_API_KEYS（多个 key 用 ; 分隔）或 OPENAI_API_KEY"
        )
        raise RuntimeError(f"未找到 API key：{hint}")
    resolved = _expand_pool_keys(resolved, max_workers)
    with _pool_lock:
        _pool = LlmWorkerPool(resolved)
        with _cache_lock:
            _client_cache.clear()
        n = _pool.workers
    default_logger.info(format_pool_status())
    return n


def reset_llm_pool(
    keys: Sequence[str], *, max_workers: Optional[int] = None
) -> int:
    """测试用：用给定 key 列表重建池。"""
    return init_llm_pool(keys=list(keys), max_workers=max_workers)


def _ensure_pool(*, cfg_key: Optional[str] = None) -> LlmWorkerPool:
    global _pool
    if _pool is None:
        init_llm_pool(cfg_key=cfg_key)
    assert _pool is not None
    return _pool


def _held() -> Optional[Tuple[int, str]]:
    return _held_cv.get()


def _acquire_sync() -> Tuple[int, str]:
    existing = _held()
    if existing is not None:
        return existing
    pool = _ensure_pool()
    t0 = time.perf_counter()
    try:
        idx = pool._q.get(timeout=180)
    except queue.Empty as e:
        raise RuntimeError("LLM 工作池等待超时 180s（可能有调用未归还 worker）") from e
    log_timing("llm_pool_wait", (time.perf_counter() - t0) * 1000.0, llm_worker=idx)
    with pool._lock:
        pool.in_use += 1
        pool.peak = max(pool.peak, pool.in_use)
    token = (idx, pool.key_at(idx))
    _held_cv.set(token)
    return token


def _release_sync(token: Tuple[int, str], *, was_nested: bool) -> None:
    if was_nested:
        return
    pool = _ensure_pool()
    _held_cv.set(None)
    with pool._lock:
        pool.in_use = max(0, pool.in_use - 1)
    pool._q.put(token[0])


@contextmanager
def borrow_llm_worker():
    nested = _held() is not None
    token = _acquire_sync()
    try:
        yield token
    finally:
        _release_sync(token, was_nested=nested)


@asynccontextmanager
async def aborrow_llm_worker():
    nested = _held() is not None
    if nested:
        yield _held()
        return
    pool = _ensure_pool()
    t0 = time.perf_counter()
    try:
        idx = await asyncio.to_thread(pool._q.get, True, 180)
    except queue.Empty as e:
        raise RuntimeError("LLM 工作池等待超时 180s（可能有调用未归还 worker）") from e
    log_timing("llm_pool_wait", (time.perf_counter() - t0) * 1000.0, llm_worker=idx)
    with pool._lock:
        pool.in_use += 1
        pool.peak = max(pool.peak, pool.in_use)
    token = (idx, pool.key_at(idx))
    _held_cv.set(token)
    try:
        yield token
    finally:
        _held_cv.set(None)
        with pool._lock:
            pool.in_use = max(0, pool.in_use - 1)
        pool._q.put(idx)


def _client_kwargs_from_cfg(
    llm_cfg: Optional[Dict[str, Any]],
    *,
    temperature: Optional[float],
    default_temperature: float,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    cfg = dict(llm_cfg or {})
    temp = default_temperature if temperature is None else temperature
    if cfg.get("temperature") is not None and temperature is None:
        temp = cfg.get("temperature")

    first_token_timeout = float(cfg.get("first_token_timeout") or 10.0)
    response_timeout = float(cfg.get("response_timeout") or 120.0)
    max_attempts = int(cfg.get("max_attempts") if cfg.get("max_attempts") is not None else 10)
    # httpx 硬超时略高于 response_timeout，便于截断挂死连接
    timeout = float(cfg.get("timeout") or cfg.get("request_timeout") or (response_timeout + 10.0))

    client_kwargs = {
        "base_url": cfg.get("base_url"),
        "model": cfg.get("model"),
        "temperature": temp,
        "timeout": timeout,
        "max_retries": 0,
        # astream_events 会挂流式 callback，迫使 ChatOpenAI 走 _astream；
        # deepseek-v4-flash 流式 chunk 常空 content，且 tool_calls 易丢 → empty_response。
        "disable_streaming": True,
    }
    retry_policy = {
        "first_token_timeout": first_token_timeout,
        "response_timeout": response_timeout,
        "max_attempts": max(1, max_attempts),
    }
    return client_kwargs, retry_policy


def _running_loop_id() -> Optional[int]:
    try:
        return id(asyncio.get_running_loop())
    except RuntimeError:
        return None


def _client_fingerprint(
    worker_idx: int, kwargs: Dict[str, Any], *, loop_id: Optional[int]
) -> Tuple[Any, ...]:
    return (
        worker_idx,
        loop_id,
        kwargs.get("base_url"),
        kwargs.get("model"),
        kwargs.get("temperature"),
        kwargs.get("timeout"),
        kwargs.get("max_retries"),
        kwargs.get("disable_streaming"),
    )


def _get_client(
    worker_idx: int,
    api_key: str,
    kwargs: Dict[str, Any],
    *,
    loop_id: Optional[int] = None,
) -> ChatOpenAI:
    fp = _client_fingerprint(worker_idx, kwargs, loop_id=loop_id)
    with _cache_lock:
        cli = _client_cache.get(fp)
        if cli is None:
            cli = ChatOpenAI(api_key=api_key, **kwargs)
            _client_cache[fp] = cli
        return cli


def _drop_client(
    worker_idx: int,
    kwargs: Dict[str, Any],
    *,
    loop_id: Optional[int] = None,
) -> None:
    fp = _client_fingerprint(worker_idx, kwargs, loop_id=loop_id)
    with _cache_lock:
        _client_cache.pop(fp, None)


def _bind(client: ChatOpenAI, tools: Optional[list], tool_kwargs: Dict[str, Any]):
    if tools:
        return client.bind_tools(tools, **(tool_kwargs or {}))
    return client


def _exception_text(exc: BaseException) -> str:
    parts = [str(exc), type(exc).__name__]
    cur: Optional[BaseException] = exc
    seen = 0
    while cur is not None and seen < 6:
        parts.append(str(cur))
        parts.append(type(cur).__name__)
        cur = cur.__cause__ or cur.__context__
        seen += 1
    return " ".join(parts).lower()


def _is_event_loop_closed_error(exc: BaseException) -> bool:
    text = _exception_text(exc)
    return "event loop is closed" in text or "loop is closed" in text


def _is_retryable_exception(exc: BaseException) -> bool:
    if isinstance(exc, LlmRetryableError):
        return True
    text = _exception_text(exc)
    name = type(exc).__name__.lower()
    if "timeout" in text or "timed out" in text or "timeout" in name:
        return True
    if _is_event_loop_closed_error(exc):
        return True
    if "connection error" in text or "apiconnectionerror" in name:
        return True
    return False


def extract_message_text(msg: Any) -> str:
    """从 AIMessage / chunk 归一化文本 content（含 list 块）。"""
    if msg is None:
        return ""
    content = getattr(msg, "content", None)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                if block.get("type") == "text":
                    parts.append(str(block.get("text") or ""))
                elif "text" in block:
                    parts.append(str(block.get("text") or ""))
        return "".join(parts)
    if content is None:
        return ""
    return str(content)


def _has_tool_calls(msg: Any) -> bool:
    calls = getattr(msg, "tool_calls", None) or []
    if calls:
        return True
    add = getattr(msg, "additional_kwargs", None) or {}
    if isinstance(add, dict) and add.get("tool_calls"):
        return True
    return False


def _is_usable_llm_result(msg: Any) -> bool:
    """有文本，或仅有 tool_calls（ReAct 首轮常见）即视为有效响应。"""
    if msg is None:
        return False
    if extract_message_text(msg).strip():
        return True
    return _has_tool_calls(msg)


async def _invoke_async(
    runnable: Runnable,
    input: Any,
    config: Optional[Dict[str, Any]],
    kwargs: Dict[str, Any],
    *,
    response_timeout: float,
) -> Any:
    try:
        result = await asyncio.wait_for(
            runnable.ainvoke(input, config=config, **kwargs),
            timeout=response_timeout,
        )
    except asyncio.TimeoutError as e:
        raise LlmRetryableError("response_timeout") from e
    if not _is_usable_llm_result(result):
        meta = getattr(result, "response_metadata", None) or {}
        default_logger.warning(
            "LLM 空响应: content_len=%s tool_calls=%s finish_reason=%s",
            len(extract_message_text(result)),
            len(getattr(result, "tool_calls", None) or []),
            meta.get("finish_reason"),
        )
        raise LlmRetryableError("empty_response")
    return result


def _invoke_sync(
    runnable: Runnable,
    input: Any,
    config: Optional[Dict[str, Any]],
    kwargs: Dict[str, Any],
    *,
    response_timeout: float,
) -> Any:
    """同步 HTTP invoke；用线程池实现 response_timeout，避免 asyncio.run(ainvoke)。

    超时后 shutdown(wait=False)，禁止在 hung HTTP 上阻塞整个 worker。
    """

    def _call() -> Any:
        return runnable.invoke(input, config=config, **kwargs)

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    fut = pool.submit(_call)
    timed_out = False
    try:
        result = fut.result(timeout=response_timeout)
    except concurrent.futures.TimeoutError as e:
        timed_out = True
        raise LlmRetryableError("response_timeout") from e
    finally:
        pool.shutdown(wait=not timed_out, cancel_futures=True)
    if not _is_usable_llm_result(result):
        meta = getattr(result, "response_metadata", None) or {}
        default_logger.warning(
            "LLM 空响应: content_len=%s tool_calls=%s finish_reason=%s",
            len(extract_message_text(result)),
            len(getattr(result, "tool_calls", None) or []),
            meta.get("finish_reason"),
        )
        raise LlmRetryableError("empty_response")
    return result


async def _call_with_retry_async(
    make_runnable,
    input: Any,
    config: Optional[Dict[str, Any]],
    kwargs: Dict[str, Any],
    *,
    first_token_timeout: float,
    response_timeout: float,
    max_attempts: int,
    on_loop_closed=None,
) -> Any:
    del first_token_timeout  # 非流式路径保留参数签名，暂不单独检测首 token
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        t0 = time.perf_counter()
        try:
            runnable = make_runnable()
            result = await _invoke_async(
                runnable, input, config, kwargs,
                response_timeout=response_timeout,
            )
            if attempt > 1:
                log_timing(
                    "llm_retry_success",
                    (time.perf_counter() - t0) * 1000.0,
                    attempt=attempt,
                    max_attempts=max_attempts,
                )
            return result
        except BaseException as exc:
            last_exc = exc
            if _handle_retry_exc(exc, attempt, max_attempts, t0, on_loop_closed):
                continue
            raise
    assert last_exc is not None
    raise last_exc


def _call_with_retry_sync(
    make_runnable,
    input: Any,
    config: Optional[Dict[str, Any]],
    kwargs: Dict[str, Any],
    *,
    first_token_timeout: float,
    response_timeout: float,
    max_attempts: int,
    on_loop_closed=None,
) -> Any:
    del first_token_timeout
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        t0 = time.perf_counter()
        try:
            runnable = make_runnable()
            result = _invoke_sync(
                runnable, input, config, kwargs,
                response_timeout=response_timeout,
            )
            if attempt > 1:
                log_timing(
                    "llm_retry_success",
                    (time.perf_counter() - t0) * 1000.0,
                    attempt=attempt,
                    max_attempts=max_attempts,
                )
            return result
        except BaseException as exc:
            last_exc = exc
            if _handle_retry_exc(exc, attempt, max_attempts, t0, on_loop_closed):
                continue
            raise
    assert last_exc is not None
    raise last_exc


def _handle_retry_exc(
    exc: BaseException,
    attempt: int,
    max_attempts: int,
    t0: float,
    on_loop_closed=None,
) -> bool:
    """共享：记录重试日志 + 必要时 on_loop_closed；返回 True 表示可继续重试。

    不可重试或耗尽时返回 False，调用方 raise。
    """
    if on_loop_closed is not None and _is_retryable_exception(exc) and attempt < max_attempts:
        try:
            on_loop_closed()
        except Exception:
            pass
    if not _is_retryable_exception(exc) or attempt >= max_attempts:
        return False
    reason = getattr(exc, "reason", None) or type(exc).__name__
    if _is_event_loop_closed_error(exc):
        reason = "event_loop_closed"
    log_timing(
        "llm_retry",
        (time.perf_counter() - t0) * 1000.0,
        attempt=attempt,
        max_attempts=max_attempts,
        reason=str(reason),
    )
    default_logger.warning(
        f"LLM 请求重试 {attempt}/{max_attempts}: {reason} ({exc})"
    )
    return True


class PooledChatOpenAI(Runnable):
    """外观与 ChatOpenAI 相近：invoke / ainvoke / bind_tools 走工作池 + 非流式重试。"""

    def __init__(
        self,
        client_kwargs: Dict[str, Any],
        *,
        retry_policy: Optional[Dict[str, Any]] = None,
        tools: Optional[list] = None,
        tool_kwargs: Optional[Dict[str, Any]] = None,
        cfg_key: Optional[str] = None,
    ):
        super().__init__()
        self._client_kwargs = dict(client_kwargs)
        rp = dict(retry_policy or {})
        self._first_token_timeout = float(rp.get("first_token_timeout") or 10.0)
        self._response_timeout = float(rp.get("response_timeout") or 120.0)
        self._max_attempts = int(rp.get("max_attempts") or 10)
        self._tools = tools
        self._tool_kwargs = dict(tool_kwargs or {})
        self._cfg_key = cfg_key

    def bind_tools(self, tools: list, **kwargs: Any) -> "PooledChatOpenAI":
        return PooledChatOpenAI(
            self._client_kwargs,
            retry_policy={
                "first_token_timeout": self._first_token_timeout,
                "response_timeout": self._response_timeout,
                "max_attempts": self._max_attempts,
            },
            tools=list(tools),
            tool_kwargs=dict(kwargs),
            cfg_key=self._cfg_key,
        )

    def invoke(self, input: Any, config: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Any:
        _ensure_pool(cfg_key=self._cfg_key)
        with borrow_llm_worker() as (idx, key):
            def make_runnable():
                return _bind(
                    _get_client(idx, key, self._client_kwargs, loop_id=None),
                    self._tools,
                    self._tool_kwargs,
                )

            def on_loop_closed():
                _drop_client(idx, self._client_kwargs, loop_id=None)

            return _call_with_retry_sync(
                make_runnable,
                input,
                config,
                kwargs,
                first_token_timeout=self._first_token_timeout,
                response_timeout=self._response_timeout,
                max_attempts=self._max_attempts,
                on_loop_closed=on_loop_closed,
            )

    async def ainvoke(
        self, input: Any, config: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> Any:
        _ensure_pool(cfg_key=self._cfg_key)
        async with aborrow_llm_worker() as (idx, key):
            loop_id = _running_loop_id()

            def make_runnable():
                return _bind(
                    _get_client(idx, key, self._client_kwargs, loop_id=loop_id),
                    self._tools,
                    self._tool_kwargs,
                )

            def on_loop_closed():
                _drop_client(idx, self._client_kwargs, loop_id=loop_id)

            return await _call_with_retry_async(
                make_runnable,
                input,
                config,
                kwargs,
                first_token_timeout=self._first_token_timeout,
                response_timeout=self._response_timeout,
                max_attempts=self._max_attempts,
                on_loop_closed=on_loop_closed,
            )


def make_chat_openai(
    llm_cfg: Optional[Dict[str, Any]] = None,
    *,
    temperature: Optional[float] = None,
    default_temperature: float = 0.2,
) -> PooledChatOpenAI:
    cfg = dict(llm_cfg or {})
    client_kwargs, retry_policy = _client_kwargs_from_cfg(
        cfg, temperature=temperature, default_temperature=default_temperature
    )
    _ensure_pool(cfg_key=cfg.get("api_key"))
    return PooledChatOpenAI(
        client_kwargs,
        retry_policy=retry_policy,
        cfg_key=cfg.get("api_key"),
    )


def probe_llm_gateway(
    llm_cfg: Optional[Dict[str, Any]] = None,
    *,
    timeout: float = 15.0,
    cfg_key: Optional[str] = None,
    key_mode: str = "auto",
) -> Tuple[bool, str]:
    """探测 LLM 网关是否可用（/models）。不可用时应中止批跑。"""
    import httpx

    cfg = dict(llm_cfg or {})
    base = str(cfg.get("base_url") or os.environ.get("OPENAI_BASE_URL") or "").rstrip("/")
    if not base:
        return False, "缺少 base_url"
    keys = parse_api_keys(cfg_key=cfg.get("api_key") or cfg_key, key_mode=key_mode)
    if not keys:
        return False, "缺少 API key（OPENAI_API_KEYS / OPENAI_API_KEY）"
    key = keys[0]
    url = f"{base}/models" if base.endswith("/v1") else f"{base}/v1/models"
    try:
        resp = httpx.get(
            url,
            headers={"Authorization": f"Bearer {key}"},
            timeout=float(timeout),
        )
    except Exception as e:
        return False, str(e)
    if resp.status_code == 200:
        return True, "ok"
    if resp.status_code in (401, 403):
        return False, f"鉴权失败 HTTP {resp.status_code}"
    if resp.status_code >= 500:
        return False, f"服务端错误 HTTP {resp.status_code}"
    return False, f"异常响应 HTTP {resp.status_code}: {resp.text[:120]}"
