#!/usr/bin/env python3
"""LLM 工作池烟测：按 OPENAI_API_KEYS 并发打网关，确认在途 ≤ worker 数。"""

from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from langchain_core.messages import HumanMessage, SystemMessage

from utils.config_loader import load_all_configs
from utils.llm import (
    format_pool_status,
    get_llm_pool_stats,
    init_llm_pool,
    make_chat_openai,
    parse_api_keys,
)
from utils.logger import setup_logger


def main() -> int:
    logger = setup_logger()
    n_keys = len(parse_api_keys())
    if n_keys < 1:
        logger.error("未设置 OPENAI_API_KEYS / OPENAI_API_KEY")
        return 2
    n = init_llm_pool()
    logger.info(format_pool_status())
    cfg = (load_all_configs().get("agents") or {}).get("diagnosis") or {}
    llm_cfg = dict(cfg.get("llm") or {})
    llm = make_chat_openai(llm_cfg, temperature=0)
    jobs = max(n * 2, n + 2)
    logger.info(f"发起 {jobs} 路并发短请求，期望 peak in_use ≤ {n}")

    def ping(i: int):
        t0 = time.perf_counter()
        resp = llm.invoke(
            [
                SystemMessage(content="Reply with exactly one word: pong"),
                HumanMessage(content=f"ping-{i}"),
            ]
        )
        text = resp.content if resp and isinstance(resp.content, str) else str(resp)
        return i, (time.perf_counter() - t0) * 1000.0, (text or "")[:80]

    t0 = time.perf_counter()
    ok = 0
    errs = []
    elapsed = []
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        futs = [pool.submit(ping, i) for i in range(jobs)]
        for fut in as_completed(futs):
            try:
                i, ms, text = fut.result()
                ok += 1
                elapsed.append(ms)
                logger.info(f"ping-{i} {ms:.0f}ms {text!r}")
            except Exception as e:
                errs.append(str(e))
                logger.error(f"ping 失败: {e}")

    wall = (time.perf_counter() - t0) * 1000.0
    stats = get_llm_pool_stats()
    summary = {
        "keys": n_keys,
        "llm_workers": n,
        "jobs": jobs,
        "ok": ok,
        "errors": len(errs),
        "wall_ms": round(wall, 1),
        "per_call_ms_mean": round(sum(elapsed) / len(elapsed), 1) if elapsed else None,
        "peak_in_use": stats["peak"],
        "in_use_end": stats["in_use"],
    }
    logger.info(json.dumps(summary, ensure_ascii=False))
    print(json.dumps(summary, ensure_ascii=False))
    if errs or ok != jobs:
        return 1
    if stats["peak"] > n:
        logger.error(f"peak {stats['peak']} 超过 worker {n}")
        return 1
    if stats["in_use"] != 0:
        logger.error("结束后 in_use 未归零")
        return 1
    logger.info("LLM 工作池烟测通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
