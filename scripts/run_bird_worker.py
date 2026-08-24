#!/usr/bin/env python3
"""单题推理子进程：供 run_bird_critic --isolate subprocess 调用，超时后父进程可强杀。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> None:
    p = argparse.ArgumentParser(description="BIRD 单题 worker（stdout 一行 JSON）")
    p.add_argument("--payload", type=str, required=True, help="payload JSON 文件路径")
    args = p.parse_args()

    payload = json.loads(Path(args.payload).read_text(encoding="utf-8"))
    from utils.config_loader import get_batch_config, load_all_configs
    from utils.llm import batch_llm_pool_options, init_llm_pool
    from utils.logger import setup_logger

    from scripts.run_bird_critic import run_one, slim_output_record

    configs = load_all_configs()
    batch_cfg = get_batch_config(configs)
    # 每子进程仅 1 个 LLM 槽；沙箱并发由父进程 workers 控制
    pool_opts = batch_llm_pool_options(batch_cfg, cli_llm_workers=1)
    init_llm_pool(**pool_opts)

    logger = setup_logger()
    rec = payload["record"]
    result = run_one(
        rec,
        dialect=str(payload["dialect"]),
        mode=str(payload["mode"]),
        logger=logger,
        slot_index=payload.get("slot_index"),
        workers=int(payload.get("workers") or 1),
        prior_pred=payload.get("prior_pred"),
    )
    out = slim_output_record(result)
    sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
