#!/usr/bin/env python3
"""单题 Effi 推理子进程：供 run_effi_sql --isolate subprocess 调用，超时后父进程可强杀。

输入：sys.argv[1] 为 JSON payload，或 @path 指向 payload 文件
输出：stdout 最后一行为 slim 结果 JSON（含 prediction）；详细日志走 stderr
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> None:
    if len(sys.argv) < 2:
        sys.stderr.write("effi_worker: 缺少 payload 参数\n")
        sys.exit(2)

    arg = sys.argv[1]
    if arg.startswith("@"):
        payload = json.loads(Path(arg[1:]).read_text(encoding="utf-8"))
    else:
        payload = json.loads(arg)

    from utils.batch_records import slim_output_record
    from utils.config_loader import get_batch_config, load_all_configs
    from utils.llm import batch_llm_pool_options, init_llm_pool
    from utils.logger import setup_logger
    from scripts.run_effi_sql import run_one_effi

    configs = load_all_configs()
    batch_cfg = get_batch_config(configs)
    # 每子进程仅 1 个 LLM 槽；跨题并发由父进程 --workers 控制
    pool_opts = batch_llm_pool_options(batch_cfg, cli_llm_workers=1)
    init_llm_pool(**pool_opts)

    logger = setup_logger()
    result = run_one_effi(
        payload["record"],
        raw=payload.get("raw") or {},
        mode=str(payload.get("mode") or "diag+opt"),
        function=str(payload.get("function") or "efficiency"),
        logger=logger,
        prior_pred=payload.get("prior_pred"),
    )
    out = slim_output_record(result)
    sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
