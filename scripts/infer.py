#!/usr/bin/env python3
"""单例端到端推理 CLI：诊断 → Generate–Verify → pred_sqls。"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.config_loader import normalize_dialect
from utils.inference_runner import run_single_instance
from utils.logger import setup_logger
from utils.llm import init_llm_pool


def parse_arguments():
    p = argparse.ArgumentParser(
        description="SQL诊断修复 端到端推理（诊断 + Generate–Verify）"
    )
    p.add_argument(
        "--sql",
        type=str,
        required=True,
        help="待处理的 SQL，多条用分号分隔",
    )
    p.add_argument(
        "--log",
        type=str,
        default=None,
        help="执行日志（平均耗时、次数等）",
    )
    p.add_argument(
        "--preprocess-sql",
        type=str,
        default=None,
        help="前置 SQL，多条用分号分隔",
    )
    p.add_argument(
        "--clean-up-sql",
        type=str,
        default=None,
        help="清理 SQL，多条用分号分隔",
    )
    p.add_argument(
        "--user-prompt",
        type=str,
        default=None,
        help="附加用户提示 / 题面 query",
    )
    p.add_argument(
        "--category",
        type=str,
        default="Efficiency",
        help="题目类别：Query / Management / Personalization / Efficiency",
    )
    p.add_argument(
        "--db-id",
        type=str,
        default=None,
        help="目标数据库名（覆盖配置，如 financial）",
    )
    p.add_argument(
        "--dialect",
        type=str,
        default="postgresql",
        choices=["postgresql", "mysql", "pg", "postgres"],
        help="数据库方言",
    )
    p.add_argument(
        "--tables",
        type=str,
        default=None,
        help="逗号分隔表名；不传则由 LLM 从 SQL 抽取",
    )
    p.add_argument("--log-file", type=str, default=None)
    p.add_argument(
        "--stream",
        action="store_true",
        default=True,
        help="诊断阶段流式输出（默认开启）",
    )
    p.add_argument(
        "--skip-diagnosis",
        action="store_true",
        help="跳过诊断，用 user-prompt 充当诊断上下文后直接优化",
    )
    p.add_argument(
        "--output",
        type=str,
        default=None,
        help="可选：写出含 diagnosis_report / pred_sqls 的 JSON",
    )
    return p.parse_args()


def main():
    args = parse_arguments()
    logger = setup_logger(log_file=args.log_file)
    logger.info("=" * 80)
    logger.info("端到端推理启动（诊断 → Generate–Verify）")
    logger.info("=" * 80)
    init_llm_pool()

    t_infer = time.perf_counter()
    result = None
    try:
        dialect = normalize_dialect(args.dialect)
        if not (args.sql or "").strip():
            raise SystemExit("--sql 为空")
        logger.info(f"方言: {dialect} category={args.category}")

        tables_override = None
        if args.tables:
            tables_override = [t.strip() for t in args.tables.split(",") if t.strip()]

        result = run_single_instance(
            sql=args.sql,
            query=args.user_prompt or "",
            category=args.category,
            dialect=dialect,
            db_id=args.db_id,
            preprocess_sql=args.preprocess_sql,
            clean_up_sql=args.clean_up_sql,
            exec_log=args.log,
            efficiency=True if str(args.category).lower() == "efficiency" else None,
            mode="opt_only" if args.skip_diagnosis else "diag+opt",
            stream=bool(args.stream),
            tables_override=tables_override,
            instance_id="infer",
            logger=logger,
        )
        logger.info(f"涉及表: {result.tables}")

        print("\n" + "=" * 80)
        print("端到端结果 pred_sqls")
        print("=" * 80)
        for i, s in enumerate(result.pred_sqls, 1):
            print(f"-- [{i}]\n{s}")
        print("=" * 80)
        print(
            f"success={result.meta.get('success')} "
            f"rounds={result.meta.get('rounds')} "
            f"fallback={result.meta.get('fallback')}"
        )
        if result.meta.get("failure_reasons"):
            print("failure_reasons:")
            for r in result.meta["failure_reasons"]:
                print(f"  - {r}")
        print()

        if args.output:
            out = {
                "pred_sqls": result.pred_sqls,
                "diagnosis_report": result.diagnosis_report,
                "success": result.meta.get("success"),
                "rounds": result.meta.get("rounds"),
                "fallback": result.meta.get("fallback"),
                "failure_reasons": result.meta.get("failure_reasons") or [],
                "error": result.error,
                "db_id": args.db_id,
                "dialect": dialect,
                "category": args.category,
            }
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(
                json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            logger.info(f"已写出 {args.output}")

        if result.error:
            logger.error(f"端到端失败: {result.error}")
            sys.exit(1)
        logger.info("端到端推理完成")
    except KeyboardInterrupt:
        logger.info("用户中断")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"端到端失败: {e}", exc_info=True)
        sys.exit(1)
    finally:
        try:
            if args.log_file:
                timing_path = Path(args.log_file).with_suffix(".timing.json")
                timing_path.write_text(
                    json.dumps(
                        {
                            "infer_total_ms": round(
                                (time.perf_counter() - t_infer) * 1000.0, 1
                            ),
                            **(result.timing_summary if result else {}),
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
        except Exception:
            pass
        logger.info("=" * 80)
        logger.info("端到端推理结束")
        logger.info("=" * 80)


if __name__ == "__main__":
    main()
