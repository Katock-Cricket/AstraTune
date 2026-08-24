#!/usr/bin/env python3
"""单例优化 CLI：仅跑 Generate–Verify（需已有诊断结论）。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.config_loader import normalize_dialect
from utils.inference_runner import run_single_instance
from utils.logger import setup_logger
from utils.llm import init_llm_pool


def parse_arguments():
    p = argparse.ArgumentParser(
        description="SQL 优化（Generate–Verify）- 单例调试入口"
    )
    p.add_argument(
        "--sql",
        type=str,
        required=True,
        help="待优化的 issue SQL（多条用分号分隔）",
    )
    p.add_argument(
        "--diagnosis",
        type=str,
        default=None,
        help="诊断结论文本（与 --diagnosis-file 二选一，优先本参数）",
    )
    p.add_argument(
        "--diagnosis-file",
        type=str,
        default=None,
        help="诊断结论文件路径（UTF-8）",
    )
    p.add_argument(
        "--user-prompt",
        type=str,
        default=None,
        help="用户问题描述（对应 BIRD query）",
    )
    p.add_argument(
        "--category",
        type=str,
        default="Efficiency",
        help="题目类别：Query / Management / Personalization / Efficiency",
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
        "--output",
        type=str,
        default=None,
        help="可选：将 pred_sqls 等写入 JSON 文件",
    )
    return p.parse_args()


def _load_diagnosis(args) -> str:
    if args.diagnosis and args.diagnosis.strip():
        return args.diagnosis.strip()
    if args.diagnosis_file:
        path = Path(args.diagnosis_file)
        if not path.is_file():
            raise SystemExit(f"诊断文件不存在: {path}")
        return path.read_text(encoding="utf-8").strip()
    raise SystemExit("请提供 --diagnosis 或 --diagnosis-file")


def main():
    args = parse_arguments()
    logger = setup_logger(log_file=args.log_file)
    logger.info("=" * 80)
    logger.info("SQL 优化（Generate–Verify）启动")
    logger.info("=" * 80)
    init_llm_pool()

    try:
        dialect = normalize_dialect(args.dialect)
        diagnosis_report = _load_diagnosis(args)
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
            efficiency=True if str(args.category).lower() == "efficiency" else None,
            mode="opt_only",
            prior_diagnosis=diagnosis_report,
            tables_override=tables_override,
            instance_id="opt",
            logger=logger,
        )
        logger.info(f"涉及表: {result.tables}")

        print("\n" + "=" * 80)
        print("优化结果 pred_sqls")
        print("=" * 80)
        for i, s in enumerate(result.pred_sqls, 1):
            print(f"-- [{i}]\n{s}")
        print("=" * 80)
        print(
            f"success={result.meta.get('success')} "
            f"rounds={result.meta.get('rounds')} "
            f"fallback={result.meta.get('fallback')} "
            f"failure_reasons={len(result.meta.get('failure_reasons') or [])}"
        )
        if result.meta.get("failure_reasons"):
            for r in result.meta["failure_reasons"]:
                print(f"  - {r}")
        print()

        if args.output:
            out = {
                "pred_sqls": result.pred_sqls,
                "success": result.meta.get("success"),
                "rounds": result.meta.get("rounds"),
                "fallback": result.meta.get("fallback"),
                "failure_reasons": result.meta.get("failure_reasons") or [],
                "error": result.error,
            }
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(
                json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            logger.info(f"已写出 {args.output}")

        if result.error:
            logger.error(f"优化失败: {result.error}")
            sys.exit(1)
        logger.info("优化完成")
    except KeyboardInterrupt:
        logger.info("用户中断")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"优化失败: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("=" * 80)
        logger.info("SQL 优化结束")
        logger.info("=" * 80)


if __name__ == "__main__":
    main()
