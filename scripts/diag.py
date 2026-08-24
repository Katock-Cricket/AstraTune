#!/usr/bin/env python3
"""单例诊断 CLI：ReAct Agent 在沙箱上定位 SQL 问题（统一推理链路，mode=diag_only）。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.config_loader import normalize_dialect
from utils.inference_runner import run_single_instance
from utils.logger import setup_logger
from utils.llm import init_llm_pool


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="SQL诊断系统 - 基于LLM Agent的数据库性能诊断工具"
    )

    parser.add_argument(
        "--sql",
        type=str,
        required=True,
        help="需要诊断的SQL语句, 多条sql用分号分隔"
    )

    parser.add_argument(
        "--log",
        type=str,
        default=None,
        help="执行日志（包含平均执行时间、执行次数等统计信息）"
    )

    parser.add_argument(
        "--preprocess-sql",
        type=str,
        default=None,
        help="测试本条sql的前置sql，多条sql用分号分隔"
    )

    parser.add_argument(
        "--clean-up-sql",
        type=str,
        default=None,
        help="测试本条sql的清理sql，用于恢复测试环境，多条sql用分号分隔"
    )

    parser.add_argument(
        "--user-prompt",
        type=str,
        default=None,
        help="附加用户提示"
    )

    parser.add_argument(
        "--db-id",
        type=str,
        default=None,
        help="目标数据库名（覆盖 config/target_db.json 中的 database，如 financial）"
    )

    parser.add_argument(
        "--dialect",
        type=str,
        default="postgresql",
        choices=["postgresql", "mysql", "pg", "postgres"],
        help="数据库方言（默认 postgresql）"
    )

    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="日志文件路径（可选，默认只输出到控制台）"
    )

    parser.add_argument(
        "--stream",
        action="store_true",
        default=True,
        help="启用流式输出，实时显示Agent推理过程"
    )

    return parser.parse_args()


def main():
    """主函数"""
    args = parse_arguments()

    logger = setup_logger(log_file=args.log_file)
    logger.info("=" * 80)
    logger.info("SQL诊断系统启动")
    logger.info("=" * 80)
    init_llm_pool()

    try:
        logger.info("步骤 1/2: 加载配置并运行统一推理链路（diag_only）")
        dialect = normalize_dialect(args.dialect)
        logger.info(f"方言: {dialect}")

        result = run_single_instance(
            sql=args.sql,
            query=args.user_prompt or "",
            category="Query",
            dialect=dialect,
            db_id=args.db_id,
            preprocess_sql=args.preprocess_sql,
            clean_up_sql=args.clean_up_sql,
            exec_log=args.log,
            mode="diag_only",
            stream=bool(args.stream),
            instance_id="diag",
            logger=logger,
        )

        logger.info(f"识别到的表: {result.tables}")

        if result.error:
            logger.error(f"诊断执行失败: {result.error}")
            sys.exit(1)

        logger.info("步骤 2/2: 输出诊断结论")
        logger.info("-" * 80)
        logger.info("诊断完成！")
        logger.info("=" * 80)

        # 流式模式下结论已通过 Rich 实时显示，非流式模式在此打印
        if not args.stream:
            print("\n" + "=" * 80)
            print("诊断结果")
            print("=" * 80)
            print(result.diagnosis_report)
            print("=" * 80 + "\n")

        logger.info("=" * 80)
        logger.info("SQL诊断系统结束")
        logger.info("=" * 80)

    except KeyboardInterrupt:
        logger.info("\n用户中断程序")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"程序执行出错: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
