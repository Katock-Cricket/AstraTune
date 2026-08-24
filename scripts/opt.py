#!/usr/bin/env python3
"""单例优化 CLI：仅跑 Generate–Verify（需已有诊断结论）。"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from diagnosis.agent import extract_involved_tables
from optimization.graph import run_optimization
from utils.config_loader import normalize_dialect
from utils.logger import setup_logger
from utils.llm import init_llm_pool
from utils.pipeline import (
    fetch_schema_ddl,
    make_verify_fns,
    normalize_tables_for_dialect,
    prepare_sandbox,
    resolve_and_validate_tables,
    setup_executors,
)


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


def _issue_sql_list(sql: str) -> list[str]:
    parts = [s.strip() for s in sql.split(";") if s.strip()]
    return parts or ([sql.strip()] if sql.strip() else [])


def main():
    args = parse_arguments()
    logger = setup_logger(log_file=args.log_file)
    logger.info("=" * 80)
    logger.info("SQL 优化（Generate–Verify）启动")
    logger.info("=" * 80)
    init_llm_pool()

    target_ex = sandbox_ex = None
    manager = None
    try:
        dialect = normalize_dialect(args.dialect)
        diagnosis_report = _load_diagnosis(args)
        issue_sql = _issue_sql_list(args.sql)
        if not issue_sql:
            raise SystemExit("--sql 为空")

        logger.info(f"方言: {dialect} category={args.category}")
        target_ex, sandbox_ex, agent_cfg, opt_cfg, _t, sandbox_cfg = setup_executors(
            dialect=dialect, db_id=args.db_id
        )
        logger.info(
            f"目标库: {_t.get('host')}:{_t.get('port')}/{_t.get('database')}"
        )
        logger.info(
            f"沙箱库: {sandbox_cfg.get('host')}:{sandbox_cfg.get('port')}/"
            f"{sandbox_cfg.get('database')}"
        )

        if args.tables:
            tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        else:
            llm_cfg = agent_cfg.get("llm", {})
            tables = extract_involved_tables(
                args.sql, llm_cfg, user_prompt=args.user_prompt or ""
            )
        tables = normalize_tables_for_dialect(tables, dialect)
        tables = resolve_and_validate_tables(
            tables,
            target_ex,
            dialect=dialect,
            original_schema="",
            hint_text=f"{args.user_prompt or ''}\n{args.sql}",
        )
        if not tables:
            raise SystemExit("未能识别表名；请用 --tables 指定")
        logger.info(f"涉及表: {tables}")

        schema = fetch_schema_ddl(target_ex, tables)
        prepared = prepare_sandbox(
            target_ex,
            sandbox_ex,
            sandbox_cfg,
            tables,
            db_id=args.db_id or (_t.get("database") if isinstance(_t, dict) else None),
            dialect=dialect,
        )
        manager = prepared["manager"]
        sandbox_info = prepared["info"]
        from utils.sandbox_fidelity import format_fidelity_text, mark_preprocess_applied

        fidelity = sandbox_info.get("fidelity") or {}
        if args.preprocess_sql:
            fidelity = mark_preprocess_applied(fidelity, True)
        fidelity_text = format_fidelity_text(fidelity)

        smart = str(opt_cfg.get("verify_isolation", "smart_reset")).lower() != "full_reset"
        reset_fn, execute_fn, explain_fn = make_verify_fns(
            manager,
            tables,
            sandbox_ex,
            dialect,
            smart_reset=smart,
            initially_dirty=False,
        )

        opt = run_optimization(
            diagnosis_report=diagnosis_report,
            issue_sql=issue_sql,
            query=args.user_prompt or "",
            schema=schema,
            category=args.category,
            dialect=dialect,
            efficiency=True if str(args.category).lower() == "efficiency" else None,
            preprocess_sql=args.preprocess_sql,
            clean_up_sql=args.clean_up_sql,
            sandbox_fidelity=fidelity_text,
            sampled_tables=list(sandbox_info.get("sampled_tables") or []),
            opt_config=opt_cfg,
            reset_fn=reset_fn,
            execute_fn=execute_fn,
            explain_fn=explain_fn,
        )

        print("\n" + "=" * 80)
        print("优化结果 pred_sqls")
        print("=" * 80)
        for i, s in enumerate(opt.pred_sqls, 1):
            print(f"-- [{i}]\n{s}")
        print("=" * 80)
        print(
            f"success={opt.success} rounds={opt.rounds} "
            f"fallback={opt.fallback} failure_reasons={len(opt.failure_reasons)}"
        )
        if opt.failure_reasons:
            for r in opt.failure_reasons:
                print(f"  - {r}")
        print()

        if args.output:
            out = {
                "pred_sqls": opt.pred_sqls,
                "success": opt.success,
                "rounds": opt.rounds,
                "fallback": opt.fallback,
                "failure_reasons": opt.failure_reasons,
                "error": opt.error,
            }
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(
                json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            logger.info(f"已写出 {args.output}")

        logger.info("优化完成")
    except KeyboardInterrupt:
        logger.info("用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"优化失败: {e}", exc_info=True)
        sys.exit(1)
    finally:
        try:
            if manager is not None:
                manager.cleanup_sandbox()
        except Exception:
            pass
        try:
            if target_ex is not None:
                target_ex.close()
        except Exception:
            pass
        try:
            if sandbox_ex is not None:
                sandbox_ex.close()
        except Exception:
            pass
        logger.info("=" * 80)
        logger.info("SQL 优化结束")
        logger.info("=" * 80)


if __name__ == "__main__":
    main()
