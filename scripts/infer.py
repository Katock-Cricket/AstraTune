#!/usr/bin/env python3
"""单例端到端推理 CLI：诊断 → Generate–Verify → pred_sqls。"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from diagnosis.agent import DiagnosisAgent, extract_involved_tables
from optimization.graph import run_optimization
from tools.sandbox_tool import SandboxTool
from utils.config_loader import normalize_dialect
from utils.logger import setup_logger
from utils.llm import init_llm_pool
from utils.stream_handler import StreamHandler
from utils.timing import clear_timing_context, log_timing, log_timing_summary, set_timing_context
import time
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


def _issue_sql_list(sql: str) -> list[str]:
    parts = [s.strip() for s in sql.split(";") if s.strip()]
    return parts or ([sql.strip()] if sql.strip() else [])


def main():
    args = parse_arguments()
    logger = setup_logger(log_file=args.log_file)
    logger.info("=" * 80)
    logger.info("端到端推理启动（诊断 → Generate–Verify）")
    logger.info("=" * 80)
    init_llm_pool()

    target_ex = sandbox_ex = None
    manager = None
    t_infer = time.perf_counter()
    try:
        dialect = normalize_dialect(args.dialect)
        set_timing_context(
            instance_id="infer",
            db_id=args.db_id or "-",
            dialect=dialect,
            worker=0,
            slot=None,
        )
        issue_sql = _issue_sql_list(args.sql)
        if not issue_sql:
            raise SystemExit("--sql 为空")

        target_ex, sandbox_ex, agent_cfg, opt_cfg, target_cfg, sandbox_cfg = setup_executors(
            dialect=dialect, db_id=args.db_id
        )
        logger.info(f"方言: {dialect} category={args.category}")
        logger.info(
            f"目标库: {target_cfg.get('host')}:{target_cfg.get('port')}/"
            f"{target_cfg.get('database')}"
        )
        logger.info(
            f"沙箱库: {sandbox_cfg.get('host')}:{sandbox_cfg.get('port')}/"
            f"{sandbox_cfg.get('database')}"
        )
        set_timing_context(
            db_id=args.db_id or target_cfg.get("database") or "-",
            slot=sandbox_cfg.get("database"),
        )

        if args.tables:
            tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        else:
            tables = extract_involved_tables(
                args.sql,
                agent_cfg.get("llm", {}),
                user_prompt=args.user_prompt or "",
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
            db_id=args.db_id or target_cfg.get("database"),
            dialect=dialect,
        )
        manager = prepared["manager"]
        sandbox_info = prepared["info"]
        from utils.sandbox_fidelity import format_fidelity_text, mark_preprocess_applied

        fidelity = sandbox_info.get("fidelity") or {}
        if args.preprocess_sql:
            fidelity = mark_preprocess_applied(fidelity, True)
            sandbox_info["fidelity"] = fidelity
        fidelity_text = format_fidelity_text(fidelity)

        diagnosis_report = ""
        if args.skip_diagnosis:
            diagnosis_report = (
                f"跳过诊断。用户问题: {args.user_prompt or '(无)'}"
            )
            logger.info("已跳过诊断阶段")
        else:
            logger.info("阶段 A: 诊断")
            tool = SandboxTool(sandbox_ex)
            tool.bind_sandbox_reset(manager, tables)
            if args.preprocess_sql:
                tool.register_preprocess_sql(args.preprocess_sql)
                tool.apply_preprocess_once()
            if args.clean_up_sql:
                tool.register_clean_up_sql(args.clean_up_sql)
            agent = DiagnosisAgent(agent_cfg, tool)
            if args.stream:
                stream_handler = StreamHandler(mode="rich")
                diagnosis_report = asyncio.run(
                    agent.diagnose_stream(
                        ori_sql=args.sql,
                        schema=schema,
                        exec_log=args.log,
                        sampled_tables=sandbox_info.get("sampled_tables"),
                        preprocess_sql=args.preprocess_sql,
                        clean_up_sql=args.clean_up_sql,
                        user_prompt=args.user_prompt,
                        sandbox_fidelity=fidelity_text,
                        stream_handler=stream_handler,
                    )
                )
            else:
                diagnosis_report = agent.diagnose(
                    ori_sql=args.sql,
                    schema=schema,
                    exec_log=args.log,
                    sampled_tables=sandbox_info.get("sampled_tables"),
                    preprocess_sql=args.preprocess_sql,
                    clean_up_sql=args.clean_up_sql,
                    user_prompt=args.user_prompt,
                    sandbox_fidelity=fidelity_text,
                )
            logger.info("诊断完成")

        logger.info("阶段 B: Generate–Verify")
        smart = str(opt_cfg.get("verify_isolation", "smart_reset")).lower() != "full_reset"
        reset_fn, execute_fn, explain_fn = make_verify_fns(
            manager,
            tables,
            sandbox_ex,
            dialect,
            smart_reset=smart,
            initially_dirty=True,
        )
        opt = run_optimization(
            diagnosis_report=diagnosis_report or "",
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
        print("端到端结果 pred_sqls")
        print("=" * 80)
        for i, s in enumerate(opt.pred_sqls, 1):
            print(f"-- [{i}]\n{s}")
        print("=" * 80)
        print(
            f"success={opt.success} rounds={opt.rounds} "
            f"fallback={opt.fallback}"
        )
        if opt.failure_reasons:
            print("failure_reasons:")
            for r in opt.failure_reasons:
                print(f"  - {r}")
        print()

        if args.output:
            out = {
                "pred_sqls": opt.pred_sqls,
                "diagnosis_report": diagnosis_report,
                "success": opt.success,
                "rounds": opt.rounds,
                "fallback": opt.fallback,
                "failure_reasons": opt.failure_reasons,
                "error": opt.error,
                "db_id": args.db_id or target_cfg.get("database"),
                "dialect": dialect,
                "category": args.category,
            }
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(
                json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            logger.info(f"已写出 {args.output}")

        logger.info("端到端推理完成")
    except KeyboardInterrupt:
        logger.info("用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"端到端失败: {e}", exc_info=True)
        sys.exit(1)
    finally:
        try:
            log_timing("infer_total", (time.perf_counter() - t_infer) * 1000.0)
            summary = log_timing_summary()
            if args.log_file:
                timing_path = Path(args.log_file).with_suffix(".timing.json")
                timing_path.write_text(
                    json.dumps(summary, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
        except Exception:
            pass
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
        logger.info("端到端推理结束")
        logger.info("=" * 80)
        clear_timing_context()


if __name__ == "__main__":
    main()
