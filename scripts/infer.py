#!/usr/bin/env python3
"""唯一单条推理 CLI：function × assembly → pred_sqls。

支持 BIRD（--sql）与 Effi-SQL（--benchmark effi --id N）。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.benchmark_data import EFFI_INPUT, POST_INPUT, POST_SCHEMA
from utils.config_loader import normalize_dialect
from utils.effi_adapter import attach_prediction, load_effi_by_id, pred_sqls_to_prediction
from utils.inference_runner import VALID_FUNCTIONS, VALID_MODES, run_single_instance
from utils.logger import setup_logger
from utils.llm import init_llm_pool
from utils.pipeline import instance_issue_sql, sql_list_to_text


def parse_arguments():
    p = argparse.ArgumentParser(
        description="AstraTune 单条推理（diag+opt / diag_only / opt_only / llm_only）"
    )
    p.add_argument(
        "--mode",
        type=str,
        default="diag+opt",
        choices=list(VALID_MODES),
        help="装配模式（四种均产出 pred_sqls）",
    )
    p.add_argument(
        "--function",
        type=str,
        default=None,
        choices=list(VALID_FUNCTIONS),
        help="职能：general / efficiency（默认由 category / Effi 推断）",
    )
    p.add_argument(
        "--benchmark",
        type=str,
        default="bird",
        choices=["bird", "effi"],
        help="题源：bird（默认）或 effi",
    )
    p.add_argument(
        "--id",
        type=str,
        default=None,
        help="Effi: 数字 id；BIRD: instance_id 如 PostgreSQL_0（配合 JSONL）",
    )
    p.add_argument(
        "--effi-input",
        type=str,
        default=str(EFFI_INPUT),
        help="Effi-SQL JSONL 路径",
    )
    p.add_argument("--sql", type=str, default=None, help="待处理 SQL（BIRD 手工输入）")
    p.add_argument("--log", type=str, default=None, help="执行日志")
    p.add_argument("--preprocess-sql", type=str, default=None)
    p.add_argument("--clean-up-sql", type=str, default=None)
    p.add_argument("--user-prompt", type=str, default=None)
    p.add_argument(
        "--category",
        type=str,
        default="Query",
        help="Query / Management / Personalization / Efficiency",
    )
    p.add_argument("--db-id", type=str, default=None)
    p.add_argument(
        "--dialect",
        type=str,
        default="postgresql",
        choices=["postgresql", "mysql", "pg", "postgres"],
    )
    p.add_argument("--tables", type=str, default=None, help="逗号分隔表名")
    p.add_argument("--log-file", type=str, default=None)
    p.add_argument(
        "--stream",
        action="store_true",
        default=True,
        help="诊断阶段流式输出（默认开启）",
    )
    p.add_argument(
        "--no-stream",
        action="store_true",
        help="关闭诊断流式输出",
    )
    p.add_argument(
        "--skip-diagnosis",
        action="store_true",
        help="兼容旧参数：等价于 --mode opt_only",
    )
    p.add_argument(
        "--diagnosis",
        type=str,
        default=None,
        help="opt_only 可选：显式诊断文本（续跑）",
    )
    p.add_argument("--output", type=str, default=None, help="写出 JSON 结果")
    return p.parse_args()


def _load_bird_instance(instance_id: str) -> dict:
    """从 PG530 schema⊕input 合并一行（不含 sol）。"""
    schema_by: dict = {}
    with Path(POST_SCHEMA).open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            iid = str(row.get("instance_id") or "")
            if iid:
                schema_by[iid] = row
    with Path(POST_INPUT).open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            if str(row.get("instance_id") or "") != instance_id:
                continue
            sch = schema_by.get(instance_id) or {}
            merged = {**sch, **row}
            merged.pop("sol_sql", None)
            merged.pop("test_cases", None)
            return merged
    raise SystemExit(f"未找到 BIRD instance_id={instance_id}")


def main():
    args = parse_arguments()
    logger = setup_logger(log_file=args.log_file)
    logger.info("=" * 80)
    logger.info("单条推理启动")
    logger.info("=" * 80)
    init_llm_pool()

    mode = "opt_only" if args.skip_diagnosis else args.mode
    stream = bool(args.stream) and not bool(args.no_stream)
    t_infer = time.perf_counter()
    result = None
    out_record: dict = {}

    try:
        dialect = normalize_dialect(args.dialect)
        tables_override = None
        if args.tables:
            tables_override = [t.strip() for t in args.tables.split(",") if t.strip()]

        if args.benchmark == "effi":
            if args.id is None:
                raise SystemExit("Effi 模式需要 --id <数字>")
            adapted = load_effi_by_id(args.effi_input, args.id)
            fn = args.function or "efficiency"
            logger.info(
                f"Effi id={args.id} db={adapted['db_id']} mode={mode} function={fn}"
            )
            result = run_single_instance(
                sql=adapted["issue_sql"],
                query=adapted["query"],
                category=adapted["category"],
                dialect="postgresql",
                db_id=adapted["db_id"],
                efficiency=True,
                mode=mode,
                function=fn,
                prior_diagnosis=args.diagnosis,
                stream=stream,
                tables_override=tables_override,
                instance_id=adapted["instance_id"],
                logger=logger,
                benchmark="effi",
                direct_target=True,
            )
            # 评测侧需要原题字段 + prediction
            raw_line = None
            with Path(args.effi_input).open(encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    if int(row.get("id", -1)) == int(args.id):
                        raw_line = row
                        break
            out_record = attach_prediction(raw_line or adapted, result.pred_sqls)
            out_record["instance_id"] = adapted["instance_id"]
            out_record["diagnosis_report"] = result.diagnosis_report
            out_record["optimization_meta"] = result.meta
            if result.error:
                out_record["error"] = result.error
        else:
            if args.id and not args.sql:
                rec = _load_bird_instance(str(args.id))
                sql = instance_issue_sql(rec)
                query = rec.get("query") or args.user_prompt or ""
                category = rec.get("category") or args.category
                db_id = args.db_id or rec.get("db_id")
                original_schema = (
                    rec.get("original_schema") or rec.get("preprocess_schema") or ""
                )
                preprocess = sql_list_to_text(rec.get("preprocess_sql")) or None
                cleanup = sql_list_to_text(rec.get("clean_up_sql")) or None
                efficiency = rec.get("efficiency")
                instance_id = str(rec.get("instance_id") or args.id)
            else:
                if not (args.sql or "").strip():
                    raise SystemExit("BIRD 模式需要 --sql 或 --id PostgreSQL_N")
                sql = args.sql
                query = args.user_prompt or ""
                category = args.category
                db_id = args.db_id
                original_schema = ""
                preprocess = args.preprocess_sql
                cleanup = args.clean_up_sql
                efficiency = (
                    True if str(category).lower() == "efficiency" else None
                )
                instance_id = "infer"

            fn = args.function
            logger.info(f"方言={dialect} category={category} mode={mode} function={fn}")
            result = run_single_instance(
                sql=sql,
                query=query,
                category=category,
                dialect=dialect,
                db_id=db_id,
                preprocess_sql=preprocess,
                clean_up_sql=cleanup,
                exec_log=args.log,
                original_schema=original_schema,
                efficiency=efficiency,
                mode=mode,
                function=fn,
                prior_diagnosis=args.diagnosis,
                stream=stream,
                tables_override=tables_override,
                instance_id=instance_id,
                logger=logger,
                benchmark="bird",
            )
            out_record = {
                "instance_id": instance_id,
                "pred_sqls": result.pred_sqls,
                "prediction": pred_sqls_to_prediction(result.pred_sqls),
                "diagnosis_report": result.diagnosis_report,
                "optimization_meta": result.meta,
                "error": result.error,
                "db_id": db_id,
                "dialect": dialect,
                "category": category,
                "mode": mode,
                "function": result.meta.get("function"),
            }

        logger.info(f"涉及表: {result.tables}")
        print("\n" + "=" * 80)
        print("pred_sqls")
        print("=" * 80)
        for i, s in enumerate(result.pred_sqls, 1):
            print(f"-- [{i}]\n{s}")
        print("=" * 80)
        print(
            f"success={result.meta.get('success')} "
            f"rounds={result.meta.get('rounds')} "
            f"fallback={result.meta.get('fallback')} "
            f"mode={result.meta.get('mode')} "
            f"function={result.meta.get('function')}"
        )
        if result.meta.get("failure_reasons"):
            print("failure_reasons:")
            for r in result.meta["failure_reasons"]:
                print(f"  - {r}")
        print()

        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(
                json.dumps(out_record, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info(f"已写出 {args.output}")

        if result.error and not result.pred_sqls:
            logger.error(f"推理失败: {result.error}")
            sys.exit(1)
        logger.info("推理完成")
    except KeyboardInterrupt:
        logger.info("用户中断")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"推理失败: {e}", exc_info=True)
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
        logger.info("推理结束")
        logger.info("=" * 80)


if __name__ == "__main__":
    main()
