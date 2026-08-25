"""AstraTune 仓库内 BIRD-CRITIC / Effi-SQL 评测数据路径（schema / 题面 / sol / dumps）。"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BENCHMARK_DATA = ROOT / "benchmark_data"

SCHEMA_DIR = BENCHMARK_DATA / "schema"
INPUT_DIR = BENCHMARK_DATA / "input"
SOL_DIR = BENCHMARK_DATA / "sol"
DUMP_PG = BENCHMARK_DATA / "dumps" / "postgresql"
DUMP_MYSQL = BENCHMARK_DATA / "dumps" / "mysql"

FLASH_SCHEMA = SCHEMA_DIR / "flash_schema.jsonl"
POST_SCHEMA = SCHEMA_DIR / "post_schema.jsonl"
OPEN_SCHEMA = SCHEMA_DIR / "open_schema.jsonl"

FLASH_INPUT = INPUT_DIR / "flash" / "flash-00000-of-00001.jsonl"
POST_INPUT = INPUT_DIR / "post" / "pg-00000-of-00001.jsonl"

FLASH_SOL = SOL_DIR / "flash_sol" / "bird-critic-1.0-flash_w_sol.jsonl"
PG_SOL = SOL_DIR / "pg_sol" / "pg_sol.jsonl"
OPEN_SOL = SOL_DIR / "open_sol" / "open_sol.jsonl"

# Effi-SQL（Diff-SQL 评测栈；dumps 不并进 AstraTune docker/）
EFFI_DIR = BENCHMARK_DATA / "effi"
EFFI_INPUT_DIR = EFFI_DIR / "input"
EFFI_INPUT = EFFI_INPUT_DIR / "effi-sql-pg.jsonl"
EFFI_INPUT_WITH_DIFFICULTY = EFFI_INPUT_DIR / "effi-sql-pg-with-difficulty.jsonl"
BIRD_INTERACT_DUMPS = BENCHMARK_DATA / "dumps" / "bird-interact"
TPCH_RAW = BENCHMARK_DATA / "tpch-3g" / "raw"
DIFF_SQL_ROOT = ROOT / "Diff-SQL"


def mysql_dump_search_dirs(
    src_file: str | None = None,
    env: dict | None = None,
) -> list[str]:
    """MySQL dump 查找顺序：环境变量 → benchmark_data → 官方 evaluation 旧路径 → 容器路径。"""
    import os

    environ = env if env is not None else os.environ
    here = Path(src_file).resolve().parent if src_file else (ROOT / "benchmark" / "evaluation" / "src")
    dirs: list[str] = []
    env_dir = (environ.get("BIRD_MYSQL_DUMP_DIR") or "").strip()
    if env_dir:
        dirs.append(env_dir)
    dirs.extend(
        [
            str(DUMP_MYSQL),
            str(here.parent / "mysql_table_dumps"),
            "/app/mysql_table_dumps",
            "/docker-entrypoint-initdb.d/mysql_table_dumps",
        ]
    )
    return dirs
