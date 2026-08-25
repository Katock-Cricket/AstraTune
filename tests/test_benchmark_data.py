from pathlib import Path

from utils.benchmark_data import (
    BENCHMARK_DATA,
    DUMP_MYSQL,
    DUMP_PG,
    FLASH_INPUT,
    FLASH_SCHEMA,
    FLASH_SOL,
    OPEN_SOL,
    PG_SOL,
    POST_INPUT,
    POST_SCHEMA,
    ROOT,
    mysql_dump_search_dirs,
)


def test_benchmark_data_layout_under_repo_root():
    assert BENCHMARK_DATA == ROOT / "benchmark_data"
    assert FLASH_SCHEMA == ROOT / "benchmark_data" / "schema" / "flash_schema.jsonl"
    assert POST_SCHEMA.name == "post_schema.jsonl"
    assert FLASH_INPUT.as_posix().endswith("input/flash/flash-00000-of-00001.jsonl")
    assert POST_INPUT.as_posix().endswith("input/post/pg-00000-of-00001.jsonl")
    assert FLASH_SOL.as_posix().endswith("sol/flash_sol/bird-critic-1.0-flash_w_sol.jsonl")
    assert PG_SOL.as_posix().endswith("sol/pg_sol/pg_sol.jsonl")
    assert OPEN_SOL.as_posix().endswith("sol/open_sol/open_sol.jsonl")
    assert DUMP_PG == ROOT / "benchmark_data" / "dumps" / "postgresql"
    assert DUMP_MYSQL == ROOT / "benchmark_data" / "dumps" / "mysql"


def test_paths_do_not_use_legacy_locations():
    blob = " ".join(
        p.as_posix()
        for p in (
            FLASH_SCHEMA,
            FLASH_INPUT,
            FLASH_SOL,
            PG_SOL,
            DUMP_PG,
            DUMP_MYSQL,
        )
    )
    assert "benchmark/baseline/data" not in blob
    assert "benchmark/evaluation/pg_sol" not in blob
    assert "benchmark/evaluation/flash_sol" not in blob
    assert "postgre_table_dumps" not in blob
    assert "evaluation/mysql_table_dumps" not in blob


def test_mysql_dump_env_overrides_first():
    dirs = mysql_dump_search_dirs(env={"BIRD_MYSQL_DUMP_DIR": "D:/custom_mysql_dumps"})
    assert dirs[0] == "D:/custom_mysql_dumps"


def test_mysql_dump_search_includes_benchmark_data_and_legacy():
    dirs = mysql_dump_search_dirs()
    assert any(Path(d).as_posix().endswith("benchmark_data/dumps/mysql") for d in dirs if d)
    assert any(d.replace("\\", "/").endswith("evaluation/mysql_table_dumps") for d in dirs)
    assert "/app/mysql_table_dumps" in dirs
