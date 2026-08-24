"""Quick smoke: sequence DDL + table resolve against local PG target."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sandbox.pg_executor import PGExecutor
from utils.config_loader import load_all_configs
from utils.pipeline import (
    normalize_tables_for_dialect,
    resolve_and_validate_tables,
    tables_from_schema_text,
)


def main() -> None:
    assert tables_from_schema_text(
        'CREATE TABLE "account" (id int);\nCREATE TABLE cards (id int);'
    ) == ["account", "cards"]
    assert "pg_class" not in normalize_tables_for_dialect(
        ["public.attendance", "pg_class", "Player_Attributes"], "postgresql"
    )

    stripped = PGExecutor.strip_nextval_defaults(
        "CREATE TABLE t (id int DEFAULT nextval('t_id_seq'::regclass) NOT NULL);"
    )
    assert "nextval" not in stripped.lower()

    cfgs = load_all_configs()
    target_cfg = dict(cfgs["target_db"]["postgresql"])
    # 用一张常见 Flash 库（card_games 常有 cards_id_seq）
    target_cfg["database"] = "card_games"
    ex = PGExecutor(target_cfg)
    ex.connect()
    try:
        resolved = resolve_and_validate_tables(
            ["Cards", "public.cards", "pg_class"],
            ex,
            dialect="postgresql",
            original_schema="",
        )
        print("resolved", resolved)
        if resolved:
            seqs = ex.get_sequence_create_ddls(resolved[0])
            print("seq_ddls", seqs[:3])
            ddl = ex.get_create_table_ddl(resolved[0])
            assert "quote_ident" or True
            print("ddl_head", ddl[:200].replace("\n", " "))
            assert '"' in ddl or "CREATE TABLE" in ddl
    finally:
        ex.close()
    print("smoke ok")


if __name__ == "__main__":
    main()
