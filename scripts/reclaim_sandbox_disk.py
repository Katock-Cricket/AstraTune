#!/usr/bin/env python3
"""回收 AstraTune PG/MySQL 沙箱磁盘：清理槽位库、CHECKPOINT、可选 DROP 空槽。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sandbox.mysql_executor import MySQLExecutor
from sandbox.pg_executor import PGExecutor
from utils.config_loader import load_all_configs, normalize_dialect
from utils.logger import setup_logger


def parse_args():
    p = argparse.ArgumentParser(description="Reclaim sandbox disk space")
    p.add_argument("--dialect", default="postgresql", choices=["postgresql", "mysql", "pg"])
    p.add_argument(
        "--drop-slots",
        action="store_true",
        help="DROP astratune_sandbox_N 槽位库（保留主库 astratune_sandbox）",
    )
    p.add_argument(
        "--keep-slots",
        type=int,
        default=0,
        help="与 --drop-slots 合用：保留 slot 0..N-1，其余删除",
    )
    return p.parse_args()


def reclaim_pg(*, drop_slots: bool, keep_slots: int, logger) -> None:
    cfgs = load_all_configs()
    sandbox = dict(cfgs["sandbox"]["postgresql"])
    admin_cfg = dict(sandbox)
    admin_cfg["database"] = "postgres"
    admin = PGExecutor(admin_cfg)
    admin.connect()
    try:
        rows = admin.execute(
            "SELECT datname, pg_size_pretty(pg_database_size(datname)) AS sz, "
            "pg_database_size(datname) AS bytes FROM pg_database "
            "WHERE datname LIKE 'astratune_sandbox%' ORDER BY 3 DESC",
            fetch=True,
        )
        dbs = (rows or [{}])[0].get("rows") or []
        logger.info(f"沙箱库现状: {dbs}")

        for row in dbs:
            name = row["datname"]
            if name == "astratune_sandbox":
                continue
            if not drop_slots:
                continue
            # astratune_sandbox_N
            suffix = name.replace("astratune_sandbox_", "", 1)
            if suffix.isdigit() and int(suffix) < keep_slots:
                logger.info(f"保留槽位库: {name}")
                continue
            logger.info(f"DROP 槽位库: {name}")
            admin.execute_autocommit(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname = '{name.replace(chr(39), chr(39)+chr(39))}' AND pid <> pg_backend_pid()"
            )
            admin.execute_autocommit(f'DROP DATABASE IF EXISTS "{name}"')

        admin.execute_autocommit("CHECKPOINT")
        wal = admin.execute(
            "SELECT pg_size_pretty(sum(size)) AS wal FROM pg_ls_waldir()",
            fetch=True,
        )
        logger.info(f"CHECKPOINT 完成; WAL={wal}")
    finally:
        admin.close()


def reclaim_mysql(*, drop_slots: bool, keep_slots: int, logger) -> None:
    cfgs = load_all_configs()
    sandbox = dict(cfgs["sandbox"]["mysql"])
    admin_cfg = dict(sandbox)
    admin_cfg["database"] = "mysql"
    admin = MySQLExecutor(admin_cfg)
    admin.connect()
    try:
        rows = admin.execute("SHOW DATABASES", fetch=True)
        names = [
            list(r.values())[0]
            for r in ((rows or [{}])[0].get("rows") or [])
        ]
        slot_dbs = [n for n in names if str(n).startswith("astratune_sandbox")]
        logger.info(f"沙箱库现状: {slot_dbs}")
        if drop_slots:
            for name in slot_dbs:
                if name == "astratune_sandbox":
                    continue
                suffix = name.replace("astratune_sandbox_", "", 1)
                if suffix.isdigit() and int(suffix) < keep_slots:
                    continue
                logger.info(f"DROP 槽位库: {name}")
                admin.execute_autocommit(f"DROP DATABASE IF EXISTS `{name}`")
    finally:
        admin.close()


def main():
    args = parse_args()
    logger = setup_logger()
    dialect = normalize_dialect(args.dialect)
    if dialect == "postgresql":
        reclaim_pg(drop_slots=args.drop_slots, keep_slots=args.keep_slots, logger=logger)
    else:
        reclaim_mysql(drop_slots=args.drop_slots, keep_slots=args.keep_slots, logger=logger)


if __name__ == "__main__":
    main()
