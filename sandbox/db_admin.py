"""沙箱域底层库管理：执行器创建、沙箱库命名与建库。

自 utils/pipeline.py 下沉，供 pipeline / template_cache / sandbox_manager
单向依赖，打破原先的函数级循环 import。
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from sandbox.mysql_executor import MySQLExecutor
from sandbox.pg_executor import PGExecutor
from utils.config_loader import normalize_dialect
from utils.logger import default_logger
from utils.timing import timed_stage


def create_executor(config: Dict[str, Any]):
    """按配置方言创建目标/沙箱执行器（未 connect）。"""
    dialect = normalize_dialect(config.get("dialect", "postgresql"))
    if dialect == "mysql":
        return MySQLExecutor(config)
    return PGExecutor(config)


def sandbox_db_name_for_slot(
    *,
    dialect: str,
    slot_index: Optional[int],
    workers: int,
    batch_cfg: Optional[Dict[str, Any]] = None,
    base_database: Optional[str] = None,
) -> str:
    """workers==1 或 slot_index is None：沿用配置中的单库名（P0 兼容）；否则 prefix + slot_index。"""
    batch_cfg = batch_cfg or {}
    base = base_database or "astratune_sandbox"
    if workers <= 1 or slot_index is None:
        return base
    prefix = batch_cfg.get("sandbox_db_prefix") or "astratune_sandbox_"
    return f"{prefix}{int(slot_index)}"


def ensure_sandbox_database(sandbox_cfg: Dict[str, Any], database: str) -> None:
    """在沙箱实例上确保 database 存在（PG CREATE DATABASE / MySQL CREATE DATABASE）。"""
    with timed_stage("sandbox_slot_ensure", database=database):
        dialect = normalize_dialect(sandbox_cfg.get("dialect", "postgresql"))
        admin_cfg = dict(sandbox_cfg)
        if dialect == "postgresql":
            admin_cfg["database"] = "postgres"
        else:
            admin_cfg["database"] = "mysql"

        admin = create_executor(admin_cfg)
        try:
            admin.connect()
            if dialect == "postgresql":
                lit = database.replace("'", "''")
                ident = database.replace('"', '""')
                exists = admin.execute(
                    f"SELECT 1 AS ok FROM pg_database WHERE datname = '{lit}'",
                    fetch=True,
                )
                found = bool(exists and exists[0].get("rows"))
                if not found:
                    admin.execute_autocommit(f'CREATE DATABASE "{ident}"')
                    default_logger.info(f"已创建 PG 沙箱库: {database}")
            else:
                lit = database.replace("`", "``")
                admin.execute_autocommit(
                    f"CREATE DATABASE IF NOT EXISTS `{lit}` CHARACTER SET utf8mb4"
                )
                default_logger.info(f"已确保 MySQL 沙箱库: {database}")
        finally:
            try:
                admin.close()
            except Exception:
                pass
