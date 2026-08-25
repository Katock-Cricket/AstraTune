import json
from pathlib import Path
from typing import Any, Dict, Optional

from utils.logger import default_logger

_CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"

_DIALECT_ALIASES = {
    "pg": "postgresql",
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "mysql": "mysql",
}


def normalize_dialect(dialect: Optional[str]) -> str:
    if not dialect:
        return "postgresql"
    key = str(dialect).strip().lower()
    if key not in _DIALECT_ALIASES:
        raise ValueError(f"不支持的方言: {dialect}")
    return _DIALECT_ALIASES[key]


def _load_json_config(filename: str) -> Dict[str, Any]:
    path = _CONFIG_DIR / filename
    if not path.is_file():
        default_logger.warning(f"配置文件不存在: {path}")
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def load_all_configs() -> Dict[str, Dict[str, Any]]:
    configs: Dict[str, Dict[str, Any]] = {}
    configs["agents"] = _load_json_config("agents.json")
    configs["sandbox"] = _load_json_config("sandbox.json")
    configs["target_db"] = _load_json_config("target_db.json")
    configs["effi_db"] = _load_json_config("effi_db.json")
    return configs


def resolve_effi_target_config(
    configs: Dict[str, Dict[str, Any]],
    db_id: str,
) -> Dict[str, Any]:
    """按 Effi-SQL db 名解析 Diff-SQL 评测库连接（7001 小库 / 5438 TPC-H）。"""
    effi = configs.get("effi_db") or {}
    key = str(db_id or "").strip().lower()
    profile_name = (effi.get("databases") or {}).get(key)
    if not profile_name:
        raise ValueError(f"未知 Effi db_id={db_id!r}；见 config/effi_db.json databases")
    profile = dict(effi.get(profile_name) or {})
    if not profile:
        raise ValueError(f"Effi profile {profile_name!r} 缺失")
    out = dict(profile)
    out.setdefault("dialect", "postgresql")
    if profile_name == "small":
        out["database"] = key
    elif not out.get("database"):
        out["database"] = "tpch_3g"
    return out


def is_effi_db_id(configs: Dict[str, Dict[str, Any]], db_id: Optional[str]) -> bool:
    if not db_id:
        return False
    databases = (configs.get("effi_db") or {}).get("databases") or {}
    return str(db_id).strip().lower() in databases


def _pick_dialect_section(cfg: Dict[str, Any], dialect: str) -> Dict[str, Any]:
    """从按方言分段的配置中选取；兼容旧扁平结构。"""
    if not cfg:
        return {}
    dialect = normalize_dialect(dialect)
    if dialect in cfg and isinstance(cfg[dialect], dict):
        section = dict(cfg[dialect])
        section.setdefault("dialect", dialect)
        return section
    # 旧格式：顶层即连接字段
    if "host" in cfg or "port" in cfg:
        out = dict(cfg)
        out.setdefault("dialect", dialect)
        return out
    return {}


def get_target_db_config(
    configs: Dict[str, Dict[str, Any]], dialect: str = "postgresql"
) -> Dict[str, Any]:
    return _pick_dialect_section(configs.get("target_db", {}), dialect)


def get_sandbox_config(
    configs: Dict[str, Dict[str, Any]], dialect: str = "postgresql"
) -> Dict[str, Any]:
    sandbox_root = configs.get("sandbox", {})
    section = _pick_dialect_section(sandbox_root, dialect)
    if section:
        return section
    # 兼容旧 diagnosis 段
    legacy = sandbox_root.get("diagnosis", {})
    if legacy:
        out = dict(legacy)
        out.setdefault("dialect", normalize_dialect(dialect))
        return out
    return {}


def get_diagnosis_config(
    configs: Dict[str, Dict[str, Any]], dialect: str = "postgresql"
) -> Dict[str, Any]:
    """
    获取诊断相关的完整配置。

    Args:
        configs: 所有配置字典
        dialect: postgresql | mysql
    """
    dialect = normalize_dialect(dialect)
    target_db_config = get_target_db_config(configs, dialect)
    sandbox_config = get_sandbox_config(configs, dialect)

    if not target_db_config and sandbox_config:
        default_logger.info("target_db配置为空，使用sandbox配置作为target")
        target_db_config = sandbox_config.copy()

    return {
        "agent": configs.get("agents", {}).get("diagnosis", {}),
        "target_db": target_db_config,
        "sandbox": sandbox_config,
    }


def get_optimization_config(configs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return dict(configs.get("agents", {}).get("optimization", {}) or {})


def get_batch_config(configs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    agents = configs.get("agents", {}) or {}
    batch = dict(agents.get("batch") or {})
    batch.setdefault("default_workers", 4)
    batch.setdefault("max_workers", 16)
    batch.setdefault("llm_workers", "from_keys")
    batch.setdefault("llm_key_mode", "auto")
    batch.setdefault("sandbox_db_prefix", "astratune_sandbox_")
    batch.setdefault("sandbox_slot_count", 16)
    batch.setdefault("resume", True)
    return batch
