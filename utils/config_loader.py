import json
from pathlib import Path
from typing import Dict, Any
from utils.logger import default_logger

_CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


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
    return configs


def get_diagnosis_config(configs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    获取诊断相关的完整配置
    
    Args:
        configs: 所有配置字典
    
    Returns:
        诊断配置，包含agent、target_db和sandbox配置
    """
    target_db_config = configs.get("target_db", {})
    sandbox_config = configs.get("sandbox", {}).get("diagnosis", {})
    
    # 向后兼容：如果target_db为空，使用sandbox配置
    if not target_db_config:
        default_logger.info("target_db配置为空，使用sandbox配置作为target")
        target_db_config = sandbox_config.copy()
    
    return {
        "agent": configs.get("agents", {}).get("diagnosis", {}),
        "target_db": target_db_config,
        "sandbox": sandbox_config
    }
