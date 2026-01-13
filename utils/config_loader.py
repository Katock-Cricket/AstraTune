import json
from pathlib import Path
from typing import Dict, Any
from utils.logger import default_logger
from configs.configs import agents_config, sandbox_config, target_db_config


def load_all_configs() -> Dict[str, Dict[str, Any]]:
    
    configs = {}
    
    configs["agents"] = agents_config
    configs["sandbox"] = sandbox_config
    configs["target_db"] = target_db_config
    
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

