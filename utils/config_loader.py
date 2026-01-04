import json
from pathlib import Path
from typing import Dict, Any
from utils.logger import default_logger


def load_config(config_path: str) -> Dict[str, Any]:
    """
    加载单个JSON配置文件
    
    Args:
        config_path: 配置文件路径
    
    Returns:
        配置字典
    
    Raises:
        FileNotFoundError: 配置文件不存在
        json.JSONDecodeError: 配置文件格式错误
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    default_logger.info(f"成功加载配置文件: {config_path}")
    return config


def load_all_configs(config_dir: str = "configs") -> Dict[str, Dict[str, Any]]:
    config_path = Path(config_dir)
    
    configs = {}
    
    # 加载agents配置
    agents_file = config_path / "agents.json"
    if agents_file.exists():
        configs["agents"] = load_config(str(agents_file))
    else:
        default_logger.warning(f"agents配置文件不存在: {agents_file}")
        configs["agents"] = {}
    
    # 加载sandbox配置
    sandbox_file = config_path / "sandbox.json"
    if sandbox_file.exists():
        configs["sandbox"] = load_config(str(sandbox_file))
    else:
        default_logger.warning(f"sandbox配置文件不存在: {sandbox_file}")
        configs["sandbox"] = {}
    
    # 加载target_db配置
    target_db_file = config_path / "target_db.json"
    if target_db_file.exists():
        configs["target_db"] = load_config(str(target_db_file))
    else:
        default_logger.warning(f"target_db配置文件不存在: {target_db_file}，将使用sandbox配置作为target")
        configs["target_db"] = {}
    
    return configs


def get_diagnosis_config(configs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    获取诊断相关的完整配置
    
    Args:
        configs: 所有配置字典
    
    Returns:
        诊断配置，包含agent、target_db和sandbox配置
    """
    # 获取target_db配置，如果不存在则使用sandbox配置
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

