import logging
import sys
from typing import Optional


def setup_logger(
    name: str = "AstraTune",
    level: int = logging.INFO,
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    配置并返回logger实例
    
    Args:
        name: logger名称
        level: 日志级别
        log_file: 可选的日志文件路径，默认不输出到文件
    
    Returns:
        配置好的logger实例
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 可选的文件handler
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


# 创建默认logger实例
default_logger = setup_logger()

