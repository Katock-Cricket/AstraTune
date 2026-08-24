import logging
import sys
from typing import Optional


def _ensure_utf8_stdio() -> None:
    """Windows 默认 GBK 控制台写中文/emoji 会炸或乱码；尽量切到 UTF-8。"""
    for stream in (sys.stdout, sys.stderr):
        reconf = getattr(stream, "reconfigure", None)
        if callable(reconf):
            try:
                reconf(encoding="utf-8", errors="replace")
            except Exception:
                pass


class _SafeStreamHandler(logging.StreamHandler):
    """编码失败时降级替换，避免 Logging error 打断业务线程。"""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            stream = self.stream
            try:
                stream.write(msg + self.terminator)
            except UnicodeEncodeError:
                enc = getattr(stream, "encoding", None) or "utf-8"
                safe = (msg + self.terminator).encode(enc, errors="replace").decode(
                    enc, errors="replace"
                )
                stream.write(safe)
            self.flush()
        except Exception:
            self.handleError(record)


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
    _ensure_utf8_stdio()

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
    console_handler = _SafeStreamHandler(sys.stdout)
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
