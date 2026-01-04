from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional


class DBExecutor(ABC):
    """数据库执行器抽象基类"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化执行器
        
        Args:
            config: 数据库配置
        """
        self.config = config
        self.connection = None
    
    @abstractmethod
    def connect(self) -> None:
        """
        连接数据库
        
        Raises:
            Exception: 连接失败时抛出异常
        """
        pass
    
    @abstractmethod
    def execute(self, sql: str, fetch: bool = True) -> Optional[List[Dict[str, Any]]]:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句（可以是多条语句，用分号分隔）
            fetch: 是否获取查询结果
        
        Returns:
            如果fetch=True，返回查询结果列表；否则返回None
        
        Raises:
            Exception: 执行失败时抛出异常
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """关闭数据库连接"""
        pass
    
    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        
        Args:
            table_name: 表名
        
        Returns:
            表是否存在
        """
        pass
    
    @abstractmethod
    def get_table_count(self, table_name: str) -> int:
        """
        获取表的行数
        
        Args:
            table_name: 表名
        
        Returns:
            表的行数
        """
        pass
    
    @abstractmethod
    def get_create_table_ddl(self, table_name: str) -> str:
        """
        获取表的CREATE TABLE语句
        
        Args:
            table_name: 表名
        
        Returns:
            CREATE TABLE DDL语句
        """
        pass
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()

