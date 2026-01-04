import re
from typing import Dict
from utils.logger import default_logger


class TableMapper:
    """表名映射器，负责维护原表名到沙箱表名的映射"""
    
    def __init__(self):
        """初始化表名映射器"""
        self.mapping: Dict[str, str] = {}  # 原表名 -> 沙箱表名
        self.reverse_mapping: Dict[str, str] = {}  # 沙箱表名 -> 原表名
    
    def add_mapping(self, original_name: str, sandbox_name: str) -> None:
        """
        添加表名映射
        
        Args:
            original_name: 原表名
            sandbox_name: 沙箱表名
        """
        self.mapping[original_name] = sandbox_name
        self.reverse_mapping[sandbox_name] = original_name
        default_logger.info(f"添加表名映射: {original_name} -> {sandbox_name}")
    
    def get_sandbox_name(self, original_name: str) -> str:
        """
        获取原表名对应的沙箱表名
        
        Args:
            original_name: 原表名
        
        Returns:
            沙箱表名，如果不存在映射则返回原表名
        """
        return self.mapping.get(original_name, original_name)
    
    def get_original_name(self, sandbox_name: str) -> str:
        """
        获取沙箱表名对应的原表名
        
        Args:
            sandbox_name: 沙箱表名
        
        Returns:
            原表名，如果不存在映射则返回沙箱表名
        """
        return self.reverse_mapping.get(sandbox_name, sandbox_name)
    
    def replace_table_names(self, sql: str) -> str:
        """
        将SQL语句中的原表名替换为沙箱表名
        
        使用正则表达式匹配表名边界，避免部分匹配
        
        Args:
            sql: 原始SQL语句
        
        Returns:
            替换后的SQL语句
        """
        replaced_sql = sql
        
        # 按表名长度降序排序，避免短表名被先替换导致长表名匹配失败
        # 例如：如果有表名 "user" 和 "user_info"，应该先替换 "user_info"
        sorted_mappings = sorted(self.mapping.items(), key=lambda x: len(x[0]), reverse=True)
        
        for original, sandbox in sorted_mappings:
            # 匹配表名边界：
            # \b 匹配单词边界
            # ` 匹配反引号（MySQL表名可能被反引号包围）
            # 使用(?i)进行不区分大小写匹配
            
            # 模式1: 反引号包围的表名 `table_name`
            pattern1 = r'`' + re.escape(original) + r'`'
            replaced_sql = re.sub(pattern1, f'`{sandbox}`', replaced_sql, flags=re.IGNORECASE)
            
            # 模式2: 单词边界的表名（没有反引号）
            pattern2 = r'\b' + re.escape(original) + r'\b'
            # 但要确保不在反引号内
            replaced_sql = re.sub(pattern2, sandbox, replaced_sql, flags=re.IGNORECASE)
        
        if replaced_sql != sql:
            default_logger.debug(f"表名替换:\n原始: {sql[:100]}...\n替换: {replaced_sql[:100]}...")
        
        return replaced_sql
    
    def get_all_sandbox_tables(self) -> list:
        """
        获取所有沙箱表名
        
        Returns:
            沙箱表名列表
        """
        return list(self.mapping.values())
    
    def clear(self) -> None:
        """清空所有映射"""
        self.mapping.clear()
        self.reverse_mapping.clear()
        default_logger.info("已清空所有表名映射")

