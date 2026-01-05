from typing import List, Dict, Any
from sandbox.executor import DBExecutor
from sandbox.table_mapper import TableMapper
from utils.logger import default_logger


class SandboxManager:
    """沙箱环境管理器，负责搭建和清理沙箱环境"""
    
    def __init__(self, target_executor: DBExecutor, sandbox_executor: DBExecutor, config: Dict[str, Any]):
        """
        初始化沙箱管理器
        
        Args:
            target_executor: 目标数据库执行器（只读，用于读取原表数据）
            sandbox_executor: 沙箱数据库执行器（读写，用于创建和操作沙箱表）
            config: 沙箱配置
        """
        self.target_executor = target_executor
        self.sandbox_executor = sandbox_executor
        self.config = config
        self.table_mapper = TableMapper()
        
        # 从配置中读取参数
        self.copy_threshold = config.get("copy_thr", 10000)
        self.sample_size = config.get("sample_size", 10000)
        self.sampling_strategy = config.get("sampling_strategy", "random")
        self.sampling_params = config.get("sampling_params", {})
        self.batch_size = config.get("batch_size", 1000)  # 批量插入大小
    
    def generate_sandbox_name(self, original_name: str) -> str:
        """
        生成沙箱表名，处理命名冲突
        
        Args:
            original_name: 原表名
        
        Returns:
            唯一的沙箱表名
        """
        base_name = f"{original_name}_sandbox"
        
        # 检查沙箱数据库中表是否存在
        if not self.sandbox_executor.table_exists(base_name):
            return base_name
        
        # 如果存在，添加数字后缀
        idx = 1
        while self.sandbox_executor.table_exists(f"{base_name}_{idx}"):
            idx += 1
        
        return f"{base_name}_{idx}"
    
    def _batch_copy_data(self, original_table: str, sandbox_table: str, limit: int = None) -> int:
        """
        批量拷贝数据（跨数据库）
        
        Args:
            original_table: 原表名（在目标数据库）
            sandbox_table: 沙箱表名（在沙箱数据库）
            limit: 限制拷贝的行数（None表示全部拷贝）
        
        Returns:
            实际拷贝的行数
        """
        default_logger.info(f"开始批量拷贝数据: {original_table} -> {sandbox_table}")
        
        try:
            # 从目标数据库读取数据
            if limit:
                if self.sampling_strategy == "random":
                    select_sql = f"SELECT * FROM `{original_table}` ORDER BY RAND() LIMIT {limit}"
                elif self.sampling_strategy == "time_based":
                    time_column = self.sampling_params.get("time_column", "created_at")
                    select_sql = f"SELECT * FROM `{original_table}` ORDER BY `{time_column}` DESC LIMIT {limit}"
                else:
                    select_sql = f"SELECT * FROM `{original_table}` ORDER BY RAND() LIMIT {limit}"
            else:
                select_sql = f"SELECT * FROM `{original_table}`"
            
            default_logger.info(f"从目标数据库读取数据: {select_sql[:100]}...")
            results = self.target_executor.execute(select_sql, fetch=True)
            
            if not results or not results[0].get("rows"):
                default_logger.warning(f"表 {original_table} 没有数据")
                return 0
            
            rows = results[0]["rows"]
            total_rows = len(rows)
            default_logger.info(f"成功读取 {total_rows} 行数据")
            
            # 批量插入到沙箱数据库
            if total_rows > 0:
                # 获取列名
                columns = list(rows[0].keys())
                columns_str = ", ".join([f"`{col}`" for col in columns])
                
                # 分批插入
                inserted_count = 0
                for i in range(0, total_rows, self.batch_size):
                    batch = rows[i:i + self.batch_size]
                    
                    # 构造VALUES部分
                    values_parts = []
                    for row in batch:
                        values = []
                        for col in columns:
                            val = row[col]
                            if val is None:
                                values.append("NULL")
                            elif isinstance(val, (int, float)):
                                values.append(str(val))
                            else:
                                # 转义字符串中的单引号和反斜杠
                                val_str = str(val).replace("\\", "\\\\").replace("'", "\\'")
                                values.append(f"'{val_str}'")
                        values_parts.append(f"({', '.join(values)})")
                    
                    # 构造INSERT语句
                    insert_sql = f"INSERT INTO `{sandbox_table}` ({columns_str}) VALUES {', '.join(values_parts)}"
                    
                    # 执行插入
                    self.sandbox_executor.execute(insert_sql, fetch=False)
                    inserted_count += len(batch)
                    
                    # if total_rows > self.batch_size:
                    #     default_logger.info(f"已插入 {inserted_count}/{total_rows} 行")
                
                default_logger.info(f"成功插入 {inserted_count} 行数据到沙箱表")
                return inserted_count
            
            return 0
            
        except Exception as e:
            default_logger.error(f"批量拷贝数据失败: {e}")
            raise
    
    def create_full_copy_table(self, original_name: str, sandbox_name: str) -> None:
        """
        创建完整拷贝表（小表）- 跨数据库版本
        
        Args:
            original_name: 原表名（在目标数据库）
            sandbox_name: 沙箱表名（在沙箱数据库）
        """
        default_logger.info(f"开始完整拷贝表: {original_name} -> {sandbox_name}")
        
        try:
            # 从目标数据库获取原表的CREATE TABLE语句
            create_ddl = self.target_executor.get_create_table_ddl(original_name)
            
            # 替换表名
            create_ddl_for_sandbox = create_ddl.replace(f"`{original_name}`", f"`{sandbox_name}`", 1)
            # 也处理没有反引号的情况
            if f"`{original_name}`" not in create_ddl:
                create_ddl_for_sandbox = create_ddl.replace(f"TABLE {original_name}", f"TABLE `{sandbox_name}`", 1)
            
            # 在沙箱数据库创建表结构
            self.sandbox_executor.execute(create_ddl_for_sandbox, fetch=False)
            default_logger.info(f"成功在沙箱数据库创建表结构: {sandbox_name}")
            
            # 批量拷贝数据（跨数据库）
            count = self._batch_copy_data(original_name, sandbox_name, limit=None)
            default_logger.info(f"成功复制{count}行数据到沙箱表: {sandbox_name}")
            
        except Exception as e:
            default_logger.error(f"完整拷贝表失败: {e}")
            raise
    
    def create_sampled_table(self, original_name: str, sandbox_name: str) -> None:
        """
        创建采样表（大表）- 跨数据库版本
        
        Args:
            original_name: 原表名（在目标数据库）
            sandbox_name: 沙箱表名（在沙箱数据库）
        """
        default_logger.info(f"开始创建采样表: {original_name} -> {sandbox_name} (策略: {self.sampling_strategy})")
        
        try:
            # 从目标数据库获取原表的CREATE TABLE语句
            create_ddl = self.target_executor.get_create_table_ddl(original_name)
            
            # 替换表名
            create_ddl_for_sandbox = create_ddl.replace(f"`{original_name}`", f"`{sandbox_name}`", 1)
            if f"`{original_name}`" not in create_ddl:
                create_ddl_for_sandbox = create_ddl.replace(f"TABLE {original_name}", f"TABLE `{sandbox_name}`", 1)
            
            # 在沙箱数据库创建表结构
            self.sandbox_executor.execute(create_ddl_for_sandbox, fetch=False)
            default_logger.info(f"成功在沙箱数据库创建表结构: {sandbox_name}")
            
            # 批量拷贝采样数据（跨数据库）
            count = self._batch_copy_data(original_name, sandbox_name, limit=self.sample_size)
            default_logger.info(f"成功采样{count}行数据到沙箱表: {sandbox_name}")
            
        except Exception as e:
            default_logger.error(f"创建采样表失败: {e}")
            raise
    
    def setup_sandbox(self, tables: List[str]) -> Dict[str, Any]:
        """
        搭建沙箱环境（跨数据库版本）
        
        Args:
            tables: 需要复制到沙箱的表名列表
        
        Returns:
            沙箱信息字典，包含表映射和采样信息
        
        Raises:
            Exception: 沙箱搭建失败时抛出异常
        """
        default_logger.info(f"开始搭建沙箱环境，涉及{len(tables)}张表")
        
        sandbox_info = {
            "tables": [],
            "sampled_tables": []  # 记录哪些表是采样的
        }
        
        try:
            for table in tables:
                # 检查目标数据库中表是否存在
                if not self.target_executor.table_exists(table):
                    raise ValueError(f"目标数据库中表不存在: {table}")
                
                # 生成沙箱表名
                sandbox_name = self.generate_sandbox_name(table)
                
                # 从目标数据库获取表的行数
                row_count = self.target_executor.get_table_count(table)
                default_logger.info(f"目标数据库中表 {table} 有 {row_count} 行数据")
                
                # 根据行数决定是完整拷贝还是采样
                if row_count <= self.copy_threshold:
                    self.create_full_copy_table(table, sandbox_name)
                    is_sampled = False
                else:
                    self.create_sampled_table(table, sandbox_name)
                    is_sampled = True
                    sandbox_info["sampled_tables"].append(table)
                
                # 添加表名映射
                self.table_mapper.add_mapping(table, sandbox_name)
                
                sandbox_info["tables"].append({
                    "original": table,
                    "sandbox": sandbox_name,
                    "original_rows": row_count,
                    "is_sampled": is_sampled
                })
            
            default_logger.info(f"沙箱环境搭建完成，共{len(tables)}张表")
            return sandbox_info
            
        except Exception as e:
            default_logger.error(f"沙箱环境搭建失败: {e}")
            # 尝试清理已创建的表
            try:
                self.cleanup_sandbox()
            except:
                pass
            raise
    
    def cleanup_sandbox(self) -> None:
        """清理沙箱环境，删除所有沙箱表"""
        sandbox_tables = self.table_mapper.get_all_sandbox_tables()
        
        if not sandbox_tables:
            default_logger.info("没有需要清理的沙箱表")
            return
        
        default_logger.info(f"开始清理沙箱环境，共{len(sandbox_tables)}张表")
        
        for table in sandbox_tables:
            try:
                # 在沙箱数据库中删除表
                if self.sandbox_executor.table_exists(table):
                    drop_sql = f"DROP TABLE `{table}`"
                    self.sandbox_executor.execute(drop_sql, fetch=False)
                    default_logger.info(f"成功删除沙箱表: {table}")
            except Exception as e:
                default_logger.error(f"删除沙箱表失败 {table}: {e}")
        
        # 清空映射
        self.table_mapper.clear()
        default_logger.info("沙箱环境清理完成")

