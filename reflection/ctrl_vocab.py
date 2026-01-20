"""
受控词表，处理知识消歧
"""

from enum import Enum

ROOT_CAUSE_CATEGORIES = {
    "索引问题": [
        # 基础缺失
        "索引缺失", "缺少索引", "未建索引", "无索引", 
        "全表扫描", "全表遍历", "Full Table Scan",
        # 索引失效/未命中
        "索引失效", "未命中索引", "不走索引", "违背最左前缀原则", 
        "最左匹配失败", "隐式类型转换", "字段类型不匹配",
        "在索引列上使用函数", "索引列运算", 
        "OR条件导致索引失效", "非覆盖索引", "回表次数过多",
        # 索引质量
        "统计信息过期", "执行计划选错索引", "索引区分度低", 
        "冗余索引", "重复索引"
    ],
    "SQL查询设计不良": [
        # 字段获取
        "查询字段冗余", "SELECT *", "查询无用字段", "全字段查询",
        # 分页与批量
        "未分页", "一次性拉取大量数据", "深分页", "大偏移量分页", "LIMIT过大",
        # 逻辑复杂度
        "复杂子查询", "嵌套查询过深", "多表关联过多", "大表JOIN", 
        "笛卡尔积", "JOIN条件缺失", "WHERE条件缺失",
        # 排序与聚合
        "文件排序", "Using filesort", "临时表排序", "复杂聚合计算"
    ],
    "数据模型与架构瓶颈": [
        # 数据量
        "单表数据量过大", "表数据过亿", "海量数据未归档",
        # 结构设计
        "大宽表", "单行记录过大", "未分库分表", "未分区", "分区缺失",
        # 分布问题
        "数据倾斜", "热点Key", "关联键分布不均", "数据分布不均匀"
    ],
    "资源与配置限制": [
        # 内存
        "Buffer Pool过小", "缓冲池不足", "内存命中率低", 
        "排序缓冲区不足", "Sort Buffer不足", "Join Buffer不足",
        # I/O与网络
        "磁盘I/O瓶颈", "IOPS过高", "网络传输慢", "网络带宽打满", "结果集过大",
        # 硬件
        "CPU飙升", "CPU资源不足"
    ],
    "并发与锁机制": [
        "锁等待", "Lock Wait", "行锁竞争", "互斥锁冲突",
        "死锁", "Deadlock", "MDL锁阻塞", "元数据锁等待",
        "高频并发查询", "QPS过高"
    ]
}

SOLUTION_CATEGORIES = {
    "索引优化": [
        # 创建与修改
        "添加索引", "创建索引", "新建联合索引", "覆盖索引优化", 
        "重构复合索引", "调整索引顺序",
        # 维护
        "删除冗余索引", "重建索引", "更新统计信息", "ANALYZE TABLE",
        "Force Index强制索引"
    ],
    "SQL重写与优化": [
        # 结构调整
        "SQL重写", "改写为JOIN", "拆分复杂查询", "分解关联查询",
        "使用UNION ALL替代OR", "优化子查询",
        # 字段与条件
        "移除SELECT *", "只查询必要字段", "去除函数操作", "修正字段类型转换",
        # 分页
        "分页优化", "游标分页", "延迟关联", "限制返回行数"
    ],
    "架构与模型调整": [
        # 分治
        "分库分表", "读写分离", "水平拆分", "垂直拆分",
        # 分区与归档
        "表分区", "按时间分区", "数据归档", "冷热分离",
        # 预计算
        "物化视图", "汇总表", "预计算聚合结果"
    ],
    "缓存与资源调优": [
        # 缓存
        "引入缓存", "Redis缓存", "应用层缓存",
        # 参数调整
        "调整数据库参数", "增大Buffer Pool", "增加Sort Buffer", 
        "开启优化器特性", "optimizer_switch调优"
    ],
    "业务逻辑降级": [
        "异步处理", "削峰填谷", "限制高频查询频率", 
        "改为模糊搜索", "调整业务逻辑", "禁止全表导出"
    ]
}


class Key(Enum):
    ROOT_CAUSE = "ROOT_CAUSE"
    SOLUTION = "SOLUTION"


def normalize(statement: str, key: Key) -> str:
    """
    归一化根因与解决方案的表述
    
    Args:
        statement: 原始词句
        key: 词表类型，"ROOT_CAUSE" 或 "SOLUTION"
        
    Returns:
        归一化后的词句
    """
    def get_category(statement: str, dict: dict) -> str:
        for category, keywords in dict.items():
            for keyword in keywords:
                if keyword in statement:
                    return category
        return statement

    if key == Key.ROOT_CAUSE:
        return get_category(statement, ROOT_CAUSE_CATEGORIES)
    elif key == Key.SOLUTION:
        return get_category(statement, SOLUTION_CATEGORIES)