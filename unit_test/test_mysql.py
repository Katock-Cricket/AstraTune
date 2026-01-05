import sys
from pathlib import Path

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sandbox.mysql_executor import MySQLExecutor
from utils.config_loader import load_all_configs, get_diagnosis_config
from utils.logger import setup_logger


# 硬编码的测试SQL语句
TEST_SQL = """
CREATE TABLE sales (id INT, slot TIMESTAMP, total INT);
INSERT INTO sales VALUES(1, '2022-12-01T12:00', 100), (2, '2022-12-01T12:30', 150), (3, '2022-12-01T13:00', 200);
CREATE INDEX idx_test ON sales (slot);

SET @current_time = TIMESTAMP('2022-12-01 12:40:00');
WITH cte AS (SELECT slot, SUM(total) OVER(ORDER BY slot) AS total, total AS rowtotal FROM sales WHERE slot < @current_time ORDER BY slot DESC LIMIT 1) SELECT total - (30 - TIMESTAMPDIFF(MINUTE, slot, @current_time))/30 * rowtotal AS total FROM cte

SET @current_time := NULL;
DROP TABLE sales;
"""


def test_mysql_connection():
    """测试MySQL数据库连接"""
    logger = setup_logger()
    logger.info("=" * 80)
    logger.info("测试1: MySQL数据库连接")
    logger.info("=" * 80)
    
    try:
        # 加载配置
        configs = load_all_configs("configs")
        diag_config = get_diagnosis_config(configs)
        target_db_config = diag_config.get("target_db", {})
        
        if not target_db_config:
            logger.error("沙箱配置为空，请检查配置文件")
            return False
        
        # 创建MySQL执行器
        executor = MySQLExecutor(target_db_config)
        
        # 连接数据库
        executor.connect()
        logger.info("✓ 数据库连接成功")
        
        # 关闭连接
        executor.close()
        logger.info("✓ 数据库连接关闭成功")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ 数据库连接测试失败: {e}")
        return False


def test_mysql_execute_hardcoded():
    """测试执行硬编码的复杂SQL语句"""
    logger = setup_logger()
    logger.info("=" * 80)
    logger.info("测试2: 执行硬编码的复杂SQL语句")
    logger.info("=" * 80)
    
    try:
        # 加载配置
        configs = load_all_configs("configs")
        diag_config = get_diagnosis_config(configs)
        target_db_config = diag_config.get("target_db", {})
        
        # 创建并连接MySQL执行器
        executor = MySQLExecutor(target_db_config)
        executor.connect()
        
        logger.info(f"执行SQL: {TEST_SQL[:100]}...")
        
        # 执行硬编码的SQL
        results = executor.execute(TEST_SQL, fetch=True)
        
        logger.info(f"SQL执行完成，共返回 {len(results)} 个结果集")
        
        # 打印每个结果集的信息
        for i, result in enumerate(results, 1):
            logger.info(f"\n结果集 {i}:")
            logger.info(f"  SQL: {result.get('sql', '')[:80]}...")
            
            if 'rows' in result:
                logger.info(f"  返回行数: {result.get('row_count', 0)}")
                logger.info(f"  执行时间: {result.get('execution_time_ms', 0)}ms")
                if result.get('rows'):
                    logger.info(f"  结果数据: {result['rows']}")
            else:
                logger.info(f"  影响行数: {result.get('affected_rows', 0)}")
                logger.info(f"  执行时间: {result.get('execution_time_ms', 0)}ms")
        
        logger.info("\n✓ 硬编码SQL执行成功")
        
        # 关闭连接
        executor.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ 硬编码SQL执行失败: {e}")
        logger.error(f"错误详情: {str(e)}", exc_info=True)
        return False


def main():
    """运行所有MySQL测试"""
    logger = setup_logger()
    logger.info("\n" + "=" * 80)
    logger.info("MySQL数据库功能测试")
    logger.info("=" * 80 + "\n")
    
    results = []
    
    # 测试1: 数据库连接
    results.append(("数据库连接", test_mysql_connection()))
    
    # 测试2: 执行硬编码SQL
    results.append(("执行硬编码SQL", test_mysql_execute_hardcoded()))
    
    # 输出测试总结
    logger.info("\n" + "=" * 80)
    logger.info("测试总结")
    logger.info("=" * 80)
    
    for test_name, result in results:
        status = "✓ 通过" if result else "✗ 失败"
        logger.info(f"{test_name}: {status}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    logger.info(f"\n总计: {passed}/{total} 个测试通过")
    logger.info("=" * 80 + "\n")
    
    return all(result for _, result in results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
