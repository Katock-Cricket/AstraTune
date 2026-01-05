import sys
from pathlib import Path

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from diagnosis.agent import DiagnosisAgent
from utils.config_loader import load_all_configs, get_diagnosis_config
from utils.logger import setup_logger


def test_agent_initialization():
    """测试Agent初始化（不使用沙箱工具）"""
    logger = setup_logger()
    logger.info("=" * 80)
    logger.info("测试1: Agent初始化（无工具）")
    logger.info("=" * 80)
    
    try:
        # 加载配置
        configs = load_all_configs("configs")
        diag_config = get_diagnosis_config(configs)
        agent_config = diag_config.get("agent", {})
        
        if not agent_config:
            logger.error("Agent配置为空，请检查配置文件")
            return False
        
        # 初始化Agent（不传入任何工具）
        agent = DiagnosisAgent(agent_config, sandbox_tool=None, rag_tool=None)
        
        logger.info("✓ Agent初始化成功")
        logger.info(f"  LLM模型: {agent_config.get('llm', {}).get('model', 'N/A')}")
        logger.info(f"  最大迭代次数: {agent.max_iter}")
        logger.info(f"  启用RAG: {agent.enable_rag}")
        logger.info(f"  启用沙箱测试: {agent.enable_test}")
        logger.info(f"  可用工具数量: {len(agent.tools)}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Agent初始化失败: {e}")
        logger.error(f"错误详情: {str(e)}", exc_info=True)
        return False


def test_agent_simple_communication():
    """测试Agent简单通信（不使用工具）"""
    logger = setup_logger()
    logger.info("=" * 80)
    logger.info("测试2: Agent简单通信")
    logger.info("=" * 80)
    
    try:
        # 加载配置
        configs = load_all_configs("configs")
        diag_config = get_diagnosis_config(configs)
        agent_config = diag_config.get("agent", {})
        
        # 初始化Agent（不使用工具）
        agent = DiagnosisAgent(agent_config, sandbox_tool=None, rag_tool=None)
        
        # 准备测试数据
        test_sql = "SELECT * FROM users WHERE id = 1"
        test_schema = """
-- 表: users
CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
        test_tables = ["users"]
        test_log = "平均执行时间: 0.5秒, 执行次数: 1000次"
        
        logger.info("发送测试诊断请求...")
        logger.info(f"  SQL: {test_sql}")
        logger.info(f"  涉及表: {test_tables}")
        
        # 调用Agent进行诊断
        conclusion = agent.diagnose(
            ori_sql=test_sql,
            schema=test_schema,
            tables=test_tables,
            exec_log=test_log,
            sampled_tables=[]
        )
        
        logger.info("\n" + "-" * 80)
        logger.info("Agent响应:")
        logger.info("-" * 80)
        logger.info(conclusion)
        logger.info("-" * 80)
        
        # 验证响应
        if conclusion and len(conclusion) > 0:
            logger.info("\n✓ Agent通信成功，收到有效响应")
            logger.info(f"  响应长度: {len(conclusion)} 字符")
            return True
        else:
            logger.warning("\n✗ Agent响应为空")
            return False
        
    except Exception as e:
        logger.error(f"✗ Agent通信测试失败: {e}")
        logger.error(f"错误详情: {str(e)}", exc_info=True)
        return False


def main():
    """运行所有Agent测试"""
    logger = setup_logger()
    logger.info("\n" + "=" * 80)
    logger.info("LangGraph驱动的DiagnosisAgent通信测试")
    logger.info("=" * 80 + "\n")
    
    results = []
    
    # 测试1: Agent初始化
    results.append(("Agent初始化", test_agent_initialization()))
    
    # 测试2: 简单通信
    results.append(("Agent简单通信", test_agent_simple_communication()))
    
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

