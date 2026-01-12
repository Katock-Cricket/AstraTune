import argparse
import sys
import asyncio
from utils.logger import setup_logger
from utils.config_loader import load_all_configs, get_diagnosis_config
from utils.stream_handler import StreamHandler
from sandbox.mysql_executor import MySQLExecutor
from sandbox.sandbox_manager import SandboxManager
from diagnosis.agent import DiagnosisAgent
from tools.sandbox_tool import SandboxTool
from tools.rag_tool import RAGTool


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="慢SQL诊断系统 - 基于LLM Agent的数据库性能诊断工具"
    )
    
    parser.add_argument(
        "--sql",
        type=str,
        required=True,
        help="需要诊断的慢SQL语句, 多条sql用分号分隔"
    )
    
    parser.add_argument(
        "--tables",
        type=str,
        default=None,
        help="相关数据库表名，多个表用逗号分隔，如不填，则不涉及已有的表"
    )
    
    parser.add_argument(
        "--log",
        type=str,
        default=None,
        help="执行日志（包含平均执行时间、执行次数等统计信息）"
    )

    parser.add_argument(
        "--preprocess-sql",
        type=str,
        default=None,
        help="测试本条sql的前置sql，多条sql用分号分隔"
    )

    parser.add_argument(
        "--clean-up-sql",
        type=str,
        default=None,
        help="测试本条sql的清理sql，用于恢复测试环境，多条sql用分号分隔"
    )

    parser.add_argument(
        "--user-prompt",
        type=str,
        default=None,
        help="附加用户提示"
    )

    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="日志文件路径（可选，默认只输出到控制台）"
    )
    
    parser.add_argument(
        "--stream",
        action="store_true",
        default=False,
        help="启用流式输出，实时显示Agent推理过程"
    )
    
    return parser.parse_args()


def main():
    """主函数"""
    # 解析参数
    args = parse_arguments()
    
    # 设置日志
    logger = setup_logger(log_file=args.log_file)
    logger.info("=" * 80)
    logger.info("慢SQL诊断系统启动")
    logger.info("=" * 80)
    
    # 解析表名列表
    if args.tables:
        tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        logger.info(f"涉及的表: {tables}")
    else:
        tables = []
    
    try:
        # 1. 加载配置
        logger.info("步骤 1/7: 加载配置文件")
        configs = load_all_configs()
        diag_config = get_diagnosis_config(configs)
        
        agent_config = diag_config.get("agent", {})
        target_db_config = diag_config.get("target_db", {})
        sandbox_config = diag_config.get("sandbox", {})
        
        if not agent_config:
            logger.error("Agent配置为空，请检查配置文件")
            sys.exit(1)
        
        if not target_db_config:
            logger.error("Target DB配置为空，请检查配置文件")
            sys.exit(1)
        
        if not sandbox_config:
            logger.error("Sandbox配置为空，请检查配置文件")
            sys.exit(1)
        
        logger.info("配置加载完成")
        logger.info(f"目标数据库: {target_db_config.get('host')}:{target_db_config.get('port')}/{target_db_config.get('database')}")
        logger.info(f"沙箱数据库: {sandbox_config.get('host')}:{sandbox_config.get('port')}/{sandbox_config.get('database')}")
        
        # 2. 连接目标数据库和沙箱数据库
        logger.info("步骤 2/7: 连接数据库")
        
        # 创建目标数据库executor（只读）
        target_executor = MySQLExecutor(target_db_config)
        try:
            target_executor.connect()
            logger.info("目标数据库连接成功")
        except Exception as e:
            logger.error(f"目标数据库连接失败: {e}")
            sys.exit(1)
        
        # 创建沙箱数据库executor（读写）
        sandbox_executor = MySQLExecutor(sandbox_config)
        try:
            sandbox_executor.connect()
            logger.info("沙箱数据库连接成功")
        except Exception as e:
            logger.error(f"沙箱数据库连接失败: {e}")
            target_executor.close()
            sys.exit(1)
        
        # 3. 获取表DDL
        logger.info("步骤 3/7: 从目标数据库获取表结构信息")
        schema_parts = []
        for table in tables:
            try:
                ddl = target_executor.get_create_table_ddl(table)
                schema_parts.append(f"-- 表: {table}")
                schema_parts.append(ddl)
                schema_parts.append("")
            except Exception as e:
                logger.error(f"获取表 {table} 的DDL失败: {e}")
                target_executor.close()
                sandbox_executor.close()
                sys.exit(1)
        
        schema = "\n".join(schema_parts)
        logger.info(f"成功获取 {len(tables)} 张表的结构信息")
        
        # 4. 搭建沙箱环境
        logger.info("步骤 4/7: 搭建沙箱环境（从目标数据库复制到沙箱数据库）")
        sandbox_manager = SandboxManager(target_executor, sandbox_executor, sandbox_config)
        
        try:
            sandbox_info = sandbox_manager.setup_sandbox(tables)
            logger.info("沙箱环境搭建成功")
            
            # 打印沙箱信息
            for table_info in sandbox_info["tables"]:
                logger.info(
                    f"  {table_info['original']} -> {table_info['sandbox']} "
                    f"({table_info['original_rows']} 行, "
                    f"{'采样' if table_info['is_sampled'] else '完整拷贝'})"
                )
        except Exception as e:
            logger.error(f"沙箱环境搭建失败: {e}")
            target_executor.close()
            sandbox_executor.close()
            sys.exit(1)
        
        # 5. 初始化Agent和工具
        logger.info("步骤 5/7: 初始化Agent")
        # Agent只使用沙箱数据库executor
        sandbox_tool = SandboxTool(sandbox_executor, sandbox_manager.table_mapper)
        # 注册前置和清理sql
        if args.preprocess_sql:
            sandbox_tool.register_preprocess_sql(args.preprocess_sql)
        if args.clean_up_sql:
            sandbox_tool.register_clean_up_sql(args.clean_up_sql)

        rag_tool = RAGTool() if agent_config.get("enable_rag", False) else None
        
        try:
            agent = DiagnosisAgent(agent_config, sandbox_tool, rag_tool)
            logger.info("Agent初始化成功")
        except Exception as e:
            logger.error(f"Agent初始化失败: {e}")
            sandbox_manager.cleanup_sandbox()
            target_executor.close()
            sandbox_executor.close()
            sys.exit(1)
        
        # 6. 执行诊断
        logger.info("步骤 6/7: 执行诊断")
        logger.info("-" * 80)
        
        try:
            if args.stream:
                # 流式模式：Rich显示 + Logger文件记录（如果指定了--log-file）
                logger.info("使用流式模式执行诊断")
                stream_handler = StreamHandler(mode="rich")
                
                # 异步执行流式诊断
                conclusion = asyncio.run(agent.diagnose_stream(
                    ori_sql=args.sql,
                    schema=schema,
                    tables=tables,
                    exec_log=args.log,
                    sampled_tables=sandbox_info["sampled_tables"],
                    preprocess_sql=args.preprocess_sql,
                    clean_up_sql=args.clean_up_sql,
                    user_prompt=args.user_prompt,
                    stream_handler=stream_handler
                ))
            else:
                # 非流式模式：收集事件后通过Logger输出
                logger.info("使用非流式模式执行诊断")
                conclusion = agent.diagnose(
                    ori_sql=args.sql,
                    schema=schema,
                    tables=tables,
                    exec_log=args.log,
                    sampled_tables=sandbox_info["sampled_tables"],
                    preprocess_sql=args.preprocess_sql,
                    clean_up_sql=args.clean_up_sql,
                    user_prompt=args.user_prompt
                )
            
            # 输出诊断结果
            logger.info("-" * 80)
            logger.info("诊断完成！")
            logger.info("=" * 80)
            
            # 流式模式下结论已经通过Rich显示，这里只做简单总结
            if not args.stream:
                print("\n" + "=" * 80)
                print("诊断结果")
                print("=" * 80)
                print(conclusion)
                print("=" * 80 + "\n")
            
        except Exception as e:
            logger.error(f"诊断执行失败: {e}")
            raise
        
        # 7. 清理沙箱和关闭连接
        logger.info("步骤 7/7: 清理沙箱环境")
        try:
            sandbox_manager.cleanup_sandbox()
        except Exception as e:
            logger.error(f"清理沙箱失败: {e}")
        
        # 关闭数据库连接
        target_executor.close()
        sandbox_executor.close()
        
        logger.info("=" * 80)
        logger.info("慢SQL诊断系统结束")
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.info("\n用户中断程序")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序执行出错: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

