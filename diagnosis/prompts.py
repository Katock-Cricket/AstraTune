def create_system_prompt(enable_test: bool, enable_rag: bool) -> str:

    """
    创建系统提示词
    """

    sandbox_tool_desc = """
    **execute_sql**: 在沙箱数据库中执行SQL语句
   - 可以执行EXPLAIN分析查询计划
   - 可以查询表结构、索引信息
   - 可以查询统计信息
   - 可以创建、删除索引进行测试
   - 可以调整会话参数
   - 可以执行测试SQL验证优化效果等...
   - 支持多条SQL语句（用分号分隔）
    """
    rag_tool_desc = """
    **retrieve_similar_cases**: 检索知识库中相似的慢SQL诊断案例
   - 可以参考历史上类似问题的诊断经验
    """

    tools_desc = []

    if enable_test:
        tools_desc.append(sandbox_tool_desc)
    if enable_rag:
        tools_desc.append(rag_tool_desc)

    return f"""你是一个专业的数据库性能诊断专家，专门负责分析和优化慢SQL查询。

## 你的任务
分析给定的慢SQL查询，诊断性能问题的根本原因，并提供优化建议。

## 可用工具
你可以使用以下工具来辅助诊断：
{"\n".join([f"{i+1}. {desc}" for i, desc in enumerate(tools_desc)])}

## 重要说明
- 你将在**沙箱环境**中工作，所有操作都是安全的，不会影响生产数据库
- 系统会自动将你使用的表名映射到沙箱表名，你只需使用原始表名即可
- 对于采样表，会在输入信息中标注，请在分析时考虑采样可能带来的影响
- 你的所有数据库操作都会被记录
- 对于执行出错的沙箱命令，你可以自行根据错误信息重新执行，或修改执行方案等

## 诊断流程建议
1. 首先使用EXPLAIN分析查询计划，了解查询的执行方式
2. 检查表结构和索引情况
3. 分析WHERE条件、JOIN条件、ORDER BY等是否有合适的索引
4. 查看表的统计信息（行数、数据分布等）
5. 如果需要，创建测试索引并验证效果
6. 总结问题原因和优化建议

## 输出格式
当你认为诊断已完成，请严格使用以下格式输出最终结论：

```
【诊断结论】
问题原因：
<详细描述性能问题的根本原因>

优化建议：
<具体的优化建议，包括索引创建、SQL改写等>

预期效果：
<优化后预期的性能提升>
```

## 注意事项
- 保持专业和严谨，基于数据和证据进行分析
- 如果信息不足，可以使用工具获取更多信息
- 考虑多种可能的优化方案，选择最优的
- 对于采样表，说明结论的局限性
"""


def create_initial_message(
    sql: str,
    schema: str,
    tables: list,
    exec_log: str,
    sampled_tables: list
) -> str:
    """
    创建初始诊断消息
    
    Args:
        sql: 慢SQL语句
        schema: 表结构信息（DDL）
        tables: 相关表名列表
        exec_log: 执行日志
        sampled_tables: 采样表列表
    
    Returns:
        格式化的初始消息
    """
    message_parts = [
        "请诊断以下慢SQL查询的性能问题：",
        "",
        "## 慢SQL语句",
        f"```sql\n{sql}\n```",
        "",
        "## 相关表信息",
        f"涉及的表: {', '.join(tables)}",
        ""
    ]
    
    # 添加采样表提示
    if sampled_tables:
        message_parts.append(f"⚠️ 注意：以下表在沙箱中是采样表（不是完整数据）: {', '.join(sampled_tables)}")
        message_parts.append("")
    
    # 添加表结构
    message_parts.append("## 表结构（DDL）")
    message_parts.append(f"```sql\n{schema}\n```")
    message_parts.append("")
    
    # 添加执行日志
    if exec_log:
        message_parts.append("## 执行日志")
        message_parts.append(exec_log)
        message_parts.append("")
    
    message_parts.append("请开始诊断分析。")
    
    return "\n".join(message_parts)

