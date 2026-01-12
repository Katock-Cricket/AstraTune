def create_system_prompt(enable_test: bool = False, enable_rag: bool = False) -> str:

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

    # 构建工具描述字符串（避免在f-string中使用反斜杠）
    tools_list = "\n".join([f"{i+1}. {desc}" for i, desc in enumerate(tools_desc)])

    tools_usage_desc = ""

    # 工具使用与流程说明
    if len(tools_desc) > 0:
        tools_usage_desc = f"""
## 可用工具
你可以使用以下工具来辅助诊断：
{tools_list}

## 重要说明
- 你的数据库交互将在**沙箱环境**中工作，所有操作都是安全的，不会影响生产数据库
- 系统会自动将你使用的表名映射到沙箱表名，你只需使用原始表名即可
- 对于采样表，会在输入信息中标注，请在分析时权衡采样可能带来的影响
- 对于执行出错的命令，你可以自行根据错误信息重新执行，或修改执行方案等
- 部分sql附加前置和后置sql用于复现问题，这些前置和后置语句将在你每次发起测试时由沙箱自动执行，你不需要显式地执行前后置sql
- 部分问题可能附加用户手动填入的提示

## 诊断流程建议
1. 首先使用EXPLAIN分析查询计划，了解查询的执行方式
2. 检查表结构和索引情况
3. 分析WHERE条件、JOIN条件、ORDER BY等是否有合适的索引
4. 查看表的统计信息（行数、数据分布等）
5. 如果需要，创建测试索引并验证效果
6. 总结问题原因和优化建议
        """


    return f"""你是一个专业的数据库性能诊断专家，专门负责分析和优化慢SQL查询。

## 你的任务
分析给定的慢SQL查询，诊断性能问题的根本原因，并提供优化建议。

{tools_usage_desc}

## 输出格式
当你认为诊断已完成，请严格使用以下格式输出最终结论，将诊断结论以“【诊断结论】”开头：

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
- **请全程使用中文推理或输出结论！**
- (如果系统提供了工具)可以使用工具获取更多信息
- (如果系统提供了工具)考虑多种可能的优化方案，选择最优的
"""


def create_initial_message(
    sql: str,
    schema: str,
    tables: list,
    exec_log: str = None,
    sampled_tables: list = None,
    preprocess_sql: str = None,
    clean_up_sql: str = None,
    user_prompt: str = None
) -> str:
    """
    创建初始诊断消息
    
    Args:
        sql: 慢SQL语句
        schema: 表结构信息（DDL）
        tables: 相关表名列表
        exec_log: 执行日志
        sampled_tables: 采样表列表
        preprocess_sql: 测试本条sql的前置sql
        clean_up_sql：测试本条sql的清理sql，用于恢复测试环境
        user_prompt: 用户提示
    
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
        "",
        "## 相关表结构（DDL）",
        f"```sql\n{schema}\n```",
        ""
    ]
    
    # 添加采样表提示
    if sampled_tables:
        message_parts.append(f"⚠️ 注意：以下表在沙箱中是采样表（不是完整数据）: {', '.join(sampled_tables)}")
        message_parts.append("")
    
    # 添加执行日志
    if exec_log:
        message_parts.append("## 相关执行日志")
        message_parts.append(exec_log)
        message_parts.append("")

    # 添加前置sql
    if preprocess_sql:
        message_parts.append("## 执行本条慢sql的前置sql")
        message_parts.append(f"```sql\n{preprocess_sql}\n```")
        message_parts.append("")

    # 添加清理sql
    if clean_up_sql:
        message_parts.append("## 执行完本条慢sql后，执行的清理sql")
        message_parts.append(f"```sql\n{clean_up_sql}\n```")
        message_parts.append("")

    # 添加用户提示
    if user_prompt:
        message_parts.append("## 附加用户提示")
        message_parts.append(user_prompt)
        message_parts.append("")

    message_parts.append("请开始诊断分析。")
    
    return "\n".join(message_parts)

