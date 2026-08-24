import re


def create_table_extraction_system_prompt() -> str:
    """从问题 SQL + 用户描述中识别涉及的关系表（不含诊断结论）。"""
    return """你是数据库专家。你的唯一任务：列出诊断/修复时沙箱需要准备的**基表**（真实表名，不含别名）。

规则：
- 必须包含 SQL 中 FROM / JOIN / UPDATE / INSERT INTO / DELETE FROM / MERGE 等访问的表。
- 若同时给出用户问题描述，还须包含描述里点名的业务表（即使当前 SQL 用错了表，例如描述说 order 表而 SQL 写了 loan）。
- 使用数据库中的实际表名；若带 schema 前缀（如 public.orders），请保留完整形式。
- 去重，不要猜测描述与 SQL 都未出现的表。
- **不要**输出解释、Markdown、诊断内容。

输出格式（严格仅一行 JSON 对象，无其它文字）：
{"tables":["schema.table1","table2"]}

若无法确定任何表，输出：{"tables":[]}
"""


def create_table_extraction_user_message(sql: str, user_prompt: str = "") -> str:
    up = (user_prompt or "").strip()
    up_block = f"## 用户问题描述\n{up}\n\n" if up else ""
    return f"""请分析以下内容并输出 JSON（合并 SQL 与用户描述中的表）：

{up_block}## SQL
```sql
{sql}
```
"""


def create_system_prompt(enable_test: bool = False) -> str:
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
    reset_tool_desc = """
    **reset_sandbox_environment**: 全量重建沙箱（清空后从目标库重新拷贝表并重跑前置 SQL）
   - **仅当**沙箱被不可逆破坏、题面 clean_up 无法恢复、且会影响后续验证时才可调用
   - 必须传入具体中文 reason；禁止预防性/随便 reset
    """

    tools_desc = []

    if enable_test:
        tools_desc.append(sandbox_tool_desc)
        tools_desc.append(reset_tool_desc)

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
- **方言**：以任务给定方言为准（BIRD Flash / 本沙箱常见为 **PostgreSQL**）。禁止使用 psql 客户端元命令（如 \\d、\\dt）；禁止查询 sqlite_master。
- 查表结构：`information_schema.columns`；查索引：`pg_indexes`（PG）或 `SHOW INDEX`（MySQL）
- 表通常已由系统搭好，不要一上来就重复 CREATE 已有表；若报 already exists，改为查询现有对象
- 对于采样表，会在输入信息中标注，请在分析时权衡采样可能带来的影响
- 对于执行出错的命令，你可以自行根据错误信息重新执行，或修改执行方案等
- 部分sql附加前置和后置sql用于复现问题，这些前置语句在沙箱初始化时自动执行，在需要清理或复现问题时，你可以自行执行他们。
- 部分问题可能附加用户手动填入的提示

## 诊断流程建议
1. 先用 information_schema / EXPLAIN 摸清对象与计划（不要用 \\d）
2. 检查表结构和索引情况
3. 分析WHERE条件、JOIN条件、ORDER BY等是否有合适的索引
4. 查看表的统计信息（行数、数据分布等）
5. 如果需要，创建测试索引并验证效果
6. 总结问题原因和优化建议（覆盖语法/语义/方言/DDL/性能等）
        """


    return f"""你是一个专业的数据库诊断专家，负责分析泛化 SQL 问题（语法错误、语义错误、方言迁移、DDL/触发器、结果不正确、以及性能）。

## 你的任务
分析给定的SQL，诊断问题的根本原因，并提供可落地的修复/优化建议。

{tools_usage_desc}

## 输出格式
当你认为诊断已完成，请严格使用以下格式输出最终结论，将诊断结论以“【诊断结论】”开头：

```
【诊断结论】
问题原因：
<详细描述问题的根本原因>

优化建议：
<具体的优化建议，包括索引创建、SQL改写等>

预期效果：
<优化后预期的效果>
```

## 注意事项
- 保持专业和严谨，基于数据和证据进行分析
- **请全程使用中文推理或输出结论！**
- (如果系统提供了工具)可以使用工具获取更多信息；**优先** information_schema / EXPLAIN / 小 LIMIT 探测，避免盲目试错
- **效率**：证据足够就立刻输出【诊断结论】，不要为「再试一次」耗尽迭代；同一类 SQL 失败不要原样重试超过 1 次，应改写或换思路
- **事务**：工具侧已自动处理失败回滚；看到 aborted/失败提示后直接发下一条正确 SQL 即可
- (如果系统提供了工具)考虑多种可能的优化方案，选择最优的
- **格式**：用户只要 hh:mm:ss 且不要前导零时，结论里应给出 `to_char(..., 'FMHH:MI:SS')`（12 小时 + FM）。即使用户样例出现 `0:00:00`，也不要用 HH24 / EXTRACT(HOUR) / regexp_replace，除非明确要求 24 小时制
- **对象**：前置 SQL 若 CREATE 了新表，该表往往才是修复目标（不要只改用户口头上的生产表名）
- **方言**：PostgreSQL 的 DELETE/UPDATE 没有顶层 LIMIT；无 charindex（用 strpos）
- **ON CONFLICT**：冲突列必须落在同一条 UNIQUE 索引上，不要拆成两组部分唯一索引
- **Mgmt 触发器**：结论里给出完整 CREATE FUNCTION + CREATE TRIGGER
- **互为 FK / chicken-egg**：去掉一侧 FK，用触发器保证「至少一行」
"""


def _tables_from_schema_ddl(schema: str) -> list:
    """从由 get_create_table_ddl 拼接的 schema 文本中解析 `-- 表: name` 行。"""
    found = re.findall(r"^--\s*表:\s*(\S+)", schema, re.MULTILINE)
    return list(dict.fromkeys(found))


def create_initial_message(
    sql: str,
    schema: str,
    exec_log: str = None,
    sampled_tables: list = None,
    preprocess_sql: str = None,
    clean_up_sql: str = None,
    user_prompt: str = None,
    sandbox_fidelity: str = None,
) -> str:
    """
    创建初始诊断消息
    """
    tables = _tables_from_schema_ddl(schema)
    tables_display = ", ".join(tables) if tables else "（见下方 DDL 注释中的表名）"
    message_parts = [
        "请诊断以下SQL的问题：",
        "",
        "## 问题SQL语句",
        f"```sql\n{sql}\n```",
        "",
        "## 相关表信息",
        f"涉及的表: {tables_display}",
        "",
        "## 相关表结构（DDL）",
        f"```sql\n{schema}\n```",
        ""
    ]
    
    # 添加采样表提示
    if sampled_tables:
        message_parts.append(f"[注意] 以下表在沙箱中是采样表（不是完整数据）: {', '.join(sampled_tables)}")
        message_parts.append("")

    if sandbox_fidelity and str(sandbox_fidelity).strip():
        message_parts.append("## 沙箱保真度（相对 target 生产库）")
        message_parts.append(str(sandbox_fidelity).strip())
        message_parts.append("")
    
    # 添加执行日志
    if exec_log:
        message_parts.append("## 相关执行日志")
        message_parts.append(exec_log)
        message_parts.append("")

    # 添加前置sql
    if preprocess_sql:
        message_parts.append("## 执行本条问题sql的前置sql")
        message_parts.append(
            "（诊断开始时已自动执行一次；一般无需再跑。"
            "若你 DROP/破坏了相关对象导致缺表缺列，可自行再执行。）"
        )
        message_parts.append(f"```sql\n{preprocess_sql}\n```")
        message_parts.append("")

    # 添加清理sql
    if clean_up_sql:
        message_parts.append("## 清理sql（可选）")
        message_parts.append(
            "（不要在每次查询后自动清理；仅当需要恢复环境时自行执行。）"
        )
        message_parts.append(f"```sql\n{clean_up_sql}\n```")
        message_parts.append("")

    # 添加用户提示
    if user_prompt:
        message_parts.append("## 附加用户提示")
        message_parts.append(user_prompt)
        message_parts.append("")

    message_parts.append("请开始诊断分析。")
    
    return "\n".join(message_parts)

