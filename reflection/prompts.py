import json

def create_system_prompt() -> str:
    schema = ""
    with open("reflection/schema.json", "r", encoding="utf-8") as f:
        schema = json.load(f)
    return f"""
你是一个专业的数据库性能诊断专家兼该领域知识图谱管理专家，专门负责从数据库SQL修复记录中分析和提取诊断/修复经验。

## 你的任务
分析给定的SQL修复记录，按Schema中的要求提取结构化的诊断/修复经验。

## 输出格式
返回JSON，严格按照如下Schema格式返回，请勿返回任何其他内容。
```json
{schema}
```

## 注意事项
- 保持专业和严谨，基于数据和证据进行分析
- schema中要求填写的字段都务必填写，不要遗漏。
    """

def create_initial_message(
    sql: str | list[str], 
    sol_sql: str | list[str],
    schema: str = None, 
    tables: list = None, 
    exec_log: str = None, 
    report: str = None, 
) -> str:

    message_parts = [
        "请基于以下修复记录，按schema中的要求从中提取结构化的诊断/修复经验。",
        "",
        "## 原问题SQL",
        f"```sql\n{sql}\n```",
        "",
        "## 修复SQL",
        f"```sql\n{sol_sql}\n```",
    ]

    if schema:
        message_parts.append(f"## 相关表结构（DDL）\n```sql\n{schema}\n```")
    if tables:
        message_parts.append(f"## 相关表信息\n涉及的表: {', '.join(tables)}")
    if exec_log:
        message_parts.append(f"## 相关执行日志\n{exec_log}")
    if report:
        message_parts.append(f"## 修复报告\n{report}")
    
    message_parts.append("请开始分析提取。")

    return "\n".join(message_parts)