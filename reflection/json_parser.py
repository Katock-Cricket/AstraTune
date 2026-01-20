import json
import re
from typing import TypedDict, Optional, Tuple
from pathlib import Path


class JsonParseResult(TypedDict):
    """JSON解析结果"""
    success: bool
    data: dict
    error: Optional[str]


def validate_schema(data: dict, schema: dict) -> Tuple[bool, Optional[str]]:
    """
    验证JSON数据是否符合schema定义的结构
    
    Args:
        data: 要验证的JSON数据
        schema: schema定义（从schema.json加载）
    
    Returns:
        tuple[bool, Optional[str]]: (是否有效, 错误信息)
    """
    errors = []
    
    # 验证所有顶层字段是否存在
    required_fields = set(schema.keys())
    data_fields = set(data.keys())
    missing_fields = required_fields - data_fields
    
    if missing_fields:
        errors.append(f"缺少必需字段: {', '.join(sorted(missing_fields))}")
    
    # 验证每个字段的类型和结构
    for field_name, expected_value in schema.items():
        if field_name not in data:
            continue  # 已经在missing_fields中处理
        
        actual_value = data[field_name]
        expected_type = type(expected_value)
        
        # 特殊处理：根据schema中的示例值推断类型要求
        if field_name == "system":
            if not isinstance(actual_value, str):
                errors.append(f"字段 '{field_name}' 应为字符串类型，实际为 {type(actual_value).__name__}")
        elif field_name == "issue_sql":
            if not isinstance(actual_value, str):
                errors.append(f"字段 '{field_name}' 应为字符串类型，实际为 {type(actual_value).__name__}")
        elif field_name == "db_id":
            if not isinstance(actual_value, str):
                errors.append(f"字段 '{field_name}' 应为字符串类型，实际为 {type(actual_value).__name__}")
        elif field_name == "tables":
            if not isinstance(actual_value, list):
                errors.append(f"字段 '{field_name}' 应为数组类型，实际为 {type(actual_value).__name__}")
            else:
                for i, item in enumerate(actual_value):
                    if not isinstance(item, str):
                        errors.append(f"字段 '{field_name}[{i}]' 应为字符串类型，实际为 {type(item).__name__}")
        elif field_name == "fields":
            if not isinstance(actual_value, list):
                errors.append(f"字段 '{field_name}' 应为数组类型，实际为 {type(actual_value).__name__}")
            else:
                for i, field_item in enumerate(actual_value):
                    if not isinstance(field_item, dict):
                        errors.append(f"字段 '{field_name}[{i}]' 应为对象类型，实际为 {type(field_item).__name__}")
                    else:
                        # 验证field对象的必需字段
                        field_required = {"table", "name", "type"}
                        field_actual = set(field_item.keys())
                        field_missing = field_required - field_actual
                        if field_missing:
                            errors.append(f"字段 '{field_name}[{i}]' 缺少必需字段: {', '.join(sorted(field_missing))}")
                        else:
                            # 验证field字段的类型
                            if "table" in field_item and not isinstance(field_item["table"], str):
                                errors.append(f"字段 '{field_name}[{i}].table' 应为字符串类型")
                            if "name" in field_item and not isinstance(field_item["name"], str):
                                errors.append(f"字段 '{field_name}[{i}].name' 应为字符串类型")
                            if "type" in field_item and not isinstance(field_item["type"], str):
                                errors.append(f"字段 '{field_name}[{i}].type' 应为字符串类型")
        elif field_name == "root_cause":
            if not isinstance(actual_value, dict):
                errors.append(f"字段 '{field_name}' 应为对象类型，实际为 {type(actual_value).__name__}")
            else:
                root_cause_required = {"category", "detail"}
                root_cause_actual = set(actual_value.keys())
                root_cause_missing = root_cause_required - root_cause_actual
                if root_cause_missing:
                    errors.append(f"字段 '{field_name}' 缺少必需字段: {', '.join(sorted(root_cause_missing))}")
                else:
                    if "category" in actual_value and not isinstance(actual_value["category"], str):
                        errors.append(f"字段 '{field_name}.category' 应为字符串类型")
                    if "detail" in actual_value and not isinstance(actual_value["detail"], str):
                        errors.append(f"字段 '{field_name}.detail' 应为字符串类型")
        elif field_name == "solution":
            if not isinstance(actual_value, dict):
                errors.append(f"字段 '{field_name}' 应为对象类型，实际为 {type(actual_value).__name__}")
            else:
                solution_required = {"category", "detail", "sol_sql"}
                solution_actual = set(actual_value.keys())
                solution_missing = solution_required - solution_actual
                if solution_missing:
                    errors.append(f"字段 '{field_name}' 缺少必需字段: {', '.join(sorted(solution_missing))}")
                else:
                    if "category" in actual_value and not isinstance(actual_value["category"], str):
                        errors.append(f"字段 '{field_name}.category' 应为字符串类型")
                    if "detail" in actual_value and not isinstance(actual_value["detail"], str):
                        errors.append(f"字段 '{field_name}.detail' 应为字符串类型")
                    if "sol_sql" in actual_value and not isinstance(actual_value["sol_sql"], str):
                        errors.append(f"字段 '{field_name}.sol_sql' 应为字符串类型")
    
    if errors:
        return False, "Schema验证失败: " + "; ".join(errors)
    
    return True, None


def parse_json(text: str, schema_path: Optional[str] = None) -> JsonParseResult:
    """
    解析一段plain text中包含的JSON文本

    Args:
        text: 包含JSON的plain text
        schema_path: 可选的schema文件路径，用于验证JSON结构

    Returns:
        JsonParseResult: JSON解析结果
    """
    if not text:
        return {
            "success": False,
            "data": {},
            "error": "输入文本为空"
        }
    
    json_candidates = []
    # 记录已添加的候选（基于规范化内容去重，忽略空白字符）
    added_candidates = set()
    
    # 1. 尝试从markdown代码块中提取JSON (```json ... ``` 或 ``` ... ```)
    markdown_json_pattern = r'```(?:json)?\s*\n?(.*?)```'
    markdown_matches = re.findall(markdown_json_pattern, text, re.DOTALL | re.IGNORECASE)
    for match in markdown_matches:
        candidate = match.strip()
        if candidate:
            candidate_normalized = re.sub(r'\s+', '', candidate)
            if candidate_normalized not in added_candidates:
                json_candidates.append(candidate)
                added_candidates.add(candidate_normalized)
    
    # 2. 同时尝试直接匹配JSON对象（可能文本中既有markdown格式也有直接格式）
    # 使用更准确的方法：找到所有可能的JSON对象开始位置，然后尝试解析
    # 从后往前查找，找到所有可能的JSON对象
    start_positions = []
    for i, char in enumerate(text):
        if char == '{':
            start_positions.append(i)
    
    # 对每个开始位置，尝试找到匹配的结束位置
    for start_idx in reversed(start_positions):
        # 从开始位置往后查找，计算括号匹配
        brace_count = 0
        in_string = False
        escape_next = False
        
        for i in range(start_idx, len(text)):
            char = text[i]
            
            if escape_next:
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                continue
            
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            
            if not in_string:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        # 找到匹配的结束位置
                        candidate = text[start_idx:i + 1].strip()
                        # 去重：如果内容相同（忽略空白），不重复添加
                        candidate_normalized = re.sub(r'\s+', '', candidate)
                        if candidate_normalized not in added_candidates:
                            json_candidates.append(candidate)
                            added_candidates.add(candidate_normalized)
                        break
    
    # 3. 尝试解析所有候选JSON，收集有效的解析结果
    valid_results = []
    last_error = None
    
    for candidate in json_candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                valid_results.append(parsed)
        except json.JSONDecodeError as e:
            last_error = str(e)
            continue
    
    # 4. 如果找到多个有效的JSON，返回最后一个
    if valid_results:
        parsed_data = valid_results[-1]
        
        # 5. 如果提供了schema_path，进行schema验证
        if schema_path:
            try:
                # 加载schema文件
                schema_file = Path(schema_path)
                if not schema_file.exists():
                    return {
                        "success": False,
                        "data": {},
                        "error": f"Schema文件不存在: {schema_path}"
                    }
                
                with open(schema_file, 'r', encoding='utf-8') as f:
                    schema = json.load(f)
                
                # 验证数据是否符合schema
                is_valid, validation_error = validate_schema(parsed_data, schema)
                if not is_valid:
                    return {
                        "success": False,
                        "data": {},
                        "error": validation_error
                    }
            except json.JSONDecodeError as e:
                return {
                    "success": False,
                    "data": {},
                    "error": f"Schema文件格式错误: {str(e)}"
                }
            except Exception as e:
                return {
                    "success": False,
                    "data": {},
                    "error": f"Schema验证过程出错: {str(e)}"
                }
        
        return {
            "success": True,
            "data": parsed_data,
            "error": None
        }
    
    # 6. 如果没找到有效的JSON，返回错误信息
    if json_candidates:
        # 返回最后一个候选的解析错误
        if last_error:
            return {
                "success": False,
                "data": {},
                "error": f"JSON解析失败: {last_error}"
            }
        else:
            return {
                "success": False,
                "data": {},
                "error": "找到JSON片段但解析失败"
            }
    
    return {
        "success": False,
        "data": {},
        "error": "未找到有效的JSON片段"
    } 