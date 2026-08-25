"""llm_only 装配：单次 LLM 直接产出 pred_sqls（无诊断 ReAct、无 Generate–Verify）。"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Sequence, Union

from langchain_core.messages import HumanMessage, SystemMessage

from utils.llm import extract_message_text, make_chat_openai
from utils.timing import timed_stage

_LLM_ONLY_SYSTEM = """你是 SQL 改写助手。根据用户问题、schema 与 issue SQL，输出修复/优化后的 SQL。
只输出一个 JSON 对象，不要 Markdown 围栏：
{"pred_sqls": ["SQL1", "SQL2"], "rationale": "简短理由"}
pred_sqls 为按序执行的 body SQL 列表；不要编造 schema 中不存在的表/列。
方言须匹配 dialect。Efficiency 题须保持结果语义集合等价并尽量降低开销。"""


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    if not text or not isinstance(text, str):
        return None
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass
    for m in re.finditer(r"\{[\s\S]*\}", s):
        try:
            obj = json.loads(m.group())
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    return None


def _normalize_pred_sqls(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(";") if p.strip()]
        return parts or ([raw.strip()] if raw.strip() else [])
    if isinstance(raw, list):
        out: List[str] = []
        for item in raw:
            if item is None:
                continue
            s = str(item).strip()
            if s:
                out.append(s)
        return out
    return [str(raw).strip()] if str(raw).strip() else []


def run_llm_only(
    *,
    issue_sql: Union[str, Sequence[str]],
    query: str,
    schema: str,
    dialect: str = "postgresql",
    category: str = "Query",
    function: str = "general",
    llm_config: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """单次 LLM → pred_sqls；解析失败抛 ValueError。"""
    if isinstance(issue_sql, str):
        sql_text = issue_sql
    else:
        sql_text = "\n---\n".join(str(x).strip() for x in issue_sql if str(x).strip())
    focus = (
        "Efficiency：保持语义集合等价，降低耗时/QEP cost。"
        if str(function).lower() == "efficiency" or str(category).lower() == "efficiency"
        else "按用户问题修复语义/语法/投影。"
    )
    user = f"""dialect: {dialect}
category: {category}
function: {function}
{focus}

## 用户问题
{query or "(无)"}

## issue_sql
{sql_text}

## schema
{schema or "(无)"}
"""
    llm = make_chat_openai(dict(llm_config or {}), default_temperature=0.3)
    with timed_stage("llm_only"):
        resp = llm.invoke(
            [SystemMessage(content=_LLM_ONLY_SYSTEM), HumanMessage(content=user)]
        )
    content = extract_message_text(resp)
    obj = _extract_json_object(content)
    if not obj:
        raise ValueError("llm_only: 输出无法解析为 JSON")
    pred = _normalize_pred_sqls(obj.get("pred_sqls") or obj.get("sql") or obj.get("sqls"))
    if not pred:
        raise ValueError("llm_only: 空 pred_sqls")
    return pred
