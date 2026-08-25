"""结构化 Intent Spec：Diag 与 Generate 之间的意图规格（fail-open）。"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from langchain_core.messages import HumanMessage, SystemMessage

from utils.llm import extract_message_text, make_chat_openai
from utils.logger import default_logger

_SPEC_EXTRACT_SYSTEM = """你是 SQL 修复意图分析器。根据用户问题、诊断报告与 schema，输出一个 JSON 对象（不要 Markdown 围栏）。
字段（均可为空列表/空串/false）：
{
  "goal": "query|mgmt|pers|eff",
  "output_columns": [{"name": "列名或别名", "role": "group_key|metric|payload"}],
  "must_predicates": ["自然语言约束"],
  "agg_filters": [{"text": "描述", "column": "列名或null", "unresolved": false}],
  "mgmt_artifacts": ["function","trigger","unique_index"],
  "forbidden": ["禁止行为"],
  "evidence": [{"table": "表名", "columns_seen": ["列"], "sample_note": ""}],
  "pers_need_full_row": false
}
不要编造 schema 中不存在的表/列；无法绑定时设 unresolved=true。"""


@dataclass
class OutputColumn:
    name: str
    role: str = ""


@dataclass
class AggFilter:
    text: str
    column: Optional[str] = None
    unresolved: bool = False


@dataclass
class Evidence:
    table: str
    columns_seen: List[str] = field(default_factory=list)
    sample_note: str = ""


@dataclass
class IntentSpec:
    goal: str = ""
    output_columns: List[OutputColumn] = field(default_factory=list)
    must_predicates: List[str] = field(default_factory=list)
    agg_filters: List[AggFilter] = field(default_factory=list)
    mgmt_artifacts: List[str] = field(default_factory=list)
    forbidden: List[str] = field(default_factory=list)
    evidence: List[Evidence] = field(default_factory=list)
    pers_need_full_row: bool = False

    def is_empty(self) -> bool:
        return not (
            self.goal
            or self.output_columns
            or self.must_predicates
            or self.agg_filters
            or self.mgmt_artifacts
            or self.forbidden
            or self.evidence
            or self.pers_need_full_row
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "goal": self.goal,
            "output_columns": [asdict(c) for c in self.output_columns],
            "must_predicates": list(self.must_predicates),
            "agg_filters": [asdict(f) for f in self.agg_filters],
            "mgmt_artifacts": list(self.mgmt_artifacts),
            "forbidden": list(self.forbidden),
            "evidence": [asdict(e) for e in self.evidence],
            "pers_need_full_row": self.pers_need_full_row,
        }

    def summary(self) -> Dict[str, Any]:
        return {
            "goal": self.goal,
            "output_columns_n": len(self.output_columns),
            "agg_filters_n": len(self.agg_filters),
            "mgmt_artifacts_n": len(self.mgmt_artifacts),
            "pers_need_full_row": self.pers_need_full_row,
            "empty": self.is_empty(),
        }

    def render_for_prompt(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


def empty_intent_spec() -> IntentSpec:
    return IntentSpec()


def _parse_output_columns(raw: Any) -> List[OutputColumn]:
    out: List[OutputColumn] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if isinstance(item, dict) and item.get("name"):
            out.append(
                OutputColumn(
                    name=str(item["name"]).strip(),
                    role=str(item.get("role") or "").strip(),
                )
            )
    return out


def _parse_agg_filters(raw: Any) -> List[AggFilter]:
    out: List[AggFilter] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if isinstance(item, str) and item.strip():
            out.append(AggFilter(text=item.strip()))
        elif isinstance(item, dict) and (item.get("text") or item.get("column")):
            col = item.get("column")
            out.append(
                AggFilter(
                    text=str(item.get("text") or col or "").strip(),
                    column=str(col).strip() if col else None,
                    unresolved=bool(item.get("unresolved")),
                )
            )
    return out


def _parse_evidence(raw: Any) -> List[Evidence]:
    out: List[Evidence] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if isinstance(item, dict) and item.get("table"):
            cols = item.get("columns_seen") or []
            if isinstance(cols, str):
                cols = [cols]
            out.append(
                Evidence(
                    table=str(item["table"]).strip(),
                    columns_seen=[str(c).strip() for c in cols if str(c).strip()],
                    sample_note=str(item.get("sample_note") or "").strip(),
                )
            )
    return out


def parse_intent_spec(data: Any) -> IntentSpec:
    """解析 JSON/dict → IntentSpec；失败返回空 Spec（fail-open）。"""
    if data is None:
        return empty_intent_spec()
    if isinstance(data, str):
        try:
            data = json.loads(data.strip())
        except json.JSONDecodeError:
            return empty_intent_spec()
    if not isinstance(data, dict):
        return empty_intent_spec()
    try:
        preds = data.get("must_predicates") or []
        if isinstance(preds, str):
            preds = [preds]
        artifacts = data.get("mgmt_artifacts") or []
        if isinstance(artifacts, str):
            artifacts = [artifacts]
        forbidden = data.get("forbidden") or []
        if isinstance(forbidden, str):
            forbidden = [forbidden]
        return IntentSpec(
            goal=str(data.get("goal") or "").strip(),
            output_columns=_parse_output_columns(data.get("output_columns")),
            must_predicates=[str(p).strip() for p in preds if str(p).strip()],
            agg_filters=_parse_agg_filters(data.get("agg_filters")),
            mgmt_artifacts=[str(a).strip() for a in artifacts if str(a).strip()],
            forbidden=[str(f).strip() for f in forbidden if str(f).strip()],
            evidence=_parse_evidence(data.get("evidence")),
            pers_need_full_row=bool(data.get("pers_need_full_row")),
        )
    except Exception as e:
        default_logger.debug(f"IntentSpec 解析失败: {e}")
        return empty_intent_spec()


def _goal_from_category(category: str, efficiency: Optional[bool]) -> str:
    cat = (category or "").strip().lower()
    if efficiency is True or cat == "efficiency":
        return "eff"
    if cat == "management":
        return "mgmt"
    if cat == "personalization":
        return "pers"
    return "query"


def extract_intent_spec_heuristic(
    *,
    query: str,
    category: str,
    diagnosis_report: str,
    schema: str,
    efficiency: Optional[bool] = None,
    issue_sql: Optional[List[str]] = None,
) -> IntentSpec:
    """无 LLM 的启发式 Spec（测试与 fail-open 兜底）。"""
    q = (query or "").lower()
    diag = diagnosis_report or ""
    spec = IntentSpec(goal=_goal_from_category(category, efficiency))

    # Pers：代表行
    if spec.goal == "pers" and re.search(
        r"each|every|per\s+\w+|one row|完整|代表|group|unique", q, re.I
    ):
        spec.pers_need_full_row = True

    # 条件计数 → agg_filter
    if re.search(r"only if|only when|activated|premium|referral", q, re.I):
        spec.agg_filters.append(
            AggFilter(text="conditional count filter", unresolved=True)
        )

    # Mgmt artifacts（题面 + issue_sql）
    if spec.goal == "mgmt":
        from optimization.sql_shape import infer_mgmt_artifacts

        spec.mgmt_artifacts = infer_mgmt_artifacts(query or "", issue_sql)

    # schema 表名列名 → evidence
    for m in re.finditer(
        r"--\s*表:\s*(\w+)|CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
        schema or "",
        re.I,
    ):
        tbl = m.group(1) or m.group(2)
        if tbl:
            spec.evidence.append(Evidence(table=tbl.lower(), columns_seen=[]))

    # 诊断中提到的列
    for m in re.finditer(r"`(\w+)`|列\s*[「\"]?(\w+)[」\"]?", diag):
        col = m.group(1) or m.group(2)
        if col and spec.evidence:
            spec.evidence[0].columns_seen.append(col.lower())

    from optimization.sql_shape import named_output_columns, preferred_tables_from_diagnosis

    for name in named_output_columns(query or ""):
        spec.output_columns.append(OutputColumn(name=name, role="payload"))

    for tbl in preferred_tables_from_diagnosis(diag, issue_sql, schema or ""):
        spec.evidence.append(
            Evidence(table=tbl, columns_seen=[], sample_note="diag_preferred")
        )

    return spec


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
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


def extract_intent_spec_llm(
    *,
    query: str,
    category: str,
    diagnosis_report: str,
    schema: str,
    dialect: str,
    llm_config: Dict[str, Any],
) -> IntentSpec:
    """LLM 抽取 Intent Spec；失败返回空 Spec。"""
    try:
        llm = make_chat_openai(llm_config or {}, default_temperature=0.2)
        user = f"""dialect: {dialect}
category: {category}

## 用户问题
{query or "(无)"}

## 诊断报告
{diagnosis_report or "(无)"}

## schema（节选）
{(schema or "")[:4000]}
"""
        resp = llm.invoke(
            [SystemMessage(content=_SPEC_EXTRACT_SYSTEM), HumanMessage(content=user)]
        )
        obj = _extract_json_object(extract_message_text(resp))
        if obj:
            return parse_intent_spec(obj)
    except Exception as e:
        default_logger.warning(f"IntentSpec LLM 抽取失败: {e}")
    return empty_intent_spec()


def extract_intent_spec(
    *,
    query: str,
    category: str,
    diagnosis_report: str,
    schema: str,
    dialect: str = "postgresql",
    efficiency: Optional[bool] = None,
    llm_config: Optional[Dict[str, Any]] = None,
    use_llm: bool = True,
    issue_sql: Optional[List[str]] = None,
) -> IntentSpec:
    """合并启发式 + 可选 LLM。类目 goal / 诊断换表 / 点名输出列以启发式为准。"""
    base = extract_intent_spec_heuristic(
        query=query,
        category=category,
        diagnosis_report=diagnosis_report,
        schema=schema,
        efficiency=efficiency,
        issue_sql=issue_sql,
    )
    if not use_llm or not llm_config:
        return base
    llm_spec = extract_intent_spec_llm(
        query=query,
        category=category,
        diagnosis_report=diagnosis_report,
        schema=schema,
        dialect=dialect,
        llm_config=llm_config,
    )
    if llm_spec.is_empty():
        return base
    merged = llm_spec
    merged.goal = base.goal
    if base.pers_need_full_row:
        merged.pers_need_full_row = True
    if base.agg_filters and not merged.agg_filters:
        merged.agg_filters = list(base.agg_filters)
    seen_art = {a.lower() for a in merged.mgmt_artifacts}
    for a in base.mgmt_artifacts:
        if a.lower() not in seen_art:
            merged.mgmt_artifacts.append(a)
            seen_art.add(a.lower())
    if base.output_columns:
        merged.output_columns = list(base.output_columns)
    pref = [e for e in base.evidence if e.sample_note == "diag_preferred"]
    if pref:
        have = {e.table.lower() for e in merged.evidence}
        for e in pref:
            if e.table.lower() not in have:
                merged.evidence.append(e)
    elif base.evidence and not merged.evidence:
        merged.evidence = list(base.evidence)
    return merged
