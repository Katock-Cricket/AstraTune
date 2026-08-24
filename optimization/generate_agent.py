from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from langchain_core.messages import HumanMessage, SystemMessage

from optimization.prompts import GENERATE_SYSTEM_PROMPT, build_generate_user_message
from optimization.strategy_slots import enforce_strategy_slots
from optimization.types import Candidate
from optimization.intent_spec import IntentSpec, empty_intent_spec
from utils.llm import extract_message_text, make_chat_openai
from utils.logger import default_logger
from utils.timing import timed_stage


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


class GenerateAgent:
    def __init__(self, config: Dict[str, Any]):
        self.config = config or {}
        self.llm_config = dict(self.config.get("llm") or {})
        self.num_candidates = int(self.config.get("num_candidates", 4))
        self.llm = make_chat_openai(self.llm_config, default_temperature=0.7)

    def generate(
        self,
        *,
        diagnosis_report: str,
        issue_sql: List[str] | str,
        query: str,
        schema: str,
        category: str,
        dialect: str,
        failure_reasons: Optional[List[str]] = None,
        round_history: Optional[List[Dict[str, Any]]] = None,
        efficiency: Optional[bool] = None,
        preprocess_sql: Optional[List[str] | str] = None,
        clean_up_sql: Optional[List[str] | str] = None,
        sandbox_fidelity: str = "",
        intent_spec: Optional[IntentSpec] = None,
    ) -> List[Candidate]:
        spec = intent_spec or empty_intent_spec()
        user_msg = build_generate_user_message(
            diagnosis_report=diagnosis_report,
            issue_sql=issue_sql,
            query=query,
            schema=schema,
            category=category,
            dialect=dialect,
            failure_reasons=failure_reasons,
            round_history=round_history,
            num_candidates=self.num_candidates,
            efficiency=efficiency,
            preprocess_sql=preprocess_sql,
            clean_up_sql=clean_up_sql,
            sandbox_fidelity=sandbox_fidelity,
            intent_spec_json=spec.render_for_prompt(),
        )
        with timed_stage("generate_llm", round_hint="generate"):
            resp = self.llm.invoke(
                [
                    SystemMessage(content=GENERATE_SYSTEM_PROMPT),
                    HumanMessage(content=user_msg),
                ]
            )
        content = extract_message_text(resp)
        obj = _extract_json_object(content)
        if not obj:
            preview = content[:500] if content else "(空)"
            default_logger.error(f"Generate JSON 解析失败，输出预览: {preview}")
            raise ValueError("Generate 输出无法解析为 JSON")

        raw_list = obj.get("candidates")
        if not isinstance(raw_list, list) or not raw_list:
            raise ValueError("Generate 未返回 candidates 列表")

        candidates: List[Candidate] = []
        for i, item in enumerate(raw_list[: max(3, self.num_candidates) + 2]):
            if not isinstance(item, dict):
                continue
            pred = _normalize_pred_sqls(item.get("pred_sqls") or item.get("sql") or item.get("sqls"))
            if not pred:
                continue
            cid = str(item.get("id") or f"c{i + 1}")
            candidates.append(
                Candidate(
                    id=cid,
                    pred_sqls=pred,
                    rationale=str(item.get("rationale") or ""),
                    strategy_tag=str(item.get("strategy_tag") or ""),
                )
            )

        if not candidates:
            raise ValueError("Generate 候选全部无效（无 pred_sqls）")

        candidates = enforce_strategy_slots(
            candidates,
            category=category,
            efficiency=efficiency,
        )
        return candidates[: self.num_candidates] if len(candidates) > self.num_candidates else candidates
