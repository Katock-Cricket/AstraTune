"""Verify Agent：在确定性规则信号之上做灵活评判与反馈。"""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, List, Optional

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool

from optimization.types import Candidate, CandidateVerifyResult
from utils.llm import extract_message_text, make_chat_openai
from utils.logger import default_logger
from utils.timing import timed_stage

VERIFY_AGENT_SYSTEM = """你是 AstraTune 的 SQL 修复 Verify 智能体。
你看不到官方 sol_sql / test_cases。根据用户题面、候选 SQL、确定性规则信号、历史轮次与沙箱保真说明，判断候选是否值得交给评测。

硬性要求：
1. 只输出一个 JSON 对象，不要 Markdown 围栏。
2. 结构：
{
  "verdicts": [
    {
      "id": "c1",
      "pass": true,
      "feedback": "简短中文：否决/通过理由与给 Generate 的修改建议"
    }
  ]
}
3. 对 rules_passed=false 的候选：通常保持 pass=false，除非信号明显误报；不可把「执行失败」改成通过。
4. 对 rules_passed=true 的候选：重点抓假过——投影/时间格式、答错题（乱造表/改错对象）、Management 脚本残缺（缺 TRIGGER/互为 FK/ON CONFLICT 索引列集不对/RETURNING 未 JOIN）、Pers 非代表行或该直方图却输出单用户、Efficiency 在采样下虚假降本或双 NOT IN 未合并。
5. 若沙箱有采样表：不要仅因沙箱跑得快就通过 Efficiency；提示 Generate 用集合等价改写而非过程化重写。
6. 禁止建议查阅标准答案；反馈必须可操作。
7. 若绑定 reset_sandbox_environment 工具：仅当规则错误表明沙箱基表/schema 不可逆损坏且 clean_up 无法恢复时才调用；每轮最多一次。
"""

_SANDBOX_BROKEN_HINTS = (
    "does not exist",
    "undefined table",
    "unknown table",
    "no such table",
    "current transaction is aborted",
    "in failed sql transaction",
    "can't find file",
    "failed to find tablespace",
)


def sandbox_signals_look_broken(signals_text: str) -> bool:
    """仅当规则信号像沙箱基表/事务损坏时，才值得走 reset 探测 LLM。"""
    blob = signals_text or ""
    low = blob.lower()
    if "执行失败" not in blob and "does not exist" not in low:
        return False
    return any(k in low for k in _SANDBOX_BROKEN_HINTS)


VERIFY_FINAL_SYSTEM = """你是 AstraTune 的 SQL 修复最终仲裁 Verify 智能体。
多轮 Generate–Verify 已耗尽轮次，尚无通过候选。请综合各轮候选与否决记录，选出最可能通过官方评测的一组 pred_sqls。

硬性要求：
1. 只输出 JSON：{"pred_sqls":["SQL1",...],"rationale":"简短中文"}
2. 优先：无执行失败、Soft EX 通过、Verify 反馈较少硬伤；Efficiency 看 cost 相对基线。
3. 禁止输出 sol_sql；pred_sqls 为按序执行的 body 列表（不含 preprocess/clean_up）。
4. 若所有候选均劣，可基于 issue_sql 做最小保守修复，但须在 rationale 说明。
"""


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


class VerifyAgent:
    def __init__(
        self,
        config: Dict[str, Any],
        *,
        reset_fn: Optional[Callable[..., str]] = None,
    ):
        self.config = config or {}
        self.reset_fn = reset_fn
        llm_cfg = dict(self.config.get("llm") or {})
        # 允许 optimization.verify.llm 覆盖
        verify_block = self.config.get("verify") if isinstance(self.config.get("verify"), dict) else {}
        if verify_block.get("llm"):
            llm_cfg = dict(verify_block.get("llm") or {})
        self.llm = make_chat_openai(llm_cfg, default_temperature=0.1)
        self._reset_tool = self._build_reset_tool() if reset_fn else None

    def _build_reset_tool(self):
        from optimization.verify_tools import create_verify_reset_tool

        fn = create_verify_reset_tool(self.reset_fn)
        return tool(fn)

    def _maybe_reset_via_tool(self, *, signals_text: str) -> Optional[str]:
        """允许 Verify Agent 在评判前最多调用一次 reset 工具。"""
        if not self._reset_tool:
            return None
        llm_tools = self.llm.bind_tools([self._reset_tool])
        probe = f"""以下为本轮候选规则信号。若且仅当沙箱环境不可逆损坏且 clean_up 无法恢复，可调用 reset_sandbox_environment（最多一次）；否则直接回复「无需 reset」。

{signals_text}
"""
        try:
            with timed_stage("verify_agent_reset_probe"):
                resp = llm_tools.invoke(
                    [
                        SystemMessage(content=VERIFY_AGENT_SYSTEM),
                        HumanMessage(content=probe),
                    ]
                )
            tool_calls = getattr(resp, "tool_calls", None) or []
            if not tool_calls:
                return None
            tc = tool_calls[0]
            name = tc.get("name") if isinstance(tc, dict) else getattr(tc, "name", "")
            if name != "reset_sandbox_environment":
                return None
            args = tc.get("args") if isinstance(tc, dict) else getattr(tc, "args", {})
            reason = (args or {}).get("reason") or ""
            reason_l = str(reason).lower()
            if (
                "无需" in str(reason)
                or "不需要" in str(reason)
                or "no need" in reason_l
                or "not needed" in reason_l
            ):
                default_logger.info("VerifyAgent reset 探测：模型写了无需 reset，跳过工具")
                return None
            result = self._reset_tool.invoke({"reason": reason})
            default_logger.warning(f"VerifyAgent 调用 reset: {reason[:120]} -> {result[:120]}")
            return str(result)
        except Exception as e:
            default_logger.debug(f"VerifyAgent reset 探测跳过: {e}")
            return None

    def review(
        self,
        *,
        candidates: List[Candidate],
        per_candidate: List[CandidateVerifyResult],
        query: str,
        category: str,
        dialect: str,
        issue_sql: List[str],
        sandbox_fidelity: str = "",
        efficiency: Optional[bool] = None,
        round_history: Optional[List[Dict[str, Any]]] = None,
        allow_reset: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        返回 id -> {pass: bool, feedback: str}
        失败时返回空 dict（调用方保持规则结果）。
        """
        if not candidates:
            return {}
        by_id = {c.id: c for c in candidates}
        signals = []
        for cvr in per_candidate:
            cand = by_id.get(cvr.id)
            pred = cand.pred_sqls if cand else []
            signals.append(
                {
                    "id": cvr.id,
                    "rules_passed": cvr.passed,
                    "soft_ex": cvr.soft_ex,
                    "cost": cvr.cost,
                    "latency_ms": cvr.latency_ms,
                    "errors": list(cvr.errors or []),
                    "pred_sqls": pred,
                    "rationale": (cand.rationale if cand else "")[:300],
                }
            )

        signals_json = json.dumps(signals, ensure_ascii=False, indent=2)
        if allow_reset and self._reset_tool and sandbox_signals_look_broken(signals_json):
            self._maybe_reset_via_tool(signals_text=signals_json)
        elif allow_reset and self._reset_tool:
            default_logger.info("VerifyAgent 跳过 reset 探测（信号未显示沙箱损坏）")

        hist = round_history or []
        hist_block = (
            json.dumps(hist, ensure_ascii=False, indent=2) if hist else "（首轮）"
        )

        user = f"""dialect: {dialect}
category: {category}
efficiency: {efficiency}

## 用户问题
{query or "(无)"}

## issue_sql
{chr(10).join(issue_sql) if issue_sql else "(无)"}

## 沙箱保真度
{sandbox_fidelity or "(无)"}

## 历史轮次（各轮候选与通过/否决摘要）
{hist_block}

## 本轮候选与规则信号（JSON）
{signals_json}
"""
        try:
            with timed_stage("verify_agent_llm"):
                resp = self.llm.invoke(
                    [
                        SystemMessage(content=VERIFY_AGENT_SYSTEM),
                        HumanMessage(content=user),
                    ]
                )
            content = extract_message_text(resp)
            obj = _extract_json_object(content)
            if not obj:
                default_logger.warning("VerifyAgent JSON 解析失败，保留规则结果")
                return {}
            out: Dict[str, Dict[str, Any]] = {}
            for item in obj.get("verdicts") or []:
                if not isinstance(item, dict):
                    continue
                cid = str(item.get("id") or "")
                if not cid:
                    continue
                out[cid] = {
                    "pass": bool(item.get("pass")),
                    "feedback": str(item.get("feedback") or "").strip(),
                }
            return out
        except Exception as e:
            default_logger.warning(f"VerifyAgent 调用失败，保留规则结果: {e}")
            return {}

    def select_final(
        self,
        *,
        round_history: List[Dict[str, Any]],
        issue_sql: List[str],
        query: str,
        category: str,
        dialect: str,
        sandbox_fidelity: str = "",
        efficiency: Optional[bool] = None,
    ) -> Optional[List[str]]:
        """轮次耗尽时综合各轮候选与否决记录，产出最终 pred_sqls。"""
        if not round_history:
            return None
        user = f"""dialect: {dialect}
category: {category}
efficiency: {efficiency}

## 用户问题
{query or "(无)"}

## issue_sql
{chr(10).join(issue_sql) if issue_sql else "(无)"}

## 沙箱保真度
{sandbox_fidelity or "(无)"}

## 各轮 Generate–Verify 记录（JSON）
{json.dumps(round_history, ensure_ascii=False, indent=2)}
"""
        try:
            resp = self.llm.invoke(
                [
                    SystemMessage(content=VERIFY_FINAL_SYSTEM),
                    HumanMessage(content=user),
                ]
            )
            content = extract_message_text(resp)
            obj = _extract_json_object(content)
            if not obj:
                return None
            raw = obj.get("pred_sqls") or obj.get("sqls") or obj.get("sql")
            if isinstance(raw, str):
                parts = [p.strip() for p in raw.split(";") if p.strip()]
                return parts or ([raw.strip()] if raw.strip() else None)
            if isinstance(raw, list):
                out = [str(x).strip() for x in raw if str(x).strip()]
                return out or None
            return None
        except Exception as e:
            default_logger.warning(f"VerifyAgent select_final 失败: {e}")
            return None
