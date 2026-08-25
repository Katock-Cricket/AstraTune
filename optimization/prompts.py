def build_generate_system_prompt(*, function: str = "general", has_diagnosis: bool = True) -> str:
    fn = (function or "general").strip().lower()
    diag_line = (
        "根据 Intent Spec、诊断报告与 issue_sql 生成多样化候选修复方案。"
        if has_diagnosis
        else "根据 Intent Spec、用户问题与 issue_sql 生成多样化候选（**无诊断报告**，勿臆造诊断）。"
    )
    eff_extra = ""
    if fn == "efficiency":
        eff_extra = (
            "\n12. function=efficiency：必须集合等价改写；优先降 cost/latency；"
            "禁止改变投影语义；勿引入与题意无关的 DDL。"
        )
    return f"""你是 AstraTune 的 SQL 修复 Generate 智能体。
{diag_line}

硬性要求：
1. 只输出一个 JSON 对象，不要 Markdown 围栏，不要额外解释。
2. JSON 结构：
{{
  "candidates": [
    {{
      "id": "c1",
      "pred_sqls": ["SQL1", "SQL2"],
      "rationale": "简短理由",
      "strategy_tag": "rewrite|ddl|mixed|represent_row|set_equiv"
    }}
  ]
}}
3. 生成 3–5 个候选；pred_sqls 为按序执行的 body SQL 列表（勿含 preprocess/clean_up）。
4. 阅读 failure_reasons 与 round_history，避免重复已否决方案。
5. 不要编造 schema/用户问题中不存在的表/列；禁止凭空 CREATE 业务表。
6. 方言须匹配 dialect（postgresql 或 mysql）。
7. preprocess 已建对象勿重复 CREATE；用 IF NOT EXISTS / OR REPLACE / DROP IF EXISTS。
8. PG 函数/触发器用 dollar-quote；完整 CREATE FUNCTION/TRIGGER 放单字符串元素。
9. 用户自然语言与 issue_sql 冲突时，以用户问题与 Intent Spec 为准。
10. 类目通则：
    - Efficiency：集合等价改写降 cost，避免过程化循环；PG 无 DELETE/UPDATE 顶层 LIMIT；双 NOT IN 合并 UNION ALL。
    - Query：语义对齐用户问题；投影列与 Spec 一致；递归加终止条件。
    - Personalization：每组代表行用 DISTINCT ON 或 min(id) JOIN，勿只 SELECT 分组键。
    - Management：交付完整可执行脚本；preprocess 新建表常为评测对象。
11. 详细约束以 ## Intent Spec (JSON) 为准；strategy_tag 须与修复策略一致。{eff_extra}
"""


# 向后兼容：默认 system prompt
GENERATE_SYSTEM_PROMPT = build_generate_system_prompt()


def build_generate_user_message(
    *,
    diagnosis_report: str,
    issue_sql: list[str] | str,
    query: str,
    schema: str,
    category: str,
    dialect: str,
    failure_reasons: list[str] | None,
    round_history: list[dict] | None = None,
    num_candidates: int,
    efficiency: bool | None = None,
    preprocess_sql: list[str] | str | None = None,
    clean_up_sql: list[str] | str | None = None,
    sandbox_fidelity: str | None = None,
    intent_spec_json: str | None = None,
    function: str = "general",
) -> str:
    if isinstance(issue_sql, str):
        issue_sql_text = issue_sql
    else:
        issue_sql_text = "\n---\n".join(issue_sql or [])

    def _block(sqls: list[str] | str | None) -> str:
        if not sqls:
            return "（无）"
        if isinstance(sqls, str):
            return sqls.strip() or "（无）"
        return "\n---\n".join(str(x).strip() for x in sqls if str(x).strip()) or "（无）"

    reasons = failure_reasons or []
    reasons_block = (
        "\n".join(f"- {r}" for r in reasons) if reasons else "（无，首轮或未汇总）"
    )
    hist = round_history or []
    if hist:
        hist_lines = []
        for h in hist:
            rnd = h.get("round")
            hist_lines.append(f"### 第 {rnd} 轮")
            for c in h.get("candidates") or []:
                cid = c.get("id")
                passed = c.get("rules_passed")
                fb = c.get("agent_feedback") or ""
                errs = c.get("errors") or []
                hist_lines.append(
                    f"- {cid}: rules_passed={passed}; errors={errs[:3]}; agent_feedback={fb[:200]}"
                )
            fr = h.get("failure_reasons") or []
            if fr:
                hist_lines.append("failure_reasons: " + "; ".join(fr[:5]))
        hist_block = "\n".join(hist_lines)
    else:
        hist_block = "（无，首轮生成）"

    cat = (category or "").strip() or "Unknown"
    fn = (function or "general").strip().lower()
    is_eff = efficiency is True or cat.lower() == "efficiency" or fn == "efficiency"
    if is_eff:
        focus = "Efficiency：保持 issue 语义，显著降 QEP cost，避免超时。"
    elif cat.lower() == "query":
        focus = "Query：按 Intent Spec 与用户问题修正语义与投影。"
    elif cat.lower() == "personalization":
        focus = "Personalization：按 Spec 返回每组代表完整行。"
    else:
        focus = "Management：交付完整可执行脚本，对齐 Spec 中 mgmt_artifacts。"

    spec_block = (intent_spec_json or "").strip() or "（无，按诊断与用户问题推断）"
    fidelity_block = (sandbox_fidelity or "").strip() or "（无）"
    diag = (diagnosis_report or "").strip()
    if diag:
        diag_section = f"## 诊断报告\n{diag}"
    else:
        diag_section = (
            "## 诊断报告\n"
            "（本轮无诊断；仅依据用户问题、issue_sql、schema 与 Intent Spec 生成。）"
        )

    return f"""dialect: {dialect}
category: {cat}
function: {fn}
efficiency: {efficiency if efficiency is not None else "(未标注)"}
{focus}
期望候选数(非严格规定): {num_candidates}

## Intent Spec (JSON)（硬约束，优先于泛化经验）
{spec_block}

## 用户问题（意图以本段为准；表名/列名冲突时优先信这里）
{query or "(无)"}

## issue_sql（待修复 body；Verify/官方评测会在其前后自动包裹前置/清理 SQL）
{issue_sql_text}

## preprocess_sql（环境准备，勿写入 pred_sqls；其中已创建的对象不要再 CREATE）
{_block(preprocess_sql)}

## clean_up_sql（环境恢复，勿写入 pred_sqls）
{_block(clean_up_sql)}

## 沙箱保真度（相对 target；采样/统计注入失败时勿过度自信）
{fidelity_block}

## schema
{schema or "(无)"}

{diag_section}

## 上一轮 Verify failure_reasons（务必阅读）
{reasons_block}

## 历史各轮候选与通过/否决记录（务必阅读，勿重复已否决方案）
{hist_block}
"""
