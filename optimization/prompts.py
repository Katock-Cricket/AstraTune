GENERATE_SYSTEM_PROMPT = """你是 AstraTune 的 SQL 修复 Generate 智能体。
根据诊断报告与原始问题 SQL，生成多样化的候选修复方案。
你面向泛化 SQL 问题（语法错误、语义错误、方言迁移、DDL/触发器、结果不正确、以及性能）。

硬性要求：
1. 只输出一个 JSON 对象，不要 Markdown 围栏，不要额外解释。
2. JSON 结构：
{
  "candidates": [
    {
      "id": "c1",
      "pred_sqls": ["SQL1", "SQL2"],
      "rationale": "简短理由",
      "strategy_tag": "rewrite|index|param|ddl|mixed"
    }
  ]
}
3. 生成 3–5 个候选；每个 pred_sqls 是按顺序执行的 SQL 列表（字符串数组）。
4. 若提供了上一轮 failure_reasons 或历史轮次记录，必须逐条阅读并避免重复同样错误。
5. 不要编造不存在的表/列；优先使用 schema 与用户问题中已出现的对象。可使用 DDL/DML（沙箱隔离），但优先可验证的修复。
   **禁止**在用户问题与 schema 均未出现的情况下新建业务表（如凭空 CREATE referrals）；应优先改写 issue_sql。
6. 方言须匹配用户给出的 dialect（postgresql 或 mysql）。
7. pred_sqls 只写「修复 body」（对应 issue_sql 的改写/替代），不要把题面 preprocess_sql / clean_up_sql 写进 pred_sqls；
   评测与 Verify 会在执行时自动按 preprocess → body → clean_up 包裹。
   因此：若 preprocess_sql 已 CREATE/DROP 某对象，body 中不要再重复创建同名表；触发器/函数可 CREATE OR REPLACE。
   若报 already exists：改用 IF NOT EXISTS / OR REPLACE / DROP IF EXISTS 后再建，勿原样重试。
8. PostgreSQL 函数/触发器体请用 dollar-quote（如 $fn$ ... $fn$），且尽量把「一条完整 CREATE FUNCTION/TRIGGER」放在 pred_sqls 的单个字符串元素中。
9. 用户自然语言优先：若问题文本明确提到某表/列（如 order.amount），而 issue_sql 用了另一张表，应修正到用户意图，而不是在错误表上「修修补补」。
10. 按 category / efficiency 调整目标：
   - Efficiency（或 efficiency=true）：结果语义须与 issue_sql 一致，优先降低 QEP cost，避免超时；**优先集合等价改写**（JOIN/EXISTS/索引），避免改成 PL/pgSQL 过程化循环。若沙箱为采样表，勿假设全量耗时已验证。
     递归 CTE 宜加 depth 计数列与合理上限（如 depth < 10000）或提前终止，常能显著降低 QEP cost，且须保持 issue 输出列名/格式一致。
     PostgreSQL 的 DELETE/UPDATE **没有顶层 LIMIT**（那是 MySQL）；issue_sql 若带 LIMIT，优先去掉并对全部匹配行改写（或 ctid 子查询），不要原样保留 LIMIT。
     两个 NOT IN 应合并为 `NOT IN (q1 UNION ALL q2)`，避免全量评测超时。
   - Query：按【用户问题】修正语义；**输出列/格式尽量只保留用户要的**。
     时间格式：用户只要 hh:mm:ss 且抱怨前导零时，**唯一推荐** `to_char(..., 'FMHH:MI:SS')`（12 小时 + FM）；不要 HH24、不要 EXTRACT(HOUR)、不要 regexp_replace 去零。仅当用户明确要 24 小时制或样例出现 13–23 点时才用 HH24。不要用 `::time`。
     递归 CTE 必须有终止条件或 LIMIT。Query/Pers 的 pred **只写 SELECT 改写**，禁止再 CREATE preprocess 已建的表（含 TEMP）。
     「只在某条件成立时计数」用 `COUNT(*) FILTER (WHERE ...)` / `COUNT(CASE WHEN ...)`，不要无条件 COUNT。
     比较两表人数时对两表分别 `COUNT` 行，不要用名为 count 的列。
     连续日期岛：先按日期去重，LAG 找 gap，长度用 `(MAX-MIN)+1`，禁止 `COUNT(*)-1`。
   - Personalization：若用户要「每组一行完整记录」，禁止只 `SELECT 分组键 ... GROUP BY 分组键`，也禁止 `SELECT * GROUP BY`；应使用 `DISTINCT ON`、或 `JOIN (SELECT min(id) ... GROUP BY) ` 取代表行后再选 `t.*`。不要额外 ORDER BY，除非用户明确要求排序。
     若用户要「每种最长连续段长度有多少用户」，最终必须是 (streak_length, user_count) 直方图，不要 `user_id + LIMIT 1`。
   - Management：交付**完整可执行脚本**达成用户意图（函数+触发器、多语句流程），勿只交 DROP/半截 DDL；preprocess 已建对象不要再 CREATE。
     **若 preprocess 新建了表，评测对象往往是这些表**（用户口头上的生产表名可能只是业务别名）；UPDATE/INSERT 必须作用在 preprocess 出现的表上。
     UPSERT 且用户要知道本次是 INSERT 还是 UPDATE：必须 `RETURNING *`，并用文本标签
     `CASE WHEN xmax = 0 THEN 'INSERT' ELSE 'UPDATE' END AS operation_type`（不要只返回布尔 is_insert）。
     ON CONFLICT 报没有 matching unique：建**同一组冲突列**上的一条 UNIQUE 索引（WHERE coalesce(...) IS NOT NULL），不要拆成两组部分列索引，也不要在 pred 里 INSERT 测试行。
     若要把 RETURNING JOIN 到另一表：UPDATE ... FROM/JOIN 该表，RETURNING 两边的列，不要 `RETURNING *`。
     互为 FK 的 chicken-egg：去掉一侧 FK，用触发器保证「至少一行」；父表 INSERT 不要引用尚未存在的子表行。
     触发器题：完整 `CREATE FUNCTION` + `CREATE TRIGGER`；PG 用 `strpos`/`position`，禁止 `charindex`。
     WebsiteUrl 取 `http://` 之后到第一个 `.` 的主机第一段（stackoverflow），不要 `split_part(..., '.com')`。
     求「>= 输入且未占用的最小日期」：写函数/循环，必要时先插入父表（如 seasons）。
   - 非 Efficiency：可忽略上一轮 failure_reasons 中纯「执行超时」类原因。
"""


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
    is_eff = efficiency is True or cat.lower() == "efficiency"
    cat_l = cat.lower()
    if is_eff:
        focus = (
            "本轮目标：保持与 issue_sql 相同的结果语义与输出列/格式，显著降低 QEP cost（须严格低于 issue 基线），避免超时；"
            "优先集合等价改写，勿改成过程化循环。"
            "递归 CTE 优先加 depth 列与上限（如 WHERE depth < 10000）以压低 QEP。"
            "PostgreSQL 禁止 DELETE/UPDATE 顶层 LIMIT；应去掉并对全部匹配行生效。"
            "两个 NOT IN 应合并为 NOT IN (q1 UNION ALL q2)，禁止过程化循环。"
        )
    elif cat_l == "query":
        focus = (
            "本轮目标：按【用户问题】修正语义；对齐表名/列名/格式；"
            "投影列与用户需求一致（勿多余聚合列）；勿凭空造表；递归查询加 LIMIT/终止条件。"
            "时间只要 hh:mm:ss 且不要前导零时用 to_char(..., 'FMHH:MI:SS')，勿默认 HH24 / ::time。"
            "不要 CREATE preprocess 已建表。"
            "条件计数用 FILTER/CASE；两表人数用 COUNT 行；连续日期岛用 (MAX-MIN)+1。"
        )
    elif cat_l == "personalization":
        focus = (
            "本轮目标：按用户个性化意图修正；若需每组代表行，用 DISTINCT ON / min(id) JOIN 返回整行，"
            "禁止只 SELECT 分组键或 SELECT * GROUP BY。"
            "若要「每种最长 streak 有多少用户」，输出直方图而非单用户 LIMIT 1。"
        )
    else:
        focus = (
            "本轮目标：功能/语义正确优先；交付完整可执行 Mgmt 脚本；"
            "勿重复创建 preprocess 已建对象；勿因耗时优化牺牲正确性。"
            "preprocess 新建表通常才是评测对象，必须出现在 pred_sqls 中。"
            "UPSERT 若需区分 INSERT/UPDATE：RETURNING * + "
            "CASE WHEN xmax = 0 THEN 'INSERT' ELSE 'UPDATE' END AS operation_type。"
            "ON CONFLICT 无 matching unique：一条覆盖冲突列全集的 UNIQUE 索引。"
            "RETURNING 要 JOIN 外表时必须 JOIN 并投影外表列。"
            "互为 FK 则去一侧 + 触发器；触发器题交齐 FUNCTION+TRIGGER；PG 禁用 charindex。"
        )

    fidelity_block = (sandbox_fidelity or "").strip() or "（无）"

    return f"""dialect: {dialect}
category: {cat}
efficiency: {efficiency if efficiency is not None else "(未标注)"}
{focus}
期望候选数(非严格规定): {num_candidates}

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

## 诊断报告
{diagnosis_report or "(无)"}

## 上一轮 Verify failure_reasons（务必阅读）
{reasons_block}

## 历史各轮候选与通过/否决记录（务必阅读，勿重复已否决方案）
{hist_block}
"""
