from __future__ import annotations

import json
import re
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from optimization.types import Candidate, CandidateVerifyResult, VerifyReport
from utils.logger import default_logger
from utils.sql_wrap import as_sql_list, has_cleanup, wrap_with_pre_post
from utils.timing import log_timing, timed_stage


def normalize_row(row: Any) -> Tuple[Any, ...]:
    """将一行结果规范化为可哈希、可比较的元组（忽略 dict 键序）。"""
    if isinstance(row, dict):
        items = sorted((str(k), _cell(v)) for k, v in row.items())
        return tuple(items)
    if isinstance(row, (list, tuple)):
        return tuple(_cell(v) for v in row)
    return (_cell(row),)


def _cell(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return str(v)
    return str(v)


def soft_ex_equal(rows_a: Sequence[Any], rows_b: Sequence[Any]) -> bool:
    """集合语义比较：忽略行序与重复次数差异时仍用 Counter 更稳；此处用 multiset。"""
    from collections import Counter

    ca = Counter(normalize_row(r) for r in rows_a)
    cb = Counter(normalize_row(r) for r in rows_b)
    return ca == cb


def extract_pg_total_cost(explain_payload: Any) -> Optional[float]:
    """从 PG EXPLAIN (FORMAT JSON) 结果抽取 Total Cost。"""
    data = explain_payload
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return None
    if isinstance(data, list) and data:
        plan = data[0].get("Plan") if isinstance(data[0], dict) else None
        if isinstance(plan, dict) and "Total Cost" in plan:
            try:
                return float(plan["Total Cost"])
            except (TypeError, ValueError):
                return None
    if isinstance(data, dict):
        plan = data.get("Plan")
        if isinstance(plan, dict) and "Total Cost" in plan:
            try:
                return float(plan["Total Cost"])
            except (TypeError, ValueError):
                return None
    return None


def extract_mysql_query_cost(explain_payload: Any) -> Optional[float]:
    """从 MySQL EXPLAIN FORMAT=JSON 抽取 query_cost。"""
    data = explain_payload
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return None
    if isinstance(data, list) and data:
        # pymysql 可能返回 [{'EXPLAIN': '{...json...}'}] 或直接 dict
        first = data[0]
        if isinstance(first, dict):
            for key in ("EXPLAIN", "explain", "json"):
                if key in first and isinstance(first[key], str):
                    try:
                        data = json.loads(first[key])
                        break
                    except json.JSONDecodeError:
                        pass
            else:
                if "query_block" in first:
                    data = first
    if not isinstance(data, dict):
        return None
    cost_info = data.get("query_block", {}).get("cost_info", {})
    if not isinstance(cost_info, dict):
        return None
    raw = cost_info.get("query_cost")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def extract_qep_cost(explain_payload: Any, dialect: str) -> Optional[float]:
    d = (dialect or "").lower()
    if d in ("mysql",):
        return extract_mysql_query_cost(explain_payload)
    return extract_pg_total_cost(explain_payload)


def _is_selectish(sql: str) -> bool:
    s = sql.strip().lstrip("(").upper()
    return s.startswith(("SELECT", "WITH", "SHOW", "EXPLAIN", "DESC", "DESCRIBE"))


# 规则函数体已迁移至 optimization/rules/（common / category / flash_overfit），
# 此处再导出以保持 from optimization.verify import <fn> 的向后兼容（含 tests/test_verify_rules.py）。
from optimization.rules import (  # noqa: E402
    RuleContext,
    circular_fk_chicken_egg,
    conditional_count_without_filter,
    consecutive_date_island_count,
    efficiency_merge_notin,
    extract_preprocess_created_tables,
    mgmt_trigger_incomplete,
    missing_intent_tables,
    missing_preprocess_created_tables,
    mysql_dialect_funcs_in_pg,
    next_free_date_mismatch,
    on_conflict_index_mismatch,
    personalization_group_key_only,
    pers_streak_histogram_mismatch,
    pg_delete_update_limit,
    query_output_example_mismatch,
    query_time_format_mismatch,
    returning_join_mismatch,
    rules_for,
    table_count_uses_count_column,
    upsert_operation_type_mismatch,
)


def is_efficiency_case(category: str = "", efficiency: Optional[bool] = None) -> bool:
    """Efficiency 题：efficiency 标志为 True，或 category 为 Efficiency。缺省视为非 Efficiency。"""
    if efficiency is True:
        return True
    return (category or "").strip().lower() == "efficiency"
    """用户问题点名且 schema 存在的表，若 pred 未引用则视为意图偏离（Query/Pers 硬约束）。"""
    if not (query or "").strip():
        return []
    try:
        from utils.pipeline import (
            schema_tables_mentioned_in_text,
            tables_from_schema_text,
        )
    except Exception:
        return []
    required = schema_tables_mentioned_in_text(
        query, tables_from_schema_text(schema or "")
    )
    if not required:
        return []
    blob = "\n".join(str(s) for s in (pred_sqls or [])).lower()
    missing: List[str] = []
    for name in required:
        tl = name.lower()
        # 要求作为标识符出现：引号包裹，或紧跟 FROM/JOIN/INTO/UPDATE/TABLE
        pat = (
            rf'(?:"{re.escape(tl)}"|`{re.escape(tl)}`|'
            rf"\b(?:from|join|into|update|table)\s+{re.escape(tl)}\b)"
        )
        if not re.search(pat, blob, flags=re.IGNORECASE):
            missing.append(name)
    return missing


_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+"
    r'(?:ONLY\s+)?([`"]?[A-Za-z_][\w.]*[`"]?)',
    re.IGNORECASE,
)


def is_efficiency_case(category: str = "", efficiency: Optional[bool] = None) -> bool:
    """Efficiency 题：efficiency 标志为 True，或 category 为 Efficiency。缺省视为非 Efficiency。"""
    if efficiency is True:
        return True
    return (category or "").strip().lower() == "efficiency"


def _join_sqls(sqls: List[str]) -> str:
    parts = [s.strip().rstrip(";") for s in sqls if s and str(s).strip()]
    return ";\n".join(parts)


class Verifier:
    """
    确定性 Verify：Soft EX / QEP / 超时。
    不硬拦 DDL/DML；不调 LLM。
    非 Efficiency：耗时超时不硬否决；QEP 仅作排序。
    """

    def __init__(
        self,
        *,
        dialect: str,
        category: str,
        exec_timeout_ms: int = 5000,
        efficiency: Optional[bool] = None,
        reset_fn: Optional[Callable[[], None]] = None,
        execute_fn: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
        explain_fn: Optional[Callable[[str], Any]] = None,
        preprocess_sql: Any = None,
        clean_up_sql: Any = None,
        query: str = "",
        schema: str = "",
        qep_hard_gate: bool = True,
    ):
        self.dialect = (dialect or "postgresql").lower()
        self.category = (category or "").strip()
        self.efficiency = efficiency
        self.is_efficiency = is_efficiency_case(self.category, efficiency)
        self.exec_timeout_ms = int(exec_timeout_ms)
        self.reset_fn = reset_fn
        self.execute_fn = execute_fn
        self.explain_fn = explain_fn
        self.preprocess_sql = as_sql_list(preprocess_sql)
        self.clean_up_sql = as_sql_list(clean_up_sql)
        self.query = query or ""
        self.schema = schema or ""
        self.qep_hard_gate = bool(qep_hard_gate)

    def _mark_clean_if_restored(self) -> None:
        """有 clean_up 且包执行成功后，沙箱应回到基线，可跳过下一轮 full reset。"""
        if not has_cleanup(self.clean_up_sql):
            return
        mark = getattr(self.reset_fn, "mark_clean", None)
        if callable(mark):
            try:
                mark()
            except Exception:
                pass

    def _run_sqls(self, sqls: List[str]) -> Tuple[List[Any], float, Optional[str]]:
        """对 body 做 preprocess→body→cleanup 包裹后执行；返回 (最后可比较结果行, latency_ms, error)。"""
        if not self.execute_fn:
            return [], 0.0, "execute_fn 未配置"
        with timed_stage("sql_wrap", body_stmts=len(sqls)):
            wrapped = wrap_with_pre_post(
                sqls,
                preprocess_sql=self.preprocess_sql,
                clean_up_sql=self.clean_up_sql,
            )
            sql_text = _join_sqls(wrapped)
        t0 = time.perf_counter()
        try:
            with timed_stage("verify_exec", stmts=len(wrapped)):
                results = self.execute_fn(sql_text) or []
        except Exception as e:
            latency = (time.perf_counter() - t0) * 1000.0
            return [], latency, str(e)
        latency = (time.perf_counter() - t0) * 1000.0

        # 取「body 段」最后带 rows 的结果，避免被 cleanup 的空结果覆盖语义
        pre_n = len(self.preprocess_sql)
        body_n = len([s for s in as_sql_list(sqls) if s and str(s).strip()])
        body_slice = results[pre_n : pre_n + body_n] if body_n else results

        rows: List[Any] = []
        for item in body_slice:
            if isinstance(item, dict) and "rows" in item:
                rows = list(item.get("rows") or [])
        if not rows:
            for item in results:
                if isinstance(item, dict) and "rows" in item:
                    rows = list(item.get("rows") or [])

        self._mark_clean_if_restored()
        return rows, latency, None

    def _measure_cost(self, sqls: List[str]) -> Optional[float]:
        if not self.explain_fn:
            return None
        t0 = time.perf_counter()
        try:
            return self._measure_cost_body(sqls)
        finally:
            log_timing("verify_qep", (time.perf_counter() - t0) * 1000.0)

    def _measure_cost_body(self, sqls: List[str]) -> Optional[float]:
        # 先落地前置，再对 body 可解释语句测 cost，最后尽力 cleanup
        if self.preprocess_sql and self.execute_fn:
            try:
                self.execute_fn(_join_sqls(self.preprocess_sql))
            except Exception as e:
                default_logger.debug(f"QEP 前置 SQL 失败: {e}")
                return None

        total = 0.0
        measured = False
        try:
            for sql in as_sql_list(sqls):
                s = sql.strip()
                if not s:
                    continue
                upper = s.upper()
                if not (
                    upper.startswith("SELECT")
                    or upper.startswith("INSERT")
                    or upper.startswith("UPDATE")
                    or upper.startswith("DELETE")
                    or upper.startswith("WITH")
                ):
                    continue
                try:
                    payload = self.explain_fn(s)
                    cost = extract_qep_cost(payload, self.dialect)
                    if cost is not None:
                        total += cost
                        measured = True
                except Exception as e:
                    default_logger.debug(f"EXPLAIN 失败: {e}")
        finally:
            if self.clean_up_sql and self.execute_fn:
                try:
                    self.execute_fn(_join_sqls(self.clean_up_sql))
                    self._mark_clean_if_restored()
                except Exception as e:
                    default_logger.debug(f"QEP 后清理失败: {e}")

        return total if measured else None

    def verify_candidates(
        self,
        candidates: List[Candidate],
        issue_sql: List[str],
    ) -> VerifyReport:
        with timed_stage("verify_rules", candidates=len(candidates)):
            return self._verify_candidates_body(candidates, issue_sql)

    def _verify_candidates_body(
        self,
        candidates: List[Candidate],
        issue_sql: List[str],
    ) -> VerifyReport:
        per: List[CandidateVerifyResult] = []
        failure_reasons: List[str] = []
        passed_list: List[Tuple[Candidate, CandidateVerifyResult]] = []
        relative_best: Optional[Candidate] = None
        relative_score = float("inf")

        # Soft EX：仅 Efficiency 硬要求与 issue 结果一致（语义应保留）。
        # Query/Personalization 的 issue 往往本身错误，结果差异不应否决正确修复。
        # issue 若含 PG 非法 DELETE/UPDATE LIMIT，基线本身跑不通，不能当语义真值。
        issue_rows: Optional[List[Any]] = None
        issue_cost: Optional[float] = None
        soft_ex_applicable = self.is_efficiency and any(
            _is_selectish(s) for s in issue_sql
        )
        if self.is_efficiency and pg_delete_update_limit(self.dialect, issue_sql):
            soft_ex_applicable = False

        # Soft EX 基线与 QEP 基线共用一次 reset（只读 issue 不二次重建）
        if self.reset_fn:
            try:
                self.reset_fn()
            except Exception as e:
                default_logger.warning(f"Verify 基线 reset 失败: {e}")

        if soft_ex_applicable:
            try:
                issue_rows, _, err = self._run_sqls(issue_sql)
                if err:
                    soft_ex_applicable = False
                    issue_rows = None
                    default_logger.warning(f"issue_sql Soft EX 基线失败，跳过功能对比: {err}")
            except Exception as e:
                soft_ex_applicable = False
                issue_rows = None
                default_logger.warning(f"issue_sql Soft EX 基线异常，跳过: {e}")
        elif self.category.lower() != "management":
            default_logger.debug(
                "非 Efficiency：跳过 Soft EX 与 issue 结果硬对比"
            )

        if self.is_efficiency:
            try:
                issue_cost = self._measure_cost(issue_sql)
            except Exception as e:
                default_logger.warning(f"issue_sql QEP 基线失败: {e}")
        else:
            # 非 Eff 仍可测 cost 供排序，失败忽略
            try:
                issue_cost = self._measure_cost(issue_sql)
            except Exception:
                issue_cost = None

        for cand in candidates:
            if self.reset_fn:
                try:
                    self.reset_fn()
                except Exception as e:
                    default_logger.warning(f"候选隔离重置失败: {cand.id}: {e}")

            errors: List[str] = []
            soft_ex = "skipped"
            cost: Optional[float] = None
            latency_ms: Optional[float] = None
            passed = True

            rows, latency_ms, err = self._run_sqls(cand.pred_sqls)
            if err:
                passed = False
                hint = ""
                el = (err or "").lower()
                if "already exists" in el:
                    hint = (
                        " | 提示: 对象已存在；改用 CREATE IF NOT EXISTS / OR REPLACE，"
                        "或 DROP IF EXISTS 后再建，勿原样重试 CREATE"
                    )
                errors.append(f"执行失败: {err}{hint}")
                failure_reasons.append(f"[{cand.id}] 执行失败: {err}{hint}")
            elif latency_ms is not None and latency_ms > self.exec_timeout_ms:
                if self.is_efficiency:
                    passed = False
                    errors.append(f"执行超时: {latency_ms:.1f}ms > {self.exec_timeout_ms}ms")
                    failure_reasons.append(
                        f"[{cand.id}] 执行超时: {latency_ms:.1f}ms ≥ {self.exec_timeout_ms}ms"
                    )
                else:
                    default_logger.info(
                        f"[{cand.id}] 非 Efficiency 耗时 {latency_ms:.1f}ms > "
                        f"{self.exec_timeout_ms}ms，不硬否决"
                    )

            # Soft EX
            if passed and soft_ex_applicable and issue_rows is not None:
                # 仅当候选末条可产生结果集时比较；Management 已 skipped
                if any(_is_selectish(s) for s in cand.pred_sqls):
                    if soft_ex_equal(rows, issue_rows):
                        soft_ex = "pass"
                    else:
                        soft_ex = "fail"
                        passed = False
                        errors.append("Soft EX 失败：结果集不一致")
                        failure_reasons.append(f"[{cand.id}] Soft EX 失败：结果集不一致")
                else:
                    soft_ex = "skipped"
            elif self.category.lower() == "management":
                soft_ex = "skipped"

            # 规则注册表：按类目预筛后逐条判定（原 if 链顺序不变）
            if passed:
                ctx = RuleContext(
                    dialect=self.dialect,
                    category=self.category,
                    category_lower=self.category.lower(),
                    is_efficiency=self.is_efficiency,
                    query=self.query,
                    schema=self.schema,
                    pred_sqls=list(cand.pred_sqls),
                    preprocess_sql=list(self.preprocess_sql or []),
                    rows=rows,
                )
                for rule in rules_for(ctx.category_lower, ctx.is_efficiency):
                    msg = rule.check(ctx)
                    if msg:
                        passed = False
                        errors.append(msg)
                        failure_reasons.append(f"[{cand.id}] {msg}")
                        if rule.veto_log:
                            default_logger.info(f"[{cand.id}] {rule.veto_log}")
                        break  # 与原 if 链一致：单条规则否决后不再判后续规则

            # pg_delete_update_limit：即使执行失败也补 LIMIT 提示（原 elif err 分支）
            lim = pg_delete_update_limit(self.dialect, cand.pred_sqls)
            if lim and not any("DELETE/UPDATE 不支持顶层 LIMIT" in e for e in errors):
                if passed:
                    passed = False
                    errors.append(lim)
                    failure_reasons.append(f"[{cand.id}] {lim}")
                    default_logger.info(f"[{cand.id}] 否决 PG DELETE/UPDATE LIMIT")
                elif err:
                    pass  # 执行失败分支已附加 LIMIT 提示

            # QEP：在候选执行后的沙箱态上测（保留候选 DDL/索引）；
            # 只读候选不会污染，后续候选的 smart reset 可跳过重建。
            try:
                cost = self._measure_cost(cand.pred_sqls)
            except Exception as e:
                default_logger.debug(f"候选 QEP 失败 {cand.id}: {e}")

            if self.is_efficiency:
                if cost is None or issue_cost is None:
                    # 缺 cost 时不单凭此否决，但记原因
                    if passed and (cost is None or issue_cost is None):
                        errors.append("QEP cost 缺失，Efficiency 无法硬比较")
                elif cost >= issue_cost:
                    msg = f"QEP cost {cost} ≥ issue {issue_cost}"
                    if self.qep_hard_gate:
                        passed = False
                        errors.append(msg)
                        failure_reasons.append(f"[{cand.id}] {msg}")
                    else:
                        errors.append(msg + "（采样沙箱仅排序，不硬否决）")
                        default_logger.info(
                            f"[{cand.id}] {msg}；采样沙箱跳过 QEP 硬门禁"
                        )

            # 相对排序键：Efficiency 用 cost，否则用 latency
            if self.is_efficiency:
                rank_key = float(cost) if cost is not None else float("inf")
            else:
                rank_key = float(latency_ms) if latency_ms is not None else float("inf")
            if not passed:
                # 失败候选仍可参与相对最优（代价越高越差）
                rank_key = rank_key if rank_key != float("inf") else 1e18
                rank_key += 1e12  # 未通过排在通过者之后

            cvr = CandidateVerifyResult(
                id=cand.id,
                passed=passed,
                soft_ex=soft_ex,
                cost=cost,
                latency_ms=latency_ms,
                errors=errors,
                rank_key=rank_key,
            )
            per.append(cvr)
            if passed:
                passed_list.append((cand, cvr))
            if rank_key < relative_score:
                relative_score = rank_key
                relative_best = cand

        best: Optional[Candidate] = None
        if passed_list:
            passed_list.sort(key=lambda x: x[1].rank_key)
            best = passed_list[0][0]
            # 清理：有通过者时 failure_reasons 可保留供日志，但不阻断
            failure_reasons = [r for r in failure_reasons if not r.startswith(f"[{best.id}]")]

        return VerifyReport(
            passed=best is not None,
            best=best,
            per_candidate=per,
            failure_reasons=failure_reasons if best is None else [],
            relative_best=relative_best,
            relative_score=relative_score,
        )


def build_failure_reasons(report: VerifyReport) -> List[str]:
    if report.failure_reasons:
        return list(report.failure_reasons)
    out: List[str] = []
    for c in report.per_candidate:
        for e in c.errors:
            out.append(f"[{c.id}] {e}")
    return out
