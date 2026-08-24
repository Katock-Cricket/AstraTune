import re
from typing import Any, Callable, Dict, List, Optional

from sandbox.executor import DBExecutor
from utils.logger import default_logger
from utils.sql_wrap import is_effectively_empty_sql
from utils.timing import sql_kind, timed_stage


class SandboxTool:
    """沙箱工具，封装数据库操作供Agent调用。"""

    def __init__(self, executor: DBExecutor):
        self.executor = executor
        self.preprocess_sql = None
        self.clean_up_sql = None
        self._preprocess_applied = False
        self._manager = None
        self._sandbox_tables: List[str] = []
        self._on_full_reset: Optional[Callable[[Dict[str, Any]], None]] = None

    def register_preprocess_sql(self, preprocess_sql: str) -> None:
        self.preprocess_sql = preprocess_sql
        self._preprocess_applied = False

    def register_clean_up_sql(self, clean_up_sql: str) -> None:
        self.clean_up_sql = clean_up_sql

    def bind_sandbox_reset(
        self,
        manager,
        tables: List[str],
        *,
        on_full_reset: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        """绑定 cleanup+setup 能力，供 reset 工具使用。"""
        self._manager = manager
        self._sandbox_tables = list(tables or [])
        self._on_full_reset = on_full_reset

    def reset_environment(self, reason: str) -> str:
        """
        全量重建沙箱（cleanup + setup + 前置 SQL）。
        仅当沙箱被不可逆破坏、clean_up 无法恢复、且会影响后续验证时使用。
        """
        r = (reason or "").strip()
        if len(r) < 8:
            return (
                "拒绝 reset：请说明沙箱为何无法通过 clean_up/后置 SQL 恢复，"
                "以及为何会影响后续验证（至少一句完整描述）。"
            )
        if not self._manager or not self._sandbox_tables:
            return "拒绝 reset：当前会话未绑定沙箱 manager/表列表，无法重建。"
        low = r.lower()
        weak = (
            "试试",
            "不确定",
            "maybe",
            "just in case",
            "随便",
            "预防",
        )
        if any(w in low for w in weak) and not any(
            k in low
            for k in (
                "drop schema",
                "drop table",
                "truncate",
                "aborted",
                "already exists",
                "does not exist",
                "不可逆",
                "无法恢复",
                "corrupt",
            )
        ):
            return (
                "拒绝 reset：原因不够具体。请确认 clean_up 无法恢复且环境已污染后续验证。"
            )
        try:
            default_logger.warning(f"Agent 请求沙箱 reset: {r[:200]}")
            info = self._manager.reset_sandbox(self._sandbox_tables)
            self._preprocess_applied = False
            pre_msg = self.apply_preprocess_once(force=True)
            if self._on_full_reset:
                try:
                    self._on_full_reset(info)
                except Exception as e:
                    default_logger.debug(f"reset 回调失败: {e}")
            sampled = info.get("sampled_tables") or []
            return (
                f"沙箱已全量重建（{len(self._sandbox_tables)} 张表）。"
                f"采样表: {sampled or '无'}。前置 SQL: {pre_msg}"
            )
        except Exception as e:
            self._safe_rollback()
            return f"沙箱 reset 失败: {e}"

    def apply_preprocess_once(self, *, force: bool = False) -> str:
        """
        诊断开始前执行一次前置 SQL（CREATE/ALTER/INSERT 等）。
        默认幂等：已成功应用过则跳过，避免「already exists」污染后续工具调用。
        """
        if not self.preprocess_sql:
            return "无前置 SQL"
        if self._preprocess_applied and not force:
            return "前置 SQL 已应用，跳过"
        try:
            self.executor.execute(self.preprocess_sql, fetch=True)
            self._preprocess_applied = True
            default_logger.info("沙箱前置 SQL 已执行一次")
            return "前置 SQL 执行成功"
        except Exception as e:
            self._safe_rollback()
            msg = f"前置 SQL 执行失败: {e}"
            default_logger.warning(msg)
            return msg

    def apply_cleanup_once(self) -> str:
        """诊断结束或需要恢复时显式调用；不在每次 execute_sql 后自动跑。"""
        if not self.clean_up_sql:
            return "无清理 SQL"
        try:
            self.executor.execute(self.clean_up_sql, fetch=True)
            # 清理后环境可能不再含 preprocess 产物，允许再次 apply_preprocess
            self._preprocess_applied = False
            default_logger.info("沙箱清理 SQL 已执行")
            return "清理 SQL 执行成功"
        except Exception as e:
            self._safe_rollback()
            msg = f"清理 SQL 执行失败: {e}"
            default_logger.warning(msg)
            return msg

    def _safe_rollback(self) -> None:
        conn = getattr(self.executor, "connection", None)
        if conn is None:
            return
        try:
            conn.rollback()
        except Exception:
            pass

    def _normalize_agent_sql(self, sql: str) -> str:
        """把常见非 SQL（psql 元命令 / sqlite 目录）改写成可执行查询。"""
        s = (sql or "").strip()
        if not s:
            return s
        m = re.match(r"^\\d\+?\s+([A-Za-z_][\w$]*|\"[^\"]+\")\s*;?\s*$", s)
        if m:
            raw = m.group(1).strip().strip('"')
            lit = raw.replace("'", "''")
            return (
                "SELECT column_name, data_type, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_schema NOT IN ('pg_catalog','information_schema') "
                f"AND lower(table_name) = lower('{lit}') "
                "ORDER BY ordinal_position"
            )
        if re.search(r"\bsqlite_master\b", s, re.I):
            if re.search(r"type\s*=\s*['\"]index['\"]", s, re.I):
                return (
                    "SELECT indexname AS name, indexdef AS sql "
                    "FROM pg_indexes "
                    "WHERE schemaname NOT IN ('pg_catalog','information_schema') "
                    "ORDER BY tablename, indexname"
                )
            return (
                "SELECT table_schema, table_name, table_type "
                "FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog','information_schema') "
                "ORDER BY table_schema, table_name"
            )
        return s

    def _enrich_error(self, err: str, sql: str) -> str:
        e = str(err)
        hints = []
        low = e.lower()
        sql_l = (sql or "").lower()
        if "sqlite_master" in sql_l or "sqlite_master" in low:
            hints.append(
                "本沙箱是 PostgreSQL，没有 sqlite_master；请用 pg_indexes / information_schema"
            )
        if (sql or "").lstrip().startswith("\\"):
            hints.append(
                "不要使用 psql 元命令（如 \\d）；请改用 information_schema 或 pg_catalog"
            )
        if "current transaction is aborted" in low:
            hints.append("上一语句失败后事务已中止；请修正 SQL 后重试（工具已自动 rollback）")
        if "empty query" in low:
            hints.append("提交了空语句或仅注释；请发送完整可执行 SQL")
        if hints:
            return f"SQL执行失败: {e} | 提示: " + "；".join(hints)
        return f"SQL执行失败: {e}"

    def execute_sql(self, sql: str) -> str:
        """
        仅执行 Agent 传入的 SQL。

        不再每次自动跑 preprocess/clean_up：
        Flash 题面前置常含 CREATE/ALTER，重复执行会 already exists，
        并易把 PG 会话留在 aborted 事务态，导致后续全挂。
        """
        # 若上一轮异常后仍 INERROR，先清掉，避免连环 aborted
        self._safe_rollback()
        conn = getattr(self.executor, "connection", None)
        old_ac = None
        try:
            # Agent 探测 SQL 用 autocommit，避免多语句批中一条失败拖死后续整段会话
            if conn is not None and hasattr(conn, "autocommit"):
                old_ac = conn.autocommit
                conn.autocommit = True
            normalized = self._normalize_agent_sql(sql)
            if is_effectively_empty_sql(normalized):
                return (
                    "SQL执行失败: can't execute an empty query | "
                    "提示: 请发送完整可执行 SQL"
                )
            with timed_stage(
                "execute_sql",
                kind=sql_kind(normalized),
                chars=len(normalized),
            ):
                results = self.executor.execute(normalized, fetch=True)
            return self._format_results(results)
        except Exception as e:
            self._safe_rollback()
            return self._enrich_error(str(e), sql)
        finally:
            if conn is not None and old_ac is not None:
                try:
                    conn.autocommit = old_ac
                except Exception:
                    pass

    def _format_results(self, results: list) -> str:
        if not results:
            return "执行成功，无返回结果"

        formatted_parts = []

        for i, result in enumerate(results, 1):
            sql = result.get("sql", "")

            exec_time = result.get("execution_time")
            exec_time_ms = result.get("execution_time_ms")
            time_str = (
                f"执行时间: {exec_time}秒 ({exec_time_ms}ms)"
                if exec_time is not None
                else ""
            )

            if "rows" in result:
                rows = result["rows"]
                row_count = result["row_count"]

                formatted_parts.append(f"[语句 {i}] {sql}")
                if time_str:
                    formatted_parts.append(time_str)
                formatted_parts.append(f"返回 {row_count} 行:")

                max_display_rows = 20
                display_rows = rows[:max_display_rows]

                if display_rows:
                    for row in display_rows:
                        formatted_parts.append(str(row))
                    if row_count > max_display_rows:
                        formatted_parts.append(
                            f"... (还有 {row_count - max_display_rows} 行未显示)"
                        )
                else:
                    formatted_parts.append("(空结果集)")

            elif "affected_rows" in result:
                affected = result["affected_rows"]
                message = result.get("message", "")

                formatted_parts.append(f"[语句 {i}] {sql}")
                if time_str:
                    formatted_parts.append(time_str)
                formatted_parts.append(f"影响 {affected} 行，{message}")

            formatted_parts.append("")

        return "\n".join(formatted_parts)


def create_sandbox_tool_function(sandbox_tool: SandboxTool):
    """创建供 LangChain/LangGraph 使用的 execute_sql 工具函数。"""

    def execute_sql(sql: str) -> str:
        """
        在沙箱数据库中执行 SQL（仅执行你传入的语句）。

        方言以当前任务为准（Flash/本项目沙箱多为 PostgreSQL）。
        禁止：psql 元命令（\\d）、sqlite_master。
        查列用 information_schema.columns；查索引用 pg_indexes。
        题面「前置 SQL」已在诊断开始时自动执行过一次；不要重复 CREATE 已有表。
        清理 SQL 仅在需要恢复环境时自行执行。

        Args:
            sql: 要执行的 SQL，可多条（分号分隔）

        Returns:
            执行结果描述
        """
        return sandbox_tool.execute_sql(sql)

    return execute_sql


def create_reset_sandbox_tool_function(sandbox_tool: SandboxTool):
    """创建供 LangChain/LangGraph 使用的 reset 沙箱工具函数。"""

    def reset_sandbox_environment(reason: str) -> str:
        """
        全量重建沙箱（清空后从目标库重新拷贝/采样表，并重跑题面前置 SQL）。

        **仅当同时满足**才调用：
        1. 沙箱被不可逆破坏（如误 DROP 基表、污染 public schema、事务 aborted 且无法继续探测）；
        2. 题面 clean_up_sql / 你自己的清理 SQL 无法恢复；
        3. 不 reset 会导致后续 execute_sql 或 Verify 结果不可信。

        禁止：仅为「试试」、预防性、或可用 clean_up 恢复时调用（代价高、耗时长）。

        Args:
            reason: 中文说明为何必须 reset（必填，具体描述破坏点）

        Returns:
            重建结果描述
        """
        return sandbox_tool.reset_environment(reason)

    return reset_sandbox_environment
