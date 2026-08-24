"""配置加载、执行器创建、抽表与沙箱搭建（供 diag / run_bird_critic 复用）。"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sandbox.db_admin import (
    create_executor as db_admin_create_executor,
    ensure_sandbox_database as db_admin_ensure_sandbox_database,
    sandbox_db_name_for_slot as db_admin_sandbox_db_name_for_slot,
)
from sandbox.sandbox_manager import SandboxManager
from utils.config_loader import (
    get_batch_config,
    get_diagnosis_config,
    get_optimization_config,
    load_all_configs,
    normalize_dialect,
)
from utils.logger import default_logger
from utils.timing import timed_stage


def load_jsonl(path: str | Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def dump_jsonl(records: List[Dict[str, Any]], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for obj in records:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        f.flush()
    tmp.replace(path)


def dedupe_jsonl_by_id(
    path: str | Path, *, id_key: str = "instance_id"
) -> int:
    """流式去重：同 id 保留最后一条，原地替换。返回唯一条数。"""
    path = Path(path)
    if not path.is_file():
        return 0
    last: Dict[Any, str] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            iid = obj.get(id_key)
            if iid is not None:
                last[iid] = line
    tmp = path.with_name(path.name + ".dedupe.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for line in last.values():
            f.write(line + "\n")
        f.flush()
    tmp.replace(path)
    return len(last)


def merge_schema_and_input(
    schema_records: List[Dict[str, Any]],
    input_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    schema_map = {r.get("instance_id"): r for r in schema_records}
    combined: List[Dict[str, Any]] = []
    for rec in input_records:
        iid = rec.get("instance_id")
        base = schema_map.get(iid, {})
        combined.append({**base, **rec})
    return combined


def filter_by_dialect(
    records: List[Dict[str, Any]], dialect: str
) -> List[Dict[str, Any]]:
    target = normalize_dialect(dialect)
    out = []
    for r in records:
        d = r.get("dialect")
        if d is None:
            # 完整子集可能缺 dialect，调用方已按方言选择输入时放行
            out.append(r)
            continue
        try:
            if normalize_dialect(str(d)) == target:
                out.append(r)
        except ValueError:
            continue
    return out


def create_executor(config: Dict[str, Any]):
    """按配置方言创建执行器（已下沉至 sandbox.db_admin，此处 re-export 兼容）。"""
    return db_admin_create_executor(config)


def sandbox_db_name_for_slot(
    *,
    dialect: str,
    slot_index: Optional[int],
    workers: int,
    batch_cfg: Optional[Dict[str, Any]] = None,
    base_database: Optional[str] = None,
) -> str:
    """workers==1 或 slot_index is None：沿用配置中的单库名（P0 兼容）。否则：prefix + slot_index。"""
    return db_admin_sandbox_db_name_for_slot(
        dialect=dialect,
        slot_index=slot_index,
        workers=workers,
        batch_cfg=batch_cfg,
        base_database=base_database,
    )


def ensure_sandbox_database(sandbox_cfg: Dict[str, Any], database: str) -> None:
    """在沙箱实例上确保 database 存在（已下沉至 sandbox.db_admin，此处 re-export 兼容）。"""
    db_admin_ensure_sandbox_database(sandbox_cfg, database)


def setup_executors(
    *,
    dialect: str = "postgresql",
    db_id: Optional[str] = None,
    configs: Optional[Dict[str, Any]] = None,
    slot_index: Optional[int] = None,
    workers: int = 1,
) -> Tuple[Any, Any, Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    返回 (target_executor, sandbox_executor, agent_config, opt_config, target_cfg, sandbox_cfg)
    执行器已 connect。
    """
    configs = configs or load_all_configs()
    dialect = normalize_dialect(dialect)
    diag = get_diagnosis_config(configs, dialect=dialect)
    agent_config = diag.get("agent", {})
    target_cfg = dict(diag.get("target_db") or {})
    sandbox_cfg = dict(diag.get("sandbox") or {})
    opt_config = get_optimization_config(configs)
    batch_cfg = get_batch_config(configs)

    if db_id:
        target_cfg["database"] = db_id

    base_db = sandbox_cfg.get("database") or "astratune_sandbox"
    slot_db = sandbox_db_name_for_slot(
        dialect=dialect,
        slot_index=slot_index,
        workers=workers,
        batch_cfg=batch_cfg,
        base_database=base_db,
    )
    if slot_db != base_db:
        ensure_sandbox_database(sandbox_cfg, slot_db)
    sandbox_cfg["database"] = slot_db

    target_ex = create_executor(target_cfg)
    sandbox_ex = create_executor(sandbox_cfg)
    with timed_stage(
        "target_connect",
        slot_db=slot_db,
        target_db=target_cfg.get("database"),
    ):
        target_ex.connect()
    with timed_stage("sandbox_connect", slot_db=slot_db):
        sandbox_ex.connect()
    return target_ex, sandbox_ex, agent_config, opt_config, target_cfg, sandbox_cfg


def normalize_tables_for_dialect(tables: List[str], dialect: str) -> List[str]:
    """剥 schema 前缀、去重；过滤明显系统表名。"""
    out: List[str] = []
    for t in tables:
        s = (t or "").strip().strip("`").strip('"')
        if not s:
            continue
        if "." in s:
            s = s.split(".")[-1].strip().strip("`").strip('"')
        low = s.lower()
        if low.startswith("pg_") or low in {
            "information_schema",
            "columns",
            "tables",
            "dual",
        }:
            continue
        out.append(s)
    return list(dict.fromkeys(out))


_CREATE_TABLE_RE = re.compile(
    r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"([^"]+)"|`([^`]+)`|([a-zA-Z_][\w$]*))',
    re.IGNORECASE,
)


def tables_from_schema_text(schema_text: str) -> List[str]:
    """从 original_schema / DDL 文本解析 CREATE TABLE 名。"""
    if not schema_text:
        return []
    names: List[str] = []
    for m in _CREATE_TABLE_RE.finditer(schema_text):
        name = m.group(1) or m.group(2) or m.group(3)
        if name:
            names.append(name)
    return list(dict.fromkeys(names))


def schema_tables_mentioned_in_text(
    text: str, schema_tables: List[str]
) -> List[str]:
    """从用户描述/SQL 文本中匹配 schema 已有表名（引号或 table/from/join 邻近），避免漏掉「SQL 用错表」。"""
    if not text or not schema_tables:
        return []
    low = text.lower()
    hit: List[str] = []
    for name in schema_tables:
        n = (name or "").strip()
        if not n:
            continue
        nl = n.lower()
        if f"'{nl}'" in low or f'"{nl}"' in low or f"`{nl}`" in low:
            hit.append(name)
            continue
        if re.search(
            rf"\b(?:table(?:\s+named)?|from|join|into|update|references)\s+[\"'`]?{re.escape(nl)}[\"'`]?\b",
            low,
        ):
            hit.append(name)
            continue
        if re.search(rf"\b{re.escape(nl)}\s+table\b", low):
            hit.append(name)
    return list(dict.fromkeys(hit))


def _resolve_one_table(target_executor, name: str) -> Optional[str]:
    if hasattr(target_executor, "resolve_table_name"):
        hit = target_executor.resolve_table_name(name)
        if hit:
            return hit
    if target_executor.table_exists(name):
        return name
    return None


def resolve_and_validate_tables(
    tables: List[str],
    target_executor,
    *,
    dialect: str,
    original_schema: str = "",
    hint_text: str = "",
) -> List[str]:
    """
    将 LLM 抽表结果与目标库求交；并并入 hint_text（用户问题+SQL）中点名的 schema 表；
    仍空时回退 original_schema / 全库基表。
    """
    dialect = normalize_dialect(dialect)
    with timed_stage("target_probe_tables"):
        candidates = normalize_tables_for_dialect(tables, dialect)
        schema_tables = normalize_tables_for_dialect(
            tables_from_schema_text(original_schema), dialect
        )
        # 用户描述点名的表必须进沙箱（即使 issue_sql 用了错误表）
        for name in schema_tables_mentioned_in_text(hint_text, schema_tables):
            candidates.append(name)
        candidates = list(dict.fromkeys(candidates))

        resolved: List[str] = []
        for name in candidates:
            hit = _resolve_one_table(target_executor, name)
            if hit:
                resolved.append(hit)

        if resolved:
            return list(dict.fromkeys(resolved))

        # 回退：schema 文本中的表 ∩ 目标库
        for name in schema_tables:
            hit = _resolve_one_table(target_executor, name)
            if hit:
                resolved.append(hit)

        if resolved:
            default_logger.warning(
                f"抽表无效，已用 original_schema 回退 {len(resolved)} 张表"
            )
            return list(dict.fromkeys(resolved))

        # 最后：目标库全部基表（仅当 schema 回退也空且库表不太多）
        if hasattr(target_executor, "list_base_tables"):
            all_tables = target_executor.list_base_tables()
            if 0 < len(all_tables) <= 40:
                default_logger.warning(
                    f"抽表与 schema 均无效，回退目标库全部 {len(all_tables)} 张基表"
                )
                return all_tables

        return []


def fetch_schema_ddl(target_executor, tables: List[str]) -> str:
    parts: List[str] = []
    with timed_stage("schema_fetch", tables=len(tables)):
        for table in tables:
            ddl = target_executor.get_create_table_ddl(table)
            parts.append(f"-- 表: {table}")
            parts.append(ddl)
            parts.append("")
    return "\n".join(parts)


def prepare_sandbox(
    target_executor,
    sandbox_executor,
    sandbox_cfg: Dict[str, Any],
    tables: List[str],
    *,
    db_id: Optional[str] = None,
    dialect: Optional[str] = None,
) -> Dict[str, Any]:
    manager = SandboxManager(target_executor, sandbox_executor, sandbox_cfg)
    try:
        dialect_n = normalize_dialect(
            dialect or sandbox_cfg.get("dialect") or "postgresql"
        )
    except Exception:
        dialect_n = str(dialect or sandbox_cfg.get("dialect") or "postgresql")

    if db_id:
        try:
            from sandbox.template_cache import prepare_with_template, template_cache_config

            if template_cache_config().get("enabled"):
                with timed_stage(
                    "sandbox_prepare",
                    db_id=db_id,
                    tables=len(tables),
                    via="template",
                ):
                    info = prepare_with_template(
                        manager=manager,
                        tables=tables,
                        db_id=str(db_id),
                        dialect=dialect_n,
                        sandbox_cfg=sandbox_cfg,
                    )
                return {"manager": manager, "info": info, "via": "template"}
        except Exception as e:
            default_logger.warning(f"模板沙箱失败，回退直接克隆: {e}")

    with timed_stage(
        "sandbox_prepare",
        db_id=db_id or "-",
        tables=len(tables),
        via="direct",
    ):
        try:
            manager.cleanup_sandbox()
        except Exception as e:
            default_logger.warning(f"沙箱预清理警告: {e}")
        info = manager.setup_sandbox(tables)
    return {"manager": manager, "info": info, "via": "direct"}


def make_explain_fn(executor, dialect: str):
    dialect = normalize_dialect(dialect)

    def explain_fn(sql: str) -> Any:
        s = sql.strip().rstrip(";")
        if dialect == "mysql":
            explain_sql = f"EXPLAIN FORMAT=JSON {s}"
        else:
            explain_sql = f"EXPLAIN (FORMAT JSON) {s}"
        results = executor.execute(explain_sql, fetch=True) or []
        if not results or not isinstance(results[0], dict):
            return None
        rows = results[0].get("rows") or []
        if not rows:
            return None
        row0 = rows[0]
        if isinstance(row0, dict):
            # PG: {"QUERY PLAN": [...]} ；MySQL: {"EXPLAIN": "..."}
            for key in ("QUERY PLAN", "query plan", "EXPLAIN", "explain"):
                if key in row0:
                    val = row0[key]
                    if isinstance(val, str):
                        try:
                            return json.loads(val)
                        except json.JSONDecodeError:
                            return val
                    return val
            return row0
        if isinstance(row0, (list, tuple)) and row0:
            val = row0[0]
            if isinstance(val, str):
                try:
                    return json.loads(val)
                except json.JSONDecodeError:
                    return val
            return val
        return row0

    return explain_fn


def make_execute_fn(executor, *, on_sql=None):
    """on_sql: 可选回调，用于脏标记（智能 reset）。"""

    def execute_fn(sql: str):
        if on_sql is not None:
            try:
                on_sql(sql)
            except Exception:
                pass
        return executor.execute(sql, fetch=True)

    return execute_fn


def make_reset_fn(
    manager: SandboxManager,
    tables: List[str],
    *,
    smart: bool = True,
    initially_dirty: bool = True,
):
    """返回 (reset_fn, mark_sql)。

    smart=True 时：若自上次 full reset 以来未执行过可能改写数据/结构的 SQL，
    则跳过 cleanup+setup（只查询/EXPLAIN 场景大幅加速）。
    初始 dirty 默认 True（假定诊断阶段已污染沙箱）。
    """
    from utils.sql_mutate import sql_may_mutate

    state = {"dirty": initially_dirty, "full_resets": 0, "skipped": 0}

    def mark_sql(sql: str) -> None:
        if sql_may_mutate(sql):
            state["dirty"] = True

    def mark_clean() -> None:
        state["dirty"] = False

    def reset_fn(force: bool = False) -> str:
        if smart and not force and not state["dirty"]:
            state["skipped"] += 1
            default_logger.info(
                f"智能 reset：跳过全量重建 (skipped={state['skipped']})"
            )
            from utils.timing import log_timing

            log_timing("sandbox_reset", 0.0, outcome="skipped", skipped=state["skipped"])
            return "skipped"

        from utils.timing import timed_stage as _ts

        with _ts("sandbox_reset", force=int(bool(force))):
            manager.reset_sandbox(tables)
        state["dirty"] = False
        state["full_resets"] += 1
        default_logger.info(
            f"智能 reset：全量重建 (full_resets={state['full_resets']})"
        )
        return "full"

    reset_fn.state = state  # type: ignore[attr-defined]
    reset_fn.mark_clean = mark_clean  # type: ignore[attr-defined]
    return reset_fn, mark_sql


def make_verify_fns(
    manager: SandboxManager,
    tables: List[str],
    sandbox_executor,
    dialect: str,
    *,
    smart_reset: bool = True,
    initially_dirty: bool = True,
):
    """一次性构造 Verify 用的 reset / execute / explain。"""
    reset_fn, mark_sql = make_reset_fn(
        manager,
        tables,
        smart=smart_reset,
        initially_dirty=initially_dirty,
    )
    execute_fn = make_execute_fn(sandbox_executor, on_sql=mark_sql)
    explain_fn = make_explain_fn(sandbox_executor, dialect)
    return reset_fn, execute_fn, explain_fn


def sql_list_to_text(sqls: Any) -> str:
    if sqls is None:
        return ""
    if isinstance(sqls, str):
        return sqls
    if isinstance(sqls, list):
        return ";\n".join(str(x).strip().rstrip(";") for x in sqls if str(x).strip())
    return str(sqls)


def instance_issue_sql(record: Dict[str, Any]) -> List[str]:
    raw = record.get("issue_sql")
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw.strip()] if raw.strip() else []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    return [str(raw)]
