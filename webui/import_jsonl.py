"""JSONL / 单条导入映射（纯函数）。剥 gold；按题面信号选 function。"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from utils.config_loader import normalize_dialect
from webui.overlay import match_connection

GOLD_KEYS = ("sol_sql", "test_cases", "gold_sql", "gold", "sol")
DEFAULT_MODE = "diag+opt"
VALID_MODES = ("diag+opt", "diag_only", "opt_only", "llm_only")
VALID_FUNCTIONS = ("general", "efficiency")


def as_sql_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        return [s] if s else []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    s = str(raw).strip()
    return [s] if s else []


def is_slow_signal(row: Dict[str, Any]) -> bool:
    if row.get("is_slow_sql") is True:
        return True
    if str(row.get("category") or "").strip().lower() == "efficiency":
        return True
    if row.get("efficiency") is True:
        return True
    if str(row.get("function") or "").strip().lower() == "efficiency":
        return True
    return False


def resolve_mode(row: Dict[str, Any], *, allow_override: bool = True) -> str:
    if allow_override:
        mode = str(row.get("mode") or "").strip().lower()
        if mode in VALID_MODES:
            return mode
    return DEFAULT_MODE


def classify_row(row: Dict[str, Any]) -> Tuple[bool, str, str]:
    """返回 (is_slow_sql, function, category)。"""
    slow = is_slow_signal(row)
    fn_raw = str(row.get("function") or "").strip().lower()
    if fn_raw in VALID_FUNCTIONS:
        fn = fn_raw
        slow = fn == "efficiency" or slow
    else:
        fn = "efficiency" if slow else "general"
    cat = str(row.get("category") or "").strip()
    if slow:
        if not cat:
            cat = "Efficiency"
    elif not cat:
        cat = "Query"
    if fn == "efficiency" and cat.lower() != "efficiency":
        cat = "Efficiency"
    return slow, fn, cat


def strip_gold(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    for key in GOLD_KEYS:
        out.pop(key, None)
    return out


def map_import_row(
    row: Dict[str, Any],
    *,
    connections: List[Dict[str, Any]],
    default_connection_id: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """映射一行题面为任务草稿。失败返回 (None, reason)。"""
    clean = strip_gold(row if isinstance(row, dict) else {})
    issue = as_sql_list(clean.get("issue_sql") or clean.get("sql"))
    if not issue:
        return None, "缺少 issue_sql"

    dialect_raw = clean.get("dialect") or "postgresql"
    try:
        dialect = normalize_dialect(str(dialect_raw))
    except ValueError:
        return None, f"不支持的方言: {dialect_raw}"

    conn = match_connection(
        connections,
        dialect=dialect,
        connection_id=clean.get("connection_id") or default_connection_id,
    )
    if conn is None:
        return None, f"无匹配的目标连接（dialect={dialect}）"

    slow, fn, category = classify_row(clean)
    instance_id = clean.get("instance_id")
    if instance_id is None or str(instance_id).strip() == "":
        instance_id = None
    else:
        instance_id = str(instance_id)

    db_id = str(clean.get("db_id") or "").strip()
    if not db_id:
        return None, "缺少 db_id"

    draft = {
        "instance_id": instance_id,
        "connection_id": conn["id"],
        "db_id": db_id,
        "dialect": dialect,
        "issue_sql": issue,
        "preprocess_sql": as_sql_list(clean.get("preprocess_sql")),
        "clean_up_sql": as_sql_list(clean.get("clean_up_sql")),
        "query": str(clean.get("query") or clean.get("user_prompt") or ""),
        "category": category,
        "is_slow_sql": slow,
        "function": fn,
        "mode": resolve_mode(clean),
        "original_schema": str(clean.get("original_schema") or ""),
    }
    return draft, None


def parse_jsonl_text(
    text: str,
    *,
    connections: List[Dict[str, Any]],
    default_connection_id: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """解析 JSONL 文本。返回 (ok_drafts, failures[{line, reason, excerpt}])。"""
    import json

    ok: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for idx, raw_line in enumerate((text or "").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as e:
            failures.append({"line": idx, "reason": f"JSON 无效: {e}", "excerpt": line[:120]})
            continue
        if not isinstance(row, dict):
            failures.append({"line": idx, "reason": "行不是对象", "excerpt": line[:120]})
            continue
        draft, err = map_import_row(
            row, connections=connections, default_connection_id=default_connection_id
        )
        if err:
            iid = row.get("instance_id")
            failures.append(
                {
                    "line": idx,
                    "reason": err,
                    "excerpt": str(iid or line[:80]),
                }
            )
            continue
        ok.append(draft)  # type: ignore[arg-type]
    return ok, failures


def apply_manual_create(payload: Dict[str, Any], *, connections: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """单条表单：必填连接、db_id、issue_sql、是否慢 SQL。"""
    row = dict(payload or {})
    if "is_slow_sql" in row and row["is_slow_sql"] is True:
        row.setdefault("function", "efficiency")
        row.setdefault("category", "Efficiency")
    return map_import_row(row, connections=connections)


def iter_export_rows(tasks: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """只导出有非空 pred_sqls 的任务。"""
    out: List[Dict[str, Any]] = []
    for t in tasks:
        preds = t.get("pred_sqls") or []
        if isinstance(preds, str):
            preds = [preds] if preds.strip() else []
        cleaned = [str(x).strip() for x in preds if str(x).strip()]
        if not cleaned:
            continue
        out.append(
            {
                "instance_id": t.get("instance_id"),
                "db_id": t.get("db_id"),
                "dialect": t.get("dialect"),
                "category": t.get("category"),
                "is_slow_sql": bool(t.get("is_slow_sql")),
                "function": t.get("function"),
                "mode": t.get("mode"),
                "issue_sql": t.get("issue_sql") or [],
                "query": t.get("query") or "",
                "pred_sqls": cleaned,
                "diagnosis_report": t.get("diagnosis_report") or "",
                "elapsed_ms": t.get("elapsed_ms"),
                "status": t.get("status"),
            }
        )
    return out
