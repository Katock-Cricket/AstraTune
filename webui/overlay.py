"""配置 overlay 纯函数：仓库 JSON 只读，运行时覆盖不写回 config/。"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

from utils.config_loader import normalize_dialect

WEBUI_WORKERS_MIN = 1
WEBUI_WORKERS_MAX = 4
WEBUI_WORKERS_DEFAULT = 2

_LLM_OVERLAY_SKIP = {"api_key_source", "api_key_env"}


def deep_merge(base: Dict[str, Any], overlay: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """字典递归合并；overlay 非 dict 值整段覆盖。不修改入参。"""
    out = copy.deepcopy(base or {})
    if not overlay:
        return out
    for key, val in overlay.items():
        if isinstance(val, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], val)
        else:
            out[key] = copy.deepcopy(val)
    return out


def clamp_workers(n: Any, default: int = WEBUI_WORKERS_DEFAULT) -> int:
    try:
        v = int(n)
    except (TypeError, ValueError):
        v = default
    return max(WEBUI_WORKERS_MIN, min(WEBUI_WORKERS_MAX, v))


def seed_targets_from_target_db(target_db: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """从 config/target_db.json 灌入两条默认具名连接。"""
    src = target_db or {}
    out: List[Dict[str, Any]] = []
    for dialect, name in (
        ("postgresql", "PostgreSQL 目标库"),
        ("mysql", "MySQL 目标库"),
    ):
        sec = src.get(dialect)
        if not isinstance(sec, dict):
            continue
        default_port = 5432 if dialect == "postgresql" else 3306
        try:
            port = int(sec.get("port") or default_port)
        except (TypeError, ValueError):
            port = default_port
        out.append(
            {
                "id": f"{dialect}-default",
                "name": name,
                "dialect": dialect,
                "host": str(sec.get("host") or "127.0.0.1"),
                "port": port,
                "user": str(sec.get("user") or ""),
                "password": str(sec.get("password") or ""),
                "note": "",
                "is_default": True,
            }
        )
    return out


def match_connection(
    connections: List[Dict[str, Any]],
    *,
    dialect: Optional[str] = None,
    connection_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if connection_id:
        cid = str(connection_id)
        for row in connections:
            if str(row.get("id")) == cid:
                return row
        return None
    if not dialect:
        return None
    try:
        d = normalize_dialect(dialect)
    except ValueError:
        return None
    same = [c for c in connections if str(c.get("dialect") or "") == d]
    for row in same:
        if row.get("is_default"):
            return row
    return same[0] if same else None


def _strip_webui_llm_keys(llm: Dict[str, Any]) -> Dict[str, Any]:
    """推理用 LLM 段：env 模式不注入 api_key；direct 保留 api_key。"""
    out = {k: v for k, v in llm.items() if k not in _LLM_OVERLAY_SKIP}
    source = str(llm.get("api_key_source") or "env").strip().lower()
    if source != "direct":
        out.pop("api_key", None)
    return out


def sanitize_agents_for_inference(agents: Dict[str, Any]) -> Dict[str, Any]:
    """去掉 WebUI 专用键，得到可交给 run_single_instance 的 agents 段。"""
    out = copy.deepcopy(agents or {})
    diag = out.get("diagnosis")
    if isinstance(diag, dict) and isinstance(diag.get("llm"), dict):
        diag["llm"] = _strip_webui_llm_keys(diag["llm"])
    opt = out.get("optimization")
    if isinstance(opt, dict):
        if isinstance(opt.get("llm"), dict):
            opt["llm"] = _strip_webui_llm_keys(opt["llm"])
        verify = opt.get("verify")
        if isinstance(verify, dict) and isinstance(verify.get("llm"), dict):
            verify["llm"] = _strip_webui_llm_keys(verify["llm"])
    return out


def resolve_env_key_aliases(agents: Dict[str, Any]) -> List[tuple[str, str]]:
    """source=env 且环境变量名不是 OPENAI_API_KEY 时，worker 需做别名。返回 (src, dest)。"""
    aliases: List[tuple[str, str]] = []
    dest = "OPENAI_API_KEY"

    def _pick(llm: Any) -> None:
        if not isinstance(llm, dict):
            return
        source = str(llm.get("api_key_source") or "env").strip().lower()
        if source != "env":
            return
        name = str(llm.get("api_key_env") or dest).strip() or dest
        if name != dest:
            aliases.append((name, dest))

    diag = (agents or {}).get("diagnosis") or {}
    _pick(diag.get("llm") if isinstance(diag, dict) else None)
    opt = (agents or {}).get("optimization") or {}
    if isinstance(opt, dict):
        _pick(opt.get("llm"))
        verify = opt.get("verify") or {}
        if isinstance(verify, dict):
            _pick(verify.get("llm"))
    # 去重，保序
    seen = set()
    uniq: List[tuple[str, str]] = []
    for item in aliases:
        if item not in seen:
            seen.add(item)
            uniq.append(item)
    return uniq


def merge_runtime_configs(
    base: Dict[str, Any],
    *,
    agents_overlay: Optional[Dict[str, Any]] = None,
    sandbox_overlay: Optional[Dict[str, Any]] = None,
    connection: Optional[Dict[str, Any]] = None,
    db_id: Optional[str] = None,
) -> Dict[str, Any]:
    """合并出 run_single_instance 用 configs。不写磁盘。"""
    merged = copy.deepcopy(base or {})
    agents = deep_merge(merged.get("agents") or {}, agents_overlay)
    merged["agents"] = sanitize_agents_for_inference(agents)
    sandbox = deep_merge(merged.get("sandbox") or {}, sandbox_overlay)
    merged["sandbox"] = sandbox
    if connection:
        dialect = normalize_dialect(str(connection.get("dialect") or "postgresql"))
        td = dict(merged.get("target_db") or {})
        section = dict(td.get(dialect) or {})
        section.update(
            {
                "dialect": dialect,
                "host": connection.get("host") or section.get("host") or "127.0.0.1",
                "port": int(connection.get("port") or section.get("port") or 5432),
                "user": connection.get("user") if connection.get("user") is not None else section.get("user"),
                "password": connection.get("password")
                if connection.get("password") is not None
                else section.get("password"),
            }
        )
        if db_id:
            section["database"] = db_id
        td[dialect] = section
        merged["target_db"] = td
    elif db_id:
        td = dict(merged.get("target_db") or {})
        for dialect, sec in list(td.items()):
            if isinstance(sec, dict):
                sec = dict(sec)
                sec["database"] = db_id
                td[dialect] = sec
        merged["target_db"] = td
    return merged
