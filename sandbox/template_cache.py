"""沙箱模板库缓存：按 (dialect, db_id, tables, copy_thr, sample_size) 快照一次，
再 copy-on-write 到各 worker 独立槽位。模板只读，槽位可写，互不 DROP 对方库。

MySQL：同实例 `CREATE TABLE LIKE` + `INSERT SELECT`（可并行读模板）。
PG：`CREATE DATABASE ... TEMPLATE`（对模板短暂独占，线程锁串行化克隆）。
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional

from utils.logger import default_logger
from utils.timing import log_timing, timed_stage

META_TABLE = "_astratune_tpl_meta"
_NAME_SAFE = re.compile(r"[^a-zA-Z0-9_]")
_TPL_LOCKS: Dict[str, threading.Lock] = {}
_TPL_LOCKS_GUARD = threading.Lock()
_LRU: OrderedDict[str, float] = OrderedDict()
_LRU_GUARD = threading.Lock()
_ACTIVE_TEMPLATES: Dict[str, int] = {}
_ACTIVE_GUARD = threading.Lock()


def template_cache_config(sandbox_root: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from utils.config_loader import load_all_configs

    root = sandbox_root
    if root is None:
        root = (load_all_configs().get("sandbox") or {})
    block = root.get("template_cache") if isinstance(root, dict) else None
    if not isinstance(block, dict):
        block = {}
    enabled = block.get("enabled", True)
    return {
        "enabled": bool(enabled),
        "max_templates": int(block.get("max_templates") or 4),
        "prefix": str(block.get("prefix") or "astratune_tpl_"),
    }


def fingerprint(
    *,
    dialect: str,
    db_id: str,
    tables: List[str],
    copy_thr: Any,
    sample_size: Any,
) -> str:
    payload = {
        "d": str(dialect or ""),
        "db": str(db_id or ""),
        "t": sorted({str(x) for x in (tables or []) if str(x).strip()}),
        "c": int(copy_thr) if copy_thr is not None else 0,
        "s": int(sample_size) if sample_size is not None else 0,
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]


def template_db_name(
    *,
    dialect: str,
    db_id: str,
    tables: List[str],
    copy_thr: Any = None,
    sample_size: Any = None,
    prefix: str = "astratune_tpl_",
) -> str:
    fp = fingerprint(
        dialect=dialect,
        db_id=db_id,
        tables=tables,
        copy_thr=copy_thr,
        sample_size=sample_size,
    )
    safe_db = _NAME_SAFE.sub("_", str(db_id or "db"))[:18].strip("_") or "db"
    # MySQL 库名 ≤64
    name = f"{prefix}{safe_db}_{fp}"
    return name[:63]


def _lock_for(name: str) -> threading.Lock:
    with _TPL_LOCKS_GUARD:
        if name not in _TPL_LOCKS:
            _TPL_LOCKS[name] = threading.Lock()
        return _TPL_LOCKS[name]


def _quote_mysql(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _quote_pg(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _ident(name: str, dialect: str) -> str:
    return _quote_mysql(name) if dialect == "mysql" else _quote_pg(name)


def _touch_lru(name: str) -> None:
    with _LRU_GUARD:
        _LRU.pop(name, None)
        _LRU[name] = time.time()


def _is_meta_table(name: str) -> bool:
    return (name or "").lower() == META_TABLE.lower()


def bind_restore(manager, restore_fn, *, template_name: Optional[str] = None) -> None:
    release_template_binding(manager)
    manager._template_restore_fn = restore_fn
    if template_name:
        manager._template_db_name = template_name
        _inc_active_template(str(template_name))


def release_template_binding(manager) -> None:
    name = getattr(manager, "_template_db_name", None)
    if name:
        _dec_active_template(str(name))
        manager._template_db_name = None
    manager._template_restore_fn = None


def _inc_active_template(name: str) -> None:
    with _ACTIVE_GUARD:
        _ACTIVE_TEMPLATES[name] = _ACTIVE_TEMPLATES.get(name, 0) + 1


def _dec_active_template(name: str) -> None:
    with _ACTIVE_GUARD:
        n = _ACTIVE_TEMPLATES.get(name, 0) - 1
        if n <= 0:
            _ACTIVE_TEMPLATES.pop(name, None)
        else:
            _ACTIVE_TEMPLATES[name] = n


def _is_template_in_use(name: str) -> bool:
    with _ACTIVE_GUARD:
        return _ACTIVE_TEMPLATES.get(name, 0) > 0


def has_restore(manager) -> bool:
    return callable(getattr(manager, "_template_restore_fn", None))


def restore_via_manager(manager) -> Optional[Dict[str, Any]]:
    fn = getattr(manager, "_template_restore_fn", None)
    if not callable(fn):
        return None
    return fn()


def prepare_with_template(
    *,
    manager,
    tables: List[str],
    db_id: str,
    dialect: str,
    sandbox_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """
    返回 sandbox_info。失败时调用方应回退到 cleanup+setup。
    """
    cfg = template_cache_config()
    if not cfg["enabled"] or not db_id or not tables:
        raise RuntimeError("template cache disabled or missing db_id/tables")

    dialect = str(dialect or "postgresql").lower()
    if dialect in ("pg", "postgres"):
        dialect = "postgresql"
    tpl_name = template_db_name(
        dialect=dialect,
        db_id=db_id,
        tables=tables,
        copy_thr=sandbox_cfg.get("copy_thr"),
        sample_size=sandbox_cfg.get("sample_size"),
        prefix=cfg["prefix"],
    )
    slot_db = str((getattr(manager.sandbox_executor, "config", {}) or {}).get("database") or "")
    if not slot_db or slot_db == tpl_name:
        raise RuntimeError("slot db missing or collides with template")

    lk = _lock_for(tpl_name)
    wait_t0 = time.perf_counter()
    got = lk.acquire(timeout=180)
    wait_ms = (time.perf_counter() - wait_t0) * 1000.0
    if wait_ms > 5:
        log_timing("sandbox_template_lock_wait", wait_ms, template=tpl_name, slot=slot_db)
    if not got:
        raise RuntimeError(f"template lock timeout: {tpl_name}")

    source = "unknown"
    info: Optional[Dict[str, Any]] = None
    mysql_hit_copy = False
    try:
        ready = _template_is_ready(manager, dialect, sandbox_cfg, tpl_name)
        if not ready:
            source = "template_miss"
            _evict_if_needed(manager, dialect, sandbox_cfg, cfg["max_templates"], keep=tpl_name)
            with timed_stage("sandbox_cleanup", mode="pre_setup"):
                try:
                    manager.cleanup_sandbox()
                except Exception as e:
                    default_logger.warning(f"沙箱预清理警告: {e}")
            with timed_stage("sandbox_clone_from_target", tables=len(tables)):
                info = manager.setup_sandbox(tables)
            with timed_stage("sandbox_snapshot_to_template", template=tpl_name):
                _snapshot_slot_to_template(
                    manager, dialect, sandbox_cfg, tpl_name, tables, info
                )
        elif dialect == "postgresql":
            # PG CREATE DATABASE TEMPLATE 需独占源库，命中路径也在锁内串行克隆
            source = "template_hit"
            with timed_stage("sandbox_clone_from_template", template=tpl_name, slot=slot_db):
                info = _copy_template_to_slot(
                    manager, dialect, sandbox_cfg, tpl_name, tables
                )
        else:
            # MySQL 模板只读，命中后释放建库锁，多槽位可并行 INSERT SELECT
            source = "template_hit"
            mysql_hit_copy = True
    finally:
        try:
            lk.release()
        except Exception:
            pass

    if mysql_hit_copy:
        with timed_stage("sandbox_clone_from_template", template=tpl_name, slot=slot_db):
            info = _copy_template_to_slot(
                manager, dialect, sandbox_cfg, tpl_name, tables
            )

    _touch_lru(tpl_name)
    bind_restore(
        manager,
        lambda: _restore_slot(manager, dialect, sandbox_cfg, tpl_name, tables),
        template_name=tpl_name,
    )
    if isinstance(info, dict):
        fid = info.get("fidelity") if isinstance(info.get("fidelity"), dict) else {}
        fid["template_db"] = tpl_name
        fid["template_source"] = source
        info["fidelity"] = fid
        info["template_db"] = tpl_name
        info["template_source"] = source
    log_timing("sandbox_template", 0.0, source=source, template=tpl_name, slot=slot_db)
    return info or {}


def _restore_slot(manager, dialect, sandbox_cfg, tpl_name, tables) -> Dict[str, Any]:
    slot_db = str((getattr(manager.sandbox_executor, "config", {}) or {}).get("database") or "")
    with timed_stage("sandbox_reset_from_template", template=tpl_name, slot=slot_db):
        info = _copy_template_to_slot(manager, dialect, sandbox_cfg, tpl_name, tables)
    return info


def _admin_cfg(sandbox_cfg: Dict[str, Any], dialect: str) -> Dict[str, Any]:
    cfg = dict(sandbox_cfg)
    cfg["database"] = "mysql" if dialect == "mysql" else "postgres"
    return cfg


def _make_admin(sandbox_cfg: Dict[str, Any], dialect: str):
    from utils.pipeline import create_executor

    admin = create_executor(_admin_cfg(sandbox_cfg, dialect))
    admin.connect()
    return admin


def _db_exists(admin, dialect: str, name: str) -> bool:
    if dialect == "mysql":
        lit = name.replace("'", "''")
        rows = admin.execute(
            f"SELECT SCHEMA_NAME AS n FROM information_schema.SCHEMATA "
            f"WHERE SCHEMA_NAME = '{lit}'",
            fetch=True,
        )
        return bool(rows and rows[0].get("rows"))
    lit = name.replace("'", "''")
    rows = admin.execute(
        f"SELECT 1 AS ok FROM pg_database WHERE datname = '{lit}'",
        fetch=True,
    )
    return bool(rows and rows[0].get("rows"))


def _template_is_ready(manager, dialect, sandbox_cfg, tpl_name: str) -> bool:
    admin = None
    try:
        admin = _make_admin(sandbox_cfg, dialect)
        if not _db_exists(admin, dialect, tpl_name):
            return False
    except Exception as e:
        default_logger.debug(f"检查模板库失败: {e}")
        return False
    finally:
        if admin is not None:
            try:
                admin.close()
            except Exception:
                pass
    return _read_meta_ready(sandbox_cfg, dialect, tpl_name)


def _read_meta_ready(sandbox_cfg, dialect, tpl_name: str) -> bool:
    from utils.pipeline import create_executor

    cfg = dict(sandbox_cfg)
    cfg["database"] = tpl_name
    ex = create_executor(cfg)
    try:
        ex.connect()
        if dialect == "mysql":
            sql = f"SELECT v FROM {_quote_mysql(META_TABLE)} WHERE k = 'ready'"
        else:
            sql = f'SELECT v FROM {_quote_pg(META_TABLE)} WHERE k = \'ready\''
        try:
            rows = ex.execute(sql, fetch=True)
        except Exception:
            return False
        vals = (rows[0].get("rows") if rows else None) or []
        if not vals:
            return False
        row = vals[0]
        v = row.get("v") if isinstance(row, dict) else (row[0] if row else None)
        return str(v) == "1"
    except Exception:
        return False
    finally:
        try:
            ex.close()
        except Exception:
            pass


def _ensure_db(sandbox_cfg, dialect, name: str) -> None:
    from utils.pipeline import ensure_sandbox_database

    ensure_sandbox_database(sandbox_cfg, name)


def _list_base_tables(ex, dialect: str, db_name: str) -> List[str]:
    if dialect == "mysql":
        lit = db_name.replace("'", "''")
        rows = ex.execute(
            "SELECT TABLE_NAME AS n FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = '{lit}' AND TABLE_TYPE = 'BASE TABLE'",
            fetch=True,
        )
        out = []
        for r in (rows[0].get("rows") if rows else []) or []:
            n = r.get("n") or r.get("TABLE_NAME")
            if n and not _is_meta_table(str(n)):
                out.append(str(n))
        return out
    if hasattr(ex, "list_base_tables"):
        return [t for t in (ex.list_base_tables() or []) if not _is_meta_table(t)]
    return []


def _write_meta(ex, dialect: str, info: Dict[str, Any]) -> None:
    payload = json.dumps(info, ensure_ascii=False, default=str)
    if dialect == "mysql":
        ex.execute(f"DROP TABLE IF EXISTS {_quote_mysql(META_TABLE)}", fetch=False)
        ex.execute(
            f"CREATE TABLE {_quote_mysql(META_TABLE)} ("
            "k VARCHAR(64) PRIMARY KEY, v LONGTEXT"
            ") ENGINE=InnoDB",
            fetch=False,
        )
        # 避免把超长 fidelity 里的引号搞砸：用 insert_rows
        ex.insert_rows(
            META_TABLE,
            ["k", "v"],
            [{"k": "ready", "v": "1"}, {"k": "fidelity_json", "v": payload}],
            quote="`",
            batch_size=10,
        )
        return
    q = _quote_pg(META_TABLE)
    try:
        ex.execute_autocommit(f"DROP TABLE IF EXISTS {q}")
    except Exception:
        ex.execute(f"DROP TABLE IF EXISTS {q}", fetch=False)
    try:
        if hasattr(ex, "connection") and ex.connection:
            ex.connection.rollback()
    except Exception:
        pass
    ex.execute(
        f"CREATE TABLE {q} (k TEXT PRIMARY KEY, v TEXT)",
        fetch=False,
    )
    try:
        if hasattr(ex, "connection") and ex.connection:
            ex.connection.commit()
    except Exception:
        pass
    ex.insert_rows(
        META_TABLE,
        ["k", "v"],
        [{"k": "ready", "v": "1"}, {"k": "fidelity_json", "v": payload}],
        quote='"',
        batch_size=10,
    )


def _drop_meta_from_slot(ex, dialect: str) -> None:
    try:
        if dialect == "mysql":
            ex.execute(f"DROP TABLE IF EXISTS {_quote_mysql(META_TABLE)}", fetch=False)
        else:
            if hasattr(ex, "execute_autocommit"):
                ex.execute_autocommit(f"DROP TABLE IF EXISTS {_quote_pg(META_TABLE)}")
            else:
                ex.execute(f"DROP TABLE IF EXISTS {_quote_pg(META_TABLE)}", fetch=False)
    except Exception:
        pass


def _mysql_copy_tables(ex, src_db: str, dst_db: str, tables: List[str]) -> None:
    src = _quote_mysql(src_db)
    dst = _quote_mysql(dst_db)
    ex.execute("SET FOREIGN_KEY_CHECKS = 0", fetch=False)
    try:
        existing = _list_base_tables(ex, "mysql", dst_db)
        for t in existing:
            ex.execute(f"DROP TABLE IF EXISTS {dst}.{_quote_mysql(t)}", fetch=False)
        _drop_meta_from_slot(ex, "mysql")
        for t in tables:
            qt = _quote_mysql(t)
            ex.execute(f"DROP TABLE IF EXISTS {dst}.{qt}", fetch=False)
            ex.execute(f"CREATE TABLE {dst}.{qt} LIKE {src}.{qt}", fetch=False)
            ex.execute(f"INSERT INTO {dst}.{qt} SELECT * FROM {src}.{qt}", fetch=False)
    finally:
        try:
            ex.execute("SET FOREIGN_KEY_CHECKS = 1", fetch=False)
        except Exception:
            pass


def _pg_clone_database(sandbox_cfg, src_db: str, dst_db: str, slot_executor) -> None:
    """DROP dst + CREATE DATABASE dst TEMPLATE src。src 必须存在；否则不 DROP dst。"""
    admin = _make_admin(sandbox_cfg, "postgresql")
    try:
        if not _db_exists(admin, "postgresql", src_db):
            raise RuntimeError(f'template database "{src_db}" does not exist')
        try:
            slot_executor.close()
        except Exception:
            pass
        src_lit = src_db.replace("'", "''")
        dst_lit = dst_db.replace("'", "''")
        admin.execute_autocommit(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname IN ('{src_lit}', '{dst_lit}') "
            "AND pid <> pg_backend_pid()"
        )
        admin.execute_autocommit(f"DROP DATABASE IF EXISTS {_quote_pg(dst_db)}")
        admin.execute_autocommit(
            f"CREATE DATABASE {_quote_pg(dst_db)} TEMPLATE {_quote_pg(src_db)}"
        )
    finally:
        try:
            admin.close()
        except Exception:
            pass
    slot_executor.connect()


def _snapshot_slot_to_template(
    manager, dialect, sandbox_cfg, tpl_name: str, tables: List[str], info: Dict[str, Any]
) -> None:
    slot_db = str(manager.sandbox_executor.config.get("database") or "")
    if dialect == "mysql":
        _ensure_db(sandbox_cfg, dialect, tpl_name)
        _mysql_copy_tables(manager.sandbox_executor, slot_db, tpl_name, tables)
        from utils.pipeline import create_executor

        cfg = dict(sandbox_cfg)
        cfg["database"] = tpl_name
        tex = create_executor(cfg)
        try:
            tex.connect()
            _write_meta(tex, dialect, info or {})
        finally:
            try:
                tex.close()
            except Exception:
                pass
        return

    # PG: CREATE DATABASE template TEMPLATE slot
    _pg_clone_database(sandbox_cfg, slot_db, tpl_name, manager.sandbox_executor)
    from utils.pipeline import create_executor

    cfg = dict(sandbox_cfg)
    cfg["database"] = tpl_name
    tex = create_executor(cfg)
    try:
        tex.connect()
        _write_meta(tex, dialect, info or {})
        try:
            tex.close()
        except Exception:
            pass
    except Exception:
        try:
            tex.close()
        except Exception:
            pass
        raise


def _copy_template_to_slot(
    manager, dialect, sandbox_cfg, tpl_name: str, tables: List[str]
) -> Dict[str, Any]:
    slot_db = str(manager.sandbox_executor.config.get("database") or "")
    info = _load_info_from_template(sandbox_cfg, dialect, tpl_name) or {
        "tables": [],
        "sampled_tables": [],
        "fidelity": {},
    }
    if dialect == "mysql":
        # CREATE TABLE LIKE + INSERT SELECT 会先 DROP 同名表，不必再全库 cleanup
        _mysql_copy_tables(manager.sandbox_executor, tpl_name, slot_db, tables)
        _drop_meta_from_slot(manager.sandbox_executor, dialect)
        # CREATE TABLE LIKE 不会带走 mysql.innodb_* 统计；从 target 再注入一次（无数据搬运）
        with timed_stage("sandbox_stats_reinject", tables=len(tables)):
            _reinject_mysql_stats(manager, tables, info)
        return info

    clk = _lock_for(f"{tpl_name}::clone")
    wait_t0 = time.perf_counter()
    got = clk.acquire(timeout=180)
    wait_ms = (time.perf_counter() - wait_t0) * 1000.0
    if wait_ms > 5:
        log_timing(
            "sandbox_template_lock_wait",
            wait_ms,
            template=tpl_name,
            slot=slot_db,
            kind="pg_clone",
        )
    if not got:
        raise RuntimeError(f"pg template clone lock timeout: {tpl_name}")
    try:
        _pg_clone_database(sandbox_cfg, tpl_name, slot_db, manager.sandbox_executor)
        _drop_meta_from_slot(manager.sandbox_executor, dialect)
        return info
    finally:
        try:
            clk.release()
        except Exception:
            pass


def _reinject_mysql_stats(manager, tables: List[str], info: Dict[str, Any]) -> None:
    try:
        from sandbox.statistics import inject_statistics
    except Exception:
        return
    by_resolved = {}
    for entry in info.get("tables") or []:
        if isinstance(entry, dict) and entry.get("resolved"):
            by_resolved[entry["resolved"]] = entry
    for table in tables:
        resolved = table
        if hasattr(manager.target_executor, "resolve_table_name"):
            hit = manager.target_executor.resolve_table_name(table)
            if hit:
                resolved = hit
        try:
            report = inject_statistics(
                manager.target_executor,
                manager.sandbox_executor,
                resolved,
                target_quote=manager.target_quote,
                sandbox_quote=manager.sandbox_quote,
            )
            if resolved in by_resolved and isinstance(report, dict):
                by_resolved[resolved]["stats"] = report
        except Exception as e:
            default_logger.warning(f"模板槽位统计再注入失败 {resolved}: {e}")


def _load_info_from_template(sandbox_cfg, dialect, tpl_name: str) -> Optional[Dict[str, Any]]:
    from utils.pipeline import create_executor

    cfg = dict(sandbox_cfg)
    cfg["database"] = tpl_name
    ex = create_executor(cfg)
    try:
        ex.connect()
        if dialect == "mysql":
            sql = f"SELECT v FROM {_quote_mysql(META_TABLE)} WHERE k = 'fidelity_json'"
        else:
            sql = f"SELECT v FROM {_quote_pg(META_TABLE)} WHERE k = 'fidelity_json'"
        rows = ex.execute(sql, fetch=True)
        vals = (rows[0].get("rows") if rows else None) or []
        if not vals:
            return None
        row = vals[0]
        raw = row.get("v") if isinstance(row, dict) else (row[0] if row else None)
        if not raw:
            return None
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception as e:
        default_logger.debug(f"读取模板 meta 失败: {e}")
        return None
    finally:
        try:
            ex.close()
        except Exception:
            pass


def _evict_if_needed(manager, dialect, sandbox_cfg, max_n: int, keep: str) -> None:
    if max_n <= 0:
        return
    with _LRU_GUARD:
        names = list(_LRU.keys())
    # 加上实例上已有的 tpl 库
    existing = _list_template_dbs(sandbox_cfg, dialect, sandbox_cfg.get("prefix") if False else "astratune_tpl_")
    for n in existing:
        if n not in names:
            names.append(n)
    live = [n for n in names if n != keep and not _is_template_in_use(n)]
    in_use_skipped = [n for n in names if n != keep and _is_template_in_use(n)]
    if in_use_skipped:
        default_logger.debug(f"模板淘汰跳过在用库: {in_use_skipped[:5]}")
    overflow = len(live) + 1 - max_n  # +1 for keep we are about to create
    if overflow <= 0:
        return
    victims = live[:overflow]
    for name in victims:
        default_logger.warning(f"模板库超额，淘汰 {name}")
        _drop_database(sandbox_cfg, dialect, name, slot_executor=manager.sandbox_executor)
        with _LRU_GUARD:
            _LRU.pop(name, None)


def _list_template_dbs(sandbox_cfg, dialect, prefix: str) -> List[str]:
    admin = None
    try:
        admin = _make_admin(sandbox_cfg, dialect)
        if dialect == "mysql":
            lit = prefix.replace("'", "''")
            rows = admin.execute(
                "SELECT SCHEMA_NAME AS n FROM information_schema.SCHEMATA "
                f"WHERE SCHEMA_NAME LIKE '{lit}%'",
                fetch=True,
            )
        else:
            lit = prefix.replace("'", "''")
            rows = admin.execute(
                "SELECT datname AS n FROM pg_database "
                f"WHERE datname LIKE '{lit}%'",
                fetch=True,
            )
        out = []
        for r in (rows[0].get("rows") if rows else []) or []:
            n = r.get("n") if isinstance(r, dict) else None
            if n:
                out.append(str(n))
        return out
    except Exception as e:
        default_logger.debug(f"列举模板库失败: {e}")
        return []
    finally:
        if admin is not None:
            try:
                admin.close()
            except Exception:
                pass


def _drop_database(sandbox_cfg, dialect, name: str, slot_executor=None) -> None:
    if not name or not str(name).startswith("astratune_tpl_"):
        default_logger.warning(f"拒绝删除非模板库: {name}")
        return
    admin = _make_admin(sandbox_cfg, dialect)
    try:
        if dialect == "mysql":
            admin.execute_autocommit(f"DROP DATABASE IF EXISTS {_quote_mysql(name)}")
        else:
            if slot_executor is not None:
                try:
                    cur = str((getattr(slot_executor, "config", {}) or {}).get("database") or "")
                    if cur == name:
                        slot_executor.close()
                except Exception:
                    pass
            lit = name.replace("'", "''")
            admin.execute_autocommit(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname = '{lit}' AND pid <> pg_backend_pid()"
            )
            admin.execute_autocommit(f"DROP DATABASE IF EXISTS {_quote_pg(name)}")
        default_logger.info(f"已删除模板库: {name}")
    finally:
        try:
            admin.close()
        except Exception:
            pass


def drop_slot_database(sandbox_cfg, dialect: str, name: str, slot_executor=None) -> None:
    """实验后释放 worker 槽位库（astratune_sandbox_N），不删默认 astratune_sandbox。"""
    if not name or name == "astratune_sandbox":
        return
    if not str(name).startswith("astratune_sandbox_"):
        default_logger.warning(f"拒绝删除非槽位库: {name}")
        return
    admin = _make_admin(sandbox_cfg, dialect)
    try:
        if dialect == "mysql":
            admin.execute_autocommit(f"DROP DATABASE IF EXISTS {_quote_mysql(name)}")
        else:
            if slot_executor is not None:
                try:
                    slot_executor.close()
                except Exception:
                    pass
            lit = name.replace("'", "''")
            admin.execute_autocommit(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname = '{lit}' AND pid <> pg_backend_pid()"
            )
            admin.execute_autocommit(f"DROP DATABASE IF EXISTS {_quote_pg(name)}")
        default_logger.info(f"已删除槽位库: {name}")
    finally:
        try:
            admin.close()
        except Exception:
            pass
