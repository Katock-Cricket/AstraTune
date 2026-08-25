"""短实验：沙箱模板缓存耗时 + 两槽位隔离。不跑大批量评测。"""

from __future__ import annotations

import sys
import threading
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.batch_preflight import _d_drive_free_gb
from utils.config_loader import load_all_configs, normalize_dialect
from utils.logger import setup_logger
from utils.pipeline import prepare_sandbox, setup_executors
from utils.timing import clear_timing_context, set_timing_context
from sandbox.template_cache import drop_slot_database, template_db_name, template_cache_config


def _pick_small_tables(target_ex, limit: int = 2) -> list:
    names = []
    if hasattr(target_ex, "list_base_tables"):
        names = list(target_ex.list_base_tables() or [])
    if not names:
        # MySQL
        try:
            rows = target_ex.execute("SHOW TABLES", fetch=True)
            for r in (rows[0].get("rows") if rows else []) or []:
                if isinstance(r, dict):
                    names.append(next(iter(r.values())))
        except Exception:
            pass
    picked = []
    for n in names:
        try:
            cnt = int(target_ex.get_table_count(n))
        except Exception:
            continue
        if cnt <= 5000:
            picked.append(n)
        if len(picked) >= limit:
            break
    if not picked and names:
        picked = names[:1]
    return picked


def _try_dialect(dialect: str, db_id: str) -> bool:
    logger = setup_logger()
    free = _d_drive_free_gb()
    if free is not None and free < 8:
        logger.error(f"D: 仅剩 {free:.1f} GB，停止沙箱实验")
        return False
    logger.info(f"磁盘余量约 {free:.1f} GB" if free is not None else "磁盘余量未知")

    n_workers = 4
    slots = list(range(n_workers))
    created_slots = []
    tpl_name = None
    errors = []
    markers = {}
    timings = {}

    def worker(slot: int, marker: str) -> None:
        set_timing_context(
            instance_id=f"smoke-{slot}",
            db_id=db_id,
            dialect=dialect,
            worker=slot,
            slot=slot,
        )
        target_ex = sandbox_ex = manager = None
        try:
            target_ex, sandbox_ex, _a, _o, _t, sandbox_cfg = setup_executors(
                dialect=dialect,
                db_id=db_id,
                slot_index=slot,
                workers=n_workers,
            )
            slot_db = sandbox_cfg.get("database")
            created_slots.append(slot_db)
            tables = _pick_small_tables(target_ex)
            if not tables:
                raise RuntimeError("目标库无可用表")
            logger.info(f"slot={slot} db={slot_db} tables={tables}")
            nonlocal tpl_name
            tpl_name = template_db_name(
                dialect=dialect,
                db_id=db_id,
                tables=tables,
                copy_thr=sandbox_cfg.get("copy_thr"),
                sample_size=sandbox_cfg.get("sample_size"),
            )
            prepared = prepare_sandbox(
                target_ex,
                sandbox_ex,
                sandbox_cfg,
                tables,
                db_id=db_id,
                dialect=dialect,
            )
            manager = prepared.get("manager")
            timings[f"slot{slot}_via"] = (prepared.get("info") or {}).get("template_source")
            timings[f"slot{slot}_slot"] = slot_db
            # 隔离探针：只写本槽位
            q = "`" if dialect == "mysql" else '"'
            sandbox_ex.execute(
                f"CREATE TABLE IF NOT EXISTS {q}_iso_marker{q} (id INT, note VARCHAR(64))",
                fetch=False,
            )
            lit = marker.replace("'", "''")
            sandbox_ex.execute(
                f"INSERT INTO {q}_iso_marker{q} VALUES ({slot}, '{lit}')",
                fetch=False,
            )
            rows = sandbox_ex.execute(
                f"SELECT note FROM {q}_iso_marker{q} WHERE id = {slot}",
                fetch=True,
            )
            got = (rows[0]["rows"][0] if rows and rows[0].get("rows") else {}) 
            note = got.get("note") if isinstance(got, dict) else None
            markers[slot] = {"slot_db": slot_db, "note": note, "expect": marker}
            # 二次 prepare：应走 template_hit
            prepared2 = prepare_sandbox(
                target_ex,
                sandbox_ex,
                sandbox_cfg,
                tables,
                db_id=db_id,
                dialect=dialect,
            )
            timings[f"slot{slot}_via2"] = (prepared2.get("info") or {}).get("template_source")
        except Exception as e:
            errors.append(f"slot={slot}: {e}")
            logger.error(f"slot {slot} 失败: {e}", exc_info=True)
        finally:
            try:
                if manager is not None:
                    manager.cleanup_sandbox()
            except Exception:
                pass
            try:
                if target_ex is not None:
                    target_ex.close()
            except Exception:
                pass
            try:
                if sandbox_ex is not None:
                    sandbox_ex.close()
            except Exception:
                pass
            clear_timing_context()

    n_slots = len(slots)
    marker_vals = {i: str(uuid.uuid4())[:8] for i in slots}
    threads = [
        threading.Thread(target=worker, args=(i, marker_vals[i])) for i in slots
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logger.info(f"timings_meta={timings}")
    logger.info(f"markers={markers}")
    if errors:
        logger.error("实验出错: " + " | ".join(errors))

    slot_dbs = [markers[i]["slot_db"] for i in slots if i in markers]
    isolated = (
        len(markers) == n_slots
        and all(markers.get(i, {}).get("note") == marker_vals[i] for i in slots)
        and len(set(slot_dbs)) == n_slots
    )
    logger.info(f"isolation_ok={isolated}")

    # 释放实验槽位与本次模板（不碰默认 astratune_sandbox）
    try:
        cfgs = load_all_configs()
        from utils.config_loader import get_sandbox_config

        s_cfg = get_sandbox_config(cfgs, dialect)
        for name in list(dict.fromkeys(created_slots)):
            drop_slot_database(s_cfg, dialect, name)
        if tpl_name:
            from sandbox.template_cache import _drop_database

            _drop_database(s_cfg, dialect, tpl_name)
    except Exception as e:
        logger.warning(f"实验后清理警告: {e}")
    return isolated and not errors


def main() -> None:
    logger = setup_logger()
    logger.info("smoke_sandbox_template：单方言、4 槽位隔离，禁止大批量评测")
    cfg = template_cache_config()
    logger.info(f"template_cache={cfg}")
    attempts = [
        ("mysql", "debit_card_specializing"),
        ("postgresql", "financial"),
    ]
    for dialect, db_id in attempts:
        logger.info(f"尝试 dialect={dialect} db_id={db_id}")
        try:
            ok = _try_dialect(normalize_dialect(dialect), db_id)
            if ok:
                logger.info("隔离短实验通过")
                return
        except Exception as e:
            logger.warning(f"{dialect}/{db_id} 不可用: {e}")
    raise SystemExit("沙箱短实验未通过（Docker/目标库可能未就绪）")


if __name__ == "__main__":
    main()
