"""从目标库采集统计信息并注入沙箱，使采样表的 EXPLAIN 代价更接近生产态。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from sandbox.executor import DBExecutor
from utils.logger import default_logger


def _first_rows(results: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if not results or not isinstance(results[0], dict):
        return []
    rows = results[0].get("rows") or []
    return list(rows) if rows else []


def _quote_ident(name: str, quote: str = '"') -> str:
    return f"{quote}{name.replace(quote, quote + quote)}{quote}"


def inject_statistics(
    target_executor: DBExecutor,
    sandbox_executor: DBExecutor,
    table_name: str,
    *,
    target_quote: str = '"',
    sandbox_quote: str = '"',
) -> Dict[str, Any]:
    """
    按方言将目标库统计信息注入沙箱。表级必达（尽力）；列级失败仅打 warning。
    返回 {ok: [...], fail: [...], skip: [...]} 供 sandbox_fidelity。
    """
    from sandbox.mysql_executor import MySQLExecutor
    from sandbox.pg_executor import PGExecutor

    report: Dict[str, Any] = {"ok": [], "fail": [], "skip": []}
    if isinstance(target_executor, PGExecutor) and isinstance(sandbox_executor, PGExecutor):
        report = _inject_pg_statistics(target_executor, sandbox_executor, table_name)
    elif isinstance(target_executor, MySQLExecutor) and isinstance(sandbox_executor, MySQLExecutor):
        report = _inject_mysql_statistics(
            target_executor, sandbox_executor, table_name, target_quote, sandbox_quote
        ) or report
    else:
        default_logger.warning(
            f"统计信息克隆跳过（方言不匹配或不支持）: {table_name}"
        )
        report["skip"].append("dialect_mismatch")
    return report


def _inject_pg_statistics(
    target: DBExecutor,
    sandbox: DBExecutor,
    table_name: str,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {"ok": [], "fail": [], "skip": []}
    # regclass 必须用字符串字面量；双引号标识符会被解析成列引用
    rel_lit = _sql_literal(table_name)
    rel_ident = _quote_ident(table_name)
    try:
        table_stats = _first_rows(
            target.execute(
                f"SELECT c.reltuples::float8 AS reltuples, c.relpages::int4 AS relpages "
                f"FROM pg_class c "
                f"WHERE c.oid = {rel_lit}::regclass"
            )
        )
        if not table_stats:
            default_logger.warning(f"PG 表级统计采集失败（无结果）: {table_name}")
            report["fail"].append("table_level:no_rows")
            return report

        row0 = table_stats[0]
        if isinstance(row0, dict):
            try:
                reltuples = float(row0.get("reltuples") or 0)
                relpages = int(row0.get("relpages") or 0)
            except (TypeError, ValueError) as e:
                default_logger.warning(f"PG 表级统计解析失败: {table_name}: {e}")
                report["fail"].append("table_level:parse")
                return report
        else:
            default_logger.warning(f"PG 表级统计采集失败（行格式异常）: {table_name}")
            report["fail"].append("table_level:bad_row")
            return report

        # 先 ANALYZE 生成 pg_statistic 槽位，再覆盖表级基数（避免采样基数污染 EXPLAIN）
        try:
            sandbox.execute(f"ANALYZE {rel_ident}", fetch=False)
        except Exception as e:
            default_logger.warning(f"PG ANALYZE 跳过（不影响表级注入）: {table_name}: {e}")
            report["skip"].append("analyze")

        sandbox.execute(
            f"UPDATE pg_class SET reltuples = {reltuples}, relpages = {relpages} "
            f"WHERE oid = {rel_lit}::regclass",
            fetch=False,
        )
        default_logger.info(
            f"PG 表级统计已注入: {table_name} "
            f"(reltuples={reltuples}, relpages={relpages})"
        )
        report["ok"].append("table_level")

        index_stats = _first_rows(
            target.execute(
                f"SELECT i.relname AS index_name, "
                f"i.reltuples::float8 AS reltuples, i.relpages::int4 AS relpages "
                f"FROM pg_class t "
                f"JOIN pg_index x ON x.indrelid = t.oid "
                f"JOIN pg_class i ON i.oid = x.indexrelid "
                f"WHERE t.oid = {rel_lit}::regclass"
            )
        )
        for idx in index_stats:
            idx_name = idx["index_name"]
            idx_lit = _sql_literal(idx_name)
            try:
                sandbox.execute(
                    f"UPDATE pg_class SET reltuples = {float(idx['reltuples'])}, "
                    f"relpages = {int(idx['relpages'])} "
                    f"WHERE oid = {idx_lit}::regclass",
                    fetch=False,
                )
                default_logger.info(
                    f"PG 索引级统计已注入: {table_name}.{idx_name} "
                    f"(reltuples={idx['reltuples']}, relpages={idx['relpages']})"
                )
                report["ok"].append(f"index:{idx_name}")
            except Exception as e:
                default_logger.warning(
                    f"PG 索引级统计注入失败: {table_name}.{idx_name}: {e}"
                )
                report["fail"].append(f"index:{idx_name}")

        _inject_pg_column_stats_best_effort(target, sandbox, table_name, rel_lit)
        report["ok"].append("column_level_best_effort")
        report["ok"].append("histogram_mcv_best_effort")
    except Exception as e:
        default_logger.warning(f"PG 统计信息克隆失败（不阻断沙箱）: {table_name}: {e}")
        report["fail"].append(f"pg_clone:{e}")
    return report


def _inject_pg_column_stats_best_effort(
    target: DBExecutor,
    sandbox: DBExecutor,
    table_name: str,
    rel_lit: str,
) -> None:
    try:
        cols = _first_rows(
            target.execute(
                f"SELECT a.attname, a.attnum, "
                f"s.null_frac::float8 AS null_frac, "
                f"s.avg_width::int4 AS avg_width, "
                f"s.n_distinct::float8 AS n_distinct, "
                f"s.most_common_freqs::text AS mcf, "
                f"s.histogram_bounds::text AS hist "
                f"FROM pg_attribute a "
                f"JOIN pg_class c ON c.oid = a.attrelid "
                f"LEFT JOIN pg_stats s "
                f"  ON s.schemaname = 'public' AND s.tablename = {_sql_literal(table_name)} "
                f" AND s.attname = a.attname "
                f"WHERE a.attrelid = {rel_lit}::regclass "
                f"  AND a.attnum > 0 AND NOT a.attisdropped"
            )
        )
        for col in cols:
            if col.get("null_frac") is None and col.get("avg_width") is None:
                continue
            attnum = int(col["attnum"])
            null_frac = float(col["null_frac"] or 0.0)
            avg_width = int(col["avg_width"] or 0)
            n_distinct = float(col["n_distinct"] or 0.0)
            try:
                sandbox.execute(
                    f"UPDATE pg_statistic SET "
                    f"stanullfrac = {null_frac}, "
                    f"stawidth = {avg_width}, "
                    f"stadistinct = {n_distinct} "
                    f"WHERE starelid = {rel_lit}::regclass AND staattnum = {attnum}",
                    fetch=False,
                )
            except Exception as e:
                default_logger.warning(
                    f"PG 列级统计注入失败: {table_name}.{col.get('attname')}: {e}"
                )
        default_logger.info(f"PG 列级统计尽力注入完成: {table_name} ({len(cols)} 列)")
        _inject_pg_hist_mcv_best_effort(target, sandbox, table_name, rel_lit)
    except Exception as e:
        default_logger.warning(f"PG 列级统计采集/注入跳过: {table_name}: {e}")


def _inject_pg_hist_mcv_best_effort(
    target: DBExecutor,
    sandbox: DBExecutor,
    table_name: str,
    rel_lit: str,
) -> None:
    """尽力拷贝 pg_statistic 的 stakind/stanumbers（直方图/MCV 频率）；stavalues 类型敏感则跳过。"""
    try:
        rows = _first_rows(
            target.execute(
                f"SELECT staattnum, stainherit, "
                f"stakind1, stakind2, stakind3, stakind4, stakind5, "
                f"stanumbers1::text AS n1, stanumbers2::text AS n2, "
                f"stanumbers3::text AS n3, stanumbers4::text AS n4, "
                f"stanumbers5::text AS n5 "
                f"FROM pg_statistic "
                f"WHERE starelid = {rel_lit}::regclass"
            )
        )
        ok = 0
        for row in rows:
            attnum = int(row["staattnum"])
            inherit = "true" if row.get("stainherit") else "false"
            sets = [
                f"stakind1 = {int(row['stakind1'] or 0)}",
                f"stakind2 = {int(row['stakind2'] or 0)}",
                f"stakind3 = {int(row['stakind3'] or 0)}",
                f"stakind4 = {int(row['stakind4'] or 0)}",
                f"stakind5 = {int(row['stakind5'] or 0)}",
            ]
            for i, key in enumerate(("n1", "n2", "n3", "n4", "n5"), start=1):
                raw = row.get(key)
                if raw and raw not in ("NULL", "{}"):
                    # raw 形如 {0.1,0.2} —— 作为 real[] 字面量
                    sets.append(f"stanumbers{i} = '{str(raw).replace(chr(39), chr(39)+chr(39))}'::real[]")
            try:
                sandbox.execute(
                    f"UPDATE pg_statistic SET {', '.join(sets)} "
                    f"WHERE starelid = {rel_lit}::regclass "
                    f"AND staattnum = {attnum} AND stainherit = {inherit}",
                    fetch=False,
                )
                ok += 1
            except Exception as e:
                default_logger.debug(
                    f"PG 直方图/MCV 注入跳过 {table_name} att={attnum}: {e}"
                )
        default_logger.info(
            f"PG 直方图/MCV 尽力注入: {table_name} ({ok}/{len(rows)} 列槽位)"
        )
    except Exception as e:
        default_logger.warning(f"PG 直方图/MCV 采集跳过: {table_name}: {e}")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _inject_mysql_statistics(
    target: DBExecutor,
    sandbox: DBExecutor,
    table_name: str,
    target_quote: str,
    sandbox_quote: str,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {"ok": [], "fail": [], "skip": []}
    try:
        target_db = target.config.get("database", "")
        sandbox_db = sandbox.config.get("database", "")
        rows = _first_rows(
            target.execute(
                "SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH "
                "FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = {_sql_literal(target_db)} "
                f"AND TABLE_NAME = {_sql_literal(table_name)}"
            )
        )
        if not rows or not isinstance(rows[0], dict):
            default_logger.warning(f"MySQL 表级统计采集失败（无结果）: {table_name}")
            report["fail"].append("table_level:no_rows")
            return report

        n_rows = int(rows[0].get("TABLE_ROWS") or 0)
        data_length = int(rows[0].get("DATA_LENGTH") or 0)
        index_length = int(rows[0].get("INDEX_LENGTH") or 0)

        try:
            sandbox.execute(
                "UPDATE mysql.innodb_table_stats SET "
                f"n_rows = {n_rows}, "
                f"clustered_index_size = GREATEST(1, ROUND({data_length} / 16384)), "
                f"sum_of_other_index_sizes = GREATEST(0, ROUND({index_length} / 16384)) "
                f"WHERE database_name = {_sql_literal(sandbox_db)} "
                f"AND table_name = {_sql_literal(table_name)}",
                fetch=False,
            )
            default_logger.info(
                f"MySQL 表级统计已注入: {table_name} (n_rows={n_rows})"
            )
            report["ok"].append("table_level")
        except Exception as e:
            default_logger.warning(
                f"MySQL innodb_table_stats 注入失败（可能无权限）: {table_name}: {e}"
            )
            report["fail"].append(f"table_level:{e}")

        _inject_mysql_index_stats_best_effort(target, sandbox, target_db, sandbox_db, table_name)
        report["ok"].append("index_stats_best_effort")
    except Exception as e:
        default_logger.warning(f"MySQL 统计信息克隆失败（不阻断沙箱）: {table_name}: {e}")
        report["fail"].append(f"mysql_clone:{e}")
    return report


def _inject_mysql_index_stats_best_effort(
    target: DBExecutor,
    sandbox: DBExecutor,
    target_db: str,
    sandbox_db: str,
    table_name: str,
) -> None:
    try:
        rows = _first_rows(
            target.execute(
                "SELECT index_name, stat_name, stat_value, sample_size, stat_description "
                "FROM mysql.innodb_index_stats "
                f"WHERE database_name = {_sql_literal(target_db)} "
                f"AND table_name = {_sql_literal(table_name)}"
            )
        )
        if not rows:
            default_logger.info(f"MySQL innodb_index_stats 无数据: {table_name}")
            return
        ok = 0
        for row in rows:
            idx = str(row.get("index_name") or "")
            stat_name = str(row.get("stat_name") or "")
            if not idx or not stat_name:
                continue
            try:
                sample_size = row.get("sample_size")
                sample_sql = "NULL" if sample_size is None else str(int(sample_size))
                desc = row.get("stat_description")
                desc_sql = "NULL" if desc is None else _sql_literal(str(desc))
                sandbox.execute(
                    "INSERT INTO mysql.innodb_index_stats "
                    "(database_name, table_name, index_name, last_update, "
                    "stat_name, stat_value, sample_size, stat_description) VALUES ("
                    f"{_sql_literal(sandbox_db)}, {_sql_literal(table_name)}, "
                    f"{_sql_literal(idx)}, NOW(), {_sql_literal(stat_name)}, "
                    f"{int(row.get('stat_value') or 0)}, {sample_sql}, {desc_sql}"
                    ") ON DUPLICATE KEY UPDATE "
                    f"stat_value = {int(row.get('stat_value') or 0)}, "
                    f"sample_size = {sample_sql}, "
                    f"stat_description = {desc_sql}, last_update = NOW()",
                    fetch=False,
                )
                ok += 1
            except Exception as e:
                default_logger.debug(
                    f"MySQL index_stats 单条跳过 {table_name}.{idx}.{stat_name}: {e}"
                )
        default_logger.info(
            f"MySQL innodb_index_stats 尽力注入: {table_name} ({ok}/{len(rows)})"
        )
    except Exception as e:
        default_logger.warning(
            f"MySQL innodb_index_stats 注入跳过: {table_name}: {e}"
        )
