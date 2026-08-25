"""列名含 % 时 INSERT 占位符不得被 mogrify 吃掉（测生产 PGExecutor.insert_rows）。"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sandbox.pg_executor import PGExecutor


def test_pg_insert_escapes_percent_in_idents(monkeypatch):
    import psycopg2.extras

    captured: list[str] = []

    def fake_execute_batch(cursor, sql, data, page_size=1000):
        captured.append(sql)

    monkeypatch.setattr(psycopg2.extras, "execute_batch", fake_execute_batch)

    ex = PGExecutor.__new__(PGExecutor)
    ex.connection = MagicMock()
    cursor = MagicMock()
    ex.connection.cursor.return_value.__enter__.return_value = cursor
    ex.connection.cursor.return_value.__exit__.return_value = None

    n = ex.insert_rows(
        "frpm",
        ["CDSCode", "Percent (%) Eligible FRPM (K-12)"],
        [{"CDSCode": "a", "Percent (%) Eligible FRPM (K-12)": 1.0}],
    )
    assert n == 1
    assert len(captured) == 1
    sql = captured[0]
    assert sql.count("%s") == 2
    assert "%%" in sql
    assert '"Percent (%%) Eligible FRPM (K-12)"' in sql
