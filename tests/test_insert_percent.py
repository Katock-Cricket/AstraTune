"""列名含 % 时 INSERT 占位符不得被 mogrify 吃掉。"""

from __future__ import annotations


def test_pg_insert_escapes_percent_in_idents():
    quote = '"'
    columns = ["CDSCode", "Percent (%) Eligible FRPM (K-12)"]

    def _ident(name: str) -> str:
        return f"{quote}{str(name).replace(quote, quote + quote)}{quote}".replace(
            "%", "%%"
        )

    cols = ", ".join(_ident(c) for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f'INSERT INTO {_ident("frpm")} ({cols}) VALUES ({placeholders})'
    assert sql.count("%s") == 2
    assert "%%" in sql
    assert '"Percent (%%) Eligible FRPM (K-12)"' in sql


if __name__ == "__main__":
    test_pg_insert_escapes_percent_in_idents()
    print("ok")
