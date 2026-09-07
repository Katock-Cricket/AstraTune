"""PGExecutor 纯函数：regclass 大小写安全名。"""

from sandbox.pg_executor import regclass_cast_name


def test_regclass_cast_name_preserves_camel():
    assert regclass_cast_name("Equipment") == '"Equipment"'
    assert regclass_cast_name('"Location"') == '"Location"'
    assert regclass_cast_name("alert") == '"alert"'


def test_regclass_cast_name_escapes_quotes():
    assert regclass_cast_name('weird"name') == '"weird""name"'
