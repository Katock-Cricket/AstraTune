"""diag_extract 契约测试。"""

from utils.diag_extract import extract_pred_sqls_from_diagnosis


def test_extract_from_sql_fence():
    text = "【诊断结论】\n最终SQL：\n```sql\nSELECT a FROM t;\nUPDATE t SET x=1;\n```\n"
    pred, fb = extract_pred_sqls_from_diagnosis(text)
    assert not fb
    assert any("SELECT" in p for p in pred)


def test_extract_from_json_pred_sqls():
    text = '结论完。\n{"pred_sqls": ["SELECT 1", "SELECT 2"]}\n'
    pred, fb = extract_pred_sqls_from_diagnosis(text)
    assert pred == ["SELECT 1", "SELECT 2"]
    assert fb is False


def test_fallback_issue_sql():
    pred, fb = extract_pred_sqls_from_diagnosis(
        "只有文字没有 SQL",
        fallback_issue_sql=["SELECT issue"],
    )
    assert pred == ["SELECT issue"]
    assert fb is True
