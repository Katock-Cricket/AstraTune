"""Store CRUD 冒烟（临时 SQLite）。"""

from __future__ import annotations

from webui.task_state import can_delete
from webui.store import Store


def test_store_insert_update_delete(tmp_path):
    st = Store(tmp_path / "t.sqlite")
    t = st.insert_task(
        {
            "instance_id": "x1",
            "connection_id": "postgresql-default",
            "db_id": "financial",
            "dialect": "postgresql",
            "issue_sql": ["SELECT 1"],
            "is_slow_sql": False,
            "function": "general",
            "mode": "diag+opt",
        }
    )
    assert t["status"] == "pending"
    st.add_event(t["id"], "queued", {"a": 1})
    got = st.get_task(t["id"])
    assert got["instance_id"] == "x1"
    ev = st.list_events(t["id"])
    assert ev[0]["name"] == "queued"
    st.update_task(t["id"], {**got, "status": "completed", "pred_sqls": ["SELECT 1"]})
    got2 = st.get_task(t["id"])
    assert got2["pred_sqls"] == ["SELECT 1"]
    assert can_delete(got2["status"])
    assert st.delete_task(t["id"])
    assert st.get_task(t["id"]) is None
    st.close()
