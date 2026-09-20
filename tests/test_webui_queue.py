"""WebUI 任务状态机与槽位调度。"""

from __future__ import annotations

from webui.task_state import (
    apply_cancelled,
    apply_completed,
    apply_failed,
    apply_running,
    can_delete,
    can_start,
    how_many_to_launch,
    pick_free_slot,
    prepare_start,
    select_pending,
    used_slots,
)


def test_cannot_delete_running():
    assert can_delete("pending")
    assert can_delete("completed")
    assert can_delete("failed")
    assert can_delete("cancelled")
    assert not can_delete("running")


def test_cancel_from_running():
    t = apply_running({"id": "1", "status": "pending"}, pid=99, slot_index=0)
    assert t["status"] == "running"
    c = apply_cancelled(t)
    assert c["status"] == "cancelled"
    assert c["pid"] is None
    assert can_delete(c["status"])


def test_completed_export_shape_fields():
    t = apply_completed(
        {"id": "1", "status": "running"},
        pred_sqls=["SELECT 1"],
        diagnosis_report="【诊断结论】ok",
        elapsed_ms=12.5,
    )
    assert t["status"] == "completed"
    assert t["pred_sqls"] == ["SELECT 1"]


def test_failed_with_pred_becomes_completed():
    t = apply_failed({"id": "1", "status": "running"}, error="timeout", pred_sqls=["SELECT 1"])
    assert t["status"] == "completed"
    assert t["pred_sqls"] == ["SELECT 1"]


def test_rerun_clears_pred():
    t = {"id": "1", "status": "failed", "pred_sqls": ["SELECT 1"], "error": "x", "pid": 1}
    assert can_start(t["status"])
    n = prepare_start(t)
    assert n["status"] == "pending"
    assert n["pred_sqls"] == []
    assert n["error"] is None


def test_workers_and_slots():
    assert how_many_to_launch(0, 2) == 2
    assert how_many_to_launch(2, 2) == 0
    assert how_many_to_launch(1, 16) == 3  # clamp 4
    assert pick_free_slot({0, 1}, 4) == 2
    assert pick_free_slot({0, 1, 2, 3}, 4) is None
    used = used_slots(
        [
            {"status": "running", "slot_index": 0},
            {"status": "pending", "slot_index": 1},
            {"status": "running", "slot_index": "2"},
        ]
    )
    assert used == {0, 2}


def test_select_pending_limit():
    tasks = [
        {"id": "b", "status": "pending", "created_at": "2", "queued": True},
        {"id": "a", "status": "pending", "created_at": "1", "queued": True},
        {"id": "c", "status": "running", "created_at": "0", "queued": True},
        {"id": "d", "status": "pending", "created_at": "0", "queued": False},
    ]
    got = select_pending(tasks, 1)
    assert [g["id"] for g in got] == ["a"]
