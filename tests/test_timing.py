from utils.timing import (
    clear_timing_context,
    get_timing_context,
    log_timing,
    set_timing_context,
    sql_kind,
    timed_stage,
)


def test_sql_kind():
    assert sql_kind("SELECT 1") == "SELECT"
    assert sql_kind("  (explain select 1") == "EXPLAIN"
    assert sql_kind("") == "EMPTY"


def test_timing_context_roundtrip(caplog):
    caplog.set_level("INFO", logger="AstraTune")
    clear_timing_context()
    set_timing_context(
        instance_id="a1",
        db_id="financial",
        dialect="postgresql",
        worker=2,
        slot="astratune_sandbox_2",
    )
    ctx = get_timing_context()
    assert ctx["instance_id"] == "a1"
    assert ctx["slot"] == "astratune_sandbox_2"
    with caplog.at_level("INFO"):
        log_timing("sandbox_clone_from_template", 12.3, source="hit")
    joined = " ".join(r.message for r in caplog.records)
    assert "[timing]" in joined
    assert "stage=sandbox_clone_from_template" in joined
    assert "elapsed_ms=12.3" in joined
    assert "slot=astratune_sandbox_2" in joined
    clear_timing_context()
    assert get_timing_context() == {}


def test_timed_stage_records_ms(caplog):
    caplog.set_level("INFO", logger="AstraTune")
    set_timing_context(instance_id="x", dialect="mysql")
    with caplog.at_level("INFO"):
        with timed_stage("noop"):
            pass
    joined = " ".join(r.message for r in caplog.records)
    assert "stage=noop" in joined
    assert "elapsed_ms=" in joined
    clear_timing_context()


def test_timing_summary_rollup(caplog):
    from utils.timing import log_timing_summary

    caplog.set_level("INFO", logger="AstraTune")
    clear_timing_context()
    set_timing_context(instance_id="s1", dialect="postgresql")
    log_timing("sandbox_prepare", 100.0)
    log_timing("diagnosis_total", 800.0)
    log_timing("infer_total", 1000.0)
    with caplog.at_level("INFO"):
        summary = log_timing_summary()
    assert summary["denom_ms"] == 1000.0
    by = {x["stage"]: x for x in summary["stages"]}
    assert by["diagnosis_total"]["pct"] == 80.0
    joined = " ".join(r.message for r in caplog.records)
    assert "[timing-summary]" in joined
    clear_timing_context()
