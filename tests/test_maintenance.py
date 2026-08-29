"""Periodic maintenance tests: ANALYZE scheduler + WAL monitoring / long-reader detection."""

import asyncio

import pytest

from sqtseries.engine import create_sqlite_engine, initialize_schema
from sqtseries.engine.checkpoint import CheckpointManager, _parse_result
from sqtseries.engine.maintenance import MaintenanceManager


def test_parse_checkpoint_result():
    assert _parse_result("0,123,123") == (0, 123, 123)
    assert _parse_result("1,5,0") == (1, 5, 0)
    assert _parse_result("") == (0, 0, 0)


@pytest.mark.asyncio
async def test_maintenance_runs_analyze_after_interval(tmp_path, monkeypatch):
    eng = create_sqlite_engine(str(tmp_path / "maint.sqlite"))
    initialize_schema(eng)

    import sqtseries.engine.maintenance as maint

    calls = []
    monkeypatch.setattr(maint, "run_analyze_once", lambda db: calls.append(1))

    mgr = MaintenanceManager(eng, interval=0.05)
    await mgr.start()
    # first pass waits one interval (startup already analyzed)
    assert mgr.runs == 0
    await asyncio.sleep(0.15)
    await mgr.stop()
    assert mgr.runs >= 1
    assert len(calls) >= 1
    assert mgr.last_analyze is not None
    eng.dispose()


@pytest.mark.asyncio
async def test_checkpoint_detects_blocked_readers(tmp_path, monkeypatch):
    """Consecutive busy checkpoints are tracked and cleared on success."""

    from pathlib import Path

    eng = create_sqlite_engine(str(tmp_path / "cp.sqlite"))
    initialize_schema(eng)
    # A non-empty WAL file (SQLite removes it when the last connection closes,
    # so fabricate one to exercise the monitoring path deterministically).

    wal_path = Path(eng.path + "-wal")
    wal_path.write_bytes(b"\x00" * 64)

    import sqtseries.engine.checkpoint as chk

    # busy
    monkeypatch.setattr(chk, "wal_checkpoint", lambda db, mode: "1,10,0")

    mgr = CheckpointManager(eng, interval=60.0, max_wal_bytes=0)
    await mgr._checkpoint_if_needed()
    assert mgr.busy_runs == 1
    assert mgr.last_busy == 1
    await mgr._checkpoint_if_needed()
    assert mgr.busy_runs == 2
    assert mgr.checkpoints == 0

    # success
    monkeypatch.setattr(chk, "wal_checkpoint", lambda db, mode: "0,10,10")

    await mgr._checkpoint_if_needed()
    assert mgr.busy_runs == 0
    assert mgr.checkpoints == 1
    assert mgr.stats()["wal_bytes"] == 64
    assert mgr.stats()["last_result"] == (0, 10, 10)
    eng.dispose()


@pytest.mark.asyncio
async def test_checkpoint_no_wal_no_tracking(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "cp2.sqlite"))
    initialize_schema(eng)
    mgr = CheckpointManager(eng, interval=60.0, max_wal_bytes=0)
    await mgr._checkpoint_if_needed()
    assert mgr.wal_bytes == 0
    assert mgr.busy_runs == 0
    eng.dispose()
