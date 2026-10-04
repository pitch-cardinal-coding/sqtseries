"""Regression test: the periodic checkpoint must never use TRUNCATE.

NousResearch/hermes-agent#45383 cross-linked b-tree pages on large databases
because a periodic ``PRAGMA wal_checkpoint(TRUNCATE)`` copied thousands of frames
in one exclusive-lock pass; the fix (c2a3b9ce5) moved that path to PASSIVE. Their
follow-up #80255 found TRUNCATE at close doing the same, guarded there by only
resetting when the WAL is small.

The periodic manager here used TRUNCATE, and RESEARCHES.md said never to. The
periodic pass is now PASSIVE, and shutdown uses TRUNCATE only for a small WAL.
"""

import asyncio
import os
from pathlib import Path

from sqtseries.engine import Database
from sqtseries.engine import checkpoint as ckpt_mod
from sqtseries.engine.checkpoint import (
    TRUNCATE_SAFE_WAL_BYTES,
    CheckpointManager,
    shutdown_checkpoint_mode,
)


def _record_modes(monkeypatch) -> list[str]:
    modes: list[str] = []
    real = ckpt_mod.wal_checkpoint

    def spy(db, mode="PASSIVE"):
        modes.append(mode)
        return real(db, mode)

    monkeypatch.setattr(ckpt_mod, "wal_checkpoint", spy)
    return modes


def _run_pass(db: Database) -> CheckpointManager:
    mgr = CheckpointManager(db, max_wal_bytes=1)
    asyncio.run(mgr._checkpoint_if_needed())
    return mgr


def test_periodic_pass_uses_passive_not_truncate(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "periodic.sqlite"))
    db.execute("CREATE TABLE t(x INTEGER)")
    try:
        modes = _record_modes(monkeypatch)
        _run_pass(db)

        assert modes == ["PASSIVE"], f"expected exactly PASSIVE, got {modes}"
    finally:
        db.dispose()


def test_periodic_pass_still_checkpoints(tmp_path, monkeypatch):
    """PASSIVE must not be a no-op — the WAL still has to be flushed."""
    db = Database(str(tmp_path / "flush.sqlite"))
    db.execute("CREATE TABLE t(x INTEGER)")
    try:
        _record_modes(monkeypatch)
        mgr = _run_pass(db)

        assert mgr.checkpoints == 1, f"passive run did not count: {mgr.stats()}"
    finally:
        db.dispose()


def test_shutdown_truncates_a_small_wal(tmp_path):
    db = Database(str(tmp_path / "small.sqlite"))
    db.execute("CREATE TABLE t(x INTEGER)")
    try:
        assert shutdown_checkpoint_mode(db.path) == "TRUNCATE"
    finally:
        db.dispose()


def test_shutdown_falls_back_to_passive_on_a_large_wal(tmp_path):
    db_path = str(tmp_path / "big.sqlite")
    wal = Path(db_path + "-wal")
    wal.write_bytes(b"")
    os.truncate(wal, TRUNCATE_SAFE_WAL_BYTES + 1)

    assert shutdown_checkpoint_mode(db_path) == "PASSIVE"


def test_shutdown_mode_is_truncate_without_a_wal(tmp_path):
    assert shutdown_checkpoint_mode(str(tmp_path / "absent.sqlite")) == "TRUNCATE"
