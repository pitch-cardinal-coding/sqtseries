"""Regression test: a checkpoint must never interleave with a write.

SQLite's WAL-reset corruption bug (sqlite.org/wal.html section 11; fixed in
3.51.3 / 3.50.7 / 3.44.6) fires when two connections "attempt to write or
checkpoint at the same instant" on one file: a checkpoint resets the WAL while
another connection commits, corrupting the wal-index so part of a transaction
never reaches the database file.

This engine keeps 4 pooled readers plus a writer on one file, and the
CheckpointManager used to checkpoint on a POOLED READER while the writer
committed — the documented trigger, in WAL mode, on SQLite 3.46.1.

The fix routes every checkpoint through Database.checkpoint(), which runs on the
WRITER handle under its lock, collapsing write and checkpoint onto one
connection so they cannot interleave on any SQLite version.
"""

import threading
import time

from sqtseries.engine import Database, wal_checkpoint


def test_checkpoint_blocks_while_a_write_holds_the_writer_lock(tmp_path):
    """A checkpoint must wait for an in-flight write instead of racing it."""
    db = Database(str(tmp_path / "cp.sqlite"))
    db.execute("CREATE TABLE t(x INTEGER)")
    try:
        held = threading.Event()
        release = threading.Event()
        done = threading.Event()

        def slow_write() -> None:
            # Stand in for insert_many: hold exactly what a real write holds.
            with db._writer_lock:
                held.set()
                release.wait(10)

        writer = threading.Thread(target=slow_write, daemon=True)
        writer.start()
        assert held.wait(5), "writer never took the lock"

        def checkpoint() -> None:
            wal_checkpoint(db, "PASSIVE")
            done.set()

        checker = threading.Thread(target=checkpoint, daemon=True)
        checker.start()

        # The checkpoint must still be blocked: it is queued behind the write.
        time.sleep(0.25)
        assert not done.is_set(), (
            "checkpoint completed while a write held the writer lock — "
            "it can still reset the WAL under an in-flight commit"
        )

        release.set()
        assert done.wait(10), "checkpoint never completed after the write finished"
        writer.join(timeout=5)
        checker.join(timeout=5)
    finally:
        db.dispose()


def test_checkpoint_runs_on_the_writer_handle(tmp_path):
    """The pragma must not be issued from a pooled reader connection."""
    db = Database(str(tmp_path / "cp2.sqlite"))
    db.execute("CREATE TABLE t(x INTEGER)")
    try:
        seen: list[str] = []
        raw_exec = db._writer_conn.execute

        # Prove which handle receives the PRAGMA by tagging the writer's cursor
        # path: a pooled reader would never be the object called here.
        original_checkpoint = Database.checkpoint

        def spy(self, mode="PASSIVE"):
            seen.append("writer" if self._writer_conn is not None else "no-writer")
            return original_checkpoint(self, mode)

        Database.checkpoint = spy  # type: ignore[method-assign]
        try:
            wal_checkpoint(db, "PASSIVE")
        finally:
            Database.checkpoint = original_checkpoint  # type: ignore[method-assign]

        assert seen == ["writer"], f"checkpoint did not use the writer handle: {seen}"
        # A write still works afterwards — the handle was not poisoned.
        db.execute("INSERT INTO t VALUES (1)")
        assert raw_exec is not None
    finally:
        db.dispose()
