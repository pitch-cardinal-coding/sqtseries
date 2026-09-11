"""Database/Connection/Result wrapper edge cases (raw sqlite3 layer)."""

import gc
import sqlite3
import threading
import time

import pytest

import sqtseries.engine.db as db_mod
from sqtseries.engine.db import Database, create_sqlite_engine


@pytest.fixture
def db(tmp_path):
    return create_sqlite_engine(str(tmp_path / "db.sqlite"))


def test_result_fetch_helpers(db):
    with db.connect() as conn:
        conn.executescript(
            "CREATE TABLE t(x INTEGER, y TEXT);INSERT INTO t VALUES (1, 'a'), (2, 'b');"
        )
        assert conn.execute("SELECT x, y FROM t ORDER BY x").first() == (1, "a")

        assert conn.execute("SELECT x FROM t WHERE x = 2").scalar() == 2

        assert conn.execute("SELECT x FROM t WHERE x = 99").first() is None

        assert conn.execute("SELECT x FROM t WHERE x = 99").scalar() is None

        res = conn.execute("SELECT x FROM t ORDER BY x")
        # __getitem__ fetches the next row and returns row[idx]
        # row 1, column 0
        assert res[0] == 1
        # advances: row 2, column 0
        assert res[0] == 2
        with pytest.raises(IndexError):
            # cursor exhausted
            res[5]


def test_result_iter(db):
    with db.connect() as conn:
        conn.executescript("CREATE TABLE t(x); INSERT INTO t VALUES (1),(2),(3)")

        assert list(conn.execute("SELECT x FROM t ORDER BY x")) == [(1,), (2,), (3,)]


def test_connection_commit_rollback_close(db):
    with db.connect() as conn:
        conn.executescript("CREATE TABLE t(x)")
        conn.commit()
        # no active txn; must not raise
        conn.rollback()
        conn.close()
    # The closed handle is detected as dead at check-in and NOT re-pooled;
    # the next checkout silently opens a fresh connection.
    assert db._reader_pool.qsize() == 0
    db.execute("SELECT 1")


# --- Connection pooling behaviour ---


def test_writer_self_heals_after_failed_transaction(db):
    """A failed writer txn must not poison every future write."""
    db.execute("CREATE TABLE t(x)")
    with pytest.raises(sqlite3.OperationalError), db.begin() as conn:
        conn.execute("THIS IS NOT SQL")
    # The poisoned handle was quarantined; the next transaction reconnects.
    with db.begin() as conn:
        conn.execute("INSERT INTO t VALUES (1)")
    assert db.scalar("SELECT COUNT(*) FROM t") == 1


def test_writer_self_heals_after_dead_handle(db):
    """A closed/killed writer handle is replaced lazily, not reused."""
    db.execute("CREATE TABLE t(x)")
    with db._writer_lock:
        raw = db._get_writer()
        # Simulate the handle dying underneath us.
        raw.close()
    with db.begin() as conn:
        conn.execute("INSERT INTO t VALUES (1)")
    assert db.scalar("SELECT COUNT(*) FROM t") == 1


def test_pooled_reader_reused_not_reopened(db):
    """A checked-in reader goes back to the pool and is reused."""
    with db.connect() as conn:
        first = conn.dbapi_connection
    assert db._reader_pool.qsize() == 1
    with db.connect() as conn:
        assert conn.dbapi_connection is first


def test_with_conn_does_not_close_pooled_handle(db):
    """``with conn:`` ends the txn but never closes the pooled handle."""
    with db.connect() as conn:
        conn.executescript("CREATE TABLE t(x)")
        with conn:
            conn.execute("INSERT INTO t VALUES (1)")
        # inner __exit__ committed but must not have closed the handle
        assert conn.execute("SELECT COUNT(*) FROM t").scalar() == 1
    # handle survived to be pooled
    assert db._reader_pool.qsize() == 1


def test_with_conn_rolls_back_on_error(db):
    db.execute("CREATE TABLE t(x)")
    with pytest.raises(RuntimeError), db.begin() as conn:
        conn.execute("INSERT INTO t VALUES (1)")
        raise RuntimeError("boom")
    assert db.scalar("SELECT COUNT(*) FROM t") == 0


def test_dispose_with_in_flight_checkout_closes_not_pools(db):
    """A checkout racing dispose() is closed at check-in (no stray fd)."""
    db.execute("CREATE TABLE t(x)")
    cm = db.connect()
    conn = cm.__enter__()
    try:
        db.dispose()
    finally:
        cm.__exit__(None, None, None)
    assert db._reader_pool.qsize() == 0
    with pytest.raises(sqlite3.ProgrammingError):
        conn.dbapi_connection.execute("SELECT 1")


def test_dispose_is_idempotent(db):
    with db.connect():
        pass
    with db.begin():
        pass
    db.dispose()
    db.dispose()
    assert db._writer_conn is None
    assert db._reader_pool.qsize() == 0


def test_reader_pool_size_must_be_positive(tmp_path):
    with pytest.raises(ValueError):
        Database(str(tmp_path / "db.sqlite"), reader_pool_size=0)


# --- Pool hygiene from sqlite.org forum forumpost 9e9b8627... ---


def test_reader_pool_is_lifo_warmest_reused(db):
    """LIFO: the most-returned connection is reused first (warm page cache)."""
    cm1, cm2 = db.connect(), db.connect()
    a = cm1.__enter__().dbapi_connection
    b = cm2.__enter__().dbapi_connection
    try:
        # Two concurrent checkouts -> two handles.
        assert a is not b
    finally:
        # Pool: [a].
        cm1.__exit__(None, None, None)
        # Pool: [a, b] (LIFO top = b).
        cm2.__exit__(None, None, None)
    # Next checkout is b (LIFO top); while b is held, the next is a.
    cm3, cm4 = db.connect(), db.connect()
    try:
        assert cm3.__enter__().dbapi_connection is b
        assert cm4.__enter__().dbapi_connection is a
    finally:
        cm3.__exit__(None, None, None)
        cm4.__exit__(None, None, None)


def test_abandoned_partial_select_does_not_block_checkpoint(db, tmp_path):
    """A partially-consumed SELECT on a checked-in reader must not pin a
    WAL read snapshot (verified: it blocks wal_checkpoint(TRUNCATE) even
    though in_transaction is False). Check-in closes the wrapper's cursors."""

    from sqtseries.engine.pragmas import wal_checkpoint

    db.execute("CREATE TABLE t(x)")
    with db.begin() as conn:
        conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(5000)])
    wal_checkpoint(db, "TRUNCATE")

    # Reader abandons a partial SELECT mid-iteration
    with db.connect() as conn:
        it = iter(conn.execute("SELECT x FROM t"))
        next(it)
        # Result/cursor still alive, statement unreset.
        del it

    # Writer grows the WAL after the (now pooled) reader's snapshot
    with db.begin() as conn:
        conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(5000, 9000)])

    busy, _log, _done = _parse(wal_checkpoint(db, "TRUNCATE"))
    assert busy == 0, "pooled reader pins WAL snapshot — check-in hygiene failed"


def _parse(result: str) -> tuple[int, int, int]:
    parts = [int(x) for x in result.split(",") if x.strip()]
    return (*parts, 0, 0, 0)[:3]


def test_wrapper_result_survives_before_checkin(db):
    """Cursors stay usable during the checkout (release happens at check-in)."""
    db.execute("CREATE TABLE t(x)")
    db.execute("INSERT INTO t VALUES (7)")
    with db.connect() as conn:
        res = conn.execute("SELECT x FROM t")
        assert res.scalar() == 7


def test_connection_execute_error_closes_cursor(db):
    with db.connect() as conn:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("THIS IS NOT SQL")
        # connection still usable after an error
        assert conn.execute("SELECT 1").scalar() == 1


def test_executemany_error_path(db):
    with db.connect() as conn:
        conn.executescript("CREATE TABLE t(x INTEGER NOT NULL)")
        with pytest.raises(sqlite3.IntegrityError):
            conn.executemany("INSERT INTO t VALUES (?)", [(1,), (None,)])
        assert conn.execute("SELECT COUNT(*) FROM t").scalar() == 1


def test_connect_is_reader_writes_dont_persist(db):
    """connect() is a reader connection: DML is rolled back on close."""

    with db.connect() as conn:
        conn.executescript("CREATE TABLE t(x)")
        conn.execute("INSERT INTO t VALUES (1)")
    with db.connect() as conn:
        # DDL autocommitted, but the INSERT (implicit txn) was rolled back

        assert conn.execute("SELECT name FROM sqlite_master WHERE name='t'").first()

        assert conn.execute("SELECT COUNT(*) FROM t").scalar() == 0


def test_begin_commits_on_success(db):
    with db.begin() as conn:
        conn.execute("CREATE TABLE t(x)")
        conn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    assert db.scalar("SELECT COUNT(*) FROM t") == 2


def test_begin_rolls_back_on_exception(db):
    with pytest.raises(RuntimeError), db.begin() as conn:
        conn.execute("CREATE TABLE t(x)")
        conn.execute("INSERT INTO t VALUES (1)")
        raise RuntimeError("boom")
    with db.connect() as conn:
        assert (
            conn.execute("SELECT name FROM sqlite_master WHERE name='t'").first()
            is None
        )


def test_database_execute_persists_writes(db):
    db.execute("CREATE TABLE t(x)")
    db.execute("INSERT INTO t VALUES (5)")
    assert db.scalar("SELECT x FROM t") == 5
    assert db.scalar("SELECT x FROM t WHERE x = 99") is None


def test_connection_executescript(db):
    with db.begin() as conn:
        conn.executescript("CREATE TABLE t(x)")
        conn.executescript("INSERT INTO t VALUES (1)")
    assert db.scalar("SELECT COUNT(*) FROM t") == 1


def test_connection_dbapi(db):
    with db.connect() as conn:
        assert conn.dbapi_connection is not None


def test_database_missing_parent_dir(tmp_path):
    path = str(tmp_path / "nested" / "deep" / "db.sqlite")
    db = Database(path)
    assert db.path == path


# --- reader-slot bound: deadlock prevention (Coffman conditions) -------------


def test_reader_bound_bounded_wait_no_deadlock(tmp_path, monkeypatch):
    """A waiter must never block forever on a held slot (no circular wait).

    Pool=1 and the slot is held: the second connect() times out (short
    timeout) and falls back to a transient handle — bounded waiting, the
    query still answers. The old unbounded design hung here.
    """
    monkeypatch.setattr(db_mod, "READER_SLOT_TIMEOUT_S", 0.05)
    database = Database(tmp_path / "bound.sqlite", reader_pool_size=1)
    database.execute("CREATE TABLE t(x)")
    database.execute("INSERT INTO t VALUES (7)")
    with database.connect() as holder:
        holder.execute("SELECT 1").first()
        t0 = time.monotonic()
        # Must NOT hang.
        with database.connect() as waiter:
            elapsed = time.monotonic() - t0
            assert waiter.execute("SELECT x FROM t").scalar() == 7
    # Timed out fast; transient handle served the query.
    assert elapsed < 2.0


def test_concurrent_checkouts_respect_pool_bound(tmp_path):
    """With slot bound == pool size and fast queries, no extra handles are
    ever opened after warm-up: the bound is real, not just advisory."""
    database = Database(tmp_path / "pool.sqlite", reader_pool_size=2)
    opens = {"n": 0}
    orig_open = Database._open

    def counting_open(self, *args, **kwargs):
        opens["n"] += 1
        return orig_open(self, *args, **kwargs)

    monkeypatched = False
    try:
        Database._open = counting_open  # type: ignore[method-assign]
        monkeypatched = True
        errors: list[Exception] = []

        def worker():
            try:
                for _ in range(25):
                    with database.connect() as conn:
                        conn.execute("SELECT 1").first()
            except Exception as exc:  # pragma: no cover - surfaces race bugs
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors
        assert not any(t.is_alive() for t in threads)
    finally:
        if monkeypatched:
            Database._open = orig_open  # type: ignore[method-assign]
    # 2 pooled handles warm up once; after that every checkout reuses them.
    assert opens["n"] <= 2


def test_nested_same_thread_checkout_no_deadlock(tmp_path, monkeypatch):
    """Nested reader checkout on one thread while all slots are held: takes
    a transient handle and completes (query_time_range consumers)."""
    monkeypatch.setattr(db_mod, "READER_SLOT_TIMEOUT_S", 0.05)
    database = Database(tmp_path / "nest.sqlite", reader_pool_size=1)
    database.execute("CREATE TABLE t(x)")
    with database.connect() as outer:
        outer.execute("SELECT 1").first()
        # A consumer of a suspended query_time_range generator may open
        # another checkout on the SAME thread while the only slot is held.
        assert database.get_table_names() == ["t"]


@pytest.mark.filterwarnings("ignore::ResourceWarning")
def test_abandoned_checkout_releases_slot(tmp_path):
    """A connect() generator abandoned without __exit__ (finalized by GC,
    possibly from another thread) must not leak its slot.

    The ignored ResourceWarning is this test's own exhaust: abandoning the
    checkout is the point, and the raw handle is therefore finalized by the
    collector instead of closed. Unrelated teardown paths close cleanly
    (StorageEngine.close disposes the pool).
    """
    database = Database(tmp_path / "leak.sqlite", reader_pool_size=2)
    ctx = database.connect()
    wrapper = ctx.__enter__()
    wrapper.execute("SELECT 1").first()
    # Abandon — no __exit__; GeneratorExit runs the finally.
    del ctx, wrapper
    gc.collect()
    # Both slots must be back: the semaphore recovered from the abandonment.
    assert database._reader_slots._sem._value == 2
    for _ in range(2):
        with database.connect() as conn:
            conn.execute("SELECT 1").first()


def test_open_failure_closes_handle(tmp_path, monkeypatch):
    """A pragma failure mid-_open must close the raw handle, not leak it
    to the garbage collector (ResourceWarning) or the descriptor table."""
    import warnings

    import sqtseries.engine.db as db_mod

    bad_pragmas = dict(db_mod.CONNECTION_PRAGMAS, cache_size="bogus!!!")
    monkeypatch.setattr(db_mod, "CONNECTION_PRAGMAS", bad_pragmas)
    database = Database(str(tmp_path / "fail.sqlite"))
    # Drain garbage from earlier tests first: the collect below must only
    # see this test's own handle.
    gc.collect()
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        try:
            database._open()
            raised = False
        except sqlite3.OperationalError:
            raised = True
        # The except block has exited, so no traceback pins the frame:
        # an unclosed handle is collectible exactly here.
        assert raised
        gc.collect()
    assert [w for w in record if issubclass(w.category, ResourceWarning)] == []
