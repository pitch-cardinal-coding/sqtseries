"""Resource-leak detection: no connection/fd/socket leaks across the public API."""

import gc
import os
import warnings

import pytest

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema


@pytest.fixture
def engine(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "leak.sqlite"))
    initialize_schema(eng)
    yield eng
    eng.dispose()


def _fd_count():
    from pathlib import Path

    return len(list(Path(f"/proc/{os.getpid()}/fd").iterdir()))


def test_no_resource_warnings_from_db_api(engine):
    """Using the Database API must never leak an open sqlite3 connection."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for _ in range(40):
            with engine.connect() as conn:
                conn.execute("SELECT 1").scalar()
            with engine.begin() as conn:
                conn.execute("SELECT 1").scalar()
            engine.scalar("SELECT 1")
            engine.execute("SELECT 1")
        gc.collect()
    # other tests' GC'd connections may warn too; only flag warnings tied to this db
    leaked = [
        w
        for w in caught
        if issubclass(w.category, ResourceWarning) and engine.path in str(w.message)
    ]
    assert leaked == []


def test_fd_stable_across_operations(engine):
    store = StorageEngine(engine)
    before = _fd_count()
    for i in range(100):
        store.insert_many([("m", None, float(i), 1_700_000_000_000_000_000 + i)])
        list(store.query_time_range(metric="m"))
        store.series_ids_for_metric("m")
    gc.collect()
    after = _fd_count()
    # allow a little slack for caches/jit
    assert after - before <= 3


def test_lru_caches_clear_on_close(engine):
    store = StorageEngine(engine)
    store.insert_many(
        [("m", {"i": str(i)}, 1.0, 1_700_000_000_000_000_000 + i) for i in range(50)]
    )
    assert len(store._series_cache._data) >= 1
    store.close()
    assert len(store._series_cache._data) == 0
    assert store._parts_cache is None


def test_zmq_client_close_no_fd_leak():

    from sqtseries.client import Client

    before = _fd_count()
    for _ in range(10):
        c = Client(
            host="127.0.0.1", ports={"write": 1, "query": 2, "subscribe": 3, "admin": 4}
        )
        c.close()
    gc.collect()
    after = _fd_count()
    assert after - before <= 3


def test_zmq_contexts_terminate():
    import zmq

    from sqtseries.client import Client

    for _ in range(5):
        c = Client()
        assert isinstance(c._ctx, zmq.Context)
        # must term() the context without hanging
        c.close()
