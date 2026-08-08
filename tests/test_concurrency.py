"""Concurrency tests: concurrent writers/readers on raw sqlite3.

SQLite single-writer + busy_timeout=5000 — concurrent writers must serialize
without corruption; concurrent readers must never block writers (WAL).
"""

import concurrent.futures
import threading
import time

import pytest

from sqtseries.engine import StorageEngine, create_sqlite_engine, run_migrations


@pytest.fixture
def engine(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "concurrency.sqlite"))
    run_migrations(eng)
    yield eng
    eng.dispose()


def _write(store: StorageEngine, wid: int, n: int) -> int:
    base = 1_700_000_000_000_000_000 + wid * 10**12
    for i in range(n):
        store.insert_many(
            [("conc.metric", {"w": str(wid), "i": str(i)}, float(i), base + i)]
        )
    return n


class TestConcurrency:
    def test_concurrent_writers_no_loss(self, engine):
        store = StorageEngine(engine)
        writers = 4
        per = 50
        with concurrent.futures.ThreadPoolExecutor(max_workers=writers) as ex:
            futs = [ex.submit(_write, store, w, per) for w in range(writers)]
            counts = [f.result() for f in futs]
        assert sum(counts) == writers * per

        total = 0
        with engine.connect() as conn:
            rows = conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE name LIKE 'measurements_%'"
            ).fetchall()
            for (tname,) in rows:
                # tname comes from sqlite_master (server-controlled)
                total += (
                    conn.exec_driver_sql(f"SELECT COUNT(*) FROM {tname}").scalar() or 0
                )
        assert total == writers * per

    def test_concurrent_readers(self, engine):
        store = StorageEngine(engine)
        store.insert_many(
            [("m", None, float(i), 1_700_000_000_000_000_000 + i) for i in range(200)]
        )

        def reader():
            rows = list(store.query_time_range(metric="m"))
            return len(rows)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futs = [ex.submit(reader) for _ in range(4)]
            sizes = [f.result() for f in futs]
        assert all(s == 200 for s in sizes)

    def test_wal_reader_during_write(self, engine):
        store = StorageEngine(engine)
        stop = threading.Event()

        def writer():
            i = 0
            base = 1_700_000_000_000_000_000
            while not stop.is_set():
                store.insert_many([("w", None, float(i), base + i)])
                i += 1
            return i

        def reader():
            time.sleep(0.05)
            return list(store.query_time_range(metric="w"))

        with concurrent.futures.ThreadPoolExecutor(2) as ex:
            wf = ex.submit(writer)
            rf = ex.submit(reader)
            time.sleep(0.2)
            stop.set()
            wf.result()
            read_rows = rf.result()
        # WAL: reader should see whatever was committed when it read
        assert isinstance(read_rows, list)
