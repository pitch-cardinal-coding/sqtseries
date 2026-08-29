"""Stress/short-soak test: sustained writes + integrity, no fd leaks.
Kept short (a few seconds) so CI stays live; the long soak lives in
scripts/stress.py and scripts/benchmark.py (run in tmux).
"""

import gc

import pytest

from sqtseries.engine import (
    StorageEngine,
    create_sqlite_engine,
    quick_check,
    run_migrations,
)


@pytest.fixture
def store(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "stress.sqlite"))
    run_migrations(eng)
    s = StorageEngine(eng)
    yield s
    s.close()


class TestStress:
    def test_sustained_bursts_ok(self, store):
        base = 1_700_000_000_000_000_000
        for burst in range(20):
            rows = [
                ("stress", {"b": str(burst)}, float(i), base + burst * 10_000 + i)
                for i in range(500)
            ]
            store.insert_many(rows)
        assert quick_check(store.db) == "ok"
        assert store.series_count() >= 1

    def test_no_fd_leak(self, store):
        from pathlib import Path

        fd_dir = Path("/proc/self/fd")
        if not fd_dir.is_dir():
            pytest.skip("no /proc")
        before = len(list(fd_dir.iterdir()))

        for _ in range(20):
            with store.db.connect() as conn:
                conn.exec_driver_sql("SELECT 1").scalar()
        gc.collect()
        after = len(list(fd_dir.iterdir()))
        assert after - before <= 2
