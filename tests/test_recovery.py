"""Recovery + service lifecycle tests."""

import sqlite3

import pytest

from sqtseries.engine import (
    StorageEngine,
    create_sqlite_engine,
    initialize_schema,
)
from sqtseries.recovery import (
    CorruptionError,
    check_integrity_on_startup,
    recover_wal,
)


@pytest.fixture
def engine(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "recovery.sqlite"))
    initialize_schema(eng)
    yield eng
    eng.dispose()


class TestRecovery:
    def test_quick_check_clean(self, engine):
        assert check_integrity_on_startup(engine) == 0

    def test_recover_wal(self, engine):
        store = StorageEngine(engine)
        store.insert_many([("a", None, 1.0, 1_700_000_000_000_000_000)])

        recover_wal(engine)
        assert check_integrity_on_startup(engine) == 0

    def test_strict_raises_on_corrupt(self, tmp_path):
        # produce a corrupt file: write garbage over the header page
        path = tmp_path / "bad.sqlite"
        eng = create_sqlite_engine(str(path))
        initialize_schema(eng)
        eng.dispose()

        with path.open("r+b") as f:
            f.seek(0)
            # clobber the header
            f.write(b"\x00" * 64)

        # Corruption is detected at connect time (pragma setup) or by
        # the integrity check; either way it must not pass silently.
        eng2 = create_sqlite_engine(str(path))
        try:
            with pytest.raises((CorruptionError, sqlite3.DatabaseError)):
                check_integrity_on_startup(eng2, strict=True)
        finally:
            eng2.dispose()

    def test_non_strict_corrupt_returns_problem_count(self, tmp_path, monkeypatch):
        import sqtseries.recovery as rec

        # SQLite usually detects corruption at connect time, before quick_check
        # runs — this branch is defensive. Exercise it directly.
        monkeypatch.setattr(
            rec, "quick_check", lambda db: "database disk image is malformed"
        )
        monkeypatch.setattr(
            rec, "integrity_check", lambda db: ["page 1: btree corruption"]
        )

        eng = create_sqlite_engine(str(tmp_path / "d.sqlite"))
        initialize_schema(eng)
        assert check_integrity_on_startup(eng, strict=False) == 1
        eng.dispose()
