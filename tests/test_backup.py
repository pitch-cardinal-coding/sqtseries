"""Backup (VACUUM INTO) tests."""

import sqlite3
import time

import pytest

from sqtseries.engine import (
    BackupExistsError,
    StorageEngine,
    backup_database,
    backup_latest,
    create_sqlite_engine,
    initialize_schema,
)


@pytest.fixture
def engine(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "db.sqlite"))
    initialize_schema(eng)
    yield eng
    eng.dispose()


class TestBackup:
    def test_backup_creates_valid_file(self, engine, tmp_path):
        store = StorageEngine(engine)

        now = time.time_ns()
        store.insert_many(
            [
                ("cpu.usage", {"host": "web1"}, 0.7, now),
                ("cpu.usage", None, 0.9, now + 1),
            ]
        )
        backup_dir = str(tmp_path / "backups")
        path = backup_database(engine, backup_dir)

        # verify the backup is a valid, queryable DB with our data
        con = sqlite3.connect(path)
        try:
            tables = [
                r[0]
                for r in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            ]
            assert "series" in tables
            partition = "measurements_" + time.strftime("%Y_%m")
            assert partition in tables
            count = con.execute(f"SELECT COUNT(*) FROM {partition}").fetchone()[0]

            assert count == 2
        finally:
            con.close()

    def test_backup_dir_created(self, engine, tmp_path):
        backup_dir = str(tmp_path / "nested" / "dir" / "backups")
        path = backup_database(engine, backup_dir)
        from sqtseries.engine.backup import backup_latest

        assert backup_latest(engine, backup_dir) == path

    def test_backup_same_second_raises(self, engine, tmp_path):
        backup_dir = str(tmp_path / "b")
        backup_database(engine, backup_dir)
        # same second → same filename → collision
        with pytest.raises(BackupExistsError):
            backup_database(engine, backup_dir)

    def test_latest_returns_none_when_empty(self, engine, tmp_path):
        assert backup_latest(engine, str(tmp_path / "nonexistent")) is None
