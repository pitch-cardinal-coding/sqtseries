"""Durability: data survives restart, backups are consistent, restore works."""

from datetime import UTC, datetime

from sqtseries.engine import (
    StorageEngine,
    backup_database,
    create_sqlite_engine,
    initialize_schema,
)


def _ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000_000)


def test_data_survives_restart(tmp_path):
    path = str(tmp_path / "persist.sqlite")
    eng = create_sqlite_engine(path)
    initialize_schema(eng)
    store = StorageEngine(eng)
    store.insert_many(
        [
            ("cpu", {"host": "a"}, 1.0, _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))),
            ("cpu", {"host": "b"}, 2.0, _ns(datetime(2026, 1, 1, 10, 1, tzinfo=UTC))),
        ]
    )
    # "crash" — no graceful checkpoint
    eng.dispose()

    # reopen the same file
    eng2 = create_sqlite_engine(path)
    try:
        store2 = StorageEngine(eng2)
        rows = list(store2.query_time_range(metric="cpu"))
        assert len(rows) == 2
        assert store2.series_count() == 2
        store2.close()
    finally:
        eng2.dispose()


def test_backup_is_restorable(tmp_path):
    path = str(tmp_path / "db.sqlite")
    eng = create_sqlite_engine(path)
    initialize_schema(eng)
    store = StorageEngine(eng)
    base = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
    store.insert_many(
        [("m", None, float(i), base + i * 1_000_000_000) for i in range(10)]
    )

    backup = backup_database(eng, str(tmp_path / "backups"))
    eng.dispose()

    beng = create_sqlite_engine(backup)
    try:
        bstore = StorageEngine(beng)
        rows = list(bstore.query_time_range(metric="m"))
        assert len(rows) == 10
        bstore.close()
    finally:
        beng.dispose()


def test_backup_consistent_while_writing(tmp_path):
    import threading

    path = str(tmp_path / "db.sqlite")
    eng = create_sqlite_engine(path)
    initialize_schema(eng)
    store = StorageEngine(eng)
    stop = threading.Event()

    def writer():
        i = 0
        base = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        while not stop.is_set():
            store.insert_many([("w", None, float(i), base + i)])
            i += 1

    t = threading.Thread(target=writer)
    t.start()
    try:
        backup = backup_database(eng, str(tmp_path / "backups"))
    finally:
        stop.set()
        t.join(timeout=5)

    # backup must be a valid, self-consistent snapshot
    beng = create_sqlite_engine(backup)
    try:
        from sqtseries.engine import quick_check

        assert quick_check(beng) == "ok"
        bstore = StorageEngine(beng)
        rows = list(bstore.query_time_range(metric="w"))
        bstore.close()
        # snapshot reflects some committed prefix of writes (order preserved)
        assert rows == sorted(rows, key=lambda r: r[0])
    finally:
        beng.dispose()
