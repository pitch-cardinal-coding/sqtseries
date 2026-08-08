"""Rollup aggregation tests."""

from datetime import UTC, datetime

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.partition import ensure_rollup_table, rollup_partition


def _ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000_000)


def _make_engine(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "rollup.sqlite"))
    initialize_schema(eng)
    return eng


def test_rollup_aggregates(tmp_path):
    engine = _make_engine(tmp_path)
    store = StorageEngine(engine)
    base = datetime(2026, 3, 1, 10, 0, tzinfo=UTC)
    rows = [
        ("cpu", None, 1.0, _ns(base)),
        # same hour
        ("cpu", None, 2.0, _ns(base) + 60 * 1e9),
        # next hour
        ("cpu", None, 5.0, _ns(base) + 3600 * 1e9),
    ]
    store.insert_many(rows)

    ensure_rollup_table(engine)
    rollup_partition(engine, "measurements_2026_03")

    import sqlite3

    con = sqlite3.connect(str(tmp_path / "rollup.sqlite"))
    try:
        hours = con.execute(
            "SELECT hour_start_ns, count, sum, min, max FROM rollup_hourly"
        ).fetchall()
    finally:
        con.close()
    # 2 hours: hour 10 has 2 rows (sum 3, min 1, max 2); hour 11 has 1 row
    assert len(hours) == 2
    hour_map = {h[0]: h for h in hours}
    h10 = hour_map[
        base.replace(minute=0, second=0, microsecond=0).timestamp() * 1_000_000_000
    ]
    # count
    assert h10[1] == 2
    # sum
    assert h10[2] == 3.0
    # min
    assert h10[3] == 1.0
    # max
    assert h10[4] == 2.0
    engine.dispose()


def test_rollup_replace(tmp_path):
    engine = _make_engine(tmp_path)
    store = StorageEngine(engine)
    base = datetime(2026, 4, 1, 0, 0, tzinfo=UTC)
    store.insert_many(
        [("m", None, 3.0, _ns(base)), ("m", None, 7.0, _ns(base) + 10 * 1e9)]
    )
    ensure_rollup_table(engine)
    rollup_partition(engine, "measurements_2026_04")
    n2 = rollup_partition(engine, "measurements_2026_04", replace=True)
    import sqlite3

    con = sqlite3.connect(str(tmp_path / "rollup.sqlite"))
    try:
        cnt = con.execute("SELECT COUNT(*) FROM rollup_hourly").fetchone()[0]
    finally:
        con.close()
    # one hour bucket after replace
    assert cnt == 1
    assert n2 == 1
    engine.dispose()
