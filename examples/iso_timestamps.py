"""ISO-8601 timestamps beside epoch (write + query).

Run: /home/iam/devcode/.env/sqtseries/bin/python3 examples/iso_timestamps.py
Uses an embedded DB in a temp dir; no running service needed.
"""

import tempfile
from pathlib import Path

from sqtseries.engine import create_sqlite_engine
from sqtseries.engine.db import Database
from sqtseries.engine.migrations import run_migrations
from sqtseries.engine.store import StorageEngine
from sqtseries.messaging.protocol import iso_to_ns, parse_ingest
from sqtseries.query import TimeSeriesDB


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="sqt-iso-")
    db = str(Path(tmp) / "iso.sqlite")
    run_migrations(Database(db))
    eng = create_sqlite_engine(db)
    try:
        tsdb = TimeSeriesDB(StorageEngine(eng))
        points = [
            ("2026-09-11T00:28:51.740Z", 22.5),
            ("2026-09-11T00:28:51.123456Z", 23.0),
            ("2026-09-11T00:28:51.123456789Z", 23.5),
            ("2026-09-11T14:04:00Z", 24.0),
        ]
        for ts_str, value in points:
            msg = parse_ingest(
                {"metric": "temp.outside", "value": value, "timestamp": ts_str}
            )
            tsdb.store.insert_many([msg.to_rows()])
            print(f"wrote {value} at {ts_str} -> {iso_to_ns(ts_str)} ns")

        rows = tsdb.query(
            "temp.outside", start="2026-09-11T00:00:00Z", end="2026-09-11T15:00:00Z"
        )
        print(f"ISO window returned {len(rows)} rows:")
        for ts_ns, value in rows:
            print(f"  {ts_ns} ({ts_ns / 1e9:.9f}) = {value}")

        epoch_rows = tsdb.query(
            "temp.outside",
            start=iso_to_ns("2026-09-11T00:00:00Z"),
            end=iso_to_ns("2026-09-11T15:00:00Z"),
        )
        assert rows == epoch_rows, "ISO and epoch windows must match"
        print("ISO window matches epoch window.")
    finally:
        eng.dispose()


if __name__ == "__main__":
    main()
