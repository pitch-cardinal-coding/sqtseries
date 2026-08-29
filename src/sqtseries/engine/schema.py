"""Schema DDL and partition-name helpers.
Schema (verified 2026-08-07 via EXPLAIN QUERY PLAN):
- ``series``: metric + JSON tags -> integer series_id, UNIQUE(metric, tags).
- ``measurements_YYYY_MM``: partitioned, WITHOUT ROWID, PK (series_id, timestamp_ns),
  secondary index on timestamp_ns (cross-series ranges + retention).
"""

import datetime as dt
import re

MEASUREMENTS_TABLE_RE = re.compile(r"^measurements_\d{4}_\d{2}$")

SERIES_DDL = """
CREATE TABLE IF NOT EXISTS series (
    series_id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric TEXT NOT NULL,
    tags TEXT,                      -- JSON-encoded TEXT (NOT SQLite JSON type)

    CONSTRAINT ck_tags_json CHECK (tags IS NULL OR json_valid(tags))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_series_metric_tags ON series(metric, tags);
"""


def measurements_ddl(year: int, month: int) -> str:
    """CREATE TABLE DDL for a monthly partition (WITHOUT ROWID + ts index)."""

    name = partition_name(year, month)
    return f"""

    CREATE TABLE IF NOT EXISTS {name} (
        series_id INTEGER NOT NULL,
        timestamp_ns INTEGER NOT NULL,
        value REAL NOT NULL,
        PRIMARY KEY (series_id, timestamp_ns)
    ) WITHOUT ROWID
    """


def measurements_index_ddl(year: int, month: int) -> str:
    name = partition_name(year, month)
    return f"CREATE INDEX IF NOT EXISTS ix_{name}_ts ON {name} (timestamp_ns)"


def partition_name(year: int, month: int) -> str:
    return f"measurements_{year:04d}_{month:02d}"


def month_partition_key(ts_ns: int) -> str:
    # Integer math only: ts_ns / 1e9 is float division, which loses precision
    # beyond 2^53 ns and can round the last nanoseconds of a month up into the
    # next month (verified 2026-08: 2025-12-31 23:59:59.999999999 -> Jan).

    dt_utc = dt.datetime(1970, 1, 1, tzinfo=dt.UTC) + dt.timedelta(
        microseconds=ts_ns // 1000
    )
    return partition_name(dt_utc.year, dt_utc.month)


def parse_partition_name(name: str) -> tuple[int, int]:
    """Return (year, month) for a measurements_YYYY_MM table name."""
    match = MEASUREMENTS_TABLE_RE.match(name)
    if not match:
        raise ValueError(f"not a partition name: {name!r}")
    _, year, month = name.split("_")
    return int(year), int(month)
