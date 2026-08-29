"""Rollup aggregation: pre-aggregate measurements into hourly tables.
Rollups trade storage for query speed: ``rollup_hourly`` stores per-series
per-hour aggregates (count/sum/min/max, so avg is derived as sum/count).
Only *completed* hours are ever rolled up: an hour ``H`` is complete once the
clock passes it, so the current hour is always served from the raw partitions.
A ``last_hour`` watermark in ``rollup_meta`` records how far the rollup has
advanced, so each run scans only the data accrued since the previous run and
``INSERT OR REPLACE`` keeps re-runs idempotent.
Assumption (documented): in-order ingestion for completed hours. A write
backfilled into an already-rolled past hour is not picked up until that
partition is rebuilt with ``replace=True``.
"""

import asyncio
import re
import time
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

import structlog

from ..engine.db import Database
from ..engine.schema import parse_partition_name

log = structlog.get_logger(__name__)

HOUR_NS = 3_600_000_000_000
ROLLUP_TABLE = "rollup_hourly"
ROLLUP_META = "rollup_meta"
META_LAST_HOUR = "last_hour"

# Only our own partition names are ever interpolated into SQL.
_PARTITION_RE = re.compile(r"^measurements_\d{4}_\d{2}$")


def _safe_partition(name: str) -> str:
    if not _PARTITION_RE.match(name):
        raise ValueError(f"invalid partition table name: {name!r}")
    return name


def completed_hour_ns(now_ns: int | None = None) -> int:
    """Start of the current, still-in-progress hour (a full-hour boundary)."""

    now = time.time_ns() if now_ns is None else now_ns
    return (now // HOUR_NS) * HOUR_NS


def ensure_rollup_table(db: Database) -> None:
    """Create the hourly rollup and watermark tables if missing."""
    with db.begin() as conn:
        conn.exec_driver_sql(f"""
            CREATE TABLE IF NOT EXISTS {ROLLUP_TABLE} (
                series_id INTEGER NOT NULL,
                hour_start_ns INTEGER NOT NULL,
                count INTEGER NOT NULL,
                sum REAL NOT NULL,
                min REAL NOT NULL,
                max REAL NOT NULL,
                PRIMARY KEY (series_id, hour_start_ns)
            ) WITHOUT ROWID
            """)
        conn.exec_driver_sql(f"""
            CREATE TABLE IF NOT EXISTS {ROLLUP_META} (
                k TEXT PRIMARY KEY,
                v INTEGER NOT NULL
            )
            """)


def rollup_watermark(db: Database) -> int:
    """Return the next hour to roll (all earlier hours are rolled). 0 if none."""

    if ROLLUP_META not in db.get_table_names():
        return 0
    with db.connect() as conn:
        row = conn.exec_driver_sql(
            f"SELECT v FROM {ROLLUP_META} WHERE k = ?",  # noqa: S608 - constant table name
            (META_LAST_HOUR,),
        ).first()
    return int(row[0]) if row else 0


def rollup_partition(
    db: Database,
    partition: str,
    *,
    replace: bool = False,
    cutoff_ns: int | None = None,
) -> int:
    """Aggregate a measurements partition into the hourly rollup.
    Rolls the partition's completed hours in ``[watermark, cutoff_ns)``.

    With ``replace=True`` the whole partition's month is rebuilt (its rollup

    rows are cleared first) — used for backfill recovery. The global watermark

    is advanced by :func:`rollup_new_hours`, not here. Returns rows written.

    """
    ensure_rollup_table(db)
    partition = _safe_partition(partition)

    cutoff = completed_hour_ns() if cutoff_ns is None else cutoff_ns

    if replace:
        start_ns, _ = _month_bounds(partition)
    else:
        start_ns = rollup_watermark(db)
    if start_ns >= cutoff:
        return 0

    agg_sql = f"""
        SELECT series_id, (timestamp_ns / ?) * ? AS hour,
               COUNT(*), SUM(value), MIN(value), MAX(value)
        FROM {partition}
        WHERE timestamp_ns >= ? AND timestamp_ns < ?
        GROUP BY series_id, hour
    """  # noqa: S608 - partition validated by _safe_partition
    with db.begin() as conn:
        if replace:
            conn.exec_driver_sql(
                f"DELETE FROM {ROLLUP_TABLE} "  # noqa: S608 - constant table name
                "WHERE hour_start_ns >= ? AND hour_start_ns < ?",
                (start_ns, cutoff),
            )
        rows = conn.exec_driver_sql(
            agg_sql, (HOUR_NS, HOUR_NS, start_ns, cutoff)
        ).fetchall()
        if rows:
            conn.executemany(
                f"""
                INSERT OR REPLACE INTO {ROLLUP_TABLE}
                (series_id, hour_start_ns, count, sum, min, max)
                VALUES (?, ?, ?, ?, ?, ?)
                """,  # noqa: S608 - constant table name
                [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows],
            )
    log.info("rollup complete", partition=partition, rows=len(rows))
    return len(rows)


def rollup_new_hours(
    db: Database, now_ns: int | None = None, *, skew_s: float = 0.0
) -> int:
    """Roll every partition's newly-completed hours and advance the watermark.

    Only hours that ended at least ``skew_s`` before "now" are rolled. This

    keeps the fast path exact under the client clock-skew guard: a write

    arriving up to ``skew_s`` late can never land in an hour that has already

    been rolled. Idempotent: safe to call repeatedly and after an interrupted

    run. Returns the number of rollup rows written this call.
    """
    ensure_rollup_table(db)
    now = time.time_ns() if now_ns is None else now_ns
    completed = completed_hour_ns(int(now - skew_s * 1_000_000_000))

    watermark = rollup_watermark(db)

    if watermark >= completed:
        return 0
    partitions = _partitions_for_range(db, watermark, completed)

    total = 0

    for name in partitions:
        total += rollup_partition(db, name, cutoff_ns=completed)
    with db.begin() as conn:
        conn.exec_driver_sql(
            f"INSERT OR REPLACE INTO {ROLLUP_META} (k, v) VALUES (?, ?)",  # noqa: S608 - constant
            (META_LAST_HOUR, completed),
        )
    log.info("rollup advanced", watermark=completed, rows=total)
    return total


def query_rollup_partial(
    db: Database,
    series_ids: list[int],
    start_ns: int | None,
    end_ns: int | None,
    *,
    bucket_ns: int | None,
) -> list[tuple[int | None, int, float, float, float]]:
    """Partial aggregates from the rollup, for the query fast path.

    Returns ``[(bucket_start_ns, count, sum, min, max)]`` (bucket_start_ns is

    None for the whole-window form when ``bucket_ns`` is None). Empty when no

    rollup rows match. Bound semantics: ``[start_ns, end_ns)``.
    """
    if not series_ids:
        return []
    ids = [int(s) for s in series_ids]
    placeholders = ",".join("?" for _ in ids)

    if bucket_ns is None:
        cols = "SUM(count), SUM(sum), MIN(min), MAX(max)"

        group = ""
        params: list[Any] = [*ids]
    else:
        cols = "(hour_start_ns / ?) * ? AS b, SUM(count), SUM(sum), MIN(min), MAX(max)"

        group = " GROUP BY 1"

        params = [bucket_ns, bucket_ns, *ids]
    sql = f"SELECT {cols} FROM {ROLLUP_TABLE} WHERE series_id IN ({placeholders})"  # noqa: S608 - constant table name

    if start_ns is not None:
        sql += " AND hour_start_ns >= ?"
        params.append(start_ns)
    if end_ns is not None:
        sql += " AND hour_start_ns < ?"
        params.append(end_ns)
    sql += group

    with db.connect() as conn:
        rows = conn.exec_driver_sql(sql, params).fetchall()
    if bucket_ns is None:
        if not rows:
            return []
        row = rows[0]
        # SUM(count) is NULL when no rollup rows matched
        if row[0] is None:
            return []
        return [(None, int(row[0]), float(row[1]), float(row[2]), float(row[3]))]
    return [(int(r[0]), int(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows]


def _partitions_for_range(db: Database, start_ns: int, end_ns: int) -> list[str]:
    """Measurement partitions whose month intersects [start_ns, end_ns)."""

    out = []
    for name in db.get_table_names():
        if not _PARTITION_RE.match(name):
            continue
        lo, hi = _month_bounds(name)
        if hi < start_ns or lo >= end_ns:
            continue
        out.append(name)
    return sorted(out)


def _month_bounds(partition: str) -> tuple[int, int]:
    """(start_ns, end_ns_exclusive) of a partition's month."""
    year, month = parse_partition_name(partition)
    start = datetime(year, month, 1, tzinfo=UTC)
    if month == 12:
        nxt = datetime(year + 1, 1, 1, tzinfo=UTC)
    else:
        nxt = datetime(year, month + 1, 1, tzinfo=UTC)
    return int(start.timestamp() * 1e9), int(nxt.timestamp() * 1e9)


class RollupManager:
    """Background task: keep the hourly rollup current for completed hours.

    ``skew_s`` is the client clock-skew window (``ingestion.
    reject_client_timestamp_skew_s``): hours are only rolled once they are at

    least that far past completion, so late writes can never hit a rolled hour.

    """

    def __init__(self, db: Database, *, interval: float = 300.0, skew_s: float = 0.0):
        self.db = db
        self.interval = interval
        self.skew_s = skew_s
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        # No blocking warm-up on boot: the first pass runs inside the loop task
        # immediately, so a large catch-up (e.g. first boot on an old database)
        # happens in a thread and never stalls service startup or the event loop.

        self._task = asyncio.create_task(self._run(), name="rollup-manager")

    async def run_once(self) -> None:
        try:
            await asyncio.to_thread(rollup_new_hours, self.db, skew_s=self.skew_s)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("rollup pass failed")

    async def _run(self) -> None:
        while True:
            await self.run_once()
            await asyncio.sleep(self.interval)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
