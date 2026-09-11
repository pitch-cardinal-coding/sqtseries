"""Retention policy enforcement: drop partitions older than a TTL.
Retention drops entire monthly partitions (fast DROP TABLE) when the last
day of a partition falls before the retention horizon. Dropping a partition
also removes its rows from the hourly rollup, so deleted data leaves no
aggregate trace behind.
"""

import asyncio
import re
from contextlib import suppress
from datetime import UTC, datetime, timedelta

import structlog

from ..engine.db import Database
from ..engine.schema import MEASUREMENTS_TABLE_RE
from .manager import PartitionManager
from .rollup import ROLLUP_TABLE

log = structlog.get_logger(__name__)

TTL_PATTERN = re.compile(r"^(\d+)([smhdw])$")
UNITS: dict[str, timedelta] = {
    "s": timedelta(seconds=1),
    "m": timedelta(minutes=1),
    "h": timedelta(hours=1),
    "d": timedelta(days=1),
    "w": timedelta(weeks=1),
}


def parse_ttl(ttl: str) -> timedelta:
    """Parse '30d', '12h', '4w' etc. into a timedelta."""
    match = TTL_PATTERN.match(ttl.strip().lower())
    if not match:
        raise ValueError(f"invalid TTL: {ttl!r} (use e.g. 30d, 12h, 4w)")
    amount = int(match.group(1))

    return amount * UNITS[match.group(2)]


class RetentionPolicy:
    """Drop partitions whose data is entirely older than the TTL."""

    def __init__(
        self,
        engine: Database,
        ttl: str | timedelta,
        *,
        manager: PartitionManager | None = None,
    ):
        self.engine = engine
        self.manager = manager or PartitionManager(engine)

        if isinstance(ttl, str):
            ttl = parse_ttl(ttl)
        self.ttl = ttl

    def partitions_to_retain(self, now: datetime | None = None) -> set[str]:
        """Names of partitions fully within the retention window."""
        now = now or datetime.now(UTC)
        horizon = now - self.ttl
        keep: set[str] = set()
        for name in self.manager.list_partitions():
            year, month = _parse_partition_name(name)
            last_day = _last_day_of_month(year, month)
            if last_day >= horizon:
                keep.add(name)
        return keep

    def run(self, now: datetime | None = None) -> list[str]:
        # CUD Point: Delete trigger — TTL expiry is the sole source of
        # user-data deletion (drop_partition + rollup-row cleanup below).
        """Drop expired partitions; returns dropped table names."""
        now = now or datetime.now(UTC)
        keep = self.partitions_to_retain(now)
        dropped: list[str] = []
        for name in self.manager.list_partitions():
            if name not in keep:
                year, month = _parse_partition_name(name)
                self.manager.drop_partition(year, month)
                self._drop_rollup_rows(year, month)
                dropped.append(name)
        if dropped:
            log.info("retention dropped partitions", dropped=dropped)
        return dropped

    def _drop_rollup_rows(self, year: int, month: int) -> None:
        """Remove this partition's hours from the rollup table (if it exists)."""

        if ROLLUP_TABLE not in self.engine.get_table_names():
            return
        start_ns = _month_start_ns(year, month)

        end_ns = _month_end_ns(year, month)

        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                f"DELETE FROM {ROLLUP_TABLE} "  # noqa: S608 - constant table name
                "WHERE hour_start_ns >= ? AND hour_start_ns < ?",
                (start_ns, end_ns),
            )


class RetentionManager:
    """Background task: enforce the retention policy on a schedule."""

    def __init__(
        self,
        engine: Database,
        store: object,
        *,
        ttl: str,
        interval: float,
    ):
        self.engine = engine
        # StorageEngine; used for partition-cache invalidation
        self.store = store
        self.ttl = ttl
        self.interval = interval
        self._task: asyncio.Task | None = None
        self.runs = 0
        self.dropped_total = 0

    async def start(self) -> None:
        # First pass runs immediately inside the loop task (in a thread), so
        # expired partitions are cleaned up on boot without stalling startup.

        self._task = asyncio.create_task(self._run(), name="retention-manager")

    async def run_once(self) -> None:
        try:
            await asyncio.to_thread(self._run_sync)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("retention pass failed")

    def _run_sync(self) -> None:
        manager = PartitionManager(
            self.engine, on_change=self.store.invalidate_partitions
        )
        policy = RetentionPolicy(self.engine, self.ttl, manager=manager)
        dropped = policy.run()

        if dropped:
            self.dropped_total += len(dropped)
            # Defensive; on_change also fires
            self.store.invalidate_partitions()
        self.runs += 1

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


def _parse_partition_name(name: str) -> tuple[int, int]:
    if not MEASUREMENTS_TABLE_RE.match(name):
        raise ValueError(f"invalid partition table name: {name!r}")
    _, y, m = name.split("_")
    return int(y), int(m)


def _last_day_of_month(year: int, month: int) -> datetime:
    nxt = _month_end_dt(year, month)
    return nxt - timedelta(days=1)


def _month_end_dt(year: int, month: int) -> datetime:
    if month == 12:
        return datetime(year + 1, 1, 1, tzinfo=UTC)
    return datetime(year, month + 1, 1, tzinfo=UTC)


def _month_start_ns(year: int, month: int) -> int:
    return int(datetime(year, month, 1, tzinfo=UTC).timestamp() * 1_000_000_000)


def _month_end_ns(year: int, month: int) -> int:
    return int(_month_end_dt(year, month).timestamp() * 1_000_000_000)
