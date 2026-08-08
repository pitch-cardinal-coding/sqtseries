"""Partition manager: creation, rotation, and inspection of monthly partitions."""

import re
import threading
from collections.abc import Callable
from datetime import UTC, datetime

import structlog

from ..engine.db import Database
from ..engine.schema import measurements_ddl, measurements_index_ddl, partition_name

log = structlog.get_logger(__name__)

_PARTITION_RE = re.compile(r"^measurements_\d{4}_\d{2}$")


def _safe_partition(name: str) -> str:
    """Validate a partition name before interpolating it into SQL."""
    if not _PARTITION_RE.match(name):
        raise ValueError(f"invalid partition table name: {name!r}")
    return name


class PartitionManager:
    """Create and enumerate monthly measurement partitions."""

    def __init__(self, db: Database, on_change: Callable[[], None] | None = None):
        self.db = db
        self._ddl_lock = threading.Lock()
        self._on_change = on_change

    def list_partitions(self) -> list[str]:
        """Return sorted measurement partition table names."""
        with self.db.connect() as conn:
            rows = conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'measurements_%' ORDER BY name"
            ).fetchall()
        return [str(r[0]) for r in rows]

    def ensure_partition(self, year: int, month: int) -> str:
        """Create the partition table if missing; returns its name."""
        name = partition_name(year, month)
        with self._ddl_lock:
            if name in set(self.db.get_table_names()):
                return name
            with self.db.connect() as conn:
                conn.executescript(measurements_ddl(year, month))
                conn.executescript(measurements_index_ddl(year, month))
        if self._on_change is not None:
            self._on_change()
        log.info("created partition", table=name)
        return name

    def drop_partition(self, year: int, month: int) -> None:
        """DROP an old partition table."""
        name = _safe_partition(partition_name(year, month))
        with self._ddl_lock, self.db.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
        if self._on_change is not None:
            self._on_change()
        log.info("dropped partition", table=name)

    def current_partition(self, now_ns: int | None = None) -> str:
        """Return the partition name active at ``now_ns`` (default: now)."""
        import time

        dt_utc = datetime.fromtimestamp((now_ns or time.time_ns()) / 1e9, tz=UTC)
        return partition_name(dt_utc.year, dt_utc.month)
