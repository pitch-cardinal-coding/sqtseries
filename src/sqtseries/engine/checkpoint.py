"""Background WAL checkpoint manager.

Drives checkpointing to prevent WAL file indefinite growth. Also monitors
the WAL and detects long-running readers that block checkpoints (a reader
holding a read lock prevents TRUNCATE from freeing disk;
``PRAGMA wal_checkpoint`` reports a nonzero ``busy`` count).
"""

import asyncio
from contextlib import suppress
from pathlib import Path

import structlog

from .db import Database
from .pragmas import wal_checkpoint

log = structlog.get_logger(__name__)

# Long-reader warning threshold: consecutive checkpoint attempts blocked.
_BUSY_WARN_RUNS = 3


class CheckpointManager:
    """Manage WAL checkpoints to prevent starvation."""

    def __init__(
        self,
        db: Database,
        *,
        # Check every 60 seconds
        interval: float = 60.0,
        # 64MB, matching journal_size_limit
        max_wal_bytes: int = 67108864,
    ):
        self.db = db
        self.interval = interval
        self.max_wal_bytes = max_wal_bytes
        self._task: asyncio.Task | None = None

        # monitoring stats
        self.wal_bytes: int = 0
        self.last_busy: int = 0
        # (busy, log, done) — results of the last checkpoint run
        self.last_result: tuple[int, int, int] | None = None
        self.busy_runs: int = 0
        self.checkpoints = 0

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="checkpoint-manager")

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.interval)
                await self._checkpoint_if_needed()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("checkpoint manager error")

    async def _checkpoint_if_needed(self) -> None:
        """Run TRUNCATE checkpoint if WAL exceeds threshold; track reader locks."""
        wal_path = Path(self.db.path + "-wal")
        if not wal_path.exists():
            self.wal_bytes = 0
            return
        self.wal_bytes = wal_path.stat().st_size
        if self.wal_bytes < self.max_wal_bytes:
            return

        log.info("running checkpoint", wal_size=self.wal_bytes)
        try:
            # TRUNCATE requires exclusive lock; passive does not.
            # Use TRUNCATE here to actually free disk space.
            result = wal_checkpoint(self.db, "TRUNCATE")
            busy, log_frames, done = _parse_result(result)
            self.last_result = (busy, log_frames, done)
            self.last_busy = busy
        except Exception as exc:
            log.warning("checkpoint failed: %s", exc)
            self.last_busy = 1
            return

        if busy > 0:
            # A reader is holding a read lock, blocking the checkpoint.
            self.busy_runs += 1
            if self.busy_runs >= _BUSY_WARN_RUNS:
                log.warning(
                    "checkpoint blocked by long-running reader(s) for %d "
                    "consecutive runs (busy=%d, wal=%d bytes)",
                    self.busy_runs,
                    busy,
                    self.wal_bytes,
                )
        else:
            if self.busy_runs:
                log.info(
                    "checkpoint unblocked after %d blocked run(s)",
                    self.busy_runs,
                )
            self.busy_runs = 0
            self.checkpoints += 1

    def stats(self) -> dict[str, int | tuple[int, int, int] | None]:
        """WAL monitoring stats for the admin/ops surface."""
        return {
            "wal_bytes": self.wal_bytes,
            "last_busy": self.last_busy,
            "last_result": self.last_result,
            "busy_runs": self.busy_runs,
            "checkpoints": self.checkpoints,
        }

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None


def _parse_result(result: str) -> tuple[int, int, int]:
    """Parse ``PRAGMA wal_checkpoint`` -> (busy, log, checkpointed) frames."""
    parts = [int(x) for x in result.split(",") if x.strip()]
    if len(parts) != 3:
        return (0, 0, 0)
    return (parts[0], parts[1], parts[2])
