"""Periodic maintenance: refresh query-planner statistics with ANALYZE.
SQLite's query planner uses statistics collected by ``ANALYZE``; over time
those go stale as data changes. ``MaintenanceManager`` re-runs ``ANALYZE`` on
a schedule (default hourly) in a worker thread so the event loop stays
responsive. The startup ANALYZE (``run_analyze_once``) covers the boot case.
"""

import asyncio
import time
from contextlib import suppress

import structlog

from .db import Database
from .pragmas import run_analyze_once

log = structlog.get_logger(__name__)


class MaintenanceManager:
    """Run periodic maintenance (ANALYZE) in the background."""

    def __init__(self, db: Database, *, interval: float = 3600.0):
        self.db = db
        self.interval = interval
        self._task: asyncio.Task | None = None
        self.last_analyze: float | None = None
        self.runs = 0

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="maintenance-manager")

    async def run_once(self) -> None:
        try:
            await asyncio.to_thread(run_analyze_once, self.db)
            self.last_analyze = time.time()
            self.runs += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("maintenance pass failed")

    async def _run(self) -> None:
        while True:
            # ANALYZE already ran at startup; first pass is after one interval.

            await asyncio.sleep(self.interval)
            await self.run_once()

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
