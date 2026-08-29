"""Backups via ``VACUUM INTO`` — consistent point-in-time snapshot.
Caveats (sqlite.org/lang_vacuum.html): destination must not exist; cannot
run inside a transaction; works with any auto_vacuum mode.
"""

import asyncio
import re
import time
from contextlib import suppress
from pathlib import Path

import structlog

from .db import Database

log = structlog.get_logger(__name__)


class BackupExistsError(FileExistsError):
    """Raised when the backup destination already exists."""


# Keep the prefix safe for both filenames and SQL interpolation.
_PREFIX_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def backup_database(db: Database, backup_dir: str, prefix: str = "sqtseries") -> str:
    if not _PREFIX_RE.match(prefix):
        raise ValueError(f"invalid backup prefix: {prefix!r}")
    directory = Path(backup_dir).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    backup_path = directory / f"{prefix}-{time.strftime('%Y%m%d-%H%M%S')}.db"

    if backup_path.exists():
        raise BackupExistsError(f"Backup already exists: {backup_path}")
    # VACUUM INTO cannot run inside a transaction — use autocommit connect()

    with db.connect() as conn:
        conn.exec_driver_sql(f"VACUUM INTO '{backup_path}'")
    return str(backup_path)


def backup_latest(
    db: Database, backup_dir: str, prefix: str = "sqtseries"
) -> str | None:
    directory = Path(backup_dir).expanduser()
    if not directory.is_dir():
        return None
    candidates = sorted(directory.glob(f"{prefix}-*.db"))

    return str(candidates[-1]) if candidates else None


def _is_fresh(path: str, interval: float) -> bool:
    """True if the backup file is younger than ``interval`` seconds."""

    try:
        return time.time() - Path(path).stat().st_mtime < interval
    except OSError:
        return False


class BackupManager:
    """Background task: take a consistent snapshot every ``interval`` seconds.

    Wired from ``backup.enabled``; first pass runs immediately (so a fresh

    instance gets a snapshot on boot), then every ``interval`` seconds.

    Runs in a worker thread so the event loop stays responsive even when the

    database is large. A snapshot whose filename already exists (same-second

    retry, e.g. after a restart) is skipped rather than raising.
    """

    def __init__(
        self,
        engine: Database,
        *,
        interval: float = 86400.0,
        backup_dir: str = "~/.sqtseries/backups/",
        prefix: str = "sqtseries",
    ):
        self.engine = engine
        self.interval = interval
        self.backup_dir = backup_dir
        self.prefix = prefix
        self._task: asyncio.Task | None = None
        self.runs = 0
        self.backups_created = 0
        self.last_backup: str | None = None

    async def start(self) -> None:
        # First pass runs immediately inside the loop task (in a thread), so
        # a fresh install gets a backup on boot without stalling startup.

        self._task = asyncio.create_task(self._run(), name="backup-manager")

    async def run_once(self) -> None:
        try:
            latest = await asyncio.to_thread(
                backup_latest, self.engine, self.backup_dir, self.prefix
            )
            if latest and _is_fresh(latest, self.interval):
                # snapshot from this interval already exists; count the pass

                self.runs += 1
                return
            path = await asyncio.to_thread(
                backup_database, self.engine, self.backup_dir, self.prefix
            )
            self.backups_created += 1
            self.last_backup = path
            self.runs += 1
            log.info("backup created", path=path)
        except BackupExistsError:
            # same-second retry (restart within the same second): keep the
            # existing snapshot, do not double-write.
            self.runs += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("backup pass failed")

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
