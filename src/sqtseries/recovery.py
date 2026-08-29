"""WAL recovery and corruption detection on startup."""

import structlog

from sqtseries.engine import integrity_check, quick_check, wal_checkpoint
from sqtseries.engine.db import Database

log = structlog.get_logger(__name__)


class CorruptionError(Exception):
    """Raised when the database fails integrity checks."""


def check_integrity_on_startup(db: Database, *, strict: bool = False) -> int:
    """Run quick_check on startup. strict=True raises on corruption.
    Returns number of problems (0 = clean).
    """
    status = quick_check(db)
    if status == "ok":
        log.info("quick_check passed")
        return 0

    log.warning("quick_check reported problems: %s", status)
    problems = integrity_check(db)
    for line in problems:
        log.error("integrity: %s", line)
    if strict:
        raise CorruptionError(f"database integrity check failed: {status}")
    return len(problems)


def recover_wal(db: Database) -> None:
    """Ensure WAL is active and run a PASSIVE checkpoint to compact the log."""

    with db.connect() as conn:
        mode = conn.exec_driver_sql("PRAGMA journal_mode").scalar()
    if str(mode).lower() != "wal":
        with db.connect() as conn:
            conn.exec_driver_sql("PRAGMA journal_mode = WAL")
    wal_checkpoint(db, "PASSIVE")
