"""Health-check helpers for the service."""

import time
from typing import Any

from sqtseries.engine import quick_check
from sqtseries.engine.db import Database


def collect_health(
    db: Database, *, started_at: float, version: str = "0.1.0"
) -> dict[str, Any]:
    """Assemble a health payload from a Database."""
    db_ok = False
    try:
        db_ok = quick_check(db) == "ok"
    except Exception:
        db_ok = False
    return {
        "status": "ok" if db_ok else "degraded",
        "version": version,
        "uptime": int(time.time() - started_at),
        "database": db_ok,
    }


def is_ready(db: Database) -> bool:
    try:
        return quick_check(db) == "ok"
    except Exception:
        return False
