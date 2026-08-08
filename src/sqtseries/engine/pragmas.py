"""SQLite PRAGMA helpers on the raw Database layer.

Two classes (research-verified):
- **One-time**: page_size, auto_vacuum — only on a fresh file, before tables.
- **Per-connection**: WAL etc. — applied by ``Database._open`` on every connect.
"""

from .db import Database


def run_optimize(db: Database) -> None:
    """PRAGMA optimize — update query-planner statistics (shutdown/periodic)."""
    with db.connect() as conn:
        conn.exec_driver_sql("PRAGMA optimize")


def wal_checkpoint(db: Database, mode: str = "PASSIVE") -> str:
    """PRAGMA wal_checkpoint. TRUNCATE is shutdown-only (corruption risk)."""
    mode = mode.upper()
    if mode not in ("PASSIVE", "FULL", "RESTART", "TRUNCATE"):
        raise ValueError(f"Invalid checkpoint mode: {mode}")
    with db.connect() as conn:
        row = conn.exec_driver_sql(f"PRAGMA wal_checkpoint({mode})").first()
    return ",".join(str(x) for x in row) if row else ""


def quick_check(db: Database) -> str:
    with db.connect() as conn:
        row = conn.exec_driver_sql("PRAGMA quick_check").first()
    return str(row[0]) if row else "unknown"


def integrity_check(db: Database) -> list[str]:
    with db.connect() as conn:
        rows = conn.exec_driver_sql("PRAGMA integrity_check").fetchall()
    return [str(r[0]) for r in rows]


def incremental_vacuum(db: Database, pages: int = 100) -> None:
    with db.connect() as conn:
        conn.exec_driver_sql(f"PRAGMA incremental_vacuum({pages})")


def run_analyze_once(db: Database) -> None:
    """Run ANALYZE once to refresh query-planner statistics (startup/periodic)."""
    with db.connect() as conn:
        conn.exec_driver_sql("ANALYZE")
