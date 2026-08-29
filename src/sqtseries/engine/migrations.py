"""Lightweight schema migration runner.
SQLite ALTER support is limited (table rebuild needed for most changes), so
we keep a ``schema_version`` table and apply migrations idempotently at
startup. New migrations append to ``MIGRATIONS``.
"""

from .db import Database
from .schema import SERIES_DDL, measurements_ddl, measurements_index_ddl

MIGRATIONS: list[tuple[int, str]] = [
    (
        1,
        SERIES_DDL,
    ),
]

_SCHEMA_VERSION_DDL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
)
"""


def get_schema_version(db: Database) -> int:
    with db.connect() as conn:
        conn.executescript(_SCHEMA_VERSION_DDL)
        row = conn.execute("SELECT MAX(version) FROM schema_version").first()
    return int(row[0]) if row and row[0] is not None else 0


def upgrade(db: Database, target: int | None = None) -> int:
    """Apply pending migrations; returns the new schema version."""
    current = get_schema_version(db)
    for version, ddl in sorted(MIGRATIONS):
        if version <= current:
            continue
        if target is not None and version > target:
            break
        with db.begin() as conn:
            conn.executescript(ddl)
            conn.execute(
                "INSERT INTO schema_version(version, applied_at) VALUES (?, datetime('now'))",
                (version,),
            )
        current = version
    return current


def ensure_current_partition(db: Database) -> None:
    """Create the current-month partition + its ts index if missing."""

    import time

    from .schema import month_partition_key, parse_partition_name

    now_ns = time.time_ns()
    y, m = parse_partition_name(month_partition_key(now_ns))
    name = month_partition_key(now_ns)

    if name not in set(db.get_table_names()):
        with db.connect() as conn:
            conn.executescript(measurements_ddl(y, m))
            conn.executescript(measurements_index_ddl(y, m))


def run_migrations(db: Database) -> int:
    """Upgrade to latest; then ensure the current partition exists."""
    version = upgrade(db)
    ensure_current_partition(db)
    return version
