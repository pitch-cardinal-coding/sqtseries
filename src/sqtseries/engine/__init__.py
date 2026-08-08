"""SQLite storage engine for sqtseries (raw sqlite3 layer)."""

from .backup import BackupExistsError, BackupManager, backup_database, backup_latest
from .checkpoint import CheckpointManager
from .db import Connection, Database, create_sqlite_engine
from .maintenance import MaintenanceManager
from .migrations import get_schema_version, run_migrations, upgrade
from .pragmas import (
    incremental_vacuum,
    integrity_check,
    quick_check,
    run_analyze_once,
    run_optimize,
    wal_checkpoint,
)
from .store import SeriesNotFoundError, StorageEngine, initialize_schema

__all__ = [
    "BackupExistsError",
    "BackupManager",
    "CheckpointManager",
    "Connection",
    "Database",
    "MaintenanceManager",
    "SeriesNotFoundError",
    "StorageEngine",
    "backup_database",
    "backup_latest",
    "create_sqlite_engine",
    "get_schema_version",
    "incremental_vacuum",
    "initialize_schema",
    "integrity_check",
    "quick_check",
    "run_analyze_once",
    "run_migrations",
    "run_optimize",
    "upgrade",
    "wal_checkpoint",
]
