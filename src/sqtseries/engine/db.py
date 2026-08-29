"""Raw sqlite3 layer for sqtseries.
Why raw sqlite3: benchmark-validated 2026-08-07 — ~6.7x faster on point
lookups and 1.2x on bulk inserts than alternatives (see
sqtseries-research-2026.md §8). aiosqlite is slower sequentially (thread
hops) — rejected.
API: ``connect()`` (reader), ``begin()`` (writer, BEGIN IMMEDIATE),
``execute()``, ``exec_driver_sql()``, ``scalar()/fetchall()/fetchone()/first()``,
``lastrowid``, ``dispose()``.
"""

import sqlite3
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..config import DatabaseSettings

# PRAGMAs applied to every new connection (WAL is persistent per file but
# re-affirming is harmless and keeps behavior explicit).
CONNECTION_PRAGMAS: dict[str, str] = {
    "journal_mode": "WAL",
    "synchronous": "NORMAL",
    "cache_size": "-64000",
    "mmap_size": "268435456",
    "busy_timeout": "5000",
    "temp_store": "MEMORY",
    "foreign_keys": "ON",
    "locking_mode": "NORMAL",
    "journal_size_limit": "67108864",
    "threads": "4",
    "analysis_limit": "400",
}

# Reader connections never trigger checkpoints (prevents reader-writer
# contention); the writer checkpoints every ~80MB at 8192-byte pages (10000
# pages, 10x fewer fsync spikes than the 1000-page default). See
# sqtseries-research-2026.md §9.
READER_AUTOCHECKPOINT = 0
WRITER_AUTOCHECKPOINT = 10000


class Result:
    """Thin wrapper over sqlite3.Cursor with the fetch helpers we use."""

    __slots__ = ("_cursor",)

    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor

    def fetchall(self) -> list[Any]:
        return self._cursor.fetchall()

    def fetchone(self) -> Any | None:
        return self._cursor.fetchone()

    def first(self) -> Any | None:
        return self._cursor.fetchone()

    def scalar(self) -> Any | None:
        row = self._cursor.fetchone()
        if row is None:
            return None
        return row[0]

    @property
    def lastrowid(self) -> int:
        return int(self._cursor.lastrowid)

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    def __getitem__(self, idx: int) -> Any:
        row = self._cursor.fetchone()
        if row is None:
            raise IndexError(idx)
        return row[idx]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._cursor)


class Connection:
    """Wraps a raw sqlite3 connection; context manager commits on clean exit."""

    __slots__ = ("_db", "_raw", "_transaction")

    def __init__(
        self, db: Database, raw: sqlite3.Connection, transaction: bool = False
    ):
        self._db = db
        self._raw = raw
        self._transaction = transaction

    @property
    def dbapi_connection(self) -> sqlite3.Connection:
        return self._raw

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        self._raw.close()

    def execute(self, sql: str, params: Any = None) -> Result:
        cur = self._raw.cursor()
        try:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
        except sqlite3.Error:
            cur.close()
            raise
        return Result(cur)

    def exec_driver_sql(self, sql: str, params: Any = None) -> Result:
        return self.execute(sql, params)

    def executescript(self, script: str) -> None:
        self._raw.executescript(script)

    def executemany(self, sql: str, seq_of_params: Any) -> Result:
        cur = self._raw.cursor()
        try:
            cur.executemany(sql, seq_of_params)
        except sqlite3.Error:
            cur.close()
            raise
        return Result(cur)

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if exc_type is None:
                self.commit() if self._transaction else None
        finally:
            self.close()


class Database:
    """A raw sqlite3 database file."""

    def __init__(self, path: str):
        self.path = str(Path(path).expanduser())
        parent = Path(self.path).parent

        if str(parent):
            parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self, apply_pragmas: bool = True) -> Generator[Connection]:
        """Yield a fresh READER connection (never checkpoints WAL)."""
        conn = Connection(
            self,
            self._open(
                apply_pragmas=apply_pragmas, autocheckpoint=READER_AUTOCHECKPOINT
            ),
            transaction=False,
        )
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def begin(self) -> Generator[Connection]:
        """Yield a WRITER transaction connection (owns WAL checkpointing).

        Uses ``BEGIN IMMEDIATE``: takes the write lock up front (busy_timeout

        waits for it). Plain ``BEGIN`` is deferred — it takes a read snapshot,

        and the later read->write lock upgrade returns SQLITE_BUSY immediately

        under WAL (deadlock avoidance), which busy_timeout cannot retry.

        """
        raw = self._open(apply_pragmas=True, autocheckpoint=WRITER_AUTOCHECKPOINT)

        raw.execute("BEGIN IMMEDIATE")
        conn = Connection(self, raw, transaction=True)
        try:
            yield conn
            raw.commit()
        except BaseException:
            raw.rollback()
            raise
        finally:
            conn.close()

    def dispose(self) -> None:
        """No persistent handles to close (connections are per-use)."""

        return

    def get_table_names(self) -> list[str]:
        with self.connect() as conn:
            rows = conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        return [r[0] for r in rows]

    def execute(self, sql: str, params: Any = None) -> Result:
        """Execute SQL in a committed writer transaction.
        Writes and DDL persist (the old implementation used a reader
        connection, so writes were silently rolled back on close). The
        returned Result is best used for ``rowcount``/``lastrowid``; for

        reading rows use ``scalar()`` or a ``connect()`` session.
        """
        with self.begin() as conn:
            return conn.execute(sql, params)

    def scalar(self, sql: str, params: Any = None) -> Any | None:
        with self.connect() as conn:
            return conn.execute(sql, params).scalar()

    def _open(
        self, apply_pragmas: bool = True, autocheckpoint: int = READER_AUTOCHECKPOINT
    ) -> sqlite3.Connection:
        raw = sqlite3.connect(self.path, check_same_thread=False)
        if not apply_pragmas:
            return raw
        raw.execute("PRAGMA foreign_keys = ON")
        for name, value in CONNECTION_PRAGMAS.items():
            raw.execute(f"PRAGMA {name} = {value}")
        raw.execute(f"PRAGMA wal_autocheckpoint = {autocheckpoint}")
        return raw


def create_sqlite_engine(
    path: str, settings: DatabaseSettings | None = None
) -> Database:
    """Create a Database for a SQLite file (API-compatible with old engine).

    On a fresh file, one-time file-level PRAGMAs (page_size, auto_vacuum)

    are applied BEFORE the per-connection pragmas (WAL). Per sqlite.org

    (lang_vacuum.html): auto_vacuum/page_size can only be changed after file

    creation when NOT in WAL mode — so the bootstrap connection skips WAL.

    """
    db = Database(path)
    if not Path(db.path).exists():
        with db.connect(apply_pragmas=False) as conn:
            conn.exec_driver_sql("PRAGMA page_size = 8192")
            conn.exec_driver_sql("PRAGMA auto_vacuum = INCREMENTAL")
    return db
