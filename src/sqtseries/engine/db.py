"""Raw sqlite3 layer for sqtseries.
Why raw sqlite3: benchmark-validated 2026-08-07 — ~6.7x faster on point
lookups and 1.2x on bulk inserts than the ORM prototype.
API: ``connect()`` (reader), ``begin()`` (writer, BEGIN IMMEDIATE),
``execute()``, ``exec_driver_sql()``, ``scalar()/fetchall()/fetchone()/first()``,
``lastrowid``, ``dispose()``.

Connection pooling: one persistent writer (SQLite single-writer invariant)
guarded by a threading.Lock, plus a bounded LIFO pool of reader connections.
Design points verified against the sqlite.org forum thread "Simple Connection
Pool for SQLite in Python" (forumpost 9e9b8627..., R. Binns / K. Medcalf):
- LIFO, not FIFO: the most recently returned connection has the warmest
  page cache; FIFO would rotate to the coldest one first.
- Connections are created on demand up to the bound, never all up front.
- Check-in hygiene: cursors are closed and any transaction rolled back
  before pooling. A partially-consumed SELECT holds a WAL read snapshot
  even with ``in_transaction == False`` (verified 2026-09: it blocks
  ``PRAGMA wal_checkpoint(TRUNCATE)`` until the statement is reset), so
  every cursor opened through the wrapper is released at check-in.
- The real pooling win is skipping the "fresh connection" cost — open,
  schema read+parse, PRAGMA setup — which grows with the partition count.
Handles are never pooled after dispose(), and a writer handle that raises
is quarantined and lazily replaced (self-healing) so a poisoned connection
can never wedge all writes.
"""

import contextlib
import queue
import sqlite3
import threading
import time
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
    # 8 MB per connection (negative = KiB). 64 MB-class caches on handles
    # shared across executor threads made glibc arenas balloon: pages cached
    # on thread A's arena stay in A even when thread B runs the same handle
    # (free() returns memory to its ALLOCATING thread's arena), so total
    # resident ~= threads x working set and climbed ~linearly under a 10k
    # pts/s pump (measured 2026-09-09: [anon] arena mappings of 60-100 MB
    # each in smaps; anon growth +25 MB/snapshot). 8 MB keeps the hot
    # working set of a steady-state run resident per handle without the
    # cross-thread duplication; misses hit the OS page cache (shared).
    "cache_size": "-8000",
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

# Reader pool ceiling.  Readers scale on WAL snapshots but each holds an mmap
# window and page cache; past ~4 the memory cost outweighs concurrency gains
# for our single-process workload.  (production-hardening.org § sizing table)
DEFAULT_READER_POOL_SIZE = 4

# Page cache for TRANSIENT connections — ones opened when the pool is empty
# because every pooled reader is checked out. A transient handle carrying the
# full 64 MB cache multiplied by concurrent over-checkouts was the dominant
# RSS growth under query bursts (measured 2026-09: ~3 MB/s RSS climb under a
# sustained query storm; the freed arenas are not promptly returned by the
# allocator). 8 MB still covers a hot working set for a single query.
TRANSIENT_CACHE_SIZE = "-8000"


# How long a checkout waits for a pooled slot before falling back to a
# transient handle. Bounded waiting is deadlock prevention (no indefinite
# wait → no circular wait); 5s is well inside the gateway's query budget
# (query.timeout_s, default 30s) and short enough that a stuck slot holder
# cannot freeze unrelated queries.
READER_SLOT_TIMEOUT_S = 5.0


class _ReaderSlots:
    """Bounded reader concurrency: pool size, with BOUNDED waiting.

    Deadlock prevention per the Coffman conditions (Wikipedia, "Deadlock
    (computer science)"): no checkout ever waits indefinitely — ``acquire``
    polls the BoundedSemaphore with a timeout, and on timeout the caller
    opens a TRANSIENT handle (small page cache, closed at check-in) instead
    of queueing forever. This breaks hold-and-wait and circular wait by
    construction, and queueing for up to the timeout IS the intended
    backpressure. BoundedSemaphore.release() is owner-agnostic, so a
    @contextmanager's finally running on a different thread (GC
    finalization, anyio portal handoffs) can never corrupt the count.
    """

    __slots__ = ("_sem",)

    def __init__(self, value: int):
        self._sem = threading.BoundedSemaphore(value)

    def acquire(self, timeout_s: float = READER_SLOT_TIMEOUT_S) -> bool:
        """Take a slot within ``timeout_s``; False = open a transient handle."""
        return self._sem.acquire(timeout=timeout_s)

    def release(self) -> None:
        # BoundedSemaphore.release() is thread-safe and owner-agnostic: safe
        # even when a generator's finally runs on a different thread.
        self._sem.release()


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
    """Wraps a raw sqlite3 connection for one checkout.

    ``with conn:`` ends the transaction (commit on clean exit for writer
    wrappers, rollback on error) but never closes the handle — the pooling
    context managers :meth:`Database.connect` / :meth:`Database.begin` own
    the handle lifecycle, so closing here would poison the pool.
    """

    __slots__ = ("_cursors", "_db", "_raw", "_transaction")

    def __init__(
        self,
        db: Database,
        raw: sqlite3.Connection,
        transaction: bool = False,
    ):
        self._db = db
        self._raw = raw
        self._transaction = transaction
        # Every cursor created through this wrapper. Released at check-in:
        # an abandoned partial SELECT pins a WAL read snapshot even outside
        # an explicit transaction (blocks TRUNCATE checkpoints).
        self._cursors: list[sqlite3.Cursor] = []

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
        self._cursors.append(cur)
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
        self._cursors.append(cur)
        return Result(cur)

    def _release_snapshots(self) -> None:
        """Close every cursor opened through this wrapper.

        A partially-consumed SELECT keeps its statement unreset, holding a
        WAL read snapshot even when ``in_transaction`` is False — blocking
        ``PRAGMA wal_checkpoint(TRUNCATE)`` until GC happens to collect it.
        Closing the cursor finalizes the statement deterministically, the
        same job apsw's ConnectionPool does with ``closecursors(True)``.
        """
        cursors, self._cursors = self._cursors, []
        for cur in cursors:
            with contextlib.suppress(sqlite3.Error):
                cur.close()

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        # End the transaction but never close the handle: Database.connect()
        # / begin() own the lifecycle (closing here would return a dead
        # connection to the pool or close the persistent writer).
        if exc_type is None:
            if self._transaction:
                self.commit()
        else:
            with contextlib.suppress(sqlite3.Error):
                self.rollback()


class Database:
    """A raw sqlite3 database file with connection pooling.

    One persistent writer (guarded by a lock — SQLite single-writer) and a
    bounded queue.Queue of reader connections (WAL concurrent readers).
    A writer handle that fails is quarantined and lazily replaced on the
    next transaction (self-healing); dispose() is idempotent and safe
    against in-flight checkouts.
    """

    def __init__(self, path: str, reader_pool_size: int = DEFAULT_READER_POOL_SIZE):
        self.path = str(Path(path).expanduser())
        parent = Path(self.path).parent

        if str(parent):
            parent.mkdir(parents=True, exist_ok=True)

        if reader_pool_size < 1:
            # Queue(maxsize=0) would mean an UNBOUNDED pool — never allowed.
            raise ValueError("reader_pool_size must be >= 1")

        self._reader_pool_size = reader_pool_size
        # LIFO: the last-returned connection has the warmest page cache;
        # FIFO would hand out the coldest one first (sqlite.org forum
        # forumpost 9e9b8627..., R. Binns).
        self._reader_pool: queue.LifoQueue[sqlite3.Connection] = queue.LifoQueue(
            maxsize=reader_pool_size
        )
        self._writer_conn: sqlite3.Connection | None = None
        self._writer_lock = threading.Lock()
        # Reader slot bound == pool size: a checkout past the pool either
        # queues (bounded wait) or opens a transient connection (8 MB-class
        # page cache each) — never a fresh 64 MB-cache handle per waiter.
        # See _ReaderSlots for the deadlock rationale.
        self._reader_slots = _ReaderSlots(reader_pool_size)
        self._disposed = False

    @contextmanager
    def connect(self) -> Generator[Connection]:
        # Read-only side: no CUD happens through here (autocommit SELECTs).
        # All writes go through begin() below.
        # Bounded wait: take a pool slot or fall back to a transient handle.
        # No indefinite waiting → no circular wait → no deadlock (Coffman).
        # The module global is passed explicitly (looked up per call) so the
        # timeout is tunable at runtime and in tests.
        slot = self._reader_slots.acquire(timeout_s=READER_SLOT_TIMEOUT_S)
        raw = self._checkout_reader(pooled=slot)
        wrapper = Connection(self, raw, transaction=False)
        try:
            yield wrapper
        finally:
            # Release abandoned statements BEFORE the handle is pooled, or a
            # partial SELECT would pin its WAL read snapshot and block
            # TRUNCATE checkpoints for as long as the handle sits in the pool.
            wrapper._release_snapshots()
            self._checkin_reader(raw, pooled=slot)
            if slot:
                self._reader_slots.release()

    @contextmanager
    def begin(self) -> Generator[Connection]:
        # CUD Point: every Create/Delete in the service passes through here.
        """Writer transaction — serialised by _writer_lock.

        Uses ``BEGIN IMMEDIATE``: takes the write lock up front (busy_timeout
        waits for it). Plain ``BEGIN`` is deferred — it takes a read snapshot,
        and the later read->write lock upgrade returns SQLITE_BUSY immediately
        under WAL (deadlock avoidance), which busy_timeout cannot retry.
        """
        # _get_writer() must run under the lock: outside it, two threads can
        # both see an empty slot and open handles, leaking one of them.
        with self._writer_lock:
            raw = self._get_writer()
            conn = Connection(self, raw, transaction=True)
            try:
                try:
                    raw.execute("BEGIN IMMEDIATE")
                except sqlite3.ProgrammingError:
                    # Handle died since the last transaction (e.g. closed
                    # underneath us). Same-call self-heal: quarantine and
                    # retry once on a fresh handle. OperationalError (busy
                    # timeout, corruption, ...) still propagates below.
                    self._quarantine_writer(raw)
                    raw = self._get_writer()
                    conn = Connection(self, raw, transaction=True)
                    raw.execute("BEGIN IMMEDIATE")
                yield conn
                raw.commit()
                conn._release_snapshots()
            except BaseException:
                # Quarantine (close) instead of rollback(): close() discards
                # any pending transaction implicitly, and rollback() itself
                # can raise on a poisoned handle. Either way the handle must
                # never be reused. Lock is still held here.
                self._quarantine_writer(raw)
                raise

    def dispose(self) -> None:
        """Close all pooled handles. Idempotent.

        Waits for an in-flight writer transaction (never closes under it).
        Reader checkouts that land after the drain are closed at check-in
        instead of being re-pooled, so dispose() leaks no descriptors.
        """
        # Set first so a checkout racing the drain cannot re-pool.
        self._disposed = True
        while True:
            try:
                raw = self._reader_pool.get_nowait()
            except queue.Empty:
                break
            with contextlib.suppress(Exception):
                raw.close()
        with self._writer_lock:
            if self._writer_conn is not None:
                with contextlib.suppress(Exception):
                    self._writer_conn.close()
                self._writer_conn = None

    def get_table_names(self) -> list[str]:
        with self.connect() as conn:
            rows = conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        return [r[0] for r in rows]

    def execute(self, sql: str, params: Any = None) -> Result:
        """Execute in one committed writer transaction (writes/DDL persist).

        The returned Result is for ``rowcount``/``lastrowid``; it must not
        be used for row-fetching after this method returns (the transaction
        is already closed) — use ``scalar()`` or a ``connect()`` session.
        """
        with self.begin() as conn:
            return conn.execute(sql, params)

    def scalar(self, sql: str, params: Any = None) -> Any | None:
        with self.connect() as conn:
            return conn.execute(sql, params).scalar()

    def _open(
        self,
        autocheckpoint: int = READER_AUTOCHECKPOINT,
        cache_size: str | None = None,
    ) -> sqlite3.Connection:
        raw = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        try:
            self._open_pragmas(raw, autocheckpoint, cache_size)
        except BaseException:
            # Setup failed mid-way (e.g. journal_mode race on a fresh file):
            # the handle must not escape unclosed to die at GC time.
            with contextlib.suppress(Exception):
                raw.close()
            raise
        return raw

    def _open_pragmas(
        self,
        raw: sqlite3.Connection,
        autocheckpoint: int,
        cache_size: str | None,
    ) -> None:
        # busy_timeout FIRST: it must cover every statement below. (The
        # connect(timeout=30) driver default also applies, but PRAGMA order
        # makes the guarantee explicit.)
        raw.execute("PRAGMA foreign_keys = ON")
        raw.execute(f"PRAGMA busy_timeout = {CONNECTION_PRAGMAS['busy_timeout']}")
        self._set_wal(raw)
        pragmas = CONNECTION_PRAGMAS
        if cache_size is not None:
            pragmas = {**CONNECTION_PRAGMAS, "cache_size": cache_size}
        for name, value in pragmas.items():
            if name in ("busy_timeout", "journal_mode"):
                # Applied above, before the loop.
                continue
            raw.execute(f"PRAGMA {name} = {value}")
        raw.execute(f"PRAGMA wal_autocheckpoint = {autocheckpoint}")
        return raw

    def _set_wal(self, raw: sqlite3.Connection) -> None:
        """Re-affirm WAL with a short retry.

        On a FRESH file (created outside create_sqlite_engine's bootstrap) the
        journal-mode conversion needs a brief exclusive lock, and SQLite does
        NOT invoke the busy handler for journal_mode changes — two connections
        racing their first _open() otherwise fail instantly with
        'database is locked' (reproduced in test_concurrent_checkouts, 2026-09).
        Re-affirming WAL on an already-WAL file is a no-op and never races.
        """
        for attempt in range(5):
            try:
                raw.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) or attempt == 4:
                    raise
                time.sleep(0.02 * (attempt + 1))

    def _checkout_reader(self, pooled: bool = True) -> sqlite3.Connection:
        if pooled:
            try:
                return self._reader_pool.get_nowait()
            except queue.Empty:
                # Slot owned but pool momentarily empty (another checkout is
                # between get_nowait and check-in): open a full-cache handle.
                return self._open(autocheckpoint=READER_AUTOCHECKPOINT)
        # No slot within the timeout: a transient handle with a SMALL page
        # cache. It will be closed at check-in (pool is full), so investing
        # 64 MB of page cache in it is pure waste — and the allocator keeps
        # those arenas mapped after free, inflating RSS for the lifetime.
        return self._open(
            autocheckpoint=READER_AUTOCHECKPOINT, cache_size=TRANSIENT_CACHE_SIZE
        )

    def _checkin_reader(self, conn: sqlite3.Connection, pooled: bool = True) -> None:
        # Transient handle (no slot was owned) or disposed engine: close
        # instead of re-pooling, so the pool only ever holds full-cache
        # handles and a checkout racing dispose() leaks no descriptors.
        if not pooled or self._disposed:
            with contextlib.suppress(Exception):
                conn.close()
            return
        try:
            if conn.in_transaction:
                conn.rollback()
        except sqlite3.Error:
            # Closed or otherwise broken handle — never pool a dead connection.
            with contextlib.suppress(Exception):
                conn.close()
            return
        try:
            self._reader_pool.put_nowait(conn)
        except queue.Full:
            conn.close()

    def _get_writer(self) -> sqlite3.Connection:
        """Return the writer handle; caller must hold ``_writer_lock``.

        Self-healing: if the previous transaction failed, the poisoned
        handle was quarantined (closed) and a fresh one is opened here, so
        one bad transaction can never wedge every future write.
        """
        if self._writer_conn is None:
            self._writer_conn = self._open(autocheckpoint=WRITER_AUTOCHECKPOINT)
        return self._writer_conn

    def _quarantine_writer(self, raw: sqlite3.Connection) -> None:
        """Close and untrack a failed writer handle. Caller holds the lock."""
        if self._writer_conn is raw:
            self._writer_conn = None
        with contextlib.suppress(Exception):
            raw.close()


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
        raw = sqlite3.connect(db.path)
        try:
            raw.execute("PRAGMA page_size = 8192")
            raw.execute("PRAGMA auto_vacuum = INCREMENTAL")
        finally:
            raw.close()
    return db
