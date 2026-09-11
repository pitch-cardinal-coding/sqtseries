# Research Notes

Archive of the research that informed sqtseries' design and
hardening decisions. Sources: sqlite.org (atomiccommit.html, forum post
9e9b8627), libzmq source study, pyzmq docs, the ffmpeg-zmq project's
production receivers, and deadlock literature. Each document is dated;
"Current state" notes inside a document describe what ships today.

## Contents

1. [Consolidated Project Research](#consolidated-project-research)
2. [PyZMQ + asyncio Research](#pyzmq-asyncio-research)
3. [SQLite Forum: Connection Pool Post 9e9b8627](#sqlite-forum-connection-pool-post-9e9b8627)
4. [SQLite Atomicity & Accounting](#sqlite-atomicity-accounting)
5. [ZeroMQ Resilience Options (libzmq source study)](#zeromq-resilience-options-libzmq-source-study)
6. [ffmpeg-zmq Pub/Sub Lessons](#ffmpeg-zmq-pubsub-lessons)
7. [Deadlock Theory: Reader-Bound Systems](#deadlock-theory-reader-bound-systems)

---

## Consolidated Project Research

## sqtseries Technology Research — Consolidated Findings

**Date**: 2026-08-07
**Project**: sqtseries (SQLite Time-Series Database with ZeroMQ)

---

## 2. SQLite PRAGMAs for Time-Series

### Must be set BEFORE tables exist:
- `page_size = 8192` (doubles rows per page, reduces B-tree depth)
- `auto_vacuum = INCREMENTAL` (must be set at DB creation)

### Set on every connection:
- `journal_mode = WAL` (3-10x write TPS)
- `synchronous = NORMAL` (75x faster than FULL, safe in WAL)
- `cache_size = -64000` (64MB)
- `mmap_size = 268435456` (256MB)
- `busy_timeout = 5000` (eliminates SQLITE_BUSY)
- `temp_store = MEMORY`
- `foreign_keys = ON`
- `locking_mode = NORMAL`
- `wal_autocheckpoint = 1000`
- `journal_size_limit = 67108864` (64MB WAL cap)
- `threads = 4`
- `analysis_limit = 400`
- `optimize = 0x10002` (on open for long-lived connections)

> **Current state:** reader connections use `wal_autocheckpoint = 0`
> (readers never checkpoint) and writer connections use `10000`
> (`src/sqtseries/engine/db.py`). `PRAGMA optimize = 0x10002` is not set
> on open — connections are short-lived per use; plain `PRAGMA optimize`
> runs at shutdown only.

### On connection close:
- `PRAGMA optimize` (update query planner statistics)

### WAL Checkpoint Strategy:
- Periodic: `PRAGMA wal_checkpoint(PASSIVE)` — safe, no lock
- Shutdown only: `PRAGMA wal_checkpoint(TRUNCATE)` — exclusive lock, reclaims space
- NEVER use TRUNCATE periodically — caused B-tree corruption (NousResearch/hermes-agent#45383)

### Incremental Vacuum:
- `PRAGMA incremental_vacuum(100)` during low-traffic
- Monitor: `freelist_count / page_count` — if >20%, run vacuum

### Integrity Check:
- `PRAGMA quick_check` on startup (fast, skips some deep checks)
- `PRAGMA integrity_check` only on suspicion (expensive, O(n) over all pages)

### Backup:
- `VACUUM INTO 'path'` — consistent snapshot
- Destination file must not exist
- Cannot run inside transaction

### JSON Type:
- SQLite `JSON` type name gets NUMERIC affinity, not TEXT
- Use TEXT with `CHECK(json_valid(col))` constraint
- JSON functions (->, ->>) work on TEXT in SQLite 3.38+

### Clock Skew:
- Use server-side timestamps (`time.time_ns()`)
- Reject client timestamps > threshold
- InfluxDB, TimescaleDB both use server-side timestamps by default

---

## 3. PyZMQ + asyncio

### Context:
- Use `zmq.asyncio.Context` (not `zmq.Context`)
- `ctx.term()` blocks until all sockets closed AND linger expires
- `ctx.destroy(linger=X)` is NOT threadsafe — avoid in async

### LINGER:
- Set BEFORE bind/connect (pyzmq#1407)
- LINGER=0: discard on close; LINGER=500: 500ms grace; LINGER=-1: block forever

### IPC:
- ~36μs latency vs ~46μs for TCP
- ZMQ does NOT auto-unlink IPC files (libzmq#3204)
- Use $XDG_RUNTIME_DIR or /run/user/<uid>/ for ephemeral files
- Manual cleanup on startup + shutdown

### Heartbeats:
- ZMTP native: `HEARTBEAT_IVL=1000`, `HEARTBEAT_TIMEOUT=5000`, `HEARTBEAT_TTL=5000`
- Application-level for business logic (ZMTP detects transport death, not app hangs)
- Use BOTH

### Write Queue (bounded-queue pattern):
- Internal `deque` + `POLLOUT` drain
- NEVER drop from PUSH sockets
- Monitor `ZMQ_EVENTS & POLLOUT` (more efficient than NOBLOCK + EAGAIN)

> **Current state:** live delivery to browser clients goes through the
> in-process bounded fan-out hub (`messaging/fanout.py`, per-subscriber byte
> budgets, loud drop counting) — no extra socket hop. The XPUB socket serves
> external subscribers with native best-effort semantics: frames for slow or
> not-yet-connected subscribers are dropped at its SNDHWM, which is
> documented in `docs/streaming.html`. The PUSH ingest side never drops
> (client PUSH blocks at its own SNDHWM). Do not treat this section as a
> description of the shipped pubsub.

### Reconnection:
- PUSH auto-reconnects to PULL peers
- Messages queue up to HWM during disconnection
- If HWM exceeded, PUSH blocks (no loss by default)

### HWM:
- Per-peer limit
- PUSH: blocks on HWM; PUB: drops for slow subs; ROUTER: drops silently

---

## Sources
- sqlite.org/pragma.html, wal.html, datatype3.html, limits.html, lang_vacuum.html
- github.com/zeromq/libzmq/issues/3204 (IPC cleanup)
- github.com/zeromq/pyzmq/issues/1407 (LINGER timing)
- github.com/NousResearch/hermes-agent/issues/45383 (WAL checkpoint corruption)

---

## 4. Additional Findings (2026-08-07)

### series_id Pattern
- Standard pattern: `series` table (metric + tags → series_id) + `data` table (series_id, ts, value)
- Integer FK is 4-8 bytes vs repeated tag strings
- InfluxDB uses measurement + tag set as series identity
- TimescaleDB uses dimension columns

### WITHOUT ROWID
- Critical for time-series with composite PK (series_id, timestamp_ns)
- Eliminates redundant rowid storage
- (sqlite.org/withoutrowid.html)

### SQLITE_MAX_VARIABLE_NUMBER
- 32,766 on SQLite 3.32+ (was 999 before)
- Multi-row INSERT counts each ? as a parameter
- 5000 rows × 4 cols = 20,000 params — OK on 3.32+
- Recommended: 2,000 rows (8,000 params) for safety margin
- (sqlite.org/limits.html)

### auto_vacuum=INCREMENTAL vs VACUUM INTO
- VACUUM INTO works FINE with auto_vacuum=INCREMENTAL
- The earlier claim of incompatibility was FALSE
- VACUUM INTO rebuilds entire DB into new file regardless of auto_vacuum mode
- (sqlite.org/lang_vacuum.html)

### PRAGMA page_size on existing DB
- Silently does nothing on non-empty databases
- Only effective before first CREATE TABLE
- Should NOT be set on every connection open
- Set once during initialization, then skip

### PRAGMA optimize=0x10002
- 0x10002 = examine all tables (not just recently queried) + use analysis_limit
- For long-lived connections: run on first open, then periodically
- For short-lived connections: run plain PRAGMA optimize at close time
- Running 0x10002 on every short-lived connection is wasteful
- (sqlite.org/lang_analyze.html)

### pool_recycle NOT needed for SQLite
- SQLite is embedded, not network — no server-side timeout
- pool_recycle is for MySQL/PostgreSQL where server closes idle connections
- (SA docs)

### incremental_vacuum N parameter
- N = number of pages to reclaim, not rows
- With 8192-byte pages, 100 pages = ~800KB
- Monitor freelist_count / page_count ratio

---

## 5. PyZMQ Production Patterns (2026-08-07)

### Memory Management
- COPY_THRESHOLD = 64KB: copy for small, zero-copy for large
- zmq.Frame uses ref-counting for zero-copy
- send_pyobj can leak (pickle holds refs) — use send + metadata for binary
- BufferPool pattern for high-throughput: pre-allocate, recv_into, reuse

### Thread Safety
- zmq.Context IS thread-safe (libzmq API)
- zmq.Socket is NOT thread-safe — one socket per thread
- Socket migration between threads OK during initialization only
- Thread-safe types: ZMQ_SERVER/CLIENT, ZMQ_RADIO/DISH, ZMQ_SCATTER/GATHER

### GIL Release
- pyzmq releases GIL during send/recv (since pyzmq 14.0)
- True parallelism in multi-threaded Python
- Cython backend uses nogil blocks

### High-Throughput
- zmq.NOBLOCK = zmq.DONTWAIT (identical)
- NOBLOCK recv is slow due to exception overhead — prefer polling
- io_threads=N for dedicated I/O threads
- Sync is 5-15% faster than async for pure throughput
- Async wins when mixing I/O sources

### Production Socket Options
- MAXMSGSIZE: set for untrusted peers (reject oversized)
- SNDBUF/RCVBUF: kernel buffer tuning for bursty workloads
- TCP_KEEPALIVE: detect dead TCP connections
- IPV6: enable if needed

### Memory Leak Prevention
- Use context managers (pyzmq 24+)
- Avoid send_pyobj for binary data
- Monitor ctx._sockets for leaked sockets
- zmq.Context.instance() is fine (thread-safe since 15.3)

---

## 6. External Client API Design (2026-08-07)

### Recommendation: Both ZMQ + HTTP
- ZMQ for high-throughput producers/consumers (IoT, metrics collectors)
- HTTP for dashboards, CLI, third-party (curl, browsers)
- Like Kafka: native protocol + REST Proxy

### ZMQ Client Patterns
- Producer: zmq.PUSH → connect to tcp://host:5557
- Query: zmq.REQ → connect to tcp://host:5558
- Subscribe: zmq.SUB → connect to tcp://host:5559

### HTTP Gateway (FastAPI)
- POST /api/v1/write → ingest
- GET /api/v1/read → query
- WebSocket /ws/subscribe → live stream
- GET /api/v1/health → health check

### Authentication
- Local-only: skip CURVE, use IP whitelist + firewall
- Remote: CURVE for encryption + auth (complex key management)
- Start simple, add auth as needed

### Backpressure
- ZMQ HWM alone is NOT true backpressure
- Application-level ACKs: send → service ACKs after write
- Rate limiting on HTTP (429 responses)

---

## 7. Scaling Analysis (2026-08-07)

> **Current state:** the write path is a receive-only drain loop feeding a
> **bounded** queue through `ingestion.pending_max` semaphore credits to a
> dedicated persister task (`messaging/ingress.py`). The drain loop parses
> up to 1024 frames per tick and NEVER awaits a commit; the persister
> commits one `BEGIN IMMEDIATE` transaction per batch in a worker thread.
> When credits are exhausted the drain loop suspends and ZMQ backpressure
> applies (pump blocks at its SNDHWM): bounded memory, zero silent loss.
> Measured under full concurrent query load (8 REQ clients + HTTP
> read/agg/write + admin + WS subscriber): sustained **10,007 pts/s
> absorbed, 700,223/700,223 points persisted, 0 dropped, 0 unaccounted**
> (`scripts/stress_percentiles.py`, 2026-09-11).
> `await`ing commits inline was measured capping ingest at ~1,650 pts/s —
> the decoupled persister is what buys the order of magnitude.

### Write Path (Ingestion)
- ZMQ PULL receives: ~millions/sec (limited by network)
- Write queue buffers: configurable (HWM=10,000)
- SQLite writes: ~100K rows/sec sustained (batched inserts)
- **Bottleneck**: SQLite write throughput

### Read Path (Queries)
- ZMQ REQ receives: ~millions/sec
- SQLite reads: ~100K queries/sec (simple), ~10K-50K (aggregation)
- **Bottleneck**: Query complexity

### Broadcast Path (Live Streaming)
- ZMQ PUB sends: ~millions/sec
- Subscribers: limited by network, not CPU
- **Bottleneck**: Network bandwidth

### Connection Capacity
- ZMQ connections: ~10K concurrent (practical limit)
- Memory: ~1GB per 1,000 connections
- **Bottleneck**: Memory per connection

### Overall System Limits
| Metric | Conservative | Optimistic |
|--------|-------------|------------|
| Sustained writes | 50K rows/sec | 200K rows/sec |
| Query throughput | 10K queries/sec | 50K queries/sec |
| Concurrent connections | 1,000 | 10,000 |
| Live subscribers | 100 | 1,000 |
| Memory usage | 1GB | 10GB |
| Database size | 100GB | 1TB |

### What Limits Scale
1. SQLite single-writer (WAL allows concurrent reads)
2. GIL (but pyzmq releases it for I/O)
3. Memory per connection
4. Disk I/O for writes

### What Doesn't Limit Scale
1. ZMQ throughput (millions/sec)
2. Python CPU (I/O bound, not CPU bound)
3. Network (local IPC is fast)

## 9. WAL Tuning — reader/writer autocheckpoint split (2026-08-07)

### Sources
- danReynolds/resqlite experiment 022 (2026-04): readers wal_autocheckpoint=0
  (readers must NEVER trigger checkpoints — prevents reader-writer contention);
  writer wal_autocheckpoint=10000 (~40MB, 10x fewer fsync spikes); journal_size_limit
  caps WAL growth. Microbenchmarks neutral, but tail-latency + reliability win.
- sqliteforum.com checkpoint algorithms (2026-05): PASSIVE stops when readers block;
  long-running readers prevent WAL truncation; keep read transactions short.
- productionhardening.org: default 1000-page threshold assumes moderate write velocity;
  high-write workloads should raise it.
- phiresky SQLite tuning gist: synchronous=NORMAL is corruption-safe in WAL;
  synchronous=OFF can corrupt — we keep NORMAL.

### Applied design (db.py)
- connect() (reads, autocommit) -> wal_autocheckpoint = 0   (never checkpoint)
- begin()  (write transactions = ingestion path) -> wal_autocheckpoint = 10000
- journal_size_limit = 64MB stays on all connections (bounds WAL growth)

> **Current state:** sqtseries sets `page_size = 8192`
> (`create_sqlite_engine`), so the writer's 10000-page threshold is ~80MB.
> In practice the CheckpointManager TRUNCATEs the WAL once it exceeds 64MB
> (matching `journal_size_limit`), so autocheckpoint rarely fires at all —
> the split's real role is keeping reader connections from ever checkpointing.

## 10. Engine hardening notes (2026-08-07)

### Bugs found & fixed during engine work (all research-backed)
1. auto_vacuum no-op: WAL must NOT be set on the bootstrap connection
   (sqlite.org/lang_vacuum.html: auto_vacuum changeable after file creation
   only when NOT in WAL mode). Fixed: connect(apply_pragmas=False) first.
2. Concurrent writers "database is locked": deferred BEGIN takes a read
   snapshot; the read->write upgrade returns SQLITE_BUSY immediately in WAL
   (deadlock avoidance, sqlite.org/lockingv3.html) - busy_timeout can't retry.
   Fixed: BEGIN IMMEDIATE takes the write lock up front.
3. UNIQUE constraint: inline CONSTRAINT creates sqlite_autoindex_series_1;
   moved to named CREATE UNIQUE INDEX uq_series_metric_tags (stable name).
4. VACUUM INTO cannot run inside a transaction (autocommit connect() only).
5. @contextmanager annotations: typeshed now wants Generator[..] not
   Iterator[..] (microsoft/pyright#11402, typeshed#2772). Fixed db.py.
6. WAL autocheckpoint split: readers=0 (never checkpoint), writers=10000
   (resqlite exp 022) - fewer fsync latency spikes.

### Cleanup
- requires-python bumped to >=3.14 per user decision (no __future__ imports).
- health() bug: passed StorageEngine instead of Database to quick_check.

## 11. black/ruff target-version misalignment (2026-08-07)

- Symptom: black kept rewriting `except (A, B):` -> `except A, B:` (PEP 758,
  new in 3.14), while ruff (target-version=py312) flagged the result as
  "invalid-syntax". Endless revert loop.
- Root cause: pyproject had ruff target-version=py312 while black ran under
  Python 3.14 and normalized to PEP 758 style. Tools disagreed.
- Fix: set BOTH to py314 (matches requires-python >=3.14 decision).
  Black then leaves 60/60 files unchanged; ruff passes.
- Lesson: when tools format to a newer-Python syntax, keep ruff target-version
  in sync with black's effective version; run black BEFORE ruff --fix.

## 12. Docs rewrite + official-client pattern (2026-08-07)

### Research: how quality projects document multi-language clients
- InfluxDB: per-language client pages (install -> connect -> write -> query),
  one page per language, official client libraries.
- Redis docs: "Choose a client library" table + per-language guides with
  runnable snippets; HTTP/REST as fallback for any language.
- Pattern applied: docs/clients.html with Python (bundled Client), Go (zmq4),
  Rust (zmq crate), PHP (curl HTTP), Node (fetch/WebSocket).

### Code added (pattern: ship an official client)
- src/sqtseries/client.py: high-level `sqtseries.Client` (PUSH write,
  REQ query/aggregate, SUB subscribe) exported from package root.
- Service now binds the PUB socket (was spec'd but never wired): ingest ->
  on_publish -> pubsub.publish; publish tasks tracked to avoid GC (RUF006).
- Broker query handler extended: `aggregations` (comma-separated) +
  aggregation/interval/limit/order passthrough.

### Docs fixed (were written pre-swap, contained stale content)
- sqtseries start -> sqtseries run (CLI command is `run`)
- Python 3.12+ -> 3.14+ (requires-python decision)
- Removed non-existent CLI commands (benchmark/compact/enable/disable/
  import/info/integrity/logs/restart) and admin-socket commands
  (info/compact/backup) marked as reserved/roadmap.
- api.html Python section now shows real TimeSeriesDB (embedded) + real
  Client (external). Schema diagram corrected (series_id, tags TEXT,
  UNIQUE index, ts index, WITHOUT ROWID only on measurements).

---

## PyZMQ + asyncio Research

## PyZMQ + asyncio: Comprehensive Research (2026)

## 1. PyZMQ asyncio Integration

The core integration uses `zmq.asyncio.Context` instead of `zmq.Context`. All blocking methods (`send`, `recv`, `poll`) return `asyncio.Future` objects.

```python
import zmq
import zmq.asyncio

## Must use zmq.asyncio.Context, NOT zmq.Context
ctx = zmq.asyncio.Context()


async def worker():
    sock = ctx.socket(zmq.PULL)
    sock.bind("tcp://*:5555")
    while True:
        msg = await sock.recv_multipart()  # returns Future
        # process msg
        await sock.send_multipart(reply)
```

**Key points:**
- `zmq.asyncio.Context` creates sockets that return Futures from blocking methods
- Added in pyzmq 15.0, stable and production-ready
- No pre-configuration needed as of pyzmq 17+
- Uses edge-triggered file descriptor under the hood
- On Windows: must use `asyncio.SelectorEventLoop` (Proactor loop doesn't support `add_reader`)
- For Python 3.12+: `asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)` on Windows

**Performance note:** For maximum throughput (250k+ msgs/sec), bypass Futures and use shadow sockets with DONTWAIT:

```python
ctx = zmq.asyncio.Context()
receiver = ctx.socket(zmq.PULL)
receiver.connect(url)

## Create blocking shadow for fast drain
shadow = zmq.Socket.shadow(receiver.underlying)
poller = zmq.asyncio.Poller()
poller.register(receiver, zmq.POLLIN)

while True:
    events = await poller.poll()
    while events:
        try:
            msg = shadow.recv_multipart(zmq.DONTWAIT)
        except zmq.Again:
            events = receiver.events & zmq.POLLIN
        else:
            process(msg)
```

## 2. Socket Patterns with asyncio

### PUSH/PULL (Pipeline)

```python
import asyncio
import zmq
from zmq.asyncio import Context

ctx = Context()
URL = "tcp://127.0.0.1:5555"


async def producer():
    push = ctx.socket(zmq.PUSH)
    push.bind(URL)
    for i in range(100):
        await push.send_multipart([f"task-{i}".encode()])
    push.close()


async def worker(name):
    pull = ctx.socket(zmq.PULL)
    pull.connect(URL)
    while True:
        msg = await pull.recv_multipart()
        print(f"{name} got: {msg[0].decode()}")
        # simulate work
        await asyncio.sleep(0.1)


async def main():
    await asyncio.gather(
        producer(),
        worker("w1"),
        worker("w2"),
        worker("w3"),
    )


asyncio.run(main())
```

### PUB/SUB

```python
import asyncio
import zmq
from zmq.asyncio import Context

ctx = Context()


async def publisher():
    pub = ctx.socket(zmq.PUB)
    pub.bind("tcp://*:5555")
    await asyncio.sleep(0.3)  # CRITICAL: wait for subscribers to connect
    topic = b"weather"
    while True:
        msg = b"temp=72.5"
        await pub.send_multipart([topic, msg])
        await asyncio.sleep(1)


async def subscriber(name, topic_filter):
    sub = ctx.socket(zmq.SUB)
    sub.connect("tcp://127.0.0.1:5555")
    sub.setsockopt(zmq.SUBSCRIBE, topic_filter)
    while True:
        [topic, msg] = await sub.recv_multipart()
        print(f"{name}: [{topic.decode()}] {msg.decode()}")


async def main():
    await asyncio.gather(
        publisher(),
        subscriber("sub1", b"weather"),
        subscriber("sub2", b""),
    )


asyncio.run(main())
```

**PUB/SUB gotcha:** Always sleep ~300ms after bind before sending to allow subscribers to connect and register subscriptions. Messages sent before subscription registration are lost.

### ROUTER/DEALER (Async Request-Reply)

```python
import asyncio
import zmq
from zmq.asyncio import Context

ctx = Context()


async def router_server():
    """Async server handling multiple clients"""
    router = ctx.socket(zmq.ROUTER)
    router.bind("tcp://*:5555")

    while True:
        [client_id, request] = await router.recv_multipart()
        print(f"Router got from {client_id!r}: {request.decode()}")
        # process asynchronously
        await asyncio.sleep(0.5)
        reply = b"processed: " + request
        await router.send_multipart([client_id, reply])


async def dealer_client(name):
    """Async client - can send multiple requests without waiting"""
    dealer = ctx.socket(zmq.DEALER)
    dealer.setsockopt(zmq.IDENTITY, name.encode())
    dealer.connect("tcp://127.0.0.1:5555")

    for i in range(3):
        await dealer.send_multipart([f"request-{i}".encode()])
        # don't need to wait for reply before sending next!

    # collect replies
    for _ in range(3):
        [reply] = await dealer.recv_multipart()
        print(f"{name} got: {reply.decode()}")


async def main():
    await asyncio.gather(
        router_server(),
        dealer_client("client-A"),
        dealer_client("client-B"),
    )


asyncio.run(main())
```

**ROUTER/DEALER vs REQ/REP:** REQ/REP enforces strict send-recv-send-recv alternation. ROUTER/DEALER is fully async - send multiple requests without waiting for replies. Always prefer ROUTER/DEALER for production.

## 3. Worker Pool Pattern

```python
import asyncio
import zmq
from zmq.asyncio import Context

ctx = Context()


async def broker(frontend_addr, backend_addr, num_workers=4):
    """ROUTER/DEALER proxy with worker pool"""
    frontend = ctx.socket(zmq.ROUTER)
    frontend.bind(frontend_addr)

    backend = ctx.socket(zmq.DEALER)
    backend.bind(backend_addr)

    # Spawn worker coroutines
    workers = [asyncio.create_task(worker(i, backend_addr)) for i in range(num_workers)]

    # Simple proxy: forward between frontend and backend
    async def forward(src, dst):
        while True:
            msg = await src.recv_multipart()
            await dst.send_multipart(msg)

    await asyncio.gather(
        forward(frontend, backend),
        forward(backend, frontend),
    )


async def worker(worker_id, backend_addr):
    """Worker connects to backend, processes tasks"""
    sock = ctx.socket(zmq.REP)
    sock.connect(backend_addr)

    while True:
        msg = await sock.recv_multipart()
        # simulate processing
        await asyncio.sleep(0.1)
        result = b"done"
        await sock.send_multipart([result])
```

**Load-balanced worker pool (Paranoid Pirate style):**

```python
import asyncio
import collections
import zmq
from zmq.asyncio import Context

ctx = Context()


class WorkerPool:
    def __init__(self):
        self.available = collections.deque()
        self.poll_timeout = 1000  # ms

    async def ready(self, worker_id):
        self.available.append(worker_id)

    async def send_to_worker(self, router, worker_id, msg):
        await router.send_multipart([worker_id, b"", msg])

    async def recv_from_worker(self, router):
        [worker_id, empty, reply] = await router.recv_multipart()
        return worker_id, reply


async def lb_broker():
    frontend = ctx.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5555")

    backend = ctx.socket(zmq.ROUTER)
    backend.bind("tcp://*:5556")

    pool = WorkerPool()
    pending = {}  # worker_id -> client_id

    poller = zmq.asyncio.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    while True:
        events = dict(await poller.poll(pool.poll_timeout))

        # Worker ready / reply
        if backend in events:
            [worker_id, empty, reply] = await backend.recv_multipart()
            if reply == b"READY":
                await pool.ready(worker_id)
            elif reply != b"HEARTBEAT":
                # Reply to client
                client_id = pending.pop(worker_id, None)
                if client_id:
                    await frontend.send_multipart([client_id, b"", reply])

        # Client request
        if frontend in events:
            [client_id, empty, request] = await frontend.recv_multipart()
            if pool.available:
                worker_id = pool.available.popleft()
                pending[worker_id] = client_id
                await pool.send_to_worker(backend, worker_id, request)
            # else: drop or queue
```

## 4. Backpressure Handling

**Problem:** ZMQ HWM does NOT provide true application-level backpressure. HWM governs the C-level send buffer, and PUSH sockets will block when HWM is reached - but this is a blunt instrument.

**Application-level backpressure with DEALER/ROUTER (from pyzmq issue #1638):**

```python
import asyncio
import zmq
from zmq.asyncio import Context

ctx = Context()


async def client_with_backpressure():
    """Track outstanding requests, block when too many in flight"""
    socket = ctx.socket(zmq.DEALER)
    socket.connect("tcp://127.0.0.1:5555")

    max_outstanding = 10
    outstanding = 0
    pending_replies = []

    for i in range(1000):
        while outstanding >= max_outstanding:
            # Wait for a reply before sending more
            [reply] = await socket.recv_multipart()
            outstanding -= 1
            pending_replies.append(reply)

        await socket.send_multipart([f"request-{i}".encode()])
        outstanding += 1

    # Drain remaining replies
    for _ in range(outstanding):
        [reply] = await socket.recv_multipart()
        pending_replies.append(reply)
```

**Alternative: use asyncio.Queue as application-level buffer:**

```python
async def bounded_producer(queue, max_size=1000):
    """Queue messages internally, respect backpressure"""
    push = ctx.socket(zmq.PUSH)
    push.connect("tcp://127.0.0.1:5555")
    
    while True:
        msg = await queue.get()
        try:
            await push.send_multipart(msg)
        except zmq.Again:
            # HWM hit, put it back and wait
            await asyncio.sleep(0.01)
            await queue.put(msg)
        finally:
            queue.task_done()
```

**Key insight from libzmq:** For true flow control, use DEALER/ROUTER with reply-based acknowledgment, not PUSH/PULL with HWM.

## 5. HWM Configuration Best Practices

```python
socket = ctx.socket(zmq.PUSH)

## Set both send and receive HWM
socket.setsockopt(zmq.SNDHWM, 1000)  # 1000 messages max in send queue
socket.setsockopt(zmq.RCVHWM, 1000)  # 1000 messages max in recv queue

## Or use set_hwm() shorthand (sets both)
socket.setsockopt(zmq.SNDHWM, 1000)
socket.setsockopt(zmq.RCVHWM, 1000)
```

**Behavior per socket type when HWM is reached:**

| Socket | HWM Hit Behavior |
|--------|-----------------|
| PUSH | Blocks send until queue drains |
| PULL | Drops incoming messages |
| PUB | Drops messages for that subscriber |
| SUB | Drops messages for slow subscribers |
| DEALER | Blocks send (queues messages) |
| ROUTER | Drops messages (silent discard) |
| REQ | Blocks send |
| REP | Drops incoming |

**Critical:** HWM is per-peer. A PUSH socket connected to 3 PULL workers has 3 independent queues, each with HWM limit.

**HWM does NOT provide backpressure feedback to the sender** in most patterns. For PUSH/PULL, the sender won't know messages are being dropped. Use DEALER/ROUTER with application-level acknowledgment for reliable flow control.

**Recommended values:**
- Production: `SNDHWM=1000`, `RCVHWM=1000` (tune based on message rate and memory)
- Low-latency: `SNDHWM=100`, `RCVHWM=100`
- High-throughput: `SNDHWM=10000`, `RCVHWM=10000`

## 6. LINGER Settings for Clean Shutdown

```python
## Set LINGER at socket creation time (RECOMMENDED)
socket = ctx.socket(zmq.PUSH)
socket.setsockopt(zmq.LINGER, 1000)  # 1 second

## OR set at close time
socket.close(linger=1000)

## OR set on context destroy
ctx.destroy(linger=0)  # force discard all pending

## For instant shutdown (discard everything)
socket.setsockopt(zmq.LINGER, 0)
```

**LINGER values:**
- `-1` (default): Block indefinitely until all messages sent. `ctx.term()` will hang if socket has unsent messages.
- `0`: Discard all pending messages immediately on close.
- `>0`: Wait that many milliseconds, then discard remaining.

**Best practices:**
1. **Always set LINGER explicitly** on every socket. Never rely on defaults.
2. For non-critical messages: `LINGER=0` (discard on close)
3. For critical messages: `LINGER=1000-5000` (give time to flush)
4. Set LINGER at socket creation, not just at close
5. Close sockets before `ctx.term()`, not after

```python
## Clean shutdown pattern
async def shutdown(ctx, sockets):
    for sock in sockets:
        sock.setsockopt(zmq.LINGER, 1000)  # 1s grace period
        sock.close()
    ctx.term()  # safe to call now
```

**`zmq.Context.destroy(linger=N)`** sets LINGER on all open sockets before closing them. But sockets already closed won't be affected - set LINGER on those at close time.

## 7. IPC vs TCP Performance

Based on benchmark data from omq.rb and academic studies (2025-2026):

**Throughput (PUSH/PULL, msg/s):**

| Message Size | inproc | IPC | TCP |
|---|---|---|---|
| 8 B | 1.64M | 589k | 604k |
| 128 B | 1.70M | 550k | 558k |
| 2 KiB | 1.81M | 324k | 318k |
| 32 KiB | 1.83M | 63k | 56k |
| 128 KiB | 1.82M | 16k | 14k |

**Round-trip latency (REQ/REP):**

| Message Size | inproc | IPC | TCP |
|---|---|---|---|
| 8 B | 6.5 us | 36 us | 46 us |
| 128 B | 6.5 us | 37 us | 47 us |
| 2 KiB | 6.6 us | 42 us | 52 us |
| 32 KiB | 6.6 us | 60 us | 70 us |

**Key findings:**
- **inproc is 10-30x faster** than IPC/TCP (passes by reference, no kernel crossing)
- **IPC and TCP are nearly identical** for local communication on modern Linux
- IPC is Unix domain sockets, TCP goes through loopback
- For same-machine communication, TCP loopback is effectively as fast as IPC
- Use `inproc://` for same-process communication (threads)
- Use `ipc://` or `tcp://127.0.0.1` for cross-process on same machine
- Use `tcp://` for cross-machine

**When to use IPC over TCP:**
- Slightly lower latency for small messages (~15% difference)
- No TCP keepalive overhead
- Avoids TCP port exhaustion
- Better for Docker/container scenarios with shared filesystem

**When TCP is fine (and often preferred):**
- Near-identical performance on modern Linux
- Easier to configure and debug
- Works across network boundaries
- No filesystem permission issues

## 8. Heartbeat Patterns

### Native ZMTP Heartbeats (libzmq 4.2+)

```python
import zmq
from zmq.asyncio import Context

ctx = Context()

## Server
router = ctx.socket(zmq.ROUTER)
router.setsockopt(zmq.HEARTBEAT_IVL, 1000)  # send PING every 1s
router.setsockopt(zmq.HEARTBEAT_TIMEOUT, 5000)  # timeout after 5s
router.setsockopt(zmq.HEARTBEAT_TTL, 5000)  # peer TTL 5s
router.bind("tcp://*:5555")

## Client
dealer = ctx.socket(zmq.DEALER)
dealer.setsockopt(zmq.HEARTBEAT_IVL, 1000)
dealer.setsockopt(zmq.HEARTBEAT_TIMEOUT, 5000)
dealer.setsockopt(zmq.HEARTBEAT_TTL, 5000)
dealer.connect("tcp://127.0.0.1:5555")
```

**How it works:**
- `HEARTBEAT_IVL`: Interval in ms between PING commands
- `HEARTBEAT_TIMEOUT`: How long to wait after PING before declaring dead
- `HEARTBEAT_TTL`: Time-to-live for heartbeat (peer sets this, your side respects it)
- ZMTP PING/PONG are zero-payload protocol commands, not application messages
- On timeout, the connection is silently dropped - you get EAGAIN on next send/recv
- `zmq.EVENT_DISCONNECTED` can be monitored for ROUTER sockets to detect disconnects

### Application-Level Heartbeat (Custom)

```python
import asyncio
import time
import zmq
from zmq.asyncio import Context

ctx = Context()

HEARTBEAT_INTERVAL = 1.0  # seconds
HEARTBEAT_TIMEOUT = 3.0


async def heartbeat_sender(sock):
    """Send periodic heartbeat"""
    while True:
        await sock.send_multipart([b"HEARTBEAT", str(time.time()).encode()])
        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def heartbeat_monitor(sock, identity):
    """Monitor heartbeats from a peer"""
    last_seen = time.time()

    while True:
        try:
            msg = await asyncio.wait_for(
                sock.recv_multipart(), timeout=HEARTBEAT_TIMEOUT
            )
            last_seen = time.time()
            if msg[0] == b"HEARTBEAT":
                continue
            # Process actual message
            await handle_message(msg)
        except asyncio.TimeoutError:
            elapsed = time.time() - last_seen
            if elapsed > HEARTBEAT_TIMEOUT:
                print(f"Peer {identity} is dead!")
                break
```

**Paranoid Pirate pattern** (from ZMQ guide): Workers send "ready" messages. Broker tracks which workers have responded within timeout. Unresponsive workers are removed from the pool.

## 9. Write Queue Pattern (bounded deque + POLLOUT drain)

> **Current state:** the patterns below are reference designs. What ships
> is the bounded in-process fan-out hub for browser clients plus the XPUB
> socket for external subscribers with native best-effort drop semantics
> (documented in `docs/streaming.html`). See `markdown/LEAK-INVESTIGATION.md`
> for the measured verification.

The write queue pattern queues messages internally and drains when POLLOUT is available. This is exactly how ZMQStream works internally, and how bounded message brokers handle message flow.

### ZMQStream-based Write Queue

```python
from zmq.eventloop.zmqstream import ZMQStream
from tornado import ioloop


def create_write_queue(socket):
    """
    ZMQStream queues messages internally and drains when
    socket is writable (POLLOUT). This is the bounded write-queue pattern.
    """
    stream = ZMQStream(socket)
    return stream


## Usage
ctx = zmq.Context()
sock = ctx.socket(zmq.DEALER)
sock.connect("tcp://backend:5555")

## ZMQStream handles write queuing internally
stream = ZMQStream(sock)


async def push_message(data):
    # This queues the message; ZMQStream drains when POLLOUT
    stream.send_multipart(data)


stream.on_recv(lambda msg: print(f"Got: {msg}"))
```

### Manual Write Queue with POLLOUT (Pure asyncio)

```python
import asyncio
import zmq
from zmq.asyncio import Context

ctx = Context()


class WriteQueue:
    """Internal write queue that drains on POLLOUT"""

    def __init__(self, socket, max_size=10000):
        self.socket = socket
        self.queue = asyncio.Queue(maxsize=max_size)
        self._draining = False

    async def enqueue(self, msg_parts):
        """Queue a message for sending"""
        await self.queue.put(msg_parts)
        if not self._draining:
            asyncio.create_task(self._drain())

    async def _drain(self):
        """Drain queue when socket is writable"""
        self._draining = True
        try:
            while not self.queue.empty():
                # Poll for writability
                events = await self.socket.poll(timeout=1000, flags=zmq.POLLOUT)
                if events & zmq.POLLOUT:
                    try:
                        msg = self.queue.get_nowait()
                        await self.socket.send_multipart(msg)
                    except asyncio.QueueEmpty:
                        break
                else:
                    await asyncio.sleep(0.01)
        finally:
            self._draining = False


## Usage
sock = ctx.socket(zmq.PUSH)
sock.connect("tcp://worker:5555")
wq = WriteQueue(sock, max_size=5000)


async def producer():
    for i in range(100000):
        await wq.enqueue([f"msg-{i}".encode()])
```

### High-Performance Drain Loop (Poll + DONTWAIT)

```python
async def drain_loop(socket, queue, max_per_iter=100):
    """
    Max throughput pattern: poll for POLLOUT, then drain
    up to max_per_iter messages per iteration.
    """
    poller = zmq.asyncio.Poller()
    poller.register(socket, zmq.POLLOUT)
    
    while True:
        events = await poller.poll(timeout=100)
        
        if events and socket in dict(events):
            sent = 0
            while sent < max_per_iter:
                try:
                    msg = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                
                try:
                    socket.send_multipart(msg, zmq.DONTWAIT)
                    sent += 1
                except zmq.Again:
                    # Socket buffer full, re-queue and wait
                    await queue.put(msg)
                    break
        else:
            await asyncio.sleep(0.001)
```

### ZMQStream flush() for Priority Control

```python
from zmq.eventloop.zmqstream import ZMQStream
import zmq

stream = ZMQStream(socket)

## flush() pulls pending events off the queue
## Useful for priority ordering across multiple sockets
count = stream.flush(
    flag=zmq.POLLIN | zmq.POLLOUT,  # flush both directions
    limit=100,  # max events to process
)

## flush with recv-only
count = stream.flush(flag=zmq.POLLIN, limit=50)
```

## Summary of Key Patterns

| Pattern | Use Case | Reliability |
|---------|----------|-------------|
| PUSH/PULL | Pipeline, fan-out work | Drops on HWM |
| PUB/SUB | Broadcast, event streams | Drops slow subscribers |
| ROUTER/DEALER | Async request-reply | App-managed |
| REQ/REP | Simple sync RPC | Blocks on alternation |
| PAIR | 1-to-1 exclusive pipe | None |

**Production checklist:**
1. Always use `zmq.asyncio.Context()`
2. Set `LINGER` on every socket
3. Use ROUTER/DEALER over REQ/REP
4. Set HWM explicitly on both SND and RCV
5. Use native ZMTP heartbeats for connection monitoring
6. For backpressure: use DEALER/ROUTER with reply-based flow control
7. For same-process: use `inproc://`, for same-machine: `ipc://` or `tcp://127.0.0.1`
8. For write queues: use ZMQStream or manual POLLOUT-based drain loops

---

## SQLite Forum: Connection Pool Post 9e9b8627

## SQLite Forum: Simple Connection Pool for SQLite in Python
Source: https://sqlite.org/forum/forumpost/9e9b862726a23061cfa6b50314c8ce84fa0b6c692db7474980e607ea62522a85
Saved: 2026-09-06 (sqtseries project — pool design reference for src/sqtseries/engine/db.py)

## Key nuggets (verified applicable 2026-09-06)
- R. Binns (APSW author), post 2: queue.Queue FIFO is "the worst data structure"
  for pooling — use LIFO so the most-recently-returned (warmest page cache)
  connection is reused first. Create connections on demand, never all up front.
  Demand measurable benefit before adding pooling complexity.
- K. Medcalf, post 4: check-in hygiene = rollback outstanding txn + close all
  open cursors before pooling (apsw/mpsw: closecursors(True)).
- Medcalf, post 8: the real "fresh connection" cost is first use — file open +
  schema read+parse (grows with schema complexity), not the open() itself.
- Binns post 9: handing connections across threads requires check_same_thread=False
  (sqlite3 module level), one-to-one checkout keeps it safe.

## sqtseries verification (2026-09-06, Python 3.14.4)
- Abandoned partially-consumed SELECT: in_transaction == False, yet it BLOCKS
  PRAGMA wal_checkpoint(TRUNCATE) (busy=1). rollback() does NOT release it;
  BEGIN+ROLLBACK does NOT either. cursor.close() releases it deterministically.
  -> Connection wrapper tracks cursors and closes them at pool check-in.
- Benchmark: 25-table schema, 500 point queries: fresh conn 215.5 us/query vs
  pooled 10.0 us/query = 21.65x speedup.

## Medcalf's pool sketch (post 4, mpsw/APSW) — notable details
- poolsize floor of 5; Queue bounded; put_nowait -> close on Full
- on put(): rollback (twice, defensively), closecursors(True),
  temp_store PRAGMA cycling (defensive reset), close on queue.Full
- on get(): re-apply init list (pragmas, attaches, scripts) per checkout

---

## SQLite Atomicity & Accounting

## SQLite atomicity & ingest accounting (2026-09-08)

Sources: sqlite.org/atomiccommit.html; tenthousandmeters.com/blog/sqlite-concurrent-writes-and-database-is-locked-errors/

## Key facts verified
- A failed transaction rolls back ATOMICALLY: either all rows land or none.
  => counting a failed insert_many batch as wholly "dropped" is exact.
- busy_timeout does NOT apply to journal_mode changes (fresh-file WAL race,
  fixed earlier by applying busy_timeout first + retry).
- WAL mode: writers serialize via the write lock; sustained single-writer
  commits are safe; checkpoint pressure grows the WAL when readers hold
  old snapshots (bounded via AUTOCHECKPOINT + CheckpointManager).

## ZMQ HWM semantics (pyzmq-asyncio-research-2026.md §4-5)
- PUSH->PULL: sender BLOCKS at HWM = real backpressure, no silent loss.
  Loss only occurs if producer uses DONTWAIT/NOBLOCK and swallows EAGAIN
  (sqtseries client.py uses blocking send => accounted).
- ROUTER silently drops at HWM; PUB drops for slow subscribers.
- Accounting identity: recv == persisted + dropped + invalid.

---

## ZeroMQ Resilience Options (libzmq source study)

## ZeroMQ resilience options — verified against local libzmq source (2026-09-08)

Sources: ~/devcode/zone/zeromq/libzmq/src/{options.cpp,router.cpp,socket_base.cpp}

## Verified from source
1. RECONNECT_IVL_MAX (options.cpp:403): enables EXPONENTIAL reconnect
   backoff. Stock behavior = constant 100ms retry forever (hammers a down
   peer). Set RECONNECT_IVL=100 + RECONNECT_IVL_MAX=10000.
2. ROUTER mandatory (router.cpp:200-203): with _mandatory, send to unknown
   routing_id => errno=EHOSTUNREACH, return -1. WITHOUT it, the reply is
   SILENTLY DISCARDED. router_mandatory=True + counted EHOSTUNREACH =
   no silent loss on the reply path. router_handover=1 keeps reconnect
   resilience (re-announced identity takes over the stale pipe).
3. CONFLATE (socket_base.cpp:846-850): pipe hwms become -1 with conflate
   pipes => latest-wins, pipe holds at most 1 message. Correct for
   telemetry/stats sockets; WRONG for data paths (drops intermediate rows).
4. HWM behavior per socket type (research file §5): PUSH blocks (real
   backpressure), ROUTER silently drops, PUB drops for slow subscribers —
   hence CONFLATE+latest-wins is the right posture for PUB telemetry.

## Applied to sqtseries
- context.socket_options: RECONNECT_IVL=100, RECONNECT_IVL_MAX=10s (all sockets).
- QueryBroker: router_mandatory=True, replies_dropped counter (EHOSTUNREACH/Again).
- StatsPublisher: CONFLATE=1 (SNDHWM becomes moot; latest snapshot wins).

## POST-TEST CORRECTION (verified empirically, 2026-09-08)
- CONFLATE=1 on PUB **collapses multipart frames**: [topic, payload] arrives
  as ONE truncated frame (test_publish_events caught it). REVERTED in
  stats_publisher.py — SNDHWM=1000 remains the stalled-subscriber bound.
  CONFLATE is only safe on single-part telemetry sockets.

---

## ffmpeg-zmq Pub/Sub Lessons

## ffmpeg-zmq pub/sub lessons applicable to sqtseries (2026-09-08)

Sources: /home/iam/devcode/airbits/ffmpeg-zmq/{zmq_recv_ts_1.py,zmq_recv_ts_2.py,ZMQFF.md}

## Lessons extracted and applied
1. **Drain-before-close** (zmq_recv_ts_1.py: publisher close fires
   EVENT_DISCONNECTED only AFTER pending data reached the pipe; receiver
   drains the tail before finalizing). APPLIED: Service.shutdown() now
   drains ingress (bounded 10 sweeps) BEFORE ingress.stop() closes the
   socket — frames queued in the PULL are persisted, not silently dropped
   by close (libzmq discards undelivered messages on close).
2. **Slow-joiner determinism**: SUB only receives what is published AFTER
   its subscription arrives; PUB never replays. DOCUMENTED in
   dist/docs/clients.html (subscribe section) — late subscribers query()
   the missed window instead.
3. **Monitor events for peer lifecycle**: the receiver detects publisher
   close via ZMQ_EVENT_DISCONNECTED. NOT adopted server-side: XPUB
   subscription events already give exact subscriber tracking (registry).
4. **RECONNECT_IVL=1ms floor** for fast local rediscovery: kept our 100ms
   floor + 10s exponential max (server-side sockets; 1ms is a receiver
   spinning against a not-yet-started publisher).
5. **CONFLATE multipart caveat re-confirmed**: ffmpeg's PUB is
   single-frame-per-message; our stats PUB uses [topic, payload] multipart
   — CONFLATE would corrupt it (verified earlier by test).

---

## Deadlock Theory: Reader-Bound Systems

## Deadlock theory applied to the sqtseries reader-bound (2026-09)

Sources: Wikipedia "Deadlock (computer science)" + "Deadlock prevention
algorithms"; UIC CS course notes on hold-and-wait prevention; Python docs
threading.Semaphore (release is thread-safe; no owner tracking).

## Coffman conditions (all four required for deadlock)
1. Mutual exclusion — reader slots are exclusive.
2. Hold and wait — a thread holds a slot while waiting for another.
3. No preemption — a slot cannot be forcibly taken.
4. Circular wait — waiters form a cycle.

## What failed in the first design
`_ReentrantSemaphore` with a `threading.local()` depth counter:
- VIOLATED release symmetry: a @contextmanager generator's `finally` can run
  on a DIFFERENT thread than the acquire (GC finalization, anyio portal
  handoffs in TestClient). Cross-thread release corrupted the acquiring
  thread's counter (stayed elevated forever) → each such event permanently
  leaked one slot → after pool_size leaks, every acquire blocked forever.
  Observed: full-suite hang in test_gateway_edge (passed in isolation).
- Created hold-and-wait: same-thread nested checkouts were exempted from the
  bound, but cross-thread waits while holding a slot remained possible.

## The fix (prevention by breaking conditions 2+4)
- Plain threading.BoundedSemaphore(pool_size) around checkout→checkin.
- acquire(timeout=5s): on timeout open a TRANSIENT handle (small page cache)
  instead of waiting forever → no indefinite waiting → no circular wait →
  no deadlock. Queueing IS the intended backpressure (query.timeout_s=30s
  accommodates it).
- No per-thread state at all: Semaphore.release() is thread-safe by design,
  so cross-thread finalization cannot corrupt anything.
- Same-thread nested checkouts take a real slot (they genuinely need a second
  connection) or fall back to transient under contention.

---

---

## Status update (2026-09-11): percentile + engine rerun after the SQL-downsample fix

The §7 figures (700,184/700,184, 10,006 pts/s, 2026-09-10) are superseded
for current code. Reran the identical profile on 2026-09-11
(`scripts/run-stress-rig.sh --duration 30 --rate 10000 --clients 8
--warmup-rows 400000`): **700,223/700,223 ingested=persisted=db_rows, 0
dropped/invalid/unaccounted, sustained 10,007 pts/s, leak gate PASS**.
HTTP read/aggregate latencies roughly halved vs the §7 table (no more
10k-row fetch-then-413 churn); ZMQ p50 ~694 ms is single-worker-step
fan-in serialization under 12 tight clients, unchanged by design.
Engine benchmark re-measured the same day (200K/1M/2M/5M) — see
`dist/docs/benchmarks.md` for the fresh tables.
