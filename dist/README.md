# sqtseries

A time-series database that runs on your machine, keeps everything in one file,
and answers questions about the past in milliseconds — no matter how many
millions of readings you have stored.

**8,413 lines** of Python · 58 classes · 385 functions · **766 tests passing**
· `pip install`, no compiler, no separate server, Python 3.14+.

## What it does

You feed it numbers as they happen — CPU load, room temperature, website
visitors, stock prices, energy meters, anything that changes over time. Each
reading gets a name, a value, and optionally some labels (tags). Later you ask
questions:

- *What was the average CPU load over the last hour?*
- *What was the peak temperature yesterday?*
- *Show me the 99th percentile response time for the past week.*
- *How many measurements did I record today?*

The answers come back in under a millisecond for recent data, and stay
near-constant even for queries spanning months. It does this by maintaining a
background summary of every hour, so a year-wide query reads a few dozen rows
instead of scanning millions.

## Why it stands out

**It respects your time.** `pip install sqtseries` and `sqtseries run` — two
commands and you are storing data. No server to provision, no schema to
design, no query language to learn. Timestamps accept epoch numbers or
everyday ISO-8601 strings (`2026-09-11T14:04:00Z`), and every CLI command
tells you where the dashboard, the docs, and the health check live.

**It is honest software.** Every number in these docs was measured on a real
machine and is reproducible with the scripts in the repo. Throughput,
latencies, and accounting identities (every pumped point lands in exactly one
of *persisted*, *dropped*, or *invalid*) are verified by the test suite, not
asserted. When the hourly summary answers instead of raw rows, the answer is
exact — never an approximation.

**It never loses your data.** SQLite's write-ahead log makes every write
crash-atomic. Ingestion is a bounded pipeline with real backpressure: bursts
are drained and committed one transaction per batch, slow subscribers are
counted instead of silently dropped, and nothing vanishes without being
tallied somewhere you can read.

**It tells you what it is doing.** A live admin dashboard streams health,
rates, connections, topics, and storage second by second. Every WebSocket
client and every subscriber is tracked exactly — connect, disconnect, and
subscription events flow to a stats socket you can watch, and the Topics
panel remembers every topic ever seen (with live counts, zero when idle)
alongside per-topic totals. Repeated questions are served from a short-lived
cache. Old data is retired automatically by retention, and the service runs
for years as a hardened systemd unit you install once and forget.

**It meets you where you are.** Python, Go, Rust, PHP, Node.js, shell
scripts, browsers — if your language speaks JSON over ZeroMQ or HTTP, it
speaks to sqtseries. No special client libraries, no IDL (Interface Definition Language), 
no codegen. One wire format, two transports, five languages with copy-paste samples 
in the docs.

## Speak to it from any language

| Language | How | What you get |
|----------|-----|--------------|
| Python | Bundled `Client` (ZeroMQ) or embedded engine (in-process, no server) | Write, query, aggregate, subscribe, admin |
| Go | ZeroMQ PUSH/REQ/SUB (`pebbe/zmq4`) | Same wire JSON as Python |
| Rust | ZeroMQ (`zmq` + `serde_json` crates) | Same wire JSON as Python |
| PHP | HTTP + curl (no ZMQ extension needed) | Write, read, aggregate |
| Node.js / browser JS | `fetch` + WebSocket | Write, read, live subscribe |
| Shell | `curl` | One-liners for writes, reads, health |

Timestamps accept epoch seconds or ISO-8601 (`2026-09-11T00:28:51.740Z`,
offsets and naive-as-UTC included, up to nanosecond precision). See
[Client Libraries](docs/clients.html) for complete samples in each
language.

## Use cases

This is a small sample. For complete walkthroughs with code, see the
[examples](docs/examples.html) page.

| Scenario | How sqtseries helps | Docs |
|----------|-------------------|------|
| Server monitoring | Stream CPU/memory/disk from every machine, query per-host averages and 99th-percentile peaks. Set up alerts by polling the stats socket. | [ingestion](docs/ingestion.html), [queries](docs/queries.html) |
| IoT sensor logging | Each sensor writes its readings with tags (`sensor=bathroom,floor=2`). Query any sensor over any time window in under a millisecond. | [ingestion](docs/ingestion.html), [queries](docs/queries.html) |
| Real-time dashboard | A browser connects via WebSocket and sees measurements as they arrive. The dashboard also subscribes to the stats socket to show current viewer count. | [streaming](docs/streaming.html), [admin](docs/api.html#admin) |
| Financial tick data | Store every trade with nanosecond timestamps. Answer "what was the volume-weighted average price this minute?" with a single query. | [queries](docs/queries.html) |
| Home energy tracking | Log wattage every few seconds. Show daily summaries. Drop data older than a year automatically via retention. | [configuration](docs/configuration.html) |
| Fleet telemetry | Every vehicle sends GPS + speed. Monitor live positions on a map (WebSocket). Query historical routes (HTTP). Check how many vehicles are reporting right now (admin). | [streaming](docs/streaming.html), [queries](docs/queries.html) |

## How to start

```bash
pip install sqtseries
sqtseries run
```

That is it. The service prints where everything lives — dashboard, docs,
health check — creates its database at `~/.sqtseries/data/db.sqlite`, and
listens on six local ports. Now send a reading and read it back:

```python
from sqtseries import Client

c = Client()
c.write("temp.outside", 22.5, {"sensor": "garden"})

rows = c.query("temp.outside", aggregation="avg", interval="1h")
print(rows)
```

Same thing from a shell script using `curl`:

```bash
curl -X POST http://127.0.0.1:12505/api/v1/write \
  -H "Content-Type: application/json" \
  -d '{"metric":"temp.outside","value":22.5,"tags":{"sensor":"garden"}}'
curl "http://127.0.0.1:12505/api/v1/read?metric=temp.outside"
```

Prefer readable timestamps? Pass one instead of a number:

```bash
curl -X POST http://127.0.0.1:12505/api/v1/write \
  -H "Content-Type: application/json" \
  -d '{"metric":"temp.outside","value":22.5,"timestamp":"2026-09-11T14:04:00Z"}'
```

The [Quick Start](docs/quickstart.html) walks through the full cycle:
install, configure, send data, query, subscribe live, check status, and run as
a background service.

## Design highlights

**Single-file storage.** The database is an ordinary SQLite file in WAL mode
with monthly partitions. Back it up with `sqtseries backup`, or open it with
`sqlite3` to run your own queries. It never grows unbounded: old partitions
are dropped automatically when they pass the retention TTL (default 30 days).

**Hourly summaries so wide queries stay fast.** Every hour, a background task
pre-aggregates completed hours into a `rollup_hourly` table (count, sum, min,
max per series). A query that would scan 20,000 raw data points instead reads
a few rollup rows and merges the edges — same exact answer, about 11× faster.
The current hour is always read live so results are never stale.

**Two transports, same wire format.** ZeroMQ handles high-throughput ingestion
and live streaming (ingest frames are batch-drained behind a bounded queue
and committed one transaction per batch — a sustained 10,000 pts/s pump
persisted 700,223 of 700,223 points under concurrent query load, 0 dropped,
0 unaccounted). HTTP handles
one-off scripts, dashboards, and languages without ZMQ bindings. Both speak
the same JSON shapes. Choose whichever fits, or use both.

**Measured, not claimed.** Every live path is stress-tested for latency and
correctness — under a 10,000 pts/s pump with 8 concurrent query clients,
WebSocket delivery runs at p50 0.05 ms and every HTTP/ZMQ path answers with
zero errors. Full tables: [benchmarks](docs/benchmarks.md).
pts/s = points per second; one point is one measurement (metric + value +
tags + timestamp).

**Query result caching.** Identical queries within a 5-second TTL are served
from an LRU cache (512 entries) instead of hitting SQLite again. Critical for
dashboards that poll the same metrics every few seconds.

**Connection health tracking.** Every WebSocket frame resets the activity
clock (`touch_ws`). A stale-connection sweep (`sweep_stale`) returns
connections that haven't sent data within the configured timeout, letting you
detect and evict dead clients before they leak resources.

**You always know who is connected.** The streaming socket uses XPUB, which
means the service receives subscribe and unsubscribe events directly from the
wire — no polling. WebSocket clients are tracked from accept to disconnect.
Admin commands return exact counts instantly. A separate stats PUB socket
streams connect, disconnect, and subscription events to anything that
subscribes.

**Crash-safe by default.** SQLite's WAL journal makes every write atomic. On
startup the service runs an integrity check and recovers any uncommitted WAL
frames. Shutdown checkpoints the WAL so the next start is fast. Backups via
`VACUUM INTO` are consistent snapshots even while writes are in flight.

**Hardened systemd unit.** `sqtseries install` generates a systemd service
with `NoNewPrivileges`, `ProtectSystem=strict`, and `ProtectHome=read-only`.
Only the data directory is writable. The service restarts automatically on
failure with a 5-second backoff. Install with a config to keep your chosen
ports: `sqtseries --config config.toml install` — the unit embeds
`--config` so the service boots exactly the way you configured it.

**Auto-detecting ingest port.** With `ports.auto_detect` on (the default),
the ingest port is always picked as the first free port in 12500–12700 —
usually 12500, not the configured 12501. The configured 12501 is only used
when `auto_detect = false`. The other ports (query/stream/admin/http/stats)
are fixed at their configured values. The chosen ports are written to a
runtime file next to the database, so `sqtseries status` always shows the
real ports.

## Quick reference

### Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 12501 | ZMQ PULL | Ingest measurements |
| 12502 | ZMQ REP | Run queries |
| 12503 | ZMQ XPUB | Live streaming + subscriber tracking |
| 12504 | ZMQ REP | Admin commands (health, stats, connections, backup) |
| 12505 | HTTP + WebSocket | REST API and browser streaming |
| 12506 | ZMQ PUB | Connection and subscription events |

All ports bind to `127.0.0.1`. See [ports](docs/index.html) for the full
auto-detection mechanism.

### CLI commands at a glance

```
sqtseries run          start the service (prints dashboard/docs/health links)
sqtseries stop         stop the running service
sqtseries status       pid, ports, database path + links
sqtseries ports        active ports + links
sqtseries health       database integrity check
sqtseries stats        metric and series counts
sqtseries backup       consistent snapshot (VACUUM INTO)
sqtseries optimize     refresh query-planner statistics
sqtseries vacuum       shrink the database file
sqtseries install      set up systemd unit
sqtseries uninstall    remove systemd unit
```

All commands accept `--config PATH` (TOML/YAML/JSON) and `--db PATH`
(database file override). Serve your own ports from a config file:

```bash
# config.toml: pick any ports you like
[ingestion]
port = 14001
# ... query, streaming, admin, http, stats ...

sqtseries --config config.toml run
python3 -m sqtseries --config config.toml run   # identical form
sqtseries --config config.toml install           # systemd runs the same config
```

Note: `--config` and `--db` are global flags and must come before the
subcommand (`sqtseries --config c.toml run`), not after.

`run` refuses to start over a database another live instance is serving,
and `stop` refuses to touch an instance whose database differs from the
one it resolves — see [Configuration](docs/configuration.html).

### Admin commands (over the wire)

Send `{"cmd": "..."}` to port 12504 (ZMQ REP). Available commands: `ping`,
`health`, `stats`, `connections`, `conncheck`, `subscribers`, `optimize`,
`backup`, `vacuum`. The Python Client wraps these:

```python
c.admin("stats")  # full service counters
c.admin("connections")  # active WebSocket clients
c.admin("conncheck", ids=["abc", "xyz"])  # which ids are connected
c.admin("subscribers")  # per-topic subscriber counts
```

Full details on every admin command are in the [API reference](docs/api.html#admin).

## Documentation

| Page | What it covers |
|------|---------------|
| [Quick Start](docs/quickstart.html) | End-to-end: install, send, query, subscribe, systemd |
| [Configuration](docs/configuration.html) | Every setting, env vars, config file format, duration syntax |
| [Ingestion](docs/ingestion.html) | Writing data via ZMQ, HTTP, and the Client; tags (dimensions — why and how); timestamps and the clock-skew guard |
| [Queries](docs/queries.html) | Time ranges, aggregations, intervals, downsampling, gap filling, per-tag queries (embedded API) |
| [Streaming](docs/streaming.html) | Live data via ZMQ SUB, WebSocket, and the Client |
| [Dashboard](docs/dashboard.html) | Live admin page: health, rates, connections, topics, storage |
| [Camera](docs/camera.html) | Pump a camera metrics feed into sqtseries, watch it on a live WebSocket dashboard, and answer 17 questions about it |
| [Client Libraries](docs/clients.html) | Code samples for Python, Go, Rust, PHP, and Node.js |
| [API Reference](docs/api.html) | Embedded Python API, Client, CLI, wire protocol, admin commands |
| [Architecture](docs/architecture.html) | Engine layout, schema, write path, rollup design, error handling |
| [Backup & Restore](docs/backup.html) | What to back up, restore procedure, and how vacuum works |
| [Benchmarks](docs/benchmarks.md) | Reproducible performance numbers from the development machine (Intel Core Ultra 7 255U, 22 GiB RAM, NVMe SSD) |
| [Systemd](docs/systemd.html) | Running as a service, hardening, dedicated user setup |
| [Examples](docs/examples.html) | Real-world scenarios with full code walkthroughs |
| [Example scripts](examples/) | Standalone runnable scripts for every operation |
