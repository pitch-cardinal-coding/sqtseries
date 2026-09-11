# sqtseries Benchmarks

A self-contained, reproducible experiment measuring the sqtseries engine and
the rollup fast path with `scripts/benchmark.py`.

> **Read this caveat first.** All numbers below are *engine-level*: the
> benchmark harness inserts via `store.insert_many(rows)` from
> `scripts/benchmark.py` — batched `executemany` calls inside a single
> `BEGIN IMMEDIATE` transaction. The live **service** write path also batches
> (the ingest socket is drained in bursts of up to 1024 frames and committed
> one transaction per batch through a bounded hand-off queue), but it adds
> wire protocol, validation, and concurrency on top, so service-path
> throughput is lower than the numbers here — see the closing note at the
> bottom of this page for the measured live-path figure. The numbers below
> were recorded on the project's development machine (2026-08-10) and are
> for orientation, not guarantees.

## Live-service percentiles (P50/P90/P99)

Measured 2026-09-11 with `scripts/stress_percentiles.py` on the same
development machine (Intel Core Ultra 7 255U, 12 cores / 14 threads,
22 GiB RAM, 468 GB NVMe SSD): a real service on free ports, 400,000
warm-up rows **fully absorbed**, then a sustained **10,000 pts/s** ZMQ pump
for 30 s while 8 concurrent REQ query clients, HTTP read/aggregate/write
clients, an admin poller, and a WebSocket subscriber hammer every live path.

**Every point accounted for: 700,223 of 700,223 pumped points ingested,
persisted, and present in the database — 0 dropped, 0 invalid,
0 unaccounted — while the service sustained the full 10,007 pts/s offered
under query load.**

| Path | n | P50 | P90 | P95 | P99 | Max |
|---|---|---|---|---|---|---|
| ZMQ query (REQ) | 377 | 694 ms | 833 ms | 871 ms | 918 ms | 947 ms |
| HTTP read | 628 | 71 ms | 107 ms | 125 ms | 164 ms | 197 ms |
| HTTP aggregate | 1196 | 30 ms | 66 ms | 74 ms | 94 ms | 116 ms |
| HTTP write | 173 | 124 ms | 166 ms | 185 ms | 208 ms | 226 ms |
| Admin (REQ) | 334 | 39 ms | 59 ms | 68 ms | 91 ms | 105 ms |
| WebSocket delivery | 12,248 | 0.05 ms | 0.13 ms | — | 91 ms | — |

Notes:
- "pts/s" = points per second — one point is one measurement (metric +
  value + tags + timestamp).
- **Allocator matters at this rate (measured 2026-09-10).** Under the same
  10-minute 10k pts/s run, glibc's malloc retained ~52 B/row of *freed*
  native memory in per-thread arenas (RSS +583 MB, ingest sagging to
  ~9,620 pts/s), while **jemalloc** (`LD_PRELOAD=libjemalloc.so.2`) held
  RSS growth to a bounded +22 MB and sustained the full 10,005 pts/s with
  `unaccounted: 0`. `scripts/run-stress-rig.sh` enables jemalloc
  automatically when present (`RIG_NO_JEMALLOC=1` to disable).
- **End-to-end accounting is exact, not sampled.** The pump counts a point
  only after `send()` returned with the socket writable (`POLL(0,
  POLLOUT)`), the warmup must be fully absorbed before the measurement
  window opens, and the accounting snapshot waits for the ingest drain to
  go quiet. The identity `server_ingested == persisted == db_rows` holds
  with zero dropped/invalid/unaccounted on every clean run.
- Sustained ingest (10,007 pts/s) matches the offered rate: bounded-queue
  backpressure (bounded ingress queue → PULL RCVHWM → pump blocks at its
  SNDHWM) absorbs query-load jitter without dropping or losing points.
- The ZMQ query figures are **end-to-end** REQ-client latency under
  sustained concurrent load: the broker dispatches concurrently (ROUTER),
  and what remains is handler time shared across 8 clients plus DB reader
  contention. A single client's p50 is tens of milliseconds.
- HTTP read/aggregate run the same engine under the same load; their
  distribution is tighter because `asyncio.to_thread` schedules them
  independently of the ZMQ step loop.
- WebSocket p50/p90 are sub-0.1 ms; the p99 tail is a handful of bursts
  while the subscriber's bounded fan-out queue briefly fills (drop-free,
  weight-bounded).
- The engine-level numbers below are **not** comparable to this table: they
  measure raw storage with no wire protocol, validation, or concurrency in
  the way.

## How to reproduce

> **Run it guarded.** Heavy runs (stress, memray, py-spy) go through the
> hard memory governor so a runaway run can never take the machine down:
> `scripts/run-stress-rig.sh` wraps its whole process tree in
> `scripts/mem-guard.sh` by default (`--guard-budget-mb`, `--probe-budget-mb`,
> `--no-guard` to opt out). A breach SIGKILLs the guarded tree and exits 42 —
> by design. On this machine `/tmp` is tmpfs (RAM): keep big artifacts
> elsewhere or clean them promptly.

```bash
# engine inserts + query latency, defaults: 200K rows, 1000 series,
# 24h span, batch 5000, 100 query runs
python3 scripts/benchmark.py --db /tmp/bench.sqlite --rows 200000

# the ~9x rollup win: fewer series, many points each
python3 scripts/benchmark.py --db /tmp/bench.sqlite --rows 2000000 --series 100

# explicit options
python3 scripts/benchmark.py --db /tmp/bench.sqlite --rows 5000000 \
    --batch 5000 --series 1000 --span-hours 24 --queries 50
```

The script prints an `inserts:` dict (rows/sec, seconds) and a `queries:`
dict (p50/p95 latencies for raw range, wide agg before rollup, and the same
wide agg after `rollup_new_hours`).

- **CPU:** Intel Core Ultra 7 255U (12 cores, 14 threads)
- **RAM:** 22 GiB
- **Disk:** 468 GB NVMe SSD
- **OS:** Linux (7.0 kernel; 6.x during earlier runs)
- **Database:** SQLite 3.46.1 (WAL mode)
- **Python:** 3.14.4

## Experiment design

Dataset: `--series` metric series (`bench.metric` tagged `host` + `id`),
each sampled at a fixed interval across a **24-hour span ending on a completed
hour boundary** (two hours in the past). Every measurement is therefore in a
completed hour and rollable. Ingestion uses `--batch`-row `executemany` calls
inside a `BEGIN IMMEDIATE` transaction (default batch 5,000) — this is the
engine path, not the service ingest path.

Three query workloads, all for a **single series** (the clustered
`PRIMARY KEY (series_id, timestamp_ns)` path):

| Query | What it does |
|-------|--------------|
| **range (1h)** | Raw points in a 1-hour window (raw path) |
| **wide agg (raw)** | 1-hour average buckets over the whole 24h span, **before** the rollup is built |
| **wide agg (rollup)** | The same query **after** `rollup_new_hours` builds the hourly rollup |

Latency is wall-clock time of one query, p50/p95 over the query runs (50 in the recorded runs).

## Engine ingestion throughput

| Rows | Rows/sec (recorded 2026-09-11) | Total time |
|------|--------------------:|-----------:|
| 200,000 | ~87,500 | 2.3s |
| 1,000,000 | ~71,500 | 14.0s |
| 2,000,000 (100 series) | ~100,700 | 19.9s |
| 5,000,000 | ~51,400 | 97.3s |

Throughput is page-cache-bound up to ~1M rows; beyond that it becomes
WAL-checkpoint / disk-IO-bound. The 5M-row drop is real and reproducible on
the development machine (checkpoints start hitting the 64MB
`journal_size_limit`).
These are batched-engine numbers. The live service path drains ingest
frames in bursts (up to 1024 per tick) behind a bounded hand-off queue and
commits one transaction per batch: a sustained 10,000 pts/s pump for 30 s
delivered 700,223/700,223 points under concurrent query load — 0 dropped,
0 unaccounted (2026-09-11). Verify on your own hardware (results scale
with CPU, RAM, and disk — see the development-machine spec above) with
`scripts/benchmark.py` and `scripts/stress_percentiles.py`.

## Query latency

p50/p95 over 50 runs, single series (the script default is 100 query runs).

| Dataset | Query | p50 | p95 |
|---------|-------|-----|-----|
| 200K (200 pts/series) | range (1h) | 0.03ms | 0.04ms |
| 200K (200 pts/series) | wide agg raw | 0.21ms | 0.23ms |
| 200K (200 pts/series) | wide agg rollup | 0.19ms | 0.25ms |
| 1M (1,000 pts/series) | range (1h) | 0.05ms | 0.06ms |
| 1M (1,000 pts/series) | wide agg raw | 0.65ms | 0.71ms |
| 1M (1,000 pts/series) | wide agg rollup | 0.31ms | 0.63ms |
| 2M (20,000 pts/series) | range (1h) | 0.57ms | 1.08ms |
| 2M (20,000 pts/series) | wide agg raw | 11.22ms | 13.52ms |
| 2M (20,000 pts/series) | wide agg rollup | 0.97ms | 1.23ms |
| 5M (5,000 pts/series) | range (1h) | 0.16ms | 0.18ms |
| 5M (5,000 pts/series) | wide agg raw | 2.74ms | 3.16ms |
| 5M (5,000 pts/series) | wide agg rollup | 0.39ms | 0.71ms |

## Reading the numbers

- **Single-series range queries are sub-millisecond** and grow slowly with
  points-per-series: the clustered PK index serves them directly.
- **The wide aggregate scales linearly with points-per-series when served from
  raw rows** (0.21ms → 2.74ms → 11.22ms as the series grows 200 → 5,000 → 20,000
  points). Raw `avg`/`sum`/`min`/`max`/`count` buckets are grouped inside
  SQLite (no row materialization); only `median`/`p95`/`p99`/`first`/`last`
  and gap filling stream rows to Python.
- **The rollup fast path is near-constant** (0.8–1.3ms regardless of how many
  points the series holds): it reads a handful of pre-aggregated hourly rows.
  It wins ~3.9–10x as soon as the raw scan dominates.
- **Crossover point:** the rollup carries a fixed overhead (~0.3–0.5ms) from
  its eligibility check + edge queries + merge. Below ~1,000 points per series
  over a multi-hour window, raw and rollup are within noise of each other
  (200K rows: 0.21ms vs 0.19ms); past ~1,000 points per series the rollup
  becomes a clear win at larger scale (11.6x at 20,000 points per series).
  Small or sub-hour windows never take the rollup path at all (no
  fully-inside hour to accelerate).

## Why the numbers look the way they do

- **Per-series queries use the clustered PK** (`series_id, timestamp_ns`,
  WITHOUT ROWID). `EXPLAIN QUERY PLAN` confirms `SEARCH … USING PRIMARY KEY`.
- **Where the speed comes from — read the same query twice:** the
  benchmark first measures the wide aggregate with the rollup empty (raw
  path), then builds the rollup with `rollup_new_hours` and re-measures.
  The 2M rows / 20,000-pts-per-series experiment is the headline: 11.22ms →
  0.97ms (~11.6x).
- **WAL reader/writer split**: readers never trigger checkpoints; the writer
  autocheckpoints every 10,000 pages (~80 MB at the 8 KiB page size), and the
  background CheckpointManager TRUNCATEs the WAL when it reaches 64 MB.
- **The rollup stores hourly `count,sum,min,max`** (`rollup_hourly`,
  WITHOUT ROWID, PK `(series_id, hour_start_ns)`). Wide-window
  `avg/sum/min/max/count` queries that span ≥ 2 completed hours are served
  from it; `median/p95/p99`, non-whole-hour intervals, and windows whose
  end hour is beyond the rollup watermark fall back to the raw path. Only
  completed hours are ever rolled (a watermark in `rollup_meta` tracks
  progress), and the in-progress hour's partial data is always read from
  raw and merged — so rollup results are exact, never approximate.

## See also

- [Architecture](architecture.html) — write path, rollup design, pragmas
- [Queries](queries.html) — eligibility rules for the rollup fast path