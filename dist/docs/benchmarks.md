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

## How these percentiles are measured

Read this before quoting a number from the table below.

**The default load generator is closed-loop.** Each client sends its next
request only after the previous reply arrives. That is inherent to the wire
protocols, not a shortcut: a ZeroMQ `REQ` socket is lockstep and physically
cannot send again before it receives a reply.

The consequence is *coordinated omission*. When the service stalls, the clients
stall with it — the offered load drops exactly when the system is slowest — so
the recorded distribution is **optimistically biased in the tail**. These numbers
answer one question honestly:

> At saturation with N concurrent clients, what is the response time?

They are not an arrival-rate measurement, and they understate what a real stream
of independent arrivals would experience.

**The harness can also run open-loop, and the correction is large.** With
`--model open` the query clients pace requests on a fixed schedule over a
pipelining `DEALER` socket and measure each one from the time it was *due*
rather than the time it was sent, so queueing delay caused by a stall is counted
instead of hidden. Two figures come out: `zmq_query` (service time, from the
actual send) and `zmq_query_co` (corrected, from the scheduled fire time). The
measured open-loop figures are in [Open-loop arrival-rate
measurements](#open-loop-arrival-rate-measurements) below — the gap reaches
**6.8× at p50**.

The literature puts the general gap at up to two orders of magnitude in the
tail: correcting YCSB's closed-loop measurement moved p95 from 1 ms to 83 ms
and p99 from 19 ms to 210 ms on the same run. See Gil Tene, *How NOT to Measure
Latency*, and Friedrich et al., *Coordinated Omission in NoSQL Database
Benchmarking* (BTW 2017).

**Deadline-exceeded requests are recorded, not discarded.** A client that waits
5 s and gives up has still measured a ≥5 s response. Those samples stay in the
distribution and are also counted as errors, so the tail cannot be flattered by
throwing its worst samples away.

**Percentile method: Hyndman-Fan R7 ("inclusive"), the R/Excel/numpy default**,
computed via `statistics.quantiles`. There is no universal standard — NIST notes
Hyndman and Fan catalogued nine methods — so the choice is stated rather than
implied, and the report carries it in a `pct_method` field. (An earlier revision
used `sorted[n*p]`, which returns the *maximum* for p99 at n=100 and so reported
no tail at all; at the sample sizes in the table below the two agree to within
~3%.)

**The accounting identity is checked, not assumed.** `unaccounted` is only
reported when both the pump's delivered count and the server's counter were
actually read. If either is missing the run is marked `UNEVALUABLE`, and a
negative result is marked `UNSOUND` — neither is presented as a measurement.

## Live-service percentiles (P50/P90/P99)

Measured 2026-09-30 with `scripts/stress_percentiles.py` on the same
development machine (Intel Core Ultra 7 255U, 12 cores / 14 threads,
22 GiB RAM, 468 GB NVMe SSD): a real service on free ports, 400,000
warm-up rows **fully absorbed**, then a sustained **10,000 pts/s** ZMQ pump
for 30 s while 8 concurrent REQ query clients, HTTP read/aggregate/write
clients, an admin poller, and a WebSocket subscriber hammer every live path.

**Every point accounted for: 700,198 of 700,198 pumped points ingested,
persisted, and present in the database — 0 dropped, 0 invalid,
0 unaccounted — while the service sustained the full 10,006 pts/s offered
under query load.**

| Path | n | P50 | P90 | P95 | P99 | Max |
|---|---|---|---|---|---|---|
| ZMQ query (REQ) | 306 | 817 ms | 912 ms | 934 ms | 968 ms | 1014 ms |
| HTTP read | 569 | 84 ms | 109 ms | 117 ms | 167 ms | 208 ms |
| HTTP aggregate | 1064 | 37 ms | 78 ms | 86 ms | 113 ms | 178 ms |
| HTTP write | 148 | 154 ms | 191 ms | 209 ms | 237 ms | 244 ms |
| Admin (REQ) | 289 | 52 ms | 72 ms | 79 ms | 112 ms | 133 ms |
| WebSocket delivery | 11,715 | 0.057 ms | 0.146 ms | — | 103 ms | — |

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
- Sustained ingest (10,006 pts/s) matches the offered rate: bounded-queue
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

## Open-loop arrival-rate measurements

Measured 2026-10-01 with `scripts/run-stress-rig.sh --model open` on the same
development machine, 8 open-loop query clients against the broker's `ROUTER`,
with a 2,000 pts/s ingest pump running concurrently. Each request is issued on a
fixed schedule and timed from its **scheduled** fire time.

Two runs, one unsaturated and one saturated, because the interesting result is
the comparison between them:

| Run | Offered | Achieved | `zmq_query` p50 / p99 | `zmq_query_co` p50 / p99 | Correction |
|---|---|---|---|---|---|
| Unsaturated | 250 ops/s | **250.0 ops/s** | 30.59 / 138.27 ms | 30.73 / 138.40 ms | 1.00× |
| Saturated | 1000 ops/s | 367.0 ops/s | 1,434 / 2,591 ms | 9,780 / 29,957 ms | **6.82×** |

**Read the two rows against each other.** Unsaturated, the corrected and
uncorrected figures agree to within 0.5% — which is the check that the
correction is not simply inflating every sample by a constant. Saturated, the
same correction separates them by **6.82× at p50** and 11.6× at p99. That gap
is the queueing delay a closed-loop harness cannot see, because the closed-loop
client stops offering load exactly when the system slows down.

Accounting held on both runs, so the latency is not a side effect of lost data:

| Run | Ingest sent | Persisted | Dropped | Unaccounted | `sent_ops` | `zmq_query` n |
|---|---|---|---|---|---|---|
| Unsaturated | 100,620 | 100,620 | 0 | 0 | 10,001 | 10,001 |
| Saturated | 110,415 | 110,415 | 0 | 0 | 16,514 | 16,514 |

### What the saturated run does and does not claim

`client_shed` was 309,978 on the saturated run and 122 on the unsaturated one.
The generator hit its own in-flight cap, so it could not actually offer
1,000 ops/s — it offered 367. **That run therefore publishes a corrected
latency distribution, not a certified 1,000 ops/s arrival rate.** Quoting it as
"the server handles 1,000 ops/s" would be wrong; quoting the 9,780 ms p50 as the
corrected cost of a saturated arrival stream is right.

The report carries these integrity signals for exactly this reason, and they are
checked before any number above is quoted:

| Field | Meaning | Failure it catches |
|---|---|---|
| `sent_ops` | frames the broker accepted | **0 means the generator never fired.** A pacer that sleeps without sending still reports a perfect achieved rate — this was a real bug, found by this check. |
| `zmq_query` `n` | replies matched | must equal `sent_ops`; a shortfall means unanswered requests |
| `client_shed` | generator hit its own in-flight cap | non-zero means the offered rate was not achieved, so the run does not certify it |
| `unanswered_at_window_close` | still outstanding when the window closed | non-zero means the tail was truncated |

### The cap that is easy to get wrong

`--max-inflight` is the **cross-client total**, mirroring the broker's single
global `max_inflight` (`len(broker._router_tasks) >= max_inflight` in
`src/sqtseries/messaging/broker.py`) — one pool for the whole process, not a
per-connection quota. The default 64 splits to 8 per client at 8 clients.

Reading it per-client is the same mistake this harness originally made: it lets
8 × 64 = 512 requests pile up against a broker servicing 64, the broker sheds
most of them, and the report blames the generator for being slow. If you see
`queries_shed` climbing during a load test, the requests were refused at the
door, not slow inside.

## How to reproduce

> **Run it guarded.** Heavy runs (stress, memray, py-spy) go through the
> hard memory governor so a runaway run cannot take the machine down:
> `scripts/run-stress-rig.sh` wraps its whole process tree in
> `scripts/mem-guard.sh` by default (`--guard-budget-mb`, `--probe-budget-mb`,
> `--no-guard` to opt out). A breach SIGKILLs the guarded tree and exits 42 —
> by design. On this machine `/tmp` is tmpfs (RAM): keep big artifacts
> elsewhere or clean them promptly.

```bash
# closed loop (default) — response time at saturation with N clients
scripts/run-stress-rig.sh --duration 300 --rate 2000 --clients 8 --tag baseline

# open loop — a paced arrival stream, corrected for coordinated omission.
# --query-rate is the offered aggregate query rate across all clients;
# --max-inflight is the cross-client total (default 64 -> 8 each at 8 clients).
scripts/run-stress-rig.sh --duration 300 --rate 2000 --clients 8 \
    --model open --query-rate 250 --tag arrival

# the harness can also be driven directly (unguarded — prefer the rig above)
python3 scripts/stress_percentiles.py --duration 60 --rate 2000 --clients 8 \
    --model open --query-rate 250 --json /tmp/open.json
```

Read `sent_ops`, `zmq_query.n` and `client_shed` in the resulting
`stress.json` before quoting any latency from it — see
[What the saturated run does and does not claim](#what-the-saturated-run-does-and-does-not-claim).


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

Re-measured 2026-09-30 on the same machine and the same code, the 2M/100-series
row ran at 70,252 / 79,358 / 77,249 rows/sec across three runs — against the
96,966 that the same commit reaches when the database file sits on tmpfs
(`/tmp` is RAM here) instead of the project disk. Ingestion rate is therefore
sensitive to where the file lives and to page-cache state, not to code: an
A/B of the write path against `HEAD` on identical storage bracketed the
pre-change baseline rather than exceeding it. Treat the table above as the
2026-09-11 condition, and expect ±15% run-to-run.

Throughput is page-cache-bound up to ~1M rows; beyond that it becomes
WAL-checkpoint / disk-IO-bound. The 5M-row drop is real and reproducible on
the development machine (checkpoints start hitting the 64MB
`journal_size_limit`).
These are batched-engine numbers. The live service path drains ingest
frames in bursts (up to 1024 per tick) behind a bounded hand-off queue and
commits one transaction per batch: a sustained 10,000 pts/s pump for 30 s
delivered 700,198/700,198 points under concurrent query load — 0 dropped,
0 unaccounted (2026-09-30). Verify on your own hardware (results scale
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
- **The rollup fast path is near-constant** (p50 0.19–0.97ms regardless of how many
  points the series holds): it reads a handful of pre-aggregated hourly rows.
  It wins up to ~11.6x as soon as the raw scan dominates (2M rows at 20K
  points per series: 11.22ms → 0.97ms).
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