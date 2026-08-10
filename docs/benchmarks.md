# sqtseries Benchmarks

A self-contained, reproducible experiment measuring the sqtseries engine and
the rollup fast path with `scripts/benchmark.py`.

> **Read this caveat first.** All numbers below are *engine-level*: the
> benchmark harness inserts via `store.insert_many(rows)` from
> `scripts/benchmark.py` — batched `executemany` calls inside a single
> `BEGIN IMMEDIATE` transaction. The live **service** write path is different:
> each ingesting frame is validated and persisted as its own one-row
> transaction, with no app-level batching (`database.batch_size` applies only
> to the engine/benchmark path and load tooling). Service-path throughput will
> therefore be lower than the numbers here. The numbers below were recorded on
> one workstation (2026-08-10) and are for orientation, not guarantees.

## How to reproduce

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

| Rows | Rows/sec (recorded) | Total time |
|------|--------------------:|-----------:|
| 200,000 | ~196,000 | 1.0s |
| 1,000,000 | ~130,000 | 7.7s |
| 2,000,000 (100 series) | ~165,000 | 12.1s |
| 5,000,000 | ~64,000 | 77.6s |

Throughput is page-cache-bound up to ~1M rows; beyond that it becomes
WAL-checkpoint / disk-IO-bound. The 5M-row drop is real and reproducible on
this machine (checkpoints start hitting the 64MB `journal_size_limit`).
These are batched-engine numbers; the on-wire service path (one insert
transaction per message) sustains fewer rows/sec — measure it with the
benchmark tool's engine insert and scale expectations accordingly.

## Query latency

p50/p95 over 50 runs, single series (the script default is 100 query runs).

| Dataset | Query | p50 | p95 |
|---------|-------|-----|-----|
| 200K (200 pts/series) | range (1h) | 0.15ms | 0.24ms |
| 200K (200 pts/series) | wide agg raw | 0.51ms | 1.11ms |
| 200K (200 pts/series) | wide agg rollup | 0.79ms | 1.24ms |
| 1M (1,000 pts/series) | range (1h) | 0.18ms | 0.37ms |
| 1M (1,000 pts/series) | wide agg raw | 1.07ms | 1.61ms |
| 1M (1,000 pts/series) | wide agg rollup | 0.89ms | 1.17ms |
| 2M (20,000 pts/series) | range (1h) | 0.44ms | 0.51ms |
| 2M (20,000 pts/series) | wide agg raw | 13.28ms | 15.09ms |
| 2M (20,000 pts/series) | wide agg rollup | 1.32ms | 2.26ms |
| 5M (5,000 pts/series) | range (1h) | 0.21ms | 0.34ms |
| 5M (5,000 pts/series) | wide agg raw | 3.29ms | 3.51ms |
| 5M (5,000 pts/series) | wide agg rollup | 0.85ms | 1.08ms |

## Reading the numbers

- **Single-series range queries are sub-millisecond** and grow slowly with
  points-per-series: the clustered PK index serves them directly.
- **The wide aggregate scales linearly with points-per-series when served from
  raw rows** (0.51ms → 3.29ms → 13.28ms as the series grows 200 → 5,000 → 20,000
  points), because every raw point is streamed to Python and bucketed.
- **The rollup fast path is near-constant** (0.8–1.3ms regardless of how many
  points the series holds): it reads a handful of pre-aggregated hourly rows.
  It wins ~3.9–10x as soon as the raw scan dominates.
- **Crossover point:** the rollup carries a fixed overhead (~0.3–0.5ms) from
  its eligibility check + edge queries + merge. Below ~1,000 points per series
  over a multi-hour window, that overhead exceeds the raw scan cost and the
  rollup path is *slower* (200K rows: 0.6x). At parity around 1,000 points
  per series, it becomes a clear win at larger scale. Small or sub-hour
  windows never take the rollup path at all (no fully-inside hour to
  accelerate).

## Why the numbers look the way they do

- **Per-series queries use the clustered PK** (`series_id, timestamp_ns`,
  WITHOUT ROWID). `EXPLAIN QUERY PLAN` confirms `SEARCH … USING PRIMARY KEY`.
- **Where the speed comes from — read the same query twice:** the
  benchmark first measures the wide aggregate with the rollup empty (raw
  path), then builds the rollup with `rollup_new_hours` and re-measures.
  The 2M rows / 20,000-pts-per-series experiment is the headline: 13.28ms →
  1.32ms (~10x).
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