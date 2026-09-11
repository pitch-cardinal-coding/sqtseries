# Memory investigations — what broke, what we proved, what is fixed

**Status:** two separate incidents found, both fixed, both verified by measurement.
**Last verified:** 2026-09-11 (10-minute extreme run: PASS, 0 points lost).
**Rule for this file:** every claim below comes from a run, not from reasoning.
Numbers that newer runs supersede are marked as history.

---

## In one paragraph

The app once grew memory without bound in two different ways. First, live
browser subscribers made native memory climb ~25 MB/s (fixed 2026-09-09 with
an in-process bounded fan-out hub). Second, every aggregate question copied
up to 10,000 database rows into memory and then threw them away with an
error — hundreds of times per second under load, the allocator kept GBs of
that trash (fixed 2026-09-11 by answering inside SQLite). What remains is
normal, bounded allocator behavior. Production runs under jemalloc plus a
memory guard, so the PC is safe either way.

---

## Part 1: the WebSocket leak (2026-09-09) — FIXED, still fixed

**What you would have seen:** with one live browser subscriber attached at
full publish rate, server memory climbed ~25 MB every second (~1 GB in
80 seconds). Without subscribers, memory stayed flat forever.

**What it was:** each delivered message left one ~16 KB native chunk behind
on the internal message hop between the publisher and the browser forwarder
(libzmq queue blocks, 256 × 64 bytes + 8 — the measured size matched exactly).
Python's own memory was flat, so no Python code was at fault; 19 experiments
narrowed it to that one hop (details condensed below).

**The fix (bounded-queue doctrine):** stop fanning out through message pipes at
all. `src/sqtseries/messaging/fanout.py` delivers in-process, with one
small byte-budgeted queue per subscriber (256 KiB). A slow subscriber loses
new messages loudly (counted in `/admin/stats` as `fanout_dropped`) instead
of holding the system hostage. The external broadcast socket now serves only
external subscribers.

**Proof (same 50-second harness that climbed 97 → 662 MB before):**

| | Before | After |
|---|---|---|
| Memory over 50 s with live subscriber | 97 → **662 MB**, climbing | 90 → 97 → **92 MB** (rises, returns) |
| Native in-use memory | → **991 MB** | **flat, 17–25 MB** |

Full suite at the time: 733 passed, 0 failed.

**Experiment ladder (condensed — each row is a run, none assumed):**

| # | Experiment | Result |
|---|---|---|
| 1 | Python heap trace on the live service | flat (~45 MB) — our code retains nothing |
| 2 | Native in-use memory | grows linearly 120 MB → 3,945 MB — genuinely live native memory |
| 3 | Native profiler, high-water analysis | 5 page-aligned allocations, 934 MB, on a thread that never runs Python → message library I/O thread |
| 4 | Pump only, no clients, 390 s | flat at 70–81 MB — ingest path clean |
| 5 | Pump + readers, no subscriber | flat (82 → 89 MB) |
| 6 | Publishing disabled, subscriber attached | flat — the trigger is exactly the publish→deliver step |
| 7–9 | Raw subscriber / fake forwarder / real browser client | flat, flat, **climbs** — retention is server-side, needs the real transport |
| 10 | Message compression on vs off | climbs both ways — compression exonerated |
| 11–14 | Minimal reproductions, socket-option bisection | the drop path and every socket option exonerated |
| 15–16 | Bare browser push / backpressure combos | flat — no single component leaks alone |
| 17–19 | Allocation attribution on the real service | ~700 MB across 38,228 native allocations ≈ 38,926 delivered frames → **one ~16 KB chunk retained per delivered message** |

Ruled out along the way: Python heap, arena fragmentation tunables,
SQLite caches, compression, the drop path, the async receive path,
every socket option.

A side fix from the same work: the topic `stress.m1` also matched
`stress.m10`–`stress.m19` (~7× traffic). Matching is now dot-boundary
aware. A second side fix: the result-size cap fetched one row too many
when the caller asked for `limit=1` on an indexed query — now O(1) again.

---

## Part 2: the query churn (2026-09-11) — FIXED, verified

**What you would have seen:** the 10-minute extreme run
(10,000 pts/s, 12 query clients) died after ~2 minutes at 1,553 MB.
The memory guard killed it; without the guard the PC would have frozen.

**What it was:** every aggregate question ("average per minute over the
last hour") copied up to **10,000 raw rows** out of SQLite into Python —
and then threw them away with a "too many rows" error, because the table
was bigger than the cap. Hundreds of such doomed copies per second
churned ~100 MB/s of temporary objects. The app freed all of it, but the
memory manager kept GBs of that trash under continuous churn.

**How we named it (instruments, in order):**

1. Baseline at 2,000 pts/s also failed the leak gate (+32 MB) with perfect
   accounting (260,622/260,622 points, 0 lost) — so the growth was per-point
   trash, not lost data.
2. Query-clients-only against a filled database: flat. Pump-only: small
   steady slope. The killer needed both — interaction, not one path.
3. In-process run with live introspection: Python heap flat-to-shrinking
   while anonymous memory exploded — native side, app frees everything.
4. Same run under jemalloc: growth stopped and reversed (278 → 211 MB) —
   allocator retention, confirmed by A/B.
5. Allocation diffs pointed at per-frame parse objects held by the bounded
   ingest queue (by design, capped) — exonerated as the growth source.

**The fix:**

- `StorageEngine.downsample_sqlite()`: per-bucket COUNT/SUM/MIN/MAX grouped
  **inside SQLite** — Python sees one small partial per bucket, never rows.
- Whole-window `aggregate()` keeps its exact "too many rows" contract via a
  cheap `COUNT(*)` pre-check: same error, zero trash.
- `QueryBroker` caps concurrent dispatches (`max_inflight = 64`): excess
  requests wait in the bounded socket pipe, counted loudly (`shed_total`),
  instead of piling tasks without bound.
- Raw reads, `median`/`p95`/`p99`/`first`/`last`, and gap filling keep the
  old row path with its cap — unchanged contracts.

**One deliberate non-fix, measured:** serving broker requests 8-at-a-time
per step cut query latency but let tight-loop clients complete faster,
which multiplied churn until 1.3 GB at 300 s. Reverted the same day; the
one-request-per-step serialization is load-bearing for memory. Tight-loop
clients wait longer; real clients (second-scale polling) see millisecond
answers through the new SQL path.

**Proof (final verification, 2026-09-11):**

| Run | Result |
|---|---|
| 30 s at 10,000 pts/s, 8 clients (benchmark profile) | **700,223/700,223, 0 lost, sustained 10,007/s, leak gate PASS** |
| 600 s at 10,000 pts/s, 12 clients + robot browser on dashboard | **4,624,428/4,624,428 stored, 0 lost, leak gate PASS, browser verdict clean** |
| Pump only, 90 s at 10,000 pts/s | 901,500/901,500, memory flat at 70 MB |
| Full test suite | **745 passed, 0 failed** |

---

## Part 3: the memory manager (why production preloads jemalloc)

Plain version: your program frees memory; the default manager is slow to
hand it back to the system under continuous heavy work, so reported memory
keeps climbing even though nothing is leaked. jemalloc hands it back
promptly. Measured A/B on the same 10-minute run: default manager +583 MB
and slowing down; jemalloc +22 MB bounded at full speed.

This is **not in the source code**. It is one startup setting:

```ini
Environment=LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
```

`sqtseries install` writes that line into the systemd unit automatically
when the machine has jemalloc (`sudo apt install libjemalloc2`), and
leaves it out otherwise — the service runs correctly either way. The
stress rig does the same automatically. Verified on production 2026-09-11:
unit line present, process environment confirmed, library mapped in
memory, 118,000-point soak with an exact aggregate answer.

On top of that, every heavy run goes through `scripts/mem-guard.sh`: if
any verification ever outgrows its budget again, the guard kills the run
instead of the PC (this guard is what caught both incidents above).

---

## The bounded-queue rules this codebase follows

(Bounds used: HWM 101000 on the ingest pipe, per-route weight accounting,
loud drops on every path.)

1. **Bound memory, not message counts** — byte budgets (fan-out queues,
   cache row-weight, bucket caps), HWM 101000 on the ingest pipe.
2. **At the bound: drop deliberately and loudly** — newest dropped, always
   counted (`fanout_dropped`, `shed_total`, `replies_dropped`, `dropped`).
3. **Never rely on transport-internal buffering for fan-out** — delivery is
   in-process; message pipes stay shallow.
4. **Backpressure, never silent loss** — full queues make senders wait
   (blocking sends, bounded pipes); `ingested == persisted == db_rows`
   holds on every clean run.

## Re-running the proof

```bash
# 30 s benchmark profile (percentiles + accounting + leak verdict)
scripts/run-stress-rig.sh --duration 30 --rate 10000 --clients 8 \
    --warmup-rows 400000 --tag <today>

# 10 min extreme + robot browser on the dashboard
scripts/run-stress-rig.sh --duration 600 --rate 10000 --clients 12 \
    --http-port 12599 --probe --probe-duration 620 --tag <today>
```

Results land in `/tmp/sqtseries-rig-<tag>/`: `stress.json` (percentiles +
accounting), `verdict.txt` (PASS/FAIL), `probe.log` + `probe-shots/`
(dashboard evidence). Exit 0 means everything passed.

---

## Two remaining limits (plain terms, and whether to worry)

**Limit 1: slow answers when 12 bullies shout non-stop.**
In the test, 12 clients asked questions in a tight loop with zero pause —
like 12 people shouting at one clerk who is also shelving books. Each
answer took ~1.6 seconds, mostly spent waiting in line. Real users never do
this: dashboards ask every few seconds and get answers in ~23 ms
(measured). **Verdict: not a concern.** Nothing breaks, nothing is lost;
rude clients just wait longer. Serving them faster was tried and measured
to explode memory use (1.3 GB at 300 s) — so the waiting line stays on
purpose.

**Limit 2: ~7,600 points/s with full query load, ~10,000 without.**
There is only one pen writing in the ledger (SQLite has a single writer).
While 12 people keep asking it questions, it writes a bit slower. Extra
work waits in line instead of crashing anything. **Verdict: not a concern
unless you need more than ~7,600/s sustained WITH heavy simultaneous
queries.** Ingest-only does the full 10,000/s. Hundreds of thousands per
second on one file on one machine is impossible no matter what — that
needs multiple machines (sharding), which is future work, not a bug.
