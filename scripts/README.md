# scripts/ — tooling for sqtseries

Reusable operational tooling. Everything here is self-contained and safe to
run against a dev service. `make style` covers the Python scripts.

## The one-command rig

### `run-stress-rig.sh`

Chains every verification tool below into a single instrumented run:

```bash
# 5-min baseline run with leak verdict
scripts/run-stress-rig.sh --duration 300 --rate 2000 --clients 8 --tag baseline

# 10-min extreme-rate run with the dashboard probe attached
scripts/run-stress-rig.sh --duration 600 --rate 10000 --clients 12 \
    --http-port 12599 --probe --probe-duration 620 --tag extreme

# open-loop arrival-rate run (corrects coordinated omission; see below)
scripts/run-stress-rig.sh --duration 300 --rate 2000 --clients 8 \
    --model open --query-rate 250 --tag arrival
```

Outputs land in `/tmp/sqtseries-rig-<tag>/`: `rss.log` (per-pid memory
trend), `stress.json` (percentiles **+ end-to-end accounting**), `probe.log`
(console errors / stalls / JS heap), `probe-shots/` (screenshots),
`verdict.txt` (leak gate). Exit 0 = leak gate passed (and the probe was
clean under `--strict-probe`).

## Instrumentation

### `rss-watch.sh [interval_s] [out_file]`

Lightweight per-pid sampler (RSS, threads, fds) for every matching python
process — no ptrace, runs forever, restart-safe. Input to `leak_check.py`.

### `py-spy-watch.sh [interval_s] [out_file] [dump_interval_s] [--clean]`

Continuous py-spy watcher: RSS trending plus throttled full stack dumps for
every `sqtseries`/`pytest` python process. Needs passwordless sudo for
ptrace (probed at startup; without it, RSS-only). Read the logs as:

- RSS delta climbing while idle = leak
- repeated identical deep stacks = stuck loop / deadlock
- growing thread lists = un-joined tasks

Attach it to a whole test suite:

```bash
tmux new -d -s py-spy-watch 'scripts/py-spy-watch.sh 1 /tmp/py-spy-watch.log 30'
tmux new -d -s test-run '<venv>/bin/python3 -m pytest tests -q'
```

### `leak_check.py [--warmup-snapshots 15] [--rss-tolerance-kb 20000] <rss.log>`

CI-style pass/fail verdict from an rss-watch log. Judges drift **after
warm-up**, not from birth (a starting server always grows). Fails on RSS /
fd / thread climbs that never recover; restarts are split per process
lifetime. Non-zero exit = leak-shaped behaviour.

### `dashboard_probe.py --url http://127.0.0.1:PORT [--duration 300]`

Headless Chromium probe for `/dashboard`: console errors, page exceptions,
failed responses, stalled counters (frozen >60s), JS heap trend,
start/mid/end screenshots. Exits 0 clean / 1 problems / 2 could not run.
Correctly ignores teardown noise after the server dies (it stops polling
once the endpoint is gone).

## Load generation

### `stress_percentiles.py --duration 300 --rate 2000 --clients 8`

The stress harness: starts its own service (TOML under
`/tmp/sqtseries-stress/`), pumps with a **separate process** (a pump thread under
the harness GIL collapses to ~50 pts/s — measured), hammers ZMQ
queries, HTTP read/agg/write, samples WS delivery, and prints a percentile
table. With `--json`, also writes the raw report including the
**accounting block**: `pump_sent` vs `server_ingested` vs `persisted` /
`dropped` / `invalid` vs actual `db_rows` (grace-drain window first, so
PUSH in-flight backlog is drained before reconciling). `--http-port N`
exposes the service on a fixed port for the dashboard probe.

**Query load model** — `--model closed` (default) or `--model open`:

| | `--model closed` | `--model open` |
|---|---|---|
| Client socket | `REQ` (lockstep) | `DEALER` (pipelined) against the broker's `ROUTER` |
| Pacing | next request after the last reply | fixed schedule, `--query-rate` ops/s aggregate |
| Latency measured from | actual send | the **scheduled** fire time |
| Answers | response time at saturation | behaviour under an arrival stream |

Open-loop records two figures per request: `zmq_query` (service time, from the
actual send) and `zmq_query_co` (corrected, from the scheduled time). The gap
between them is the queueing delay a closed-loop harness hides — measured at
**6.8× at p50** once the service saturates, and ~1.00× when it does not, which
is the check that the correction is not just inflating every sample.

```bash
scripts/run-stress-rig.sh --duration 300 --rate 2000 --clients 8 \
    --model open --query-rate 250 --tag arrival
```

**`--max-inflight` is the cross-client total, not per-client.** It mirrors the
broker's single global `max_inflight` (`len(broker._router_tasks) >=
max_inflight` in `src/sqtseries/messaging/broker.py`) — one pool for the whole
process. The default 64 splits to 8 per client at 8 clients. Reading it
per-client lets 8 × 64 = 512 requests pile up against a broker servicing 64; the
broker sheds them, and the report then blames the generator for being slow.

**Check three fields before quoting any open-loop number** — they exist to catch
a generator that is broken rather than a server that is slow:

| Field | Failure it catches |
|---|---|
| `sent_ops` | **0 means the generator never fired.** A pacer that only sleeps still reports a perfect achieved rate. |
| `zmq_query` `n` | must equal `sent_ops`; a shortfall means unanswered requests |
| `client_shed` | non-zero means the generator hit its own cap, so the offered rate was **not** achieved and the run does not certify that rate |

### `benchmark.py`


Engine-level micro-benchmarks (pooled vs fresh reader connections, batched
inserts). The numbers cited in `dist/docs/benchmarks.md` come from here and
from `stress_percentiles.py`.

## Packaging / deployment

### `build_wheel.sh` / `test_wheel.sh`

Build the wheel and smoke-test it in a fresh venv.

### `deploy.sh`

Deployment helper (see header comment for targets).
