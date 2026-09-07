# sqtseries tests

Automated test suite for the sqtseries time-series database. It covers the
engine, the query path, messaging, the HTTP gateway, the CLI, and the
background maintenance tasks, plus property tests, fuzzing, and
resource-leak checks.

## Quick start

```bash
source ~/.env/sqtseries/bin/activate
python3 -m pytest tests/ -q
```

Run from the project root. The suite is self-contained: each test uses a
temporary database file under `tmp_path`, so no pre-existing data or running
service is required.

## How to run

```bash
# Default: collect and run everything, quiet
python3 -m pytest tests/ -q

# With coverage
python3 -m pytest tests/ -q --cov=sqtseries

# Per-file runner: hard per-suite timeout, 20s heartbeat, results kept
./tests/run_all.sh                          # all suites (runs from repo root)
./tests/run_all.sh test_query.py            # filtered (bare names allowed)
SUITE_TIMEOUT=600 ./tests/run_all.sh        # per-suite hard timeout
COVERAGE=1 ./tests/run_all.sh               # pytest-cov append + combine/report
# Per-suite stdout+stderr lands in tests/results/<suite>.out (gitignored).

# A single file
python3 -m pytest tests/test_query.py -q

# A single test by name
python3 -m pytest tests/test_query.py -q -k <name-fragment>

# Show the 10 slowest tests
python3 -m pytest tests/ -q --durations=10
```

Tests that spawn a real service with sockets use OS-assigned free ports
(`free_port()` / `free_ports` fixtures in `conftest.py`), so they never
conflict with a locally running `sqtseries` or each other. Tests run on
`asyncio_mode = "auto"`, so `async def` tests are awaited without explicit
markers.

## Test layout

| File | Covers |
|------|--------|
| `test_engine.py`, `test_store_edge.py`, `test_db.py`, `test_pragmas.py`, `test_persistence.py` | SQLite engine: schema, partitioned storage, PRAGMAs, reopen/durability |
| `test_query.py`, `test_agg_edge.py`, `test_rollup.py`, `test_rollup_query.py` | Query builder, aggregation functions, rollup fast path, gap fill, downsampling |
| `test_messaging.py`, `test_messaging_edge.py`, `test_protocol.py`, `test_protocol_fuzz.py`, `test_zmq_edge.py` | ZMQ transports, wire protocol validation, fuzz-lite parser checks, PUB/SUB slow-joiner + XPUB subscriber tracking |
| `test_gateway.py`, `test_gateway_edge.py`, `test_ratelimit.py` | HTTP REST + WebSocket gateway, rate limiting |
| `test_client.py`, `test_service.py`, `test_service_edge.py` | Remote Client, service lifecycle, graceful shutdown |
| `test_cli.py`, `test_cli_edge.py`, `test_cli2.py` | CLI commands, error paths |
| `test_cli_db.py` | `--db` flag, multi-database conflict detection, systemd config passthrough |
| `test_config.py`, `test_config_edge.py` | Settings precedence, env vars, file loading, durations |
| `test_health.py`, `test_runtime.py`, `test_ports.py` | Health probe, runtime state file, port allocation |
| `test_recovery.py`, `test_backup.py`, `test_maintenance.py` | Startup integrity, consistent backups, WAL checkpointing |
| `test_backup_and_timeout.py` | Automatic backup scheduler, server-side query timeout |
| `test_partition.py`, `test_retention.py` | Partition manager, TTL retention drops |
| `test_async_cleanup.py`, `test_resource_leaks.py`, `test_concurrency.py` | Task cancellation, fd/thread leak checks, concurrent writers |
| `test_regressions.py` | Guards against bugs from the QA audit |
| `test_docs_claims.py` | Verifies behaviors claimed in `dist/docs/` |
| `test_stress.py` | Lightweight stress smoke over many data points |
| `test_property.py` | Hypothesis property tests of aggregation invariants |
| `test_misc_edge.py` | Various edge cases shared across components |
| `test_examples_run.py`, `test_question_examples_advanced.py`, `test_camera_feed.py` | Runs the example scripts (including the camera feed + overlay server, which live in `examples/camera/`) against a live in-process service |
| `test_camera_overlay_page.py` | Playwright (headless Chromium) test of the live camera dashboard page: WebSocket connect, live updates, reconnect after a server restart, live connected-clients, and the `conn_validate.py` harness |
| `test_dashboard_page.py` | Playwright test of the admin dashboard page: snapshot render, live values, reconnect after restart, mobile viewport, console-clean |
| `test_websocket_edge.py` | WebSocket resilience & functionality edge cases: keepalive, close codes (1008/1009/1013), topic-prefix filtering, large/malformed/oversized frames, concurrent subscribers, slow-consumer isolation, churn (no listener/registry leaks), message ordering, connection cap |
| `test_connection_tracking.py` | Connection registry, XPUB subscriber tracking, admin conncheck/subscribers |
| `test_cache_and_health.py` | Query result cache, connection health sweep + batching, TCP keepalive defaults |
| `test_lifecycle_shutdown.py` | Process lifecycle: prompt SIGTERM exit with a disconnected WebSocket, camera-overlay SIGHUP, `sqtseries stop` waits for exit + ports rebindable |
| `test_edge_cases.py`, `test_logging.py` | Generic edge cases; logging configuration and rotation |
| `test_stress_concurrent.py` | Concurrent connect/disconnect stress on the registry |
| `test_systemd_edge.py` | Systemd unit template edge cases (mocked systemctl) |

Helper code lives in `conftest.py` (shared fixtures such as `free_port`/`free_ports` and sample TOML/JSON config files).

## Common commands

```bash
# Run one file, verbose
python3 -m pytest tests/test_query.py -v

# Stop on the first failure
python3 -m pytest tests/ -x
```

## Development notes

1. Run a single file first so iteration is fast.
2. Keep tests fast: avoid long sleeps in async tests; prefer short
   timeouts and `pytest-timeout` exposes stuck tests (60s global).
3. Temporary files: use `tmp_path` (already available), never fixed paths
   like `/tmp/test.db`.
4. Follow the existing style: `parametrize` for multiple cases, async tests
   without explicit markers (auto mode), comments on their own line.
5. Add new fixtures to `conftest.py` and share them; the suite already has
   fixtures for a temp DB path and sample TOML/JSON config files.

## Profiling with py-spy-watch

`scripts/py-spy-watch.sh` continuously watches **every** matching python
process — the pytest runner itself (tests run engines in-process) plus each
spawned `python3 -m sqtseries` service or example script — logging an RSS
trend line per pid with a `+N kB` delta, and appending a full `py-spy dump`
stack trace every `dump_interval` seconds per pid. Use it alongside a
running server **or** a whole test-suite/examples run.

```bash
# Start the watcher in a tmux session
# usage: scripts/py-spy-watch.sh [interval_s] [out_file] [dump_interval_s] [--clean]
tmux new -d -s py-spy-watch "bash scripts/py-spy-watch.sh 1 /tmp/sqtseries-py-spy.log 30"

# In another window: run the tests or examples
tmux new -d -s tests "/home/iam/devcode/.env/sqtseries/bin/python3 -m pytest tests/ -q"

# Inspect the log (RSS trend lines + periodic full stacks)
tail -f /tmp/sqtseries-py-spy.log

# When done
tmux kill-session -t py-spy-watch
tmux kill-session -t tests
```

Matching requires **both** the command line and the process `comm` to look
like a python process, so bash wrappers that merely carry the pattern in
their `bash -c` string never match (a lesson from airbits' watcher). It
needs `sudo` for ptrace (yama/ptrace_scope=1); passwordless sudo is probed
at startup — without it the log carries RSS trend lines only. py-spy 0.4.2
attaches fine on Python 3.14.4 (verified 2026-09).

### Quick start

```bash
# Start the watcher in a tmux session (1-second interval, custom log)
tmux new -d -s py-spy-watch "bash scripts/py-spy-watch.sh 1 /tmp/sqtseries-py-spy.log"

# In another window: run the tests
tmux new -d -s tests "python3 -m pytest tests/ -q"

# Inspect the log
tail -f /tmp/sqtseries-py-spy.log

# When done
tmux kill-session -t py-spy-watch
tmux kill-session -t tests
```

### Why a unique output file?

The default log path (`/tmp/py-spy-watch.log`) is shared with the
airbits project's own watcher. Always pass a project-specific path to
avoid interleaved dumps:

```bash
bash scripts/py-spy-watch.sh 2 /tmp/sqtseries-py-spy.log
```

### How targeting works

The script matches processes whose command line contains `python3` and
`sqtseries` (servers, example scripts) plus the `pytest` runner itself,
filtered by `/proc/PID/comm` so shell wrappers never match.

`sudo` is required for ptrace under `yama/ptrace_scope=1` (the
kernel default on most distros).

### What the dumps tell you

Each dump shows the full Python call stack of every thread. Look for:

- **Stuck loops** — the same stack appearing on every dump suggests a busy
  loop or deadlock.
- **Growing thread count** — many threads accumulating over time indicates
  a leak in task/thread creation.
- **Surprising allocations** — deep stacks in unexpected modules (e.g.
  large buffering inside a query) can hint at memory pressure.

The `test_resource_leaks.py`, `test_async_cleanup.py`, and
`test_concurrency.py` suites validate fd stability, task cleanup, and
concurrent safety; py-spy complements them by showing runtime behaviour
under load.

## Profiling with rss-watch (no-ptrace fallback)

`scripts/rss-watch.sh` is a lightweight memory/thread/fd monitor that needs
no ptrace and no sudo. py-spy does attach on this stack (py-spy 0.4.2 +
Python 3.14.4, verified 2026-09), but it requires sudo for ptrace and
briefly pauses the target on every dump — when you only want resource
trending, or cannot use sudo, this script reads `/proc/PID/status` for
every `python3.*sqtseries` process and logs VmRSS, VmSize, thread count,
and fd count at each interval.

### Quick start

```bash
tmux new -d -s rss-watch "bash scripts/rss-watch.sh 2 /tmp/sqtseries-rss.log"

# In another window: run the tests or examples
tmux new -d -s tests "python3 -m pytest tests/ -q"

# Inspect the log
tail -f /tmp/sqtseries-rss.log

# When done
tmux kill-session -t rss-watch
tmux kill-session -t tests
```

### What the logs tell you

Each snapshot shows per-process rss, vsz, threads, and fds. Look for:

- **Rss climb** — steady VmRSS growth over time = memory leak.
- **Thread growth** — rising thread count = un-joined tasks or threads.
- **Fd growth** — rising fd count = socket/connection/file leak.

Use a unique output file per project to avoid interleaved logs:

```bash
bash scripts/rss-watch.sh 2 /tmp/sqtseries-rss.log
```

## CI-style leak gate

`scripts/leak_check.py` turns an rss-watch log into a pass/fail verdict:

```bash
# Run the workload under rss-watch, then judge the log
python3 scripts/leak_check.py /tmp/sqtseries-rss.log

# Recipe for a bounded CI-style run (all in tmux sessions):
#   1. start rss-watch (2s interval)
#   2. start the server / test suite / example run
#   3. run the workload long enough that post-warm-up snapshots exist
#   4. leak_check exits 0 (pass), 1 (leak), or 2 (no data — never silent-pass)
python3 scripts/leak_check.py \
    --warmup-snapshots 10 --rss-tolerance-kb 20000 /tmp/sqtseries-rss.log
```

Verdicts are based on **drift after warm-up**, not from process birth: a
starting server always grows once (imports, sockets, thread pool, caches),
so the first `--warmup-snapshots` of each process lifetime are skipped and
whatever remains must be flat. A spike that fully recovers passes (load
peak); a climb that never returns fails. Histories split automatically at
process-restart markers (fresh python process: fds <= 4, threads == 1,
rss < 8 MB), so pid reuse and restarting services are judged per lifetime.
Tune `--warmup-snapshots` to your rss-watch interval (10 snapshots = 20 s
at the default 2 s), and size `--rss-tolerance-kb` to your workload.

## Troubleshooting

| Symptom | Reason / fix |
|---------|--------------|
| "Address already in use" | Should not happen: every service fixture binds OS-assigned free ports (`free_ports`). If it does, a leftover process may hold a port — check `ss -tlnp` and kill it. |
| Slow suite | Run a single file, or `--durations` to find the slowest tests |
| Weird failures after editing config | New settings often need new `test_config*` cases; the loader reads env vars, so unset `SQT_SERIES_*` before running |
| py-spy attach errors ("Failed to find python version", ptrace denied) | Use the venv's py-spy (0.4.2 attaches on Python 3.14.4, verified 2026-09) and attach with sudo: `sudo -n /home/iam/devcode/.env/sqtseries/bin/py-spy dump --pid <PID>` (yama/ptrace_scope=1). If you still cannot attach, `scripts/rss-watch.sh` needs no ptrace at all. |

## Documentation links

- [Docs](dist/docs/index.html)
- [Project README](README.md)