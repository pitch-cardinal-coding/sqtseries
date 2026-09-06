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

`scripts/py-spy-watch.sh` continuously samples `py-spy dump` on live
`python3 -m sqtseries` processes and appends every stack trace to a log
file. Use it alongside a running server **or** a test run that spawns
subprocesses (the script targets any `python3.*sqtseries` process).

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

The script uses `pgrep -f "python3.*sqtseries"` to find targets. This
matches processes whose command line contains both `python3` *and*
`sqtseries` — i.e. any sqtseries server instance or subprocess spawned
by the test suite. It avoids matching bash wrappers, non-Python helper
scripts, or unrelated processes from other projects.

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

## Troubleshooting

| Symptom | Reason / fix |
|---------|--------------|
| "Address already in use" | Should not happen: every service fixture binds OS-assigned free ports (`free_ports`). If it does, a leftover process may hold a port — check `ss -tlnp` and kill it. |
| Slow suite | Run a single file, or `--durations` to find the slowest tests |
| Weird failures after editing config | New settings often need new `test_config*` cases; the loader reads env vars, so unset `SQT_SERIES_*` before running |
| py-spy-watch "Failed to find python version" | The watcher picked up a non-Python process. The default `pgrep` pattern should prevent this; if it persists, check `pgrep -af "python3.*sqtseries"` to see what it matches. |

## Documentation links

- [Docs](dist/docs/index.html)
- [Project README](README.md)