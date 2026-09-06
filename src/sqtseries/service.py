"""Service manager: startup sequence, main event loop, graceful shutdown.
Startup: WAL recovery, port acquisition, runtime state, background
managers. Shutdown: drain-on-shutdown, TRUNCATE checkpoint at the end.
"""

import asyncio
import os
import signal
import sqlite3
import time
from pathlib import Path
from typing import Any

import structlog

from .config import Settings, validate_settings
from .engine import (
    BackupManager,
    CheckpointManager,
    MaintenanceManager,
    StorageEngine,
    backup_database,
    create_sqlite_engine,
    run_analyze_once,
    run_migrations,
    run_optimize,
    wal_checkpoint,
)
from .health import collect_health
from .messaging import (
    AdminBroker,
    ConnectionRegistry,
    Ingress,
    ProtocolError,
    PubSub,
    QueryBroker,
    StatsPublisher,
    WorkerPool,
)
from .messaging.protocol import parse_admin
from .messaging.query_cache import QueryResultCache
from .partition import RetentionManager, RollupManager
from .partition.retention import parse_ttl
from .ports import PortAllocator
from .query import TimeSeriesDB
from .query.agg import parse_interval
from .recovery import check_integrity_on_startup, recover_wal
from .runtime import RuntimeState

log = structlog.get_logger(__name__)


class ServiceError(Exception):
    pass


class Service:
    """Run the sqtseries daemon in-process."""

    def __init__(self, settings: Settings):
        errors = validate_settings(settings)
        if errors:
            raise ServiceError(f"invalid config: {'; '.join(errors)}")
        self.settings = settings
        self.started_at: float = time.time()
        self.engine = None
        self.store: StorageEngine | None = None
        self.ts: TimeSeriesDB | None = None
        self.ingress: Ingress | None = None
        self.broker: QueryBroker | None = None
        self.admin_broker: AdminBroker | None = None
        self.pool: WorkerPool | None = None
        self.checkpoint_manager: CheckpointManager | None = None
        self.rollup_manager: RollupManager | None = None
        self.maintenance_manager: MaintenanceManager | None = None
        self.retention_manager: RetentionManager | None = None
        self.backup_manager: BackupManager | None = None
        self.pubsub: Any = None
        self.stats_publisher: StatsPublisher | None = None
        self.http_server: Any = None
        self.http_task: asyncio.Task | None = None
        self._publish_tasks: set[asyncio.Task] = set()
        self.runtime = RuntimeState(_runtime_path(settings))
        self._runtime_owned = False
        self.connection_registry = ConnectionRegistry()
        self._query_cache = QueryResultCache()
        self._http_counters: dict[str, int] = {"writes": 0, "queries": 0}

    async def start(self) -> None:
        """Start the service; on partial failure, clean up what started."""

        try:
            await self._start()
        except BaseException:
            await self.shutdown()
            raise

    async def _start(self) -> None:
        db_path = self.settings.db_path_expanded()
        self.engine = create_sqlite_engine(db_path, self.settings.database)

        run_migrations(self.engine)
        check_integrity_on_startup(self.engine, strict=False)
        recover_wal(self.engine)

        self.store = StorageEngine(self.engine)
        self.ts = TimeSeriesDB(self.store)

        self.checkpoint_manager = CheckpointManager(self.engine)
        await self.checkpoint_manager.start()

        if self.settings.rollup.enabled:
            self.rollup_manager = RollupManager(
                self.engine,
                interval=float(parse_interval(self.settings.rollup.interval)),
                skew_s=self.settings.ingestion.reject_client_timestamp_skew_s,
            )
            await self.rollup_manager.start()

        if self.settings.maintenance.enabled:
            self.maintenance_manager = MaintenanceManager(
                self.engine,
                interval=float(
                    parse_interval(self.settings.maintenance.analyze_interval)
                ),
            )
            await self.maintenance_manager.start()

        if self.settings.retention.enabled:
            self.retention_manager = RetentionManager(
                self.engine,
                self.store,
                ttl=self.settings.retention.default_ttl,
                interval=parse_ttl(
                    self.settings.retention.check_interval
                ).total_seconds(),
            )
            await self.retention_manager.start()

        if self.settings.backup.enabled:
            self.backup_manager = BackupManager(
                self.engine,
                interval=float(parse_interval(self.settings.backup.interval)),
                backup_dir=self.settings.backup.path,
            )
            await self.backup_manager.start()

        self.pubsub = PubSub(
            f"tcp://127.0.0.1:{self.settings.streaming.port}",
            linger_seconds=self.settings.streaming.linger_seconds,
            registry=self.connection_registry,
        )
        await self.pubsub.start()

        allocator = PortAllocator.from_settings(self.settings.ports)
        # the auto-detected ingest port must not collide with the fixed ports
        # (stats included: a missed reservation lets the detector pick 12506 for
        # ingest and the StatsPublisher bind then fails -> service won't start)

        allocator.reserved = {
            self.settings.query.port,
            self.settings.streaming.port,
            self.settings.admin.port,
            self.settings.http.port,
            self.settings.stats.port,
        }
        ingest_port = self.settings.ingestion.port
        if self.settings.ports.auto_detect:
            ingest_port = allocator.alloc(auto_detect=True)
        self.ingress = Ingress(
            f"tcp://127.0.0.1:{ingest_port}",
            self.settings.ingestion,
            sink=self._sink,
            on_publish=self._on_publish,
        )
        await self.ingress.start()

        self.broker = QueryBroker(
            f"tcp://127.0.0.1:{self.settings.query.port}",
            self.settings.query,
            handler=self._query_handler,
            handler_timeout_s=self.settings.query.timeout_s,
        )
        await self.broker.start()

        self.admin_broker = AdminBroker(
            f"tcp://127.0.0.1:{self.settings.admin.port}",
            self.settings.query,
            handler=self._admin_handler,
            # admin ops (backup/vacuum) may legitimately run long
            handler_timeout_s=None,
        )
        await self.admin_broker.start()

        async def step() -> bool:
            # The pump must not busy-poll when idle: a NOBLOCK recv loop that
            # only yields via asyncio.sleep(0) keeps the event-loop thread
            # re-grabbing the GIL, which starves CPU-bound worker threads (e.g.
            # a slow query offloaded via asyncio.to_thread). Return whether any
            # socket had work; the worker sleeps longer when idle (see
            # WorkerPool._run).
            did = False
            if await self.ingress.run_once(block=False):
                did = True
            if await self.broker.run_once(block=False):
                did = True
            if await self.admin_broker.run_once(block=False):
                did = True
            return did

        self.pool = WorkerPool(step, size=1)
        await self.pool.start()

        if self.settings.stats.enabled:
            self.stats_publisher = StatsPublisher(
                f"tcp://127.0.0.1:{self.settings.stats.port}",
                registry=self.connection_registry,
            )
            await self.stats_publisher.start()

        await self._start_http_gateway()

        # ANALYZE only after every socket has bound, so a failed boot (e.g. a
        # port conflict during a restart loop) doesn't hammer the DB's planner
        # stats on every attempt.
        run_analyze_once(self.engine)

        self.runtime.write(
            pid=os.getpid(),
            ports={
                "ingest": ingest_port,
                "query": self.settings.query.port,
                "stream": self.settings.streaming.port,
                "admin": self.settings.admin.port,
                "http": self.settings.http.port,
                "stats": self.settings.stats.port,
            },
            db_path=db_path,
        )
        self._runtime_owned = True
        log.info(
            "service started",
            ports={"ingest": ingest_port, "query": self.settings.query.port},
        )

    async def _start_http_gateway(self) -> None:
        """Run the FastAPI REST + WebSocket gateway (uvicorn) in-process.

        Serves port 12505 (http.port) sharing the event loop: the gateway reads

        the engine, query facade, and pubsub directly via ``app.state``.

        """
        import uvicorn

        from .gateway import create_app

        app = create_app(
            store=self.store,
            tsdb=self.ts,
            settings=self.settings.http,
            pubsub=self.pubsub,
            ingestion=self.settings.ingestion,
            registry=self.connection_registry,
            query_timeout_s=self.settings.query.timeout_s,
            stats_provider=self.dashboard_snapshot,
        )
        self._http_counters = app.state.http_counters
        config = uvicorn.Config(
            app,
            host=self.settings.http.host,
            port=self.settings.http.port,
            # structlog owns logging
            log_config=None,
            access_log=False,
            # Belt-and-suspenders: a stop/SIGTERM must always complete. If a
            # connection task refuses to end (e.g. a stuck WebSocket), uvicorn
            # cancels the stragglers after this many seconds instead of waiting
            # forever, so `sqtseries stop` is deterministic.
            timeout_graceful_shutdown=5,
            # WebSocket limits & keepalive (explicit for clarity):
            # - ws_max_size bounds one inbound frame (16 MiB) — a larger frame
            #   closes the connection with 1009.
            # - protocol-level ping every 20s (timeout 20s) drops dead peers.
            # - the app-level {"type":"ping"} every 30s idle is separate and
            #   tells clients the stream is alive even without new data.
            ws_max_size=16 * 1024 * 1024,
            ws_ping_interval=20.0,
            ws_ping_timeout=20.0,
        )
        # Service.run() owns signals: uvicorn 0.52's serve() installs its own
        # SIGINT/SIGTERM handlers via capture_signals() (it no longer honors an
        # `install_signal_handlers` config option), but run()'s
        # loop.add_signal_handler() then replaces them, so should_exit is only
        # ever set here in shutdown() — never by uvicorn's own handle_exit.

        self.http_server = uvicorn.Server(config)
        self.http_task = asyncio.create_task(self.http_server.serve())
        # Wait up to ~5s for the socket to bind
        for _ in range(500):
            if self.http_server.started:
                return
            await asyncio.sleep(0.01)
        # server failed to bind (e.g. port in use); uvicorn exits with
        # SystemExit(3) inside the task — surface a clean error instead

        try:
            await self.http_task
        except BaseException as exc:
            raise RuntimeError(
                f"HTTP gateway failed to start on port {self.settings.http.port}: {exc}"
            ) from exc
        raise RuntimeError(
            f"HTTP gateway failed to start on port {self.settings.http.port}"
        )

    def _sink(self, metric: str, tags: Any, value: float, ts_ns: int) -> None:
        """Persist an ingested measurement immediately.
        Writes are serialized by SQLite itself (single-writer + BEGIN IMMEDIATE

        in ``Database.begin()`` + busy_timeout), so a separate app-level write

        queue would only add flush latency without correctness benefit.

        """
        if self.store is not None:
            try:
                self.store.insert_many([(metric, tags, value, ts_ns)])
            except Exception:
                log.exception("sink insert failed", metric=metric)

    def _on_publish(self, topic: bytes, payload: dict[str, Any]) -> None:
        """Republish an ingested measurement to live subscribers."""
        if self.pubsub is not None:
            task = asyncio.create_task(self._publish(topic, payload))
            self._publish_tasks.add(task)
            task.add_done_callback(self._publish_tasks.discard)

    async def _publish(self, topic: bytes, payload: dict[str, Any]) -> None:
        try:
            await self.pubsub.publish(topic, payload)
        except Exception:
            log.warning("publish failed", exc_info=True)

    def _query_handler(self, query: dict[str, Any]) -> dict[str, Any]:
        if self.ts is None:
            return {
                "status": "error",
                "error": {"code": "NOT_READY", "message": "query engine unavailable"},
            }
        # Check cache first — identical queries within TTL are served from
        # cache, avoiding redundant SQLite scans.
        cached = self._query_cache.get(query)
        if cached is not None:
            return cached
        metric = query.get("metric")

        start = query.get("start")

        end = query.get("end")
        try:
            limit = query.get("limit")

            if limit is not None and (
                not isinstance(limit, int) or isinstance(limit, bool) or limit < 1
            ):
                raise ValueError("limit must be a positive integer")
            aggs = query.get("aggregations")
            if aggs:
                funcs = [f.strip() for f in str(aggs).split(",") if f.strip()]

                result = {
                    "status": "ok",
                    "data": self.ts.aggregate(
                        metric=metric, start=start, end=end, funcs=funcs
                    ),
                }
                self._query_cache.put(query, result)
                return result
            data = self.ts.query(
                metric=metric,
                start=start,
                end=end,
                aggregation=query.get("aggregation"),
                interval=query.get("interval"),
                limit=limit,
                order=query.get("order", "asc"),
            )
        except (ValueError, KeyError, TypeError, IndexError, OverflowError) as exc:
            return {
                "status": "error",
                "error": {"code": "INVALID_QUERY", "message": str(exc)},
            }
        result = {
            "status": "ok",
            "data": [{"timestamp": ts / 1e9, "value": v} for ts, v in data],
        }
        self._query_cache.put(query, result)
        return result

    def _admin_handler(self, query: dict[str, Any]) -> dict[str, Any]:
        """Serve admin commands over the admin REP socket (port 12504)."""

        if self.engine is None:
            return {
                "status": "error",
                "error": {"code": "NOT_READY", "message": "service unavailable"},
            }
        cmd = parse_admin(query)
        if cmd == "ping":
            return {"status": "ok", "pong": True}
        if cmd == "health":
            return self.health()
        if cmd == "stats":
            return self._admin_stats()
        if cmd == "optimize":
            run_optimize(self.engine)
            return {"status": "ok", "optimized": True}
        if cmd == "backup":
            try:
                path = backup_database(self.engine, self.settings.backup.path)
            except Exception as exc:
                return {
                    "status": "error",
                    "error": {"code": "BACKUP_FAILED", "message": str(exc)},
                }
            return {"status": "ok", "path": path}
        if cmd == "vacuum":
            try:
                with self.engine.connect() as conn:
                    conn.exec_driver_sql("VACUUM")
            except sqlite3.OperationalError as exc:
                return {
                    "status": "error",
                    "error": {
                        "code": "VACUUM_BUSY",
                        "message": f"vacuum failed while running; "
                        f"stop the service and use `sqtseries vacuum`: {exc}",
                    },
                }
            return {"status": "ok", "vacuumed": True}
        if cmd == "connections":
            return self._admin_connections()
        if cmd == "conncheck":
            ids = query.get("ids", [])
            return self._admin_conncheck(ids)
        if cmd == "subscribers":
            return self._admin_subscribers()
        raise ProtocolError(f"unknown admin command: {cmd}")

    def _admin_stats(self) -> dict[str, Any]:
        """Assemble live service statistics for the admin socket."""
        payload: dict[str, Any] = {
            "status": "ok",
            "uptime_s": round(time.time() - self.started_at, 2),
        }
        if self.ingress is not None:
            istats = self.ingress.stats()
            http_writes = getattr(self, "_http_counters", {}).get("writes", 0)
            payload["ingested"] = istats["recv"] + http_writes
            payload["invalid"] = istats["invalid"]
            payload["ingest_errors"] = istats["errors"]
        if self.broker is not None:
            http_queries = getattr(self, "_http_counters", {}).get("queries", 0)
            payload["queries"] = self.broker.stats()["requests"] + http_queries
        if self.admin_broker is not None:
            payload["admin_requests"] = self.admin_broker.stats()["requests"]
        if self.checkpoint_manager is not None:
            cstats = self.checkpoint_manager.stats()
            payload["wal_bytes"] = cstats["wal_bytes"]
            payload["checkpoint_busy_runs"] = cstats["busy_runs"]
            payload["checkpoints"] = cstats["checkpoints"]
        if self.maintenance_manager is not None:
            payload["analyze_runs"] = self.maintenance_manager.runs
        if self.retention_manager is not None:
            payload["retention_runs"] = self.retention_manager.runs
            payload["partitions_dropped"] = self.retention_manager.dropped_total
        if self.backup_manager is not None:
            payload["backup_runs"] = self.backup_manager.runs
            payload["backups_created"] = self.backup_manager.backups_created

            if self.backup_manager.last_backup:
                payload["last_backup"] = self.backup_manager.last_backup
        if self.pubsub is not None:
            pstats = self.pubsub.stats()
            payload["published"] = pstats["published"]
            payload["subscribers"] = pstats["active"]
        if self.store is not None:
            payload["series"] = self.store.series_count()
            payload["metrics"] = len(self.store.list_metrics())
        # Query cache stats
        qstats = self._query_cache.stats()
        payload["query_cache_size"] = qstats["size"]
        payload["query_cache_hits"] = qstats["hits"]
        payload["query_cache_misses"] = qstats["misses"]
        return payload

    def dashboard_snapshot(self) -> dict[str, Any]:
        """Full dashboard snapshot: admin counters plus lists and storage."""
        from pathlib import Path

        payload = self._admin_stats()
        payload["server_time"] = time.time()
        payload["version"] = "0.1.0"
        payload["ws_connections"] = self.connection_registry.ws_count
        payload["connections"] = self.connection_registry.list_connections()
        sub = self._admin_subscribers()
        payload["zmq_subscribers"] = sub["zmq_subscribers"]
        payload["subscriptions"] = sub["subscriptions"]
        db_path = self.settings.db_path_expanded()
        payload["db_path"] = str(db_path)
        try:
            payload["db_bytes"] = Path(db_path).stat().st_size
        except OSError:
            payload["db_bytes"] = None
        payload["ports"] = {
            "ingest": self.settings.ingestion.port,
            "query": self.settings.query.port,
            "streaming": self.settings.streaming.port,
            "admin": self.settings.admin.port,
            "http": self.settings.http.port,
            "stats": self.settings.stats.port,
        }
        try:
            names = self.store.db.get_table_names() if self.store else []
            payload["partitions"] = sum(n.startswith("measurements_") for n in names)
        except Exception:
            payload["partitions"] = None
        try:
            from .partition.rollup import rollup_watermark

            payload["rollup_watermark"] = (
                rollup_watermark(self.store.db) if self.store else None
            )
        except Exception:
            payload["rollup_watermark"] = None
        return payload

    def _admin_connections(self) -> dict[str, Any]:
        """Return the list of active WebSocket connections."""
        return {"status": "ok", "data": self.connection_registry.list_connections()}
        """Return the list of active WebSocket connections."""
        return {"status": "ok", "data": self.connection_registry.list_connections()}

    def _admin_conncheck(self, ids: list[str]) -> dict[str, Any]:
        """Check which connection IDs are still present."""
        present = [cid for cid in ids if self.connection_registry.check_connection(cid)]
        return {"status": "ok", "present": present}

    def _admin_subscribers(self) -> dict[str, Any]:
        """Return per-topic ZMQ subscriber counts."""
        snapshot = self.connection_registry.snapshot()
        return {
            "status": "ok",
            "zmq_subscribers": snapshot["zmq_subscribers"],
            "subscriptions": snapshot["subscriptions"],
        }

    def health(self) -> dict[str, Any]:
        """Return a health payload (sync-friendly for the HTTP layer)."""

        return collect_health(self.engine, started_at=self.started_at)

    async def run(self) -> None:
        """Run until a shutdown signal (SIGINT/SIGTERM), then return cleanly.

        Uses an asyncio.Event instead of ``loop.stop()``: stopping the loop

        mid-run makes ``asyncio.run`` raise (exit code 1), which systemd's

        ``Restart=on-failure`` treats as a crash and restarts a service that

        was deliberately stopped.
        """

        loop = asyncio.get_running_loop()

        shutdown_event = asyncio.Event()

        def _signal(signum: int | None = None, frame: Any = None) -> None:
            loop.call_soon_threadsafe(shutdown_event.set)

        # Non-main thread: add_signal_handler is not available, fall back

        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, shutdown_event.set)
        except (NotImplementedError, RuntimeError):  # fmt: skip
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, _signal)
        await shutdown_event.wait()

    async def shutdown(self) -> None:
        """Graceful shutdown sequence."""
        log.info("shutting down")
        if self.checkpoint_manager is not None:
            await self.checkpoint_manager.stop()
        if self.rollup_manager is not None:
            await self.rollup_manager.stop()
        if self.maintenance_manager is not None:
            await self.maintenance_manager.stop()
        if self.retention_manager is not None:
            await self.retention_manager.stop()
        if self.backup_manager is not None:
            await self.backup_manager.stop()
        if self.pool is not None:
            await self.pool.stop()
        if self.ingress is not None:
            await self.ingress.stop()
        if self.broker is not None:
            await self.broker.stop()
        if self.admin_broker is not None:
            await self.admin_broker.stop()
        if self.http_server is not None:
            self.http_server.should_exit = True
            if self.http_task is not None:
                await asyncio.gather(self.http_task, return_exceptions=True)

                self.http_task = None
        # drain in-flight publishes BEFORE stopping pubsub, so they aren't
        # dropped by a stopped socket
        if self._publish_tasks:
            await asyncio.gather(*self._publish_tasks, return_exceptions=True)

            self._publish_tasks.clear()
        if self.pubsub is not None:
            await self.pubsub.stop()
        if self.stats_publisher is not None:
            await self.stats_publisher.stop()
        if self.store is not None:
            self.store.close()
            self.store = None
        if self.engine is not None:
            try:
                run_optimize(self.engine)
                # Shutdown only: TRUNCATE the WAL for a clean close
                wal_checkpoint(self.engine, "TRUNCATE")
            except Exception:
                log.exception("shutdown maintenance failed")
            finally:
                self.engine.dispose()
                self.engine = None
        if self._runtime_owned:
            # only remove a runtime file we actually wrote (a failed second
            # start must not delete the healthy instance's state)
            self.runtime.remove()
        log.info("shutdown complete")


def _runtime_path(settings: Settings) -> str:
    """Runtime state file lives next to the database file."""
    return str(Path(settings.db_path_expanded()).parent / "runtime.json")
