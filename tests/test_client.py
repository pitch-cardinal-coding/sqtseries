"""High-level Python Client tests (real ZMQ against a Service)."""

import asyncio
import threading
import time

import pytest

from sqtseries.client import Client
from sqtseries.config import Settings
from sqtseries.service import Service


@pytest.fixture
async def running_service(tmp_path, free_ports):
    """Start a real Service on OS-assigned free ports; tear down after."""

    s = Settings(
        database={"path": str(tmp_path / "client.sqlite"), "batch_size": 500},
        ingestion={"port": free_ports["ingest"]},
        query={"port": free_ports["query"]},
        streaming={"port": free_ports["streaming"]},
        admin={"port": free_ports["admin"]},
        http={"port": free_ports["http"]},
        stats={"port": free_ports["stats"]},
        ports={"auto_detect": False},
    )
    svc = Service(s)
    await svc.start()

    # Stop the pool (loop-bound tasks); pump manually so the service serves
    # while the sync client calls run inside asyncio.to_thread.
    await svc.pool.stop()
    pump_task = asyncio.create_task(_pump(svc))
    try:
        yield svc, s
    finally:
        pump_task.cancel()
        await asyncio.gather(pump_task, return_exceptions=True)
        await svc.shutdown()


async def _pump(svc):
    while True:
        await svc.ingress.drain_many()
        await svc.ingress.flush()
        await svc.broker.run_once(block=False)
        await svc.admin_broker.run_once(block=False)
        await asyncio.sleep(0.005)


@pytest.fixture
def client(running_service):
    _, s = running_service
    c = Client(
        ports={
            "write": s.ingestion.port,
            "query": s.query.port,
            "subscribe": s.streaming.port,
            "admin": s.admin.port,
        }
    )
    yield c
    c.close()


def _block(coro):
    """Run a sync client call in a thread so the pump loop keeps ticking."""

    return asyncio.run_coroutine_threadsafe(coro, asyncio.get_running_loop()).result()


class TestClient:
    async def test_write_then_query(self, client):
        await asyncio.to_thread(client.write, "cpu.usage", 0.72, {"host": "web1"})

        await asyncio.to_thread(client.write, "cpu.usage", 0.80, {"host": "web1"})
        # let PUSH frames drain into the store
        await asyncio.sleep(0.3)
        rows = await asyncio.to_thread(client.query, "cpu.usage")
        assert len(rows) == 2
        assert all("timestamp" in r and "value" in r for r in rows)

    async def test_write_many(self, client):
        await asyncio.to_thread(
            client.write_many,
            [
                {"metric": "a", "value": 1.0},
                {"metric": "a", "value": 2.0, "tags": {"x": "y"}},
            ],
        )
        await asyncio.sleep(0.3)
        rows = await asyncio.to_thread(client.query, "a")
        assert len(rows) == 2

    async def test_query_aggregation(self, client):
        for v in (1.0, 2.0, 3.0):
            await asyncio.to_thread(client.write, "temp", v)
        # drain writes before aggregating
        await asyncio.sleep(0.4)
        result = await asyncio.to_thread(client.aggregate, "temp", funcs=["avg", "max"])

        assert result["avg"] == pytest.approx(2.0)
        assert result["max"] == pytest.approx(3.0)

    async def test_query_missing_metric_returns_empty(self, client):
        rows = await asyncio.to_thread(client.query, "does.not.exist")
        assert rows == []

    async def test_subscribe_streams(self, client):
        received = []

        stop = threading.Event()

        def sub_loop():
            for payload in client.subscribe("cpu."):
                if stop.is_set():
                    break
                if payload is not None:
                    received.append(payload)

        t = threading.Thread(target=sub_loop, daemon=True)
        t.start()
        # SUB slow-joiner
        await asyncio.sleep(0.3)
        for _ in range(3):
            await asyncio.to_thread(client.write, "cpu.usage", 0.5)
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.3)
        stop.set()
        t.join(timeout=2)
        assert len(received) >= 1

    def test_close_idempotent(self, client):
        client.close()
        # no error
        client.close()

    async def test_write_then_close_flushes(self, running_service):
        """A measurement written right before close() must still be delivered.

        close(linger=0) would discard it (regression: write-then-close
        delivered 0 of 1 messages); the write socket keeps a flush linger.

        """
        svc, s = running_service
        c = Client(ports={"write": s.ingestion.port})
        try:
            c.write("flush.test", 0.42)
            c.close()
        finally:
            c.close()
        # let the PUSH frame drain through the service's ingest socket
        await asyncio.sleep(0.3)
        for _ in range(100):
            await svc.ingress.drain_many()
            await asyncio.sleep(0.005)
        assert svc.ingress.recv_count >= 1

    async def test_query_timeout_recovers(self, monkeypatch):
        """After a recv timeout the REQ socket must not stay EFSM-broken.

        A timed-out recv leaves REQ awaiting a reply; the next send on the same

        socket raises EFSM. The client must drop the socket and recreate it so

        subsequent calls raise ClientError, not a raw zmq ZMQError.
        """
        import socket as _socket

        from sqtseries.client import Client, ClientError

        s = _socket.socket()
        s.bind(("127.0.0.1", 0))
        dead_port = s.getsockname()[1]
        s.close()
        monkeypatch.setattr("sqtseries.client.RECV_TIMEOUT_MS", 200)

        c = Client(host="127.0.0.1", ports={"query": dead_port, "admin": dead_port})

        try:
            for _ in range(2):
                with pytest.raises(ClientError):
                    await asyncio.to_thread(c.query, "anything")
            # admin path uses a separate REQ socket; same recovery required

            with pytest.raises(ClientError):
                await asyncio.to_thread(c.admin, "ping")
        finally:
            c.close()


class TestAdmin:
    async def test_admin_ping(self, client):
        reply = await asyncio.to_thread(client.admin, "ping")
        assert reply["status"] == "ok"
        assert reply["pong"] is True

    async def test_admin_health(self, client):
        reply = await asyncio.to_thread(client.admin, "health")
        assert reply["status"] == "ok"
        assert "uptime" in reply

    async def test_admin_stats(self, client):
        await asyncio.to_thread(client.write, "cpu.usage", 1.0)
        await asyncio.sleep(0.3)
        reply = await asyncio.to_thread(client.admin, "stats")
        assert reply["status"] == "ok"
        assert reply["ingested"] >= 1
        assert reply["series"] >= 1
        assert reply["metrics"] >= 1

    async def test_admin_unknown_command(self, client):
        from sqtseries.client import ClientError

        with pytest.raises(ClientError):
            await asyncio.to_thread(client.admin, "frobnicate")

    async def test_admin_optimize(self, client):
        reply = await asyncio.to_thread(client.admin, "optimize")
        assert reply["status"] == "ok"

    async def test_admin_backup(self, client, running_service):
        reply = await asyncio.to_thread(client.admin, "backup")
        assert reply["status"] == "ok"
        assert "path" in reply
        from pathlib import Path

        assert Path(reply["path"]).exists()

    async def test_admin_vacuum(self, client):
        reply = await asyncio.to_thread(client.admin, "vacuum")
        # either succeeds or reports VACUUM_BUSY (running service) — never crashes

        assert reply["status"] in ("ok", "error")
        if reply["status"] == "error":
            assert reply["error"]["code"] == "VACUUM_BUSY"


class TestClientErrors:
    async def test_query_error_raises(self, client):
        from sqtseries.client import ClientError

        await asyncio.to_thread(client.write, "cpu.usage", 0.5)
        await asyncio.sleep(0.3)
        with pytest.raises(ClientError):
            await asyncio.to_thread(
                client.query, "cpu.usage", aggregation="not-a-real-func"
            )

    async def test_admin_error_raises(self, client):
        from sqtseries.client import ClientError

        with pytest.raises(ClientError):
            await asyncio.to_thread(client.admin, "bogus-cmd")

    async def test_zero_limit_reaches_server(self, client):
        """limit=0 is sent (not silently dropped) and rejected by the server."""
        from sqtseries.client import ClientError

        await asyncio.to_thread(client.write, "cpu.usage", 0.5)
        await asyncio.sleep(0.3)
        with pytest.raises(ClientError):
            await asyncio.to_thread(client.query, "cpu.usage", limit=0)


class TestSubscribeResync:
    def test_uptime_drop_recreates_socket(self, monkeypatch):
        """A server restart (uptime decrease) rebuilds the SUB socket."""
        import zmq

        from sqtseries.client import Client

        created = []

        class FakeSock:
            def __init__(self):
                self.subscribed = []

            def setsockopt(self, opt, value):
                if opt == zmq.SUBSCRIBE:
                    self.subscribed.append(value)

            def poll(self, timeout=0, flags=0):
                time.sleep(0.005)
                return 0

            def close(self, linger=0):
                pass

        c = Client()
        try:

            def fake_sub_sock():
                sock = FakeSock()
                created.append(sock)
                c._sub_sock = sock
                return sock

            monkeypatch.setattr(c, "_get_sub_sock", fake_sub_sock)
            uptimes = iter([100.0, 5.0, 5.0, 5.0])

            def fake_uptime():
                try:
                    return next(uptimes)
                except StopIteration:
                    return 5.0

            monkeypatch.setattr(c, "_server_uptime", fake_uptime)
            gen = c.subscribe("re.", timeout=0.01, resync_interval_s=0.01)
            for _ in range(30):
                next(gen)
                if len(created) >= 2:
                    break
            gen.close()
            assert len(created) >= 2
            assert all(s.subscribed == [b"re."] for s in created)
        finally:
            c.close()


class TestSubscribeCloseRace:
    def test_close_ends_parked_subscribe_iterator_silently(
        self, free_ports, running_service
    ):
        """close() while a subscribe() iterator is parked in poll() must end
        that iterator as a normal stop (no raised exception) instead of
        leaking an unhandled ZMQError in the subscribing thread.
        Regression guard for the PytestUnhandledThreadExceptionWarning that
        test_subscriber_recovers_after_restart used to emit via its own
        collector thread."""
        import threading

        _, s = running_service
        c = Client(
            ports={
                "write": s.ingestion.port,
                "query": s.query.port,
                "subscribe": s.streaming.port,
                "admin": s.admin.port,
            }
        )
        ended = threading.Event()
        error: list[BaseException] = []

        def collector():
            try:
                for _ in c.subscribe("race.", timeout=0.05):
                    pass
            except BaseException as exc:  # surfaced to the assertion below
                error.append(exc)
            finally:
                ended.set()

        t = threading.Thread(target=collector, daemon=True)
        t.start()
        time.sleep(0.3)  # let the iterator park inside poll() (timeout=0.05)
        try:
            c.close()  # must end the parked iterator as a normal stop
        finally:
            t.join(timeout=10)
        assert not t.is_alive(), "subscribe iterator did not end after close()"
        assert not error, f"subscribe raised on client close: {error!r}"


class TestSubscribeAcrossRestart:
    async def test_subscriber_recovers_after_restart(self, tmp_path, free_ports):
        """Bundled subscriber re-registers and receives after a restart."""
        from sqtseries.client import Client
        from sqtseries.config import Settings
        from sqtseries.service import Service

        def make_service():
            s = Settings(
                database={"path": str(tmp_path / "re.sqlite"), "batch_size": 500},
                ingestion={"port": free_ports["ingest"]},
                query={"port": free_ports["query"]},
                streaming={"port": free_ports["streaming"]},
                admin={"port": free_ports["admin"]},
                http={"port": free_ports["http"]},
                stats={"port": free_ports["stats"]},
                ports={"auto_detect": False},
            )
            return s, Service(s)

        async def pump(svc):
            while True:
                await svc.ingress.drain_many()
                await svc.ingress.flush()
                await svc.broker.run_once(block=False)
                await svc.admin_broker.run_once(block=False)
                await asyncio.sleep(0.005)

        import threading

        _s, svc = make_service()
        await svc.start()
        await svc.pool.stop()
        pump_task = asyncio.create_task(pump(svc))
        c = Client(
            ports={
                "write": free_ports["ingest"],
                "query": free_ports["query"],
                "subscribe": free_ports["streaming"],
                "admin": free_ports["admin"],
            }
        )
        got = []
        stop = False

        def collector():
            for payload in c.subscribe("re."):
                if stop:
                    break
                if payload is not None:
                    got.append(payload["value"])

        t = threading.Thread(target=collector, daemon=True)
        try:
            t.start()
            await asyncio.to_thread(c.write, "re.x", 1.0)
            await asyncio.sleep(2)
            assert got == [1.0]
            pump_task.cancel()
            await asyncio.gather(pump_task, return_exceptions=True)
            await svc.shutdown()
            _, svc2 = make_service()
            await svc2.start()
            await svc2.pool.stop()
            pump_task = asyncio.create_task(pump(svc2))
            try:
                deadline = time.monotonic() + 45
                while svc2.connection_registry.zmq_sub_count < 1:
                    assert time.monotonic() < deadline, "resubscribe never arrived"
                    await asyncio.sleep(0.5)
                await asyncio.to_thread(c.write, "re.x", 2.0)
                deadline = time.monotonic() + 30
                while got != [1.0, 2.0]:
                    assert time.monotonic() < deadline, f"post-restart data lost: {got}"
                    await asyncio.sleep(0.5)
            finally:
                pump_task.cancel()
                await asyncio.gather(pump_task, return_exceptions=True)
                await svc2.shutdown()
        finally:
            stop = True
            c.close()
