"""WebSocket resilience & functionality edge-case tests.
Covers the gateway's live sockets (/ws/subscribe, /ws/connections) against a
real in-process Service, guided by RFC 6455 / MDN server guidance: keepalive,
close codes, payload size limits, malformed/fragmented client data, concurrency,
slow-consumer isolation, churn, topic-prefix filtering, and message ordering.
Authentication/security tests are intentionally omitted (none is implemented).
"""

import asyncio
import json
import socket
import time

import httpx
import pytest
import websockets

from sqtseries.config import Settings
from sqtseries.service import Service


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def running_service(tmp_path):
    ports = {
        name: _free_port()
        for name in ("ingestion", "query", "streaming", "admin", "http", "stats")
    }
    s = Settings(
        database={"path": str(tmp_path / "ws.sqlite"), "batch_size": 500},
        ingestion={"port": ports["ingestion"]},
        query={"port": ports["query"]},
        streaming={"port": ports["streaming"]},
        admin={"port": ports["admin"]},
        http={"port": ports["http"]},
        stats={"port": ports["stats"]},
        ports={"auto_detect": False},
    )
    svc = Service(s)
    await svc.start()
    try:
        yield svc, ports
    finally:
        await svc.shutdown()


def _ws_url(ports, metric="*") -> str:
    return f"ws://127.0.0.1:{ports['http']}/ws/subscribe?metric={metric}"


async def _publish_http(ports, metric: str, value: float, tags=None) -> None:
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{ports['http']}", timeout=30
    ) as c:
        r = await c.post(
            "/api/v1/write", json={"metric": metric, "value": value, "tags": tags}
        )
        assert r.status_code == 200, r.text


class TestSubscribeFunctionality:
    async def test_topic_prefix_filtering(self, running_service):
        """metric=cpu. delivers cpu.load but not mem.load; * delivers both."""

        svc, ports = running_service
        async with websockets.connect(_ws_url(ports, "cpu.")) as ws:
            # Let the subscription register.
            await asyncio.sleep(0.5)
            await svc.pubsub.publish(
                b"cpu.load", {"metric": "cpu.load", "tags": None, "value": 1.0}
            )
            await svc.pubsub.publish(
                b"mem.load", {"metric": "mem.load", "tags": None, "value": 2.0}
            )
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))

            assert msg["metric"] == "cpu.load"
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(ws.recv(), timeout=1.5)

        async with websockets.connect(_ws_url(ports, "*")) as ws:
            await asyncio.sleep(0.5)
            await svc.pubsub.publish(
                b"cpu.load", {"metric": "cpu.load", "tags": None, "value": 1.0}
            )
            await svc.pubsub.publish(
                b"mem.load", {"metric": "mem.load", "tags": None, "value": 2.0}
            )
            seen = set()

            deadline = time.monotonic() + 5

            while time.monotonic() < deadline and seen != {"cpu.load", "mem.load"}:
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))

                seen.add(msg["metric"])
            assert seen == {"cpu.load", "mem.load"}

    async def test_http_write_reaches_subscriber(self, running_service):
        """An HTTP write is broadcast to live subscribers (camera pump path)."""

        _, ports = running_service
        async with websockets.connect(_ws_url(ports, "edge.")) as ws:
            await asyncio.sleep(0.5)
            await _publish_http(ports, "edge.http", 42.5, {"camera_id": "cam"})

            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))

            assert msg["metric"] == "edge.http"
            assert msg["value"] == 42.5
            assert msg["tags"] == {"camera_id": "cam"}

    async def test_keepalive_ping_after_idle(self, running_service):
        """After ~30s idle the server sends {"type":"ping"} (RFC keepalive)."""

        _, ports = running_service
        async with websockets.connect(_ws_url(ports)) as ws:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=35))

            assert msg == {"type": "ping"}

    async def test_message_order_preserved(self, running_service):
        """Frames arrive in publish order."""
        svc, ports = running_service
        async with websockets.connect(_ws_url(ports, "order.")) as ws:
            await asyncio.sleep(0.5)
            for i in range(100):
                await svc.pubsub.publish(
                    b"order.x", {"metric": "order.x", "tags": None, "value": float(i)}
                )
            values = []

            deadline = time.monotonic() + 10

            while len(values) < 100 and time.monotonic() < deadline:
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))

                values.append(msg["value"])
            assert values == [float(i) for i in range(100)]

    async def test_large_payload_round_trip(self, running_service):
        """A ~2 MB frame (large tag value) is delivered intact."""
        _, ports = running_service
        big = "x" * (2 * 1024 * 1024)
        async with websockets.connect(
            _ws_url(ports, "big."), max_size=8 * 1024 * 1024
        ) as ws:
            await asyncio.sleep(0.5)
            await _publish_http(ports, "big.blob", 1.0, {"blob": big})
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))

            assert msg["metric"] == "big.blob"
            assert msg["tags"]["blob"] == big


class TestSubscribeResilience:
    async def test_malformed_client_data_does_not_break_stream(self, running_service):
        """Garbage text / binary from a read-only client is ignored; the

        stream keeps flowing for everyone."""
        svc, ports = running_service
        async with websockets.connect(_ws_url(ports, "robust.")) as ws:
            await asyncio.sleep(0.3)
            await ws.send("this is not json {{{")
            await ws.send(b"\x00\x01\x02garbage")
            await asyncio.sleep(0.2)
            await svc.pubsub.publish(
                b"robust.ok", {"metric": "robust.ok", "tags": None, "value": 7.0}
            )
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))

            assert msg["metric"] == "robust.ok"

    async def test_abrupt_client_disconnect_others_unaffected(self, running_service):
        """A client that drops the transport without a close frame must not

        affect other subscribers."""
        svc, ports = running_service
        async with websockets.connect(_ws_url(ports, "abrupt.")) as good:
            # second client drops abruptly
            bad = await websockets.connect(_ws_url(ports, "abrupt."))
            await asyncio.sleep(0.5)
            transport = bad.transport
            # Hard kill, no close handshake.
            transport.abort()
            await asyncio.sleep(0.5)

            await svc.pubsub.publish(
                b"abrupt.ok", {"metric": "abrupt.ok", "tags": None, "value": 3.0}
            )
            msg = json.loads(await asyncio.wait_for(good.recv(), timeout=5))

            assert msg["metric"] == "abrupt.ok"
            # server-side registry cleaned the dead connection
            await asyncio.sleep(0.2)
            assert svc.connection_registry.ws_count == 1

    async def test_oversized_inbound_frame_closes_connection(self, running_service):
        """A client frame beyond ws_max_size (16 MiB) is rejected (1009) and

        the server stays healthy for others."""
        svc, ports = running_service
        victim = await websockets.connect(_ws_url(ports, "bigin."))

        good = await websockets.connect(_ws_url(ports, "bigin."))
        await asyncio.sleep(0.5)
        try:
            await victim.send("x" * (17 * 1024 * 1024))
            with pytest.raises(websockets.exceptions.ConnectionClosedError) as exc:
                await asyncio.wait_for(victim.recv(), timeout=10)
            assert exc.value.rcvd is not None
            assert exc.value.rcvd.code == 1009
        finally:
            await victim.close()

        # the healthy subscriber still receives frames
        await svc.pubsub.publish(
            b"bigin.ok", {"metric": "bigin.ok", "tags": None, "value": 5.0}
        )
        msg = json.loads(await asyncio.wait_for(good.recv(), timeout=5))

        assert msg["metric"] == "bigin.ok"
        await good.close()

    async def test_slow_subscriber_does_not_block_others(self, running_service):
        """A non-reading subscriber must not stall a reading one (HWM drop,

        not blocking)."""
        svc, ports = running_service
        reader = await websockets.connect(_ws_url(ports, "iso."))

        # Never reads.
        slow = await websockets.connect(_ws_url(ports, "iso."))

        await asyncio.sleep(0.5)

        async def pump():
            for i in range(3000):
                await svc.pubsub.publish(
                    b"iso.x", {"metric": "iso.x", "tags": None, "value": float(i)}
                )
                if i % 200 == 0:
                    # Yield so the reader can drain.
                    await asyncio.sleep(0)

        await asyncio.wait_for(pump(), timeout=20)
        await asyncio.sleep(0.5)

        got = 0

        deadline = time.monotonic() + 5

        while time.monotonic() < deadline:
            try:
                await asyncio.wait_for(reader.recv(), timeout=1)
                got += 1
            except TimeoutError:
                break
        await reader.close()
        await slow.close()
        # the reader received the bulk of the burst despite the slow peer

        assert got >= 2500, f"reader only got {got}/3000"
        assert svc.connection_registry.ws_count == 0

    async def test_rapid_churn_registry_and_listeners_converge(self, running_service):
        """Rapid connect/disconnect leaves no registry or listener residue."""

        svc, ports = running_service
        baseline_listeners = len(svc.connection_registry._listeners)
        for _ in range(15):
            ws = await websockets.connect(_ws_url(ports, "churn."))
            await asyncio.sleep(0.02)
            await ws.close()
        await asyncio.sleep(0.3)
        assert svc.connection_registry.ws_count == 0
        # no listener growth from the churn (stats publisher is the baseline)

        assert len(svc.connection_registry._listeners) == baseline_listeners

    async def test_many_concurrent_subscribers(self, running_service):
        """Every subscriber receives the same published frame."""
        svc, ports = running_service
        subs = [await websockets.connect(_ws_url(ports, "fan.")) for _ in range(15)]

        # Let all subscriptions register.
        await asyncio.sleep(0.7)
        try:
            got = [False] * len(subs)

            async def collect(i, ws):
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))

                got[i] = msg["metric"] == "fan.x"

            tasks = [asyncio.create_task(collect(i, ws)) for i, ws in enumerate(subs)]
            # keep publishing until every subscriber has received (slow-joiner-safe)

            deadline = time.monotonic() + 8

            n = 0

            while not all(got) and time.monotonic() < deadline:
                await svc.pubsub.publish(
                    b"fan.x", {"metric": "fan.x", "tags": None, "value": float(n)}
                )
                n += 1
                await asyncio.sleep(0.01)
            await asyncio.gather(*tasks)
            assert all(got), f"only {sum(got)}/{len(subs)} received"
        finally:
            for ws in subs:
                await ws.close()


class TestStreamingDisabledCloseCode:
    async def test_close_1008_when_no_pubsub(self, tmp_path):
        """/ws/subscribe closes 1008 'streaming disabled' without a pubsub."""

        from fastapi.testclient import TestClient

        from sqtseries.engine import (
            StorageEngine,
            create_sqlite_engine,
            initialize_schema,
        )
        from sqtseries.gateway import create_app

        eng = create_sqlite_engine(str(tmp_path / "no_pubsub.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        app = create_app(store=store)
        # No pubsub, no registry.
        client = TestClient(app)

        with client.websocket_connect("/ws/subscribe?metric=*") as ws:
            msg = ws.receive()
            assert msg["type"] == "websocket.close"
            assert msg["code"] == 1008


class TestConnectionCap:
    async def test_cap_closes_overflow_with_1013(self, tmp_path):
        """Beyond http.max_websocket_connections, new WebSocket connections

        are accepted then closed with 1013 ('try again later'), and slots are

        released when a connection ends."""
        import socket as _socket

        from sqtseries.config import Settings as S

        def free_port():
            s = _socket.socket()
            s.bind(("127.0.0.1", 0))
            p = s.getsockname()[1]
            s.close()
            return p

        ports = {
            n: free_port()
            for n in ("ingestion", "query", "streaming", "admin", "http", "stats")
        }
        settings = S(
            database={"path": str(tmp_path / "cap.sqlite"), "batch_size": 500},
            ingestion={"port": ports["ingestion"]},
            query={"port": ports["query"]},
            streaming={"port": ports["streaming"]},
            admin={"port": ports["admin"]},
            http={"port": ports["http"], "max_websocket_connections": 2},
            stats={"port": ports["stats"]},
            ports={"auto_detect": False},
        )
        svc = Service(settings)
        await svc.start()
        try:
            url = f"ws://127.0.0.1:{ports['http']}/ws/subscribe?metric=cap."
            a = await websockets.connect(url)
            b = await websockets.connect(url)
            # Let both register.
            await asyncio.sleep(0.3)
            # third connection exceeds the cap -> 1013
            with pytest.raises(websockets.exceptions.ConnectionClosedError) as exc:
                c = await websockets.connect(url)
                await c.recv()
            assert exc.value.rcvd is not None
            assert exc.value.rcvd.code == 1013

            # closing one frees a slot: the next connect succeeds
            await a.close()
            await b.close()
            await asyncio.sleep(0.2)
            d = await websockets.connect(url)
            await d.close()
        finally:
            await svc.shutdown()
