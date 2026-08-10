"""Tests for the connection registry, stats publisher, and admin commands."""

import asyncio

import orjson
import pytest
import zmq
import zmq.asyncio
from conftest import free_port

from sqtseries.config import Settings
from sqtseries.messaging.connection_registry import (
    ConnectionRegistry,
    new_connection_id,
)
from sqtseries.messaging.pubsub import PubSub
from sqtseries.messaging.stats_publisher import StatsPublisher
from sqtseries.service import Service


class TestConnectionRegistry:
    def test_register_unregister_ws(self):
        reg = ConnectionRegistry()
        assert reg.ws_count == 0

        reg.register_ws("abc123", "127.0.0.1:55555", "cpu.usage")
        assert reg.ws_count == 1
        assert reg.check_connection("abc123") is True
        assert reg.check_connection("nonexistent") is False

        reg.unregister_ws("abc123")
        assert reg.ws_count == 0
        assert reg.check_connection("abc123") is False

    def test_unregister_ws_idempotent(self):
        reg = ConnectionRegistry()
        reg.unregister_ws("nonexistent")

    def test_list_connections(self):
        reg = ConnectionRegistry()
        reg.register_ws("a", "1.1.1.1:1", "cpu")
        reg.register_ws("b", "2.2.2.2:2", "mem")
        conns = reg.list_connections()
        assert len(conns) == 2
        ids = [c["id"] for c in conns]
        assert "a" in ids and "b" in ids

    def test_zmq_sub_tracking(self):
        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu.usage")
        reg.register_zmq_sub("cpu.usage")
        reg.register_zmq_sub("mem.usage")
        assert reg.zmq_sub_count == 3
        assert reg.subscriber_count("cpu.usage") == 2
        assert reg.subscriber_count("mem.usage") == 1
        assert reg.subscriber_count() == 3

        reg.unregister_zmq_sub("cpu.usage")
        assert reg.subscriber_count("cpu.usage") == 1
        assert reg.zmq_sub_count == 2

        reg.unregister_zmq_sub("cpu.usage")
        assert reg.subscriber_count("cpu.usage") == 0
        assert reg.zmq_sub_count == 1

        # topic gone entirely
        reg.unregister_zmq_sub("mem.usage")
        assert reg.zmq_sub_count == 0
        assert reg.subscriber_count("mem.usage") == 0

    def test_active_topics(self):
        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        reg.register_zmq_sub("mem")
        assert reg.active_topics() == ["cpu", "mem"]

    def test_snapshot(self):
        reg = ConnectionRegistry()
        reg.register_ws("x", "peer", "topic")
        reg.register_zmq_sub("test")
        snap = reg.snapshot()
        assert snap["ws_connections"] == 1
        assert snap["zmq_subscribers"] == 1
        assert snap["active_topics"] == 1
        assert len(snap["connections"]) == 1
        assert len(snap["subscriptions"]) == 1
        assert snap["subscriptions"][0]["subscribers"] == 1

    def test_event_callbacks(self):
        events = []

        def collector(etype, payload):
            events.append((etype, payload))

        reg = ConnectionRegistry()
        reg.on_event(collector)
        reg.register_ws("abc", "1.2.3.4:5", "cpu")
        assert len(events) == 1
        assert events[0][0] == "conn"
        assert events[0][1]["connected"] is True
        assert events[0][1]["kind"] == "ws"

        reg.unregister_ws("abc")
        assert len(events) == 2
        assert events[1][0] == "conn"
        assert events[1][1]["connected"] is False

        reg.register_zmq_sub("cpu")
        assert events[2][0] == "sub"
        assert events[2][1]["subscribers"] == 1

    def test_new_connection_id(self):
        cid = new_connection_id()
        assert isinstance(cid, str)
        assert len(cid) == 12

    def test_ws_churn_no_accumulation(self):
        """Repeated connect/disconnect must never grow the registry."""
        reg = ConnectionRegistry()
        for i in range(5000):
            reg.register_ws(f"c{i}", "peer", "t")
            reg.unregister_ws(f"c{i}")
        assert reg.ws_count == 0
        assert reg.list_connections() == []

    def test_remove_listener_idempotent(self):
        events = []

        def collector(etype, payload):
            events.append(etype)

        reg = ConnectionRegistry()
        reg.on_event(collector)
        reg.remove_listener(collector)
        # removing a non-registered listener must not raise
        reg.remove_listener(collector)
        reg.register_ws("x", "peer", "t")
        assert events == []

    def test_event_timestamps(self):
        """Events carry arrival and leaving times: ws conn events have
        connected_at + left_at (left_at >= connected_at); zmq sub events have
        arrived_at + left_at + first_seen (left_at >= first_seen)."""
        events: list[tuple[str, dict]] = []
        reg = ConnectionRegistry()
        reg.on_event(lambda etype, payload: events.append((etype, payload)))

        reg.register_ws("c1", "peer", "t")
        reg.register_zmq_sub("cpu.")
        assert len(events) == 2
        _, conn = events[0]
        _, sub = events[1]
        assert isinstance(conn["connected_at"], float)
        assert isinstance(sub["arrived_at"], float)

        import time as _t

        _t.sleep(0.01)
        reg.unregister_ws("c1")
        reg.unregister_zmq_sub("cpu.")
        _, conn_leave = events[2]
        _, sub_leave = events[3]
        assert conn_leave["connected"] is False
        assert isinstance(conn_leave["connected_at"], float)
        assert isinstance(conn_leave["left_at"], float)
        assert conn_leave["left_at"] >= conn_leave["connected_at"]
        assert sub_leave["subscribers"] == 0
        assert isinstance(sub_leave["left_at"], float)
        assert isinstance(sub_leave["first_seen"], float)
        assert sub_leave["left_at"] >= sub_leave["first_seen"]

    def test_unregister_unknown_emits_nothing(self):
        """Leaving for an id/topic that is not present emits no event and
        converges to zero (never negative)."""
        events: list[tuple[str, dict]] = []
        reg = ConnectionRegistry()
        reg.on_event(lambda etype, payload: events.append((etype, payload)))

        reg.unregister_ws("ghost")  # unknown ws id -> no event
        reg.unregister_zmq_sub("cpu.")  # unknown topic -> still an event, count 0
        assert len(events) == 1
        assert events[0][0] == "sub"
        assert events[0][1]["subscribers"] == 0
        assert reg.ws_count == 0 and reg.zmq_sub_count == 0

    def test_zmq_first_seen_round_trip(self):
        """first_seen is set on 0->1, cleared on 1->0, and re-set on a later join."""
        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu.")
        first = reg._zmq_first_seen["cpu."]
        reg.register_zmq_sub("cpu.")  # 2nd subscriber: first_seen unchanged
        assert reg._zmq_first_seen["cpu."] == first
        reg.unregister_zmq_sub("cpu.")
        reg.unregister_zmq_sub("cpu.")
        assert "cpu." not in reg._zmq_first_seen
        reg.register_zmq_sub("cpu.")  # rejoin: fresh first_seen
        assert reg._zmq_first_seen["cpu."] >= first
        assert reg.snapshot()["subscriptions"][0]["first_seen"] is not None


class TestStatsPublisher:
    async def test_start_stop(self):
        reg = ConnectionRegistry()
        pub = StatsPublisher("tcp://127.0.0.1:15506", registry=reg)
        await pub.start()
        assert pub.socket is not None
        await pub.stop()
        assert pub.socket is None

    async def test_publish_events(self):
        reg = ConnectionRegistry()
        pub = StatsPublisher("tcp://127.0.0.1:15507", registry=reg)
        await pub.start()

        ctx = zmq.Context()
        sub = ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.LINGER, 0)
        sub.connect("tcp://127.0.0.1:15507")
        sub.setsockopt(zmq.SUBSCRIBE, b"")
        await asyncio.sleep(0.1)

        await pub.publish("conn", {"id": "x", "connected": True})
        await asyncio.sleep(0.1)

        for _ in range(20):
            events = sub.poll(100, zmq.POLLIN)
            if not events:
                break
            _topic, payload = sub.recv_multipart()
            data = orjson.loads(payload)
            if data.get("id") == "x":
                assert data["connected"] is True
                break
        else:
            pytest.fail("did not receive stats event")

        sub.close(linger=0)
        ctx.term()
        await pub.stop()

    async def test_publish_after_stop(self):
        """Publish after stop is a no-op (socket=None guard)."""
        reg = ConnectionRegistry()
        pub = StatsPublisher("tcp://127.0.0.1:15509", registry=reg)
        await pub.start()
        await pub.stop()
        await pub.publish("conn", {"test": True})

    async def test_emit_report(self):
        """Internal report emission through PUB socket does not raise."""
        reg = ConnectionRegistry()
        pub = StatsPublisher("tcp://127.0.0.1:15510", registry=reg)
        await pub.start()
        report = {"ws_connections": 0, "zmq_subscribers": 0, "active_topics": 0}
        await pub._emit_report(report)
        await pub.stop()

    async def test_on_registry_event_after_stop(self):
        """Registry events are no-ops after publisher is stopped."""
        reg = ConnectionRegistry()
        pub = StatsPublisher("tcp://127.0.0.1:15511", registry=reg)
        await pub.start()
        await pub.stop()
        reg.register_ws("ghost", "peer", "topic")

    async def test_stop_unhooks_registry_listener(self):
        """Repeated start/stop must not accumulate registry listeners.

        Regression: each start() registered a fresh listener on the shared
        registry and stop() never removed it, so N cycles left N dead
        listeners (and N dead publisher objects) behind.
        """
        from conftest import free_port

        reg = ConnectionRegistry()
        for _ in range(5):
            pub = StatsPublisher(f"tcp://127.0.0.1:{free_port()}", registry=reg)
            await pub.start()
            assert len(reg._listeners) == 1
            await pub.stop()
            assert len(reg._listeners) == 0


class TestPubSubXpub:
    async def test_xpub_subscription_tracking(self):
        """A real SUB client subscribing should update the registry."""
        port = free_port()
        reg = ConnectionRegistry()
        pubsub = PubSub(f"tcp://127.0.0.1:{port}", registry=reg)
        await pubsub.start()
        await asyncio.sleep(0.1)

        ctx = zmq.Context()
        sub_sock = ctx.socket(zmq.SUB)
        sub_sock.setsockopt(zmq.LINGER, 0)
        sub_sock.connect(f"tcp://127.0.0.1:{port}")
        sub_sock.setsockopt(zmq.SUBSCRIBE, b"cpu.")
        await asyncio.sleep(0.2)

        assert reg.subscriber_count("cpu.") >= 1

        sub_sock.close(linger=0)
        ctx.term()
        await asyncio.sleep(0.2)

        # After TCP close, ZMQ should auto-unsubscribe via XPUB
        assert reg.subscriber_count("cpu.") == 0

        await pubsub.stop()


class TestAdminCommands:
    @pytest.fixture
    async def svc(self, tmp_path, free_ports):
        s = Settings(
            database={"path": str(tmp_path / "admin_test.sqlite"), "batch_size": 500},
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
        await svc.pool.stop()
        pump_task = asyncio.create_task(self._pump(svc))
        try:
            yield svc
        finally:
            pump_task.cancel()
            await asyncio.gather(pump_task, return_exceptions=True)
            await svc.shutdown()

    async def _pump(self, svc):
        while True:
            await svc.ingress.run_once(block=False)
            await svc.broker.run_once(block=False)
            await svc.admin_broker.run_once(block=False)
            await asyncio.sleep(0.005)

    async def test_admin_connections(self, svc):
        """Admin connections returns an empty list when no WS clients."""
        reply = svc._admin_handler({"cmd": "connections"})
        assert reply["status"] == "ok"
        assert reply["data"] == []

    async def test_admin_conncheck_empty(self, svc):
        reply = svc._admin_handler({"cmd": "conncheck", "ids": []})
        assert reply["status"] == "ok"
        assert reply["present"] == []

    async def test_admin_subscribers_empty(self, svc):
        reply = svc._admin_handler({"cmd": "subscribers"})
        assert reply["status"] == "ok"
        assert reply["zmq_subscribers"] == 0
        assert reply["subscriptions"] == []

    async def test_admin_connections_after_ws(self, svc):
        """When a WS connects, connections reflects it (via registry)."""
        reg = svc.connection_registry
        reg.register_ws("test-conn", "10.0.0.1:9999", "test.topic")
        reply = svc._admin_handler({"cmd": "connections"})
        assert len(reply["data"]) == 1
        assert reply["data"][0]["id"] == "test-conn"
        assert reply["data"][0]["kind"] == "ws"

    async def test_admin_conncheck_present(self, svc):
        reg = svc.connection_registry
        reg.register_ws("keep", "peer", "t")
        reply = svc._admin_handler({"cmd": "conncheck", "ids": ["keep", "drop"]})
        assert reply["present"] == ["keep"]

    async def test_admin_subscribers_after_zmq_sub(self, svc):
        reg = svc.connection_registry
        reg.register_zmq_sub("cpu")
        reg.register_zmq_sub("cpu")
        reg.register_zmq_sub("mem")
        reply = svc._admin_handler({"cmd": "subscribers"})
        assert reply["zmq_subscribers"] == 3
        subs = {s["topic"]: s["subscribers"] for s in reply["subscriptions"]}
        assert subs == {"cpu": 2, "mem": 1}

    async def test_admin_conncheck_zmq_socket(self, svc):
        """ZMQ SUB clients are tracked by XPUB (topic-level), not connection IDs."""
        port = svc.settings.streaming.port

        ctx = zmq.Context()
        sub = ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.LINGER, 0)
        sub.connect(f"tcp://127.0.0.1:{port}")
        sub.setsockopt(zmq.SUBSCRIBE, b"admin.test")
        await asyncio.sleep(0.3)

        reply = svc._admin_handler({"cmd": "subscribers"})
        assert reply["zmq_subscribers"] >= 1

        sub.close(linger=0)
        ctx.term()
        await asyncio.sleep(0.3)

        reply = svc._admin_handler({"cmd": "subscribers"})
        assert reply["zmq_subscribers"] == 0

    async def test_config_stats_port(self, svc):
        # port comes from the OS-assigned free port (never a fixed number)
        assert isinstance(svc.settings.stats.port, int)
        assert 1024 <= svc.settings.stats.port <= 65535
        assert svc.settings.stats.enabled is True
