"""Tests for the connection registry, stats publisher, and admin commands."""

import asyncio
import time

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

        # Unknown ws id -> no event.
        reg.unregister_ws("ghost")
        # Unknown topic -> still an event, count 0.
        reg.unregister_zmq_sub("cpu.")

        assert len(events) == 1
        assert events[0][0] == "sub"
        assert events[0][1]["subscribers"] == 0
        assert reg.ws_count == 0 and reg.zmq_sub_count == 0

    def test_zmq_first_seen_round_trip(self):
        """first_seen is set on 0->1, cleared on 1->0, and re-set on a later join."""

        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu.")
        first = reg._zmq_first_seen["cpu."]
        # 2nd subscriber: first_seen unchanged.
        reg.register_zmq_sub("cpu.")

        assert reg._zmq_first_seen["cpu."] == first
        reg.unregister_zmq_sub("cpu.")
        reg.unregister_zmq_sub("cpu.")
        assert "cpu." not in reg._zmq_first_seen
        # Rejoin: fresh first_seen.
        reg.register_zmq_sub("cpu.")
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

    async def test_topic_totals_count_publishes(self):
        """PubSub counts publish attempts per topic."""

        port = free_port()

        reg = ConnectionRegistry()
        pubsub = PubSub(f"tcp://127.0.0.1:{port}", registry=reg)
        await pubsub.start()
        try:
            for _ in range(3):
                await pubsub.publish("cpu", {"metric": "cpu", "value": 1.0})
            await pubsub.publish("mem", {"metric": "mem", "value": 2.0})
            assert pubsub.topic_totals() == {"cpu": 3, "mem": 1}
        finally:
            await pubsub.stop()

    async def test_sigkilled_subscriber_evicts(self):
        """A subscriber killed without closing still leaves the registry."""
        import signal
        import subprocess
        import sys

        port = free_port()

        reg = ConnectionRegistry()
        pubsub = PubSub(f"tcp://127.0.0.1:{port}", registry=reg)
        await pubsub.start()
        await asyncio.sleep(0.1)

        proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time, zmq; "
                "c = zmq.Context(); s = c.socket(zmq.SUB); "
                f"s.connect('tcp://127.0.0.1:{port}'); "
                "s.setsockopt(zmq.SUBSCRIBE, b'dead.peer'); "
                "time.sleep(60)",
            ],
            start_new_session=True,
        )
        try:
            deadline = time.monotonic() + 10
            while reg.subscriber_count("dead.peer") < 1:
                assert time.monotonic() < deadline, "join never registered"
                await asyncio.sleep(0.1)

            proc.send_signal(signal.SIGKILL)
            await asyncio.sleep(0.2)
            assert proc.poll() is not None

            deadline = time.monotonic() + 25
            while reg.subscriber_count("dead.peer") != 0:
                assert time.monotonic() < deadline, "dead peer never evicted"
                await asyncio.sleep(0.5)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
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
            await svc.ingress.drain_many()
            await svc.ingress.flush()
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


class TestConnectionEdgeCases:
    """Edge cases for join/leave, event emission, and registry consistency."""

    def test_register_same_id_twice_overwrites(self):
        """Registering the same conn_id twice overwrites the first entry."""

        reg = ConnectionRegistry()
        reg.register_ws("dup", "1.1.1.1:1", "cpu")
        reg.register_ws("dup", "2.2.2.2:2", "mem")
        assert reg.ws_count == 1
        conns = reg.list_connections()
        assert conns[0]["peer"] == "2.2.2.2:2"
        assert conns[0]["topic"] == "mem"

    def test_unregister_then_reregister_same_id(self):
        """Unregister then re-register the same ID gives a clean lifecycle."""

        reg = ConnectionRegistry()
        reg.register_ws("cycle", "peer", "t")
        assert reg.ws_count == 1
        reg.unregister_ws("cycle")
        assert reg.ws_count == 0
        reg.register_ws("cycle", "peer2", "t2")
        assert reg.ws_count == 1
        conns = reg.list_connections()
        assert conns[0]["peer"] == "peer2"

    def test_touch_ws_unknown_id_no_crash(self):
        """touch_ws on an unknown ID must not raise."""
        reg = ConnectionRegistry()
        # No crash on unknown id.
        reg.touch_ws("nonexistent")

    def test_touch_ws_updates_last_activity(self):
        """touch_ws refreshes last_activity_at."""
        import time

        reg = ConnectionRegistry()
        reg.register_ws("c1", "peer", "t")
        original = reg.list_connections()[0]["last_activity_at"]
        time.sleep(0.02)
        reg.touch_ws("c1")
        updated = reg.list_connections()[0]["last_activity_at"]
        assert updated > original

    def test_multiple_connections_same_topic(self):
        """Multiple WS connections on the same topic are independent."""

        reg = ConnectionRegistry()
        reg.register_ws("a", "peer-a", "cpu")
        reg.register_ws("b", "peer-b", "cpu")
        reg.register_ws("c", "peer-c", "mem")
        assert reg.ws_count == 3
        reg.unregister_ws("a")
        assert reg.ws_count == 2
        remaining = [c["id"] for c in reg.list_connections()]
        assert "b" in remaining and "c" in remaining
        assert "a" not in remaining

    def test_ws_empty_peer(self):
        """Connection with empty peer string is accepted."""
        reg = ConnectionRegistry()
        reg.register_ws("x", "", "topic")
        conns = reg.list_connections()
        assert conns[0]["peer"] == ""

    def test_ws_unicode_topic(self):
        """Connection with Unicode topic works."""
        reg = ConnectionRegistry()
        reg.register_ws("u", "peer", "温度.传感器")
        conns = reg.list_connections()
        assert conns[0]["topic"] == "温度.传感器"

    def test_ws_long_topic(self):
        """Connection with very long topic string is accepted."""
        reg = ConnectionRegistry()

        long_topic = "a" * 10000
        reg.register_ws("long", "peer", long_topic)
        conns = reg.list_connections()
        assert conns[0]["topic"] == long_topic

    def test_ws_register_unregister_register_cycle(self):
        """Full lifecycle: register -> unregister -> register -> unregister."""

        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))

        reg.register_ws("x", "peer", "t")
        reg.unregister_ws("x")
        reg.register_ws("x", "peer2", "t2")
        reg.unregister_ws("x")

        assert reg.ws_count == 0
        conn_events = [e for e in events if e[0] == "conn"]
        assert len(conn_events) == 4  # 2 connects + 2 disconnects
        assert conn_events[0][1]["connected"] is True
        assert conn_events[1][1]["connected"] is False
        assert conn_events[2][1]["connected"] is True
        assert conn_events[3][1]["connected"] is False

    def test_list_connections_sorted_by_connected_at(self):
        """list_connections returns entries sorted by connected_at."""
        import time

        reg = ConnectionRegistry()
        reg.register_ws("first", "peer", "t")
        time.sleep(0.01)
        reg.register_ws("second", "peer", "t")
        conns = reg.list_connections()
        assert conns[0]["id"] == "first"
        assert conns[1]["id"] == "second"

    def test_ws_1000_connections(self):
        """1000 concurrent connections tracked correctly."""
        reg = ConnectionRegistry()
        for i in range(1000):
            reg.register_ws(f"ws{i}", f"peer{i}", "topic")
        assert reg.ws_count == 1000
        for i in range(1000):
            reg.unregister_ws(f"ws{i}")
        assert reg.ws_count == 0

    def test_unregister_ws_event_payload_fields(self):
        """Unregister event contains all expected fields."""
        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.register_ws("c1", "1.2.3.4:8080", "cpu")
        reg.unregister_ws("c1")
        leave = events[1][1]
        assert leave["kind"] == "ws"
        assert leave["peer"] == "1.2.3.4:8080"
        assert leave["topic"] == "cpu"
        assert leave["id"] == "c1"
        assert leave["connected"] is False
        assert isinstance(leave["connected_at"], float)
        assert isinstance(leave["left_at"], float)
        assert leave["left_at"] >= leave["connected_at"]

    def test_zmq_unsubscribe_more_than_subscribed(self):
        """Unsubscribing more times than subscribed clamps to 0."""
        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        reg.unregister_zmq_sub("cpu")
        reg.unregister_zmq_sub("cpu")
        reg.unregister_zmq_sub("cpu")
        assert reg.subscriber_count("cpu") == 0
        assert reg.zmq_sub_count == 0

    def test_zmq_rapid_subscribe_unsubscribe_no_drift(self):
        """1000 rapid subscribe/unsubscribe cycles end at 0."""
        reg = ConnectionRegistry()
        for _ in range(1000):
            reg.register_zmq_sub("cpu")
            reg.unregister_zmq_sub("cpu")
        assert reg.subscriber_count("cpu") == 0
        assert reg.zmq_sub_count == 0
        assert "cpu" not in reg.active_topics()

    def test_zmq_multiple_topics_independent(self):
        """Each topic has its own independent count."""
        reg = ConnectionRegistry()
        reg.register_zmq_sub("a")
        reg.register_zmq_sub("a")
        reg.register_zmq_sub("a")
        reg.register_zmq_sub("b")
        assert reg.subscriber_count("a") == 3
        assert reg.subscriber_count("b") == 1
        reg.unregister_zmq_sub("a")
        assert reg.subscriber_count("a") == 2
        assert reg.subscriber_count("b") == 1

    def test_zmq_subscribe_after_full_unsubscribe(self):
        """After all subscribers leave, re-subscribe starts fresh."""
        reg = ConnectionRegistry()
        reg.register_zmq_sub("x")
        reg.register_zmq_sub("x")
        reg.unregister_zmq_sub("x")
        reg.unregister_zmq_sub("x")
        assert "x" not in reg._zmq_first_seen
        reg.register_zmq_sub("x")
        assert reg.subscriber_count("x") == 1
        assert "x" in reg._zmq_first_seen

    def test_zmq_1000_subscribers_same_topic(self):
        """1000 subscribers on the same topic counted accurately."""
        reg = ConnectionRegistry()
        for _ in range(1000):
            reg.register_zmq_sub("hot Topic")
        assert reg.subscriber_count("hot Topic") == 1000
        assert reg.zmq_sub_count == 1000
        for _ in range(1000):
            reg.unregister_zmq_sub("hot Topic")
        assert reg.subscriber_count("hot Topic") == 0
        assert reg.zmq_sub_count == 0

    def test_zmq_subscriber_count_none_returns_total(self):
        """subscriber_count(None) returns total across all topics."""
        reg = ConnectionRegistry()
        reg.register_zmq_sub("a")
        reg.register_zmq_sub("a")
        reg.register_zmq_sub("b")
        assert reg.subscriber_count() == 3

    def test_zmq_active_topics_empty_when_no_subs(self):
        """active_topics returns empty list when nothing is subscribed."""

        reg = ConnectionRegistry()
        assert reg.active_topics() == []

    def test_zmq_active_topics_sorted(self):
        """active_topics returns topics in sorted order."""
        reg = ConnectionRegistry()
        reg.register_zmq_sub("z Topic")
        reg.register_zmq_sub("a Topic")
        reg.register_zmq_sub("m Topic")
        assert reg.active_topics() == ["a Topic", "m Topic", "z Topic"]

    def test_zmq_unsubscribe_event_payload_fields(self):
        """Unsubscribe event contains all expected fields."""
        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.register_zmq_sub("cpu")
        reg.unregister_zmq_sub("cpu")
        leave = events[1][1]
        assert leave["kind"] == "zmq"
        assert leave["topic"] == "cpu"
        assert leave["subscribers"] == 0
        assert isinstance(leave["left_at"], float)
        assert isinstance(leave["first_seen"], float)
        assert leave["left_at"] >= leave["first_seen"]

    def test_zmq_subscribe_event_payload_fields(self):
        """Subscribe event contains all expected fields."""
        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.register_zmq_sub("mem")
        join = events[0][1]
        assert join["kind"] == "zmq"
        assert join["topic"] == "mem"
        assert join["subscribers"] == 1
        assert isinstance(join["arrived_at"], float)
        assert "ttl" in join

    def test_mixed_ws_and_zmq_events_no_cross_contamination(self):
        """WS and ZMQ events are independent."""
        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))

        reg.register_ws("ws1", "peer", "topic")
        reg.register_zmq_sub("topic")
        reg.register_ws("ws2", "peer", "topic")
        reg.register_zmq_sub("topic")

        conn_events = [e for e in events if e[0] == "conn"]

        sub_events = [e for e in events if e[0] == "sub"]
        assert len(conn_events) == 2
        assert len(sub_events) == 2

        reg.unregister_ws("ws1")
        reg.unregister_zmq_sub("topic")

        conn_events = [e for e in events if e[0] == "conn"]

        sub_events = [e for e in events if e[0] == "sub"]
        assert len(conn_events) == 3  # 2 joins + 1 leave
        assert len(sub_events) == 3  # 2 joins + 1 leave

    def test_listener_exception_does_not_break_other_listeners(self):
        """A listener that throws does not prevent other listeners from firing."""

        good_events = []

        def bad_listener(etype, payload):
            raise RuntimeError("boom")

        def good_listener(etype, payload):
            good_events.append(etype)

        reg = ConnectionRegistry()
        reg.on_event(bad_listener)
        reg.on_event(good_listener)
        reg.register_ws("x", "peer", "t")
        assert len(good_events) == 1
        assert good_events[0] == "conn"

    def test_multiple_listeners_all_receive_events(self):
        """All registered listeners receive every event."""
        a_events = []

        b_events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: a_events.append(et))
        reg.on_event(lambda et, p: b_events.append(et))
        reg.register_ws("x", "peer", "t")
        reg.unregister_ws("x")
        assert len(a_events) == 2
        assert len(b_events) == 2

    def test_remove_listener_during_emit(self):
        """Removing a listener while events are being emitted is safe."""

        events = []

        reg = ConnectionRegistry()

        def remover(etype, payload):
            reg.remove_listener(remover)

        reg.on_event(remover)
        reg.on_event(lambda et, p: events.append(et))

        # Remover fires, removes itself.
        reg.register_ws("x", "peer", "t")

        # Remover not called again.
        reg.unregister_ws("x")
        # Both conn events received by second listener.
        assert len(events) == 2

    def test_no_listeners_no_crash(self):
        """Emitting with zero listeners is a no-op."""
        reg = ConnectionRegistry()
        # No listeners, no crash.
        reg.register_ws("x", "peer", "t")
        reg.unregister_ws("x")

    def test_snapshot_reflects_state_at_call_time(self):
        """Snapshot shows the state when called, not when created."""
        reg = ConnectionRegistry()
        reg.register_ws("a", "peer", "t")
        snap1 = reg.snapshot()
        assert snap1["ws_connections"] == 1

        reg.register_ws("b", "peer", "t")
        snap2 = reg.snapshot()
        assert snap2["ws_connections"] == 2
        # snap1 is a separate dict.
        assert snap1["ws_connections"] == 1

    def test_snapshot_empty_registry(self):
        """Snapshot of empty registry returns zeroed counts."""
        reg = ConnectionRegistry()
        snap = reg.snapshot()
        assert snap["ws_connections"] == 0
        assert snap["zmq_subscribers"] == 0
        assert snap["active_topics"] == 0
        assert snap["connections"] == []
        assert snap["subscriptions"] == []

    def test_connection_id_uniqueness(self):
        """Generated connection IDs are unique over many calls."""
        from sqtseries.messaging.connection_registry import new_connection_id

        ids = {new_connection_id() for _ in range(10000)}
        # All unique.
        assert len(ids) == 10000

    def test_connection_id_format(self):
        """Connection ID is a 12-char hex string."""
        from sqtseries.messaging.connection_registry import new_connection_id

        cid = new_connection_id()
        assert len(cid) == 12
        assert all(c in "0123456789abcdef" for c in cid)


class TestDeepEdgeCases:
    """Thorough edge cases derived from line-by-line code path analysis.

    Categories:
    - API boundary / input validation
    - State transition correctness
    - Data integrity (shallow copy leaks)
    - Listener lifecycle anomalies
    - SubscriptionTracker direct testing
    - StatsPublisher edge cases
    """

    def test_subscriber_count_empty_string_returns_total(self):
        """subscriber_count('') treats empty string as falsy → returns total.

        This is an API quirk: empty string is falsy in Python, so
        ``if topic:`` falls through to the total path. If a caller wants

        to count subscribers for an actual empty-string topic, they can't.

        """
        reg = ConnectionRegistry()
        reg.register_zmq_sub("a")
        reg.register_zmq_sub("b")
        # Empty string → total (falsy path)
        assert reg.subscriber_count("") == 2
        # None → total (explicit None path)
        assert reg.subscriber_count(None) == 2
        # Actual topic → count for that topic
        assert reg.subscriber_count("a") == 1

    def test_subscriber_count_zero_returns_total(self):
        """subscriber_count(0) treats 0 as falsy → returns total."""
        reg = ConnectionRegistry()
        reg.register_zmq_sub("x")
        assert reg.subscriber_count(0) == 1  # 0 is falsy → total

    def test_subscriber_count_no_topics_returns_zero(self):
        """subscriber_count returns 0 when no topics exist."""
        reg = ConnectionRegistry()
        assert reg.subscriber_count() == 0
        assert reg.subscriber_count("any") == 0

    def test_same_callback_registered_twice_fires_twice(self):
        """Registering the same callback twice makes it fire twice per event."""

        calls = []

        reg = ConnectionRegistry()

        def listener(etype, payload):
            calls.append(etype)

        reg.on_event(listener)
        # Duplicate.
        reg.on_event(listener)
        reg.register_ws("x", "peer", "t")
        # Listener fired twice.
        assert len(calls) == 2

    def test_remove_listener_only_removes_first_occurrence(self):
        """remove_listener removes only the first occurrence of a duplicate."""

        calls = []

        reg = ConnectionRegistry()

        def listener(etype, payload):
            calls.append(etype)

        reg.on_event(listener)
        # Duplicate.
        reg.on_event(listener)
        # Removes first only.
        reg.remove_listener(listener)
        reg.register_ws("x", "peer", "t")
        # Second copy still fires.
        assert len(calls) == 1

    def test_remove_all_copies_of_duplicate(self):
        """Removing twice removes both copies of a duplicate listener."""

        calls = []

        reg = ConnectionRegistry()

        def listener(etype, payload):
            calls.append(etype)

        reg.on_event(listener)
        reg.on_event(listener)
        reg.remove_listener(listener)
        reg.remove_listener(listener)
        reg.register_ws("x", "peer", "t")
        # Both copies removed.
        assert len(calls) == 0

    def test_listener_adds_listener_during_emit(self):
        """A listener that adds a new listener mid-emit: new listener does NOT

        fire on the current event (snapshot), fires on next."""
        calls = []

        reg = ConnectionRegistry()

        def adder(etype, payload):
            calls.append("adder")
            reg.on_event(lambda et, p: calls.append("added"))

        reg.on_event(adder)
        # Adder fires, adds "added".
        reg.register_ws("x", "peer", "t")
        # "added" was in the snapshot? No — snapshot was taken before emit.
        # Actually the snapshot IS list(self._listeners) at the time of _emit.
        # adder appends to self._listeners, but the snapshot was already taken.
        # So "added" does NOT fire on this event.
        assert calls == ["adder"]

        calls.clear()
        # Now "added" is in the snapshot.
        reg.register_ws("y", "peer", "t")
        # Both adder and added fire, and adder adds another "added"
        assert "adder" in calls
        assert "added" in calls

    def test_listener_removes_different_listener_during_emit(self):
        """Listener A removes listener B during emit. B still fires on this

        event (snapshot was taken), but not on the next."""
        calls_a = []

        calls_b = []

        reg = ConnectionRegistry()

        def listener_a(etype, payload):
            calls_a.append(etype)
            reg.remove_listener(listener_b)

        def listener_b(etype, payload):
            calls_b.append(etype)

        reg.on_event(listener_a)
        reg.on_event(listener_b)

        # Snapshot [a, b] — both fire. A removes B from _listeners.
        reg.register_ws("x", "peer", "t")
        assert len(calls_a) == 1
        # B fired because snapshot included it.
        assert len(calls_b) == 1

        calls_a.clear()
        calls_b.clear()
        # Now _listeners = [a] — only A fires.
        reg.register_ws("y", "peer", "t")
        assert len(calls_a) == 1
        # B was removed.
        assert len(calls_b) == 0

    def test_empty_conn_id_register_unregister(self):
        """Empty string is a valid conn_id — register/unregister works."""

        reg = ConnectionRegistry()
        reg.register_ws("", "peer", "topic")
        assert reg.ws_count == 1
        assert reg.check_connection("") is True
        reg.unregister_ws("")
        assert reg.ws_count == 0
        assert reg.check_connection("") is False

    def test_empty_conn_id_overwrite(self):
        """Two connections with empty ID overwrite each other."""
        reg = ConnectionRegistry()
        reg.register_ws("", "peer1", "t1")
        reg.register_ws("", "peer2", "t2")
        assert reg.ws_count == 1
        assert reg.list_connections()[0]["peer"] == "peer2"

    def test_zmq_first_seen_set_on_first_subscribe(self):
        """first_seen is set when topic goes 0 -> 1."""
        reg = ConnectionRegistry()

        before = time.time()
        reg.register_zmq_sub("cpu")
        after = time.time()
        assert before <= reg._zmq_first_seen["cpu"] <= after

    def test_zmq_first_seen_not_updated_on_additional_subscribe(self):
        """first_seen does NOT change when topic goes 1 -> 2."""
        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        first = reg._zmq_first_seen["cpu"]
        time.sleep(0.01)
        reg.register_zmq_sub("cpu")
        # Unchanged.
        assert reg._zmq_first_seen["cpu"] == first

    def test_zmq_first_seen_cleared_when_count_reaches_zero(self):
        """first_seen is removed from dict when last subscriber leaves."""

        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        assert "cpu" in reg._zmq_first_seen
        reg.unregister_zmq_sub("cpu")
        assert "cpu" not in reg._zmq_first_seen

    def test_zmq_first_seen_preserved_when_count_above_zero(self):
        """first_seen is NOT cleared when count goes 2 -> 1."""
        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        reg.register_zmq_sub("cpu")
        first = reg._zmq_first_seen["cpu"]
        reg.unregister_zmq_sub("cpu")
        assert "cpu" in reg._zmq_first_seen
        assert reg._zmq_first_seen["cpu"] == first

    def test_zmq_first_seen_fresh_after_full_cycle(self):
        """After subscribe -> unsubscribe -> subscribe, first_seen is fresh."""

        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        first = reg._zmq_first_seen["cpu"]
        reg.unregister_zmq_sub("cpu")
        time.sleep(0.01)
        reg.register_zmq_sub("cpu")
        second = reg._zmq_first_seen["cpu"]
        assert second >= first
        # Strictly later (due to sleep).
        assert second > first

    def test_zmq_unsubscribe_unknown_topic_emits_zero(self):
        """Unsubscribing an unknown topic emits event with count=0, first_seen=None."""

        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.unregister_zmq_sub("ghost")
        assert len(events) == 1
        etype, payload = events[0]
        assert etype == "sub"
        assert payload["subscribers"] == 0
        assert payload["first_seen"] is None
        assert payload["topic"] == "ghost"

    def test_list_connections_returns_copies_not_references(self):
        """Mutating a returned entry does NOT affect the registry."""
        reg = ConnectionRegistry()
        reg.register_ws("x", "peer", "topic")
        conns = reg.list_connections()
        conns[0]["peer"] = "MUTATED"
        conns[0]["topic"] = "MUTATED"
        # Registry is unaffected
        actual = reg.list_connections()[0]
        assert actual["peer"] == "peer"
        assert actual["topic"] == "topic"

    def test_snapshot_returns_copies_not_references(self):
        """Mutating snapshot dicts does NOT affect the registry."""
        reg = ConnectionRegistry()
        reg.register_ws("x", "peer", "topic")
        reg.register_zmq_sub("cpu")
        snap = reg.snapshot()
        snap["connections"][0]["peer"] = "MUTATED"
        snap["subscriptions"][0]["topic"] = "MUTATED"
        snap["ws_connections"] = 999
        snap["zmq_subscribers"] = 999
        # Registry is unaffected
        actual = reg.snapshot()
        assert actual["connections"][0]["peer"] == "peer"
        assert actual["subscriptions"][0]["topic"] == "cpu"
        assert actual["ws_connections"] == 1
        assert actual["zmq_subscribers"] == 1

    def test_ws_connect_event_has_all_fields(self):
        """WS connect event payload has every expected field with correct types."""

        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.register_ws("c1", "1.2.3.4:80", "cpu")
        payload = events[0][1]
        assert payload["kind"] == "ws"
        assert payload["id"] == "c1"
        assert payload["peer"] == "1.2.3.4:80"
        assert payload["topic"] == "cpu"
        assert payload["connected"] is True
        assert isinstance(payload["connected_at"], float)
        assert "ttl" in payload
        # No left_at on connect
        assert "left_at" not in payload

    def test_ws_disconnect_event_has_all_fields(self):
        """WS disconnect event payload has every expected field."""
        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.register_ws("c1", "1.2.3.4:80", "cpu")
        reg.unregister_ws("c1")
        payload = events[1][1]
        assert payload["kind"] == "ws"
        assert payload["id"] == "c1"
        assert payload["connected"] is False
        assert isinstance(payload["connected_at"], float)
        assert isinstance(payload["left_at"], float)
        assert payload["left_at"] >= payload["connected_at"]

    def test_zmq_subscribe_event_has_all_fields(self):
        """ZMQ subscribe event payload has every expected field."""
        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.register_zmq_sub("cpu")
        payload = events[0][1]
        assert payload["kind"] == "zmq"
        assert payload["topic"] == "cpu"
        assert payload["subscribers"] == 1
        assert isinstance(payload["arrived_at"], float)
        assert "ttl" in payload
        # No left_at on join
        assert "left_at" not in payload

    def test_zmq_unsubscribe_event_has_all_fields(self):
        """ZMQ unsubscribe event payload has every expected field."""
        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.register_zmq_sub("cpu")
        reg.unregister_zmq_sub("cpu")
        payload = events[1][1]
        assert payload["kind"] == "zmq"
        assert payload["topic"] == "cpu"
        assert payload["subscribers"] == 0
        assert isinstance(payload["left_at"], float)
        assert isinstance(payload["first_seen"], float)
        assert payload["left_at"] >= payload["first_seen"]

    def test_ws_connect_event_ttl_is_60(self):
        """WS connect events carry ttl=60."""
        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.register_ws("x", "peer", "t")
        assert events[0][1]["ttl"] == 60

    def test_zmq_subscribe_event_ttl_is_30(self):
        """ZMQ subscribe events carry ttl=30."""
        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.register_zmq_sub("cpu")
        assert events[0][1]["ttl"] == 30

    def test_check_connection_true_only_while_registered(self):
        """check_connection returns True only between register and unregister."""

        reg = ConnectionRegistry()
        assert reg.check_connection("x") is False
        reg.register_ws("x", "peer", "t")
        assert reg.check_connection("x") is True
        reg.unregister_ws("x")
        assert reg.check_connection("x") is False

    def test_check_connection_after_overwrite(self):
        """check_connection still True after same-id overwrite."""
        reg = ConnectionRegistry()
        reg.register_ws("x", "p1", "t1")
        reg.register_ws("x", "p2", "t2")
        assert reg.check_connection("x") is True
        conns = reg.list_connections()
        assert conns[0]["peer"] == "p2"

    def test_connected_at_immutable_after_register(self):
        """connected_at does not change across multiple list_connections calls."""

        reg = ConnectionRegistry()
        reg.register_ws("x", "peer", "t")
        t1 = reg.list_connections()[0]["connected_at"]
        time.sleep(0.01)
        t2 = reg.list_connections()[0]["connected_at"]
        # Same timestamp, not re-evaluated.
        assert t1 == t2

    def test_ws_count_reflects_real_time(self):
        """ws_count property returns live count, not cached."""
        reg = ConnectionRegistry()
        assert reg.ws_count == 0
        reg.register_ws("a", "p", "t")
        assert reg.ws_count == 1
        reg.register_ws("b", "p", "t")
        assert reg.ws_count == 2
        reg.unregister_ws("a")
        assert reg.ws_count == 1
        reg.unregister_ws("b")
        assert reg.ws_count == 0

    def test_zmq_sub_count_reflects_real_time(self):
        """zmq_sub_count property returns live total, not cached."""
        reg = ConnectionRegistry()
        assert reg.zmq_sub_count == 0
        reg.register_zmq_sub("a")
        reg.register_zmq_sub("a")
        reg.register_zmq_sub("b")
        assert reg.zmq_sub_count == 3
        reg.unregister_zmq_sub("a")
        assert reg.zmq_sub_count == 2
        reg.unregister_zmq_sub("a")
        reg.unregister_zmq_sub("b")
        assert reg.zmq_sub_count == 0

    def test_multiple_registries_are_isolated(self):
        """Two ConnectionRegistry instances share no state."""
        reg1 = ConnectionRegistry()

        reg2 = ConnectionRegistry()
        reg1.register_ws("x", "peer", "t")
        reg1.register_zmq_sub("cpu")
        assert reg1.ws_count == 1
        assert reg2.ws_count == 0
        assert reg1.zmq_sub_count == 1
        assert reg2.zmq_sub_count == 0

    def test_subscription_tracker_subscribe_unsubscribe_cycle(self):
        """Subscribe -> unsubscribe -> subscribe: topic is active, not lingering."""

        from sqtseries.messaging.pubsub import SubscriptionTracker

        t = SubscriptionTracker(linger_seconds=5.0)
        t.subscribe("cpu")
        assert "cpu" in t.active_topics
        assert t.stats()["active"] == 1

        t.unsubscribe("cpu")
        assert "cpu" not in t.active_topics
        assert t.stats()["lingering"] == 1

        # Re-subscribe clears linger.
        t.subscribe("cpu")
        assert "cpu" in t.active_topics
        assert t.stats()["lingering"] == 0
        assert t.stats()["active"] == 1

    def test_subscription_tracker_linger_persists_until_cleanup(self):
        """Unsubscribed topic stays in linger until cleanup removes it."""

        from sqtseries.messaging.pubsub import SubscriptionTracker

        t = SubscriptionTracker(linger_seconds=0.05)
        t.subscribe("cpu")
        t.unsubscribe("cpu")
        assert t.stats()["lingering"] == 1
        # Not cleaned up yet
        t.cleanup()
        # cleanup runs, but linger_seconds hasn't expired yet (maybe)
        # With 0.05s linger, after sleep it will be gone
        time.sleep(0.1)
        t.cleanup()
        assert t.stats()["lingering"] == 0

    def test_subscription_tracker_active_topics_after_subscribe(self):
        """active_topics returns set of currently subscribed topics."""

        from sqtseries.messaging.pubsub import SubscriptionTracker

        t = SubscriptionTracker()
        assert t.active_topics == set()
        t.subscribe("a")
        t.subscribe("b")
        assert t.active_topics == {"a", "b"}
        t.unsubscribe("a")
        assert t.active_topics == {"b"}

    def test_subscription_tracker_unsubscribe_unknown_topic(self):
        """Unsubscribing a topic that was never subscribed is a no-op."""

        from sqtseries.messaging.pubsub import SubscriptionTracker

        t = SubscriptionTracker()
        # No crash.
        t.unsubscribe("ghost")
        assert t.stats()["active"] == 0
        # Lingers even if never active.
        assert t.stats()["lingering"] == 1

    def test_subscription_tracker_double_subscribe(self):
        """Subscribing twice to the same topic: active_topics still shows it once."""

        from sqtseries.messaging.pubsub import SubscriptionTracker

        t = SubscriptionTracker()
        t.subscribe("cpu")
        # Duplicate.
        t.subscribe("cpu")
        assert t.active_topics == {"cpu"}
        # Set, not counter.
        assert t.stats()["active"] == 1

    async def test_stats_publisher_event_hook_after_stop_is_noop(self):
        """After stop(), registry events are silently dropped."""
        reg = ConnectionRegistry()
        pub = StatsPublisher(f"tcp://127.0.0.1:{free_port()}", registry=reg)

        await pub.start()
        await pub.stop()
        # Register a WS — the event hook should no-op
        reg.register_ws("x", "peer", "topic")
        reg.unregister_ws("x")
        # No crash, no tasks spawned

    async def test_stats_publisher_start_stop_cycles_no_listener_leak(self):
        """5 start/stop cycles leave 0 listeners on the registry."""
        reg = ConnectionRegistry()
        for _ in range(5):
            pub = StatsPublisher(f"tcp://127.0.0.1:{free_port()}", registry=reg)

            await pub.start()
            assert len(reg._listeners) == 1
            await pub.stop()
            assert len(reg._listeners) == 0

    async def test_stats_publisher_report_contains_uptime(self):
        """Report event includes uptime_s > 0."""
        reg = ConnectionRegistry()

        pub = StatsPublisher(
            f"tcp://127.0.0.1:{free_port()}",
            registry=reg,
            report_interval=0.1,
        )
        await pub.start()
        await asyncio.sleep(0.2)

        ctx = zmq.Context()
        sub = ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.LINGER, 0)
        sub.connect(pub.endpoint)
        sub.setsockopt(zmq.SUBSCRIBE, b"report")
        await asyncio.sleep(0.2)

        for _ in range(20):
            if sub.poll(100, zmq.POLLIN):
                _topic, payload = sub.recv_multipart()
                data = orjson.loads(payload)
                if data.get("uptime_s", 0) > 0:
                    break
        else:
            pytest.fail("did not receive report with uptime")

        sub.close(linger=0)
        ctx.term()
        await pub.stop()

    def test_zmq_events_do_not_affect_ws_count(self):
        """Registering ZMQ subscribers does not change ws_count."""
        reg = ConnectionRegistry()
        reg.register_ws("x", "p", "t")
        assert reg.ws_count == 1
        reg.register_zmq_sub("cpu")
        reg.register_zmq_sub("mem")
        # Unchanged.
        assert reg.ws_count == 1

    def test_ws_events_do_not_affect_zmq_count(self):
        """Registering WS connections does not change zmq_sub_count."""

        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        assert reg.zmq_sub_count == 1
        reg.register_ws("x", "p", "t")
        reg.register_ws("y", "p", "t")
        # Unchanged.
        assert reg.zmq_sub_count == 1

    def test_unregister_zmq_sub_unknown_still_emits_event(self):
        """Unsubscribing an unknown topic emits a 'sub' event with count=0.

        This is the current behavior: unknown topics still generate events.

        The caller (pubsub._read_subscriptions) drives this, and the registry

        faithfully records the state change (0 -> 0 with count=0).
        """
        events = []

        reg = ConnectionRegistry()
        reg.on_event(lambda et, p: events.append((et, p)))
        reg.unregister_zmq_sub("never_existed")
        assert len(events) == 1
        assert events[0][0] == "sub"
        assert events[0][1]["subscribers"] == 0
        assert events[0][1]["topic"] == "never_existed"

    def test_list_connections_sorts_by_connected_at(self):
        """Connections are returned in registration order (by connected_at)."""

        reg = ConnectionRegistry()
        # Third.
        reg.register_ws("c", "p", "t")
        time.sleep(0.01)
        # First (registered earlier).
        reg.register_ws("a", "p", "t")
        time.sleep(0.01)
        # Second.
        reg.register_ws("b", "p", "t")
        ids = [c["id"] for c in reg.list_connections()]
        # c was registered first, then a, then b
        assert ids == ["c", "a", "b"]

    def test_snapshot_subscriptions_include_first_seen(self):
        """Snapshot subscriptions list includes first_seen for each topic."""

        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        reg.register_zmq_sub("mem")
        snap = reg.snapshot()
        subs = {s["topic"]: s for s in snap["subscriptions"]}
        assert "cpu" in subs
        assert "mem" in subs
        assert isinstance(subs["cpu"]["first_seen"], float)
        assert isinstance(subs["mem"]["first_seen"], float)

    def test_snapshot_subscriptions_first_seen_none_after_full_unsub(self):
        """Snapshot shows first_seen=None for topic that was fully unsubscribed."""

        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        reg.unregister_zmq_sub("cpu")
        # Topic is gone from _zmq_subs, so snapshot has no entry for it

        snap = reg.snapshot()
        topics = [s["topic"] for s in snap["subscriptions"]]
        assert "cpu" not in topics


class TestKnownTopics:
    def test_idle_topic_listed_with_zero(self):
        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        reg.register_zmq_sub("mem")
        reg.unregister_zmq_sub("cpu")
        known = {t["topic"]: t for t in reg.known_topics()}
        assert known["cpu"]["subscribers"] == 0
        assert known["mem"]["subscribers"] == 1
        assert isinstance(known["cpu"]["first_seen"], float)

    def test_first_seen_never_moves(self):
        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        first = reg.known_topics()[0]["first_seen"]
        reg.unregister_zmq_sub("cpu")
        reg.register_zmq_sub("cpu")
        reg.register_zmq_sub("cpu")
        reg.unregister_zmq_sub("cpu")
        reg.unregister_zmq_sub("cpu")
        assert reg.known_topics()[0]["first_seen"] == first
        assert reg.known_topics()[0]["subscribers"] == 0

    def test_snapshot_includes_known_topics(self):
        reg = ConnectionRegistry()
        reg.register_zmq_sub("cpu")
        reg.unregister_zmq_sub("cpu")
        snap = reg.snapshot()
        assert [t["topic"] for t in snap["known_topics"]] == ["cpu"]
        assert snap["subscriptions"] == []
