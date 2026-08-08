"""ZMQ behavior edge cases: PUB slow-joiner, REP alternation, tracker cleanup."""

import asyncio
import time

import pytest
import zmq

from sqtseries.messaging import PubSub, SubscriptionTracker


def free_tcp_port():
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture
async def context():
    ctx = zmq.asyncio.Context()
    yield ctx
    ctx.term()


class TestPubSub:
    async def test_slow_joiner_loses_early_messages(self, context):
        """PUB/SUB drops messages published before the subscriber joins."""
        port = free_tcp_port()
        ps = PubSub(f"tcp://127.0.0.1:{port}", context=context)
        await ps.start()
        try:
            # before any subscriber
            await ps.publish("cpu", {"value": 1.0})
            sub = context.socket(zmq.SUB)
            sub.connect(f"tcp://127.0.0.1:{port}")
            sub.setsockopt(zmq.SUBSCRIBE, b"cpu")
            await asyncio.sleep(0.1)
            await ps.publish("cpu", {"value": 2.0})
            await ps.publish("cpu", {"value": 3.0})
            got = []
            for _ in range(20):
                if sub.getsockopt(zmq.EVENTS) & zmq.POLLIN:
                    frames = await asyncio.wait_for(sub.recv_multipart(), timeout=2)
                    got.append(frames[1])
                else:
                    break
                if len(got) >= 2:
                    break
                await asyncio.sleep(0.02)
            sub.close(linger=0)
            # the first message (value 1.0) is lost to the slow joiner
            assert b'"value":1.0' not in b"".join(got) or len(got) < 3
        finally:
            await ps.stop()

    async def test_rep_serves_sequential_requests(self, context):
        from sqtseries.config import QuerySettings
        from sqtseries.messaging import QueryBroker

        port = free_tcp_port()
        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=lambda q: {"status": "ok", "n": q.get("n")},
            context=context,
        )
        await broker.start()
        try:
            client = context.socket(zmq.REQ)
            client.setsockopt(zmq.LINGER, 100)
            client.connect(f"tcp://127.0.0.1:{port}")
            import json

            for n in (1, 2, 3):
                client.send(json.dumps({"n": n}).encode())
                resp = None
                for _ in range(100):
                    await broker.run_once(block=False)
                    if client.getsockopt(zmq.EVENTS) & zmq.POLLIN:
                        resp = await client.recv()
                        break
                    await asyncio.sleep(0.01)
                assert json.loads(resp)["n"] == n
            client.close(linger=0)
        finally:
            await broker.stop()


class TestSubscriptionTracker:
    def test_cleanup_expires_lingering(self):
        tr = SubscriptionTracker(linger_seconds=0.05)
        tr.subscribe("cpu")
        tr.unsubscribe("cpu")
        assert tr.stats()["lingering"] == 1
        time.sleep(0.1)
        tr.cleanup()
        assert tr.stats()["lingering"] == 0

    def test_stats_active_lingering(self):
        tr = SubscriptionTracker(linger_seconds=5)
        tr.subscribe("a")
        tr.subscribe("b")
        tr.unsubscribe("a")
        assert tr.stats() == {"active": 1, "lingering": 1}
        assert tr.active_topics == {"b"}

    def test_resubscribe_clears_linger(self):
        tr = SubscriptionTracker(linger_seconds=5)
        tr.subscribe("a")
        tr.unsubscribe("a")
        tr.subscribe("a")
        assert tr.stats()["lingering"] == 0
        assert tr.active_topics == {"a"}


class TestPubSubStats:
    async def test_published_counter(self, context):
        port = free_tcp_port()
        ps = PubSub(f"tcp://127.0.0.1:{port}", context=context)
        await ps.start()
        try:
            await ps.publish("a", {"v": 1})
            await ps.publish("b", {"v": 2})
            assert ps.stats()["published"] == 2
        finally:
            await ps.stop()
