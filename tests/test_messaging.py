"""Messaging integration tests (real ZMQ, inproc/tcp, ephemeral ports)."""

import asyncio

import pytest
import zmq
import zmq.asyncio

from sqtseries.config import IngestionSettings, QuerySettings
from sqtseries.messaging import (
    Ingress,
    PubSub,
    QueryBroker,
    WorkerPool,
)


def free_tcp_port() -> int:
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


class TestIngress:
    async def test_ingest_roundtrip(self, context):
        port = free_tcp_port()

        received = []

        def sink(rows):
            received.extend(rows)

        ing = Ingress(
            f"tcp://127.0.0.1:{port}",
            IngestionSettings(),
            sink=sink,
            context=context,
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            import orjson

            pub.send(orjson.dumps({"metric": "cpu", "value": 0.5, "tags": {"h": "a"}}))

            await asyncio.sleep(0.2)
            await ing.drain()
        finally:
            pub.close(linger=0)
            await ing.stop()
        assert len(received) == 1
        metric, tags, value, _ = received[0]
        assert metric == "cpu"
        assert value == 0.5
        assert tags == {"h": "a"}

    async def test_invalid_json_counted(self, context):
        port = free_tcp_port()
        ing = Ingress(f"tcp://127.0.0.1:{port}", IngestionSettings(), context=context)

        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            pub.send(b"{not json")
            await asyncio.sleep(0.2)
            await ing.drain()
        finally:
            pub.close(linger=0)
            await ing.stop()
        assert ing.invalid_count == 1


class TestPubSub:
    async def test_publish_delivery(self, context):
        port = free_tcp_port()
        ps = PubSub(f"tcp://127.0.0.1:{port}")

        sub = context.socket(zmq.SUB)
        sub.connect(f"tcp://127.0.0.1:{port}")
        sub.setsockopt(zmq.SUBSCRIBE, b"cpu")
        # SUB slow-joiner
        await asyncio.sleep(0.1)
        try:
            await ps.start()
            # slow-joiner: PUB drops messages sent before sub ready; send a few

            for i in range(3):
                await ps.publish("cpu", {"value": float(i)})
                await asyncio.sleep(0.1)
            topic, _ = await asyncio.wait_for(sub.recv_multipart(), timeout=2)

            assert topic == b"cpu"
        finally:
            sub.close(linger=0)
            await ps.stop()

    def test_subscription_lifecycle(self):
        from sqtseries.messaging import SubscriptionTracker

        tr = SubscriptionTracker(linger_seconds=5)
        tr.subscribe("cpu")
        assert tr.active_topics == {"cpu"}
        tr.unsubscribe("cpu")
        assert tr.active_topics == set()
        assert tr.stats()["active"] == 0
        # restubscribe clears lingering
        tr.subscribe("cpu")
        assert tr.active_topics == {"cpu"}


class TestQueryBroker:
    async def test_rep_query(self, context):
        port = free_tcp_port()

        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=lambda q: {"status": "ok", "data": [q["metric"]]},
            context=context,
        )
        await broker.start()
        try:
            client = context.socket(zmq.REQ)
            client.setsockopt(zmq.LINGER, 100)
            client.connect(f"tcp://127.0.0.1:{port}")
            import json

            client.send(json.dumps({"type": "query", "metric": "cpu"}).encode())
            # run broker until it replies
            resp = None
            for _ in range(100):
                await broker.run_once(block=False)
                if client.getsockopt(zmq.EVENTS) & zmq.POLLIN:
                    resp = await client.recv()
                    break
                await asyncio.sleep(0.01)
            client.close(linger=0)
            assert resp is not None
            payload = json.loads(resp)
            assert payload["status"] == "ok"
            assert payload["data"] == ["cpu"]
        finally:
            await broker.stop()

    async def test_broker_error_handling(self, context):
        port = free_tcp_port()

        def bad(q):
            raise RuntimeError("boom")

        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}", QuerySettings(), handler=bad, context=context
        )
        await broker.start()
        try:
            client = context.socket(zmq.REQ)
            client.connect(f"tcp://127.0.0.1:{port}")
            client.send(b'{"metric":"x"}')
            resp = None

            for _ in range(100):
                await broker.run_once(block=False)
                await asyncio.sleep(0.01)
                if client.getsockopt(zmq.EVENTS) & zmq.POLLIN:
                    resp = await client.recv()
                    break
            assert resp is not None
            import json

            payload = json.loads(resp)
            assert payload["status"] == "error"
            client.close(linger=0)
        finally:
            await broker.stop()


class TestWorkerPool:
    async def test_runs_steps(self):
        counter = []

        async def step():
            counter.append(1)
            await asyncio.sleep(0.005)

        pool = WorkerPool(step, size=2)
        await pool.start()
        await asyncio.sleep(0.05)
        await pool.stop()
        # did work before stop
        assert len(counter) >= 1

    async def test_invalid_size(self):
        with pytest.raises(ValueError):
            WorkerPool(lambda: asyncio.sleep(0), size=0)
