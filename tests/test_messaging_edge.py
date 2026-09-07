"""Messaging edge cases: ROUTER broker, error replies, ingress limits, worker errors."""

import asyncio
import json

import pytest
import zmq

from sqtseries.config import IngestionSettings, QuerySettings
from sqtseries.messaging import Ingress, PubSub, QueryBroker, WorkerPool


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


class TestBrokerErrors:
    async def test_run_once_before_start_raises(self, context):
        port = free_tcp_port()

        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=lambda q: {},
            context=context,
        )
        with pytest.raises(RuntimeError):
            await broker.run_once(block=False)
        await broker.stop()

    async def test_no_handler_returns_invalid_request(self, context):
        port = free_tcp_port()

        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}", QuerySettings(), context=context
        )
        await broker.start()
        try:
            client = context.socket(zmq.REQ)
            client.setsockopt(zmq.LINGER, 100)
            client.connect(f"tcp://127.0.0.1:{port}")
            client.send(json.dumps({"type": "query"}).encode())
            resp = None
            for _ in range(100):
                await broker.run_once(block=False)
                if client.getsockopt(zmq.EVENTS) & zmq.POLLIN:
                    resp = await client.recv()
                    break
                await asyncio.sleep(0.01)
            client.close(linger=0)
            payload = json.loads(resp)
            assert payload["status"] == "error"
            assert payload["error"]["code"] == "INVALID_REQUEST"
        finally:
            await broker.stop()

    async def test_invalid_request_reply(self, context):
        port = free_tcp_port()

        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=lambda q: {"status": "ok"},
            context=context,
        )
        await broker.start()
        try:
            client = context.socket(zmq.REQ)
            client.setsockopt(zmq.LINGER, 100)
            client.connect(f"tcp://127.0.0.1:{port}")
            client.send(b"{not json")
            resp = None
            for _ in range(100):
                await broker.run_once(block=False)
                if client.getsockopt(zmq.EVENTS) & zmq.POLLIN:
                    resp = await client.recv()
                    break
                await asyncio.sleep(0.01)
            client.close(linger=0)
            payload = json.loads(resp)
            assert payload["status"] == "error"
            assert payload["error"]["code"] == "INVALID_REQUEST"
        finally:
            await broker.stop()

    async def test_handler_error_reply(self, context):
        port = free_tcp_port()

        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=lambda q: (_ for _ in ()).throw(RuntimeError("boom")),
            context=context,
        )
        await broker.start()
        try:
            client = context.socket(zmq.REQ)
            client.setsockopt(zmq.LINGER, 100)
            client.connect(f"tcp://127.0.0.1:{port}")
            client.send(json.dumps({"type": "query"}).encode())
            resp = None
            for _ in range(100):
                await broker.run_once(block=False)
                if client.getsockopt(zmq.EVENTS) & zmq.POLLIN:
                    resp = await client.recv()
                    break
                await asyncio.sleep(0.01)
            client.close(linger=0)
            payload = json.loads(resp)
            assert payload["status"] == "error"
            assert payload["error"]["code"] == "INTERNAL_ERROR"
            assert broker.stats()["errors"] == 1
        finally:
            await broker.stop()

    async def test_router_mode_with_dealer(self, context):
        port = free_tcp_port()

        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=lambda q: {"status": "ok", "echo": q["metric"]},
            context=context,
            use_router=True,
        )
        await broker.start()
        try:
            client = context.socket(zmq.DEALER)
            client.setsockopt(zmq.LINGER, 100)
            client.setsockopt(zmq.IDENTITY, b"client-1")
            client.connect(f"tcp://127.0.0.1:{port}")
            client.send(json.dumps({"metric": "cpu"}).encode())
            frames = None
            for _ in range(100):
                await broker.run_once(block=False)
                if client.getsockopt(zmq.EVENTS) & zmq.POLLIN:
                    frames = await client.recv_multipart()
                    break
                await asyncio.sleep(0.01)
            client.close(linger=0)
            assert frames is not None
            payload = json.loads(frames[-1])
            assert payload["status"] == "ok"
            assert payload["echo"] == "cpu"
        finally:
            await broker.stop()

    async def test_router_missing_payload(self, context):
        port = free_tcp_port()

        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=lambda q: {"status": "ok"},
            context=context,
            use_router=True,
        )
        await broker.start()
        try:
            client = context.socket(zmq.DEALER)
            client.setsockopt(zmq.LINGER, 100)
            client.setsockopt(zmq.IDENTITY, b"c2")
            client.connect(f"tcp://127.0.0.1:{port}")
            # empty message -> payload frame is empty bytes
            client.send(b"")
            frames = None
            for _ in range(100):
                await broker.run_once(block=False)
                if client.getsockopt(zmq.EVENTS) & zmq.POLLIN:
                    frames = await client.recv_multipart()
                    break
                await asyncio.sleep(0.01)
            client.close(linger=0)
            assert frames is not None
            assert json.loads(frames[-1])["status"] == "error"
        finally:
            await broker.stop()


class TestIngressEdge:
    async def test_invalid_message_counted(self, context):
        port = free_tcp_port()
        ing = Ingress(f"tcp://127.0.0.1:{port}", IngestionSettings(), context=context)

        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            # JSON valid but message shape invalid
            # missing value
            pub.send(b'{"metric": "x"}')
            # missing metric
            pub.send(b'{"value": 1.0}')
            pub.send(b'{"metric": "x", "value": "not-a-number"}')
            # non-str tag
            pub.send(b'{"metric": "x", "value": 1, "tags": {"k": 5}}')
            await asyncio.sleep(0.2)
            await ing.drain()
        finally:
            pub.close(linger=0)
            await ing.stop()
        assert ing.invalid_count == 4
        assert ing.recv_count == 0

    async def test_skew_rejected(self, context):
        port = free_tcp_port()

        ing = Ingress(
            f"tcp://127.0.0.1:{port}",
            IngestionSettings(reject_client_timestamp_skew_s=5.0),
            context=context,
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            # timestamp 1 hour in the past
            pub.send(
                b'{"metric": "x", "value": 1, "timestamp": %d}'
                % int(__import__("time").time() - 3600)
            )
            await asyncio.sleep(0.2)
            await ing.drain()
        finally:
            pub.close(linger=0)
            await ing.stop()
        assert ing.invalid_count == 1

    async def test_overflowing_timestamp_counted_invalid(self, context):
        """A huge finite float timestamp must be counted invalid, not crash.

        With the clock-skew guard disabled, ``timestamp: 1e308`` passes

        validation but overflows the int conversion in ``to_rows``. It must

        be dropped as invalid (not escape as OverflowError from _handle).

        """
        ing = Ingress(
            "inproc://overflow",
            IngestionSettings(reject_client_timestamp_skew_s=0),
            context=context,
        )
        row = ing._parse(b'{"metric": "m", "value": 1.0, "timestamp": 1e308}')

        assert row is None
        assert ing.invalid_count == 1
        assert ing.recv_count == 0

    async def test_recv_error_counted(self, context):
        port = free_tcp_port()

        received = []

        def sink(rows):
            received.extend(m for m, _t, _v, _ts in rows)

        ing = Ingress(
            f"tcp://127.0.0.1:{port}",
            IngestionSettings(max_message_size=16),
            sink=sink,
            context=context,
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            # exceeds max_message_size
            pub.send(b"x" * 1024)
            await asyncio.sleep(0.2)
            await ing.drain_many()
        finally:
            pub.close(linger=0)
            await ing.stop()
        # oversized frame is dropped at the socket level (peer disconnected);
        # the important invariant is that it is never ingested.
        assert received == []


class TestPubSubEdge:
    async def test_publish_before_start_raises(self):
        ps = PubSub("tcp://127.0.0.1:1")
        with pytest.raises(RuntimeError):
            await ps.publish("cpu", {"value": 1.0})
        # stop without start is a no-op
        await ps.stop()


class TestWorkerPool:
    async def test_step_error_continues(self):
        calls = []

        async def step():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("boom")
            await asyncio.sleep(0.001)

        pool = WorkerPool(step, size=1)
        await pool.start()
        await asyncio.sleep(0.1)
        await pool.stop()
        # error didn't kill the worker
        assert len(calls) >= 2

    async def test_size_zero_raises(self):
        with pytest.raises(ValueError):
            WorkerPool(lambda: None, size=0)
