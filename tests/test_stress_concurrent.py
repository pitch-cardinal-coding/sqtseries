"""Stress test: concurrent connections, admin polling, data flow.
Rapid connect/disconnect cycles for ZMQ SUB and admin, verifying the registry
stays correct under load. Ports are chosen randomly to avoid conflicts.
"""

import asyncio
import socket

import orjson
import pytest
import zmq

from sqtseries.config import Settings
from sqtseries.service import Service


def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestStressConcurrent:
    @pytest.fixture
    async def svc(self, tmp_path):
        ports = {
            k: _free_port()
            for k in ("ingest", "query", "stream", "admin", "http", "stats")
        }
        s = Settings(
            database={"path": str(tmp_path / "stress.sqlite"), "batch_size": 100},
            ingestion={"port": ports["ingest"]},
            query={"port": ports["query"]},
            streaming={"port": ports["stream"]},
            admin={"port": ports["admin"]},
            http={"port": ports["http"]},
            stats={"port": ports["stats"]},
            ports={"auto_detect": False},
        )
        svc = Service(s)
        await svc.start()
        await svc.pool.stop()
        pump_task = asyncio.create_task(_pump(svc))
        try:
            yield svc
        finally:
            pump_task.cancel()
            await asyncio.gather(pump_task, return_exceptions=True)
            await svc.shutdown()

    async def test_rapid_sub_connect_disconnect(self, svc):
        """Rapid ZMQ SUB connect/disconnect leaves registry at 0."""
        port = svc.settings.streaming.port

        async def cycle(client_id: int):
            ctx = zmq.Context()
            sub = ctx.socket(zmq.SUB)
            sub.setsockopt(zmq.LINGER, 0)
            sub.connect(f"tcp://127.0.0.1:{port}")
            sub.setsockopt(zmq.SUBSCRIBE, f"r.{client_id % 10}".encode())

            await asyncio.sleep(0.03)
            sub.close(linger=0)
            ctx.term()

        tasks = [cycle(i) for i in range(30)]
        await asyncio.gather(*tasks)
        await asyncio.sleep(1.0)
        reply = svc._admin_handler({"cmd": "subscribers"})
        assert reply["zmq_subscribers"] == 0

    async def test_admin_during_churn(self, svc):
        """Admin subscriber count is consistent during rapid subscribe cycles."""

        stream_port = svc.settings.streaming.port

        admin_port = svc.settings.admin.port

        async def probe():
            import zmq.asyncio as azmq

            ctx = azmq.Context.instance()
            s = ctx.socket(zmq.REQ)
            s.setsockopt(zmq.LINGER, 100)
            s.connect(f"tcp://127.0.0.1:{admin_port}")
            results = []

            for _ in range(10):
                await s.send(orjson.dumps({"cmd": "subscribers"}))
                reply_buf = await s.recv()
                reply = orjson.loads(reply_buf)

                if "error" in reply:
                    results.append(("error", reply))
                    continue
                results.append(("ok", reply["zmq_subscribers"]))
            s.close(linger=0)
            return results

        async def churn():
            ctx = zmq.Context()
            for i in range(20):
                sub = ctx.socket(zmq.SUB)
                sub.setsockopt(zmq.LINGER, 0)
                sub.connect(f"tcp://127.0.0.1:{stream_port}")
                sub.setsockopt(zmq.SUBSCRIBE, f"ch.{i % 5}".encode())
                await asyncio.sleep(0.02)
                sub.close(linger=0)
            ctx.term()

        results = await asyncio.gather(probe(), churn())
        await asyncio.sleep(0.5)
        probe_results = results[0]
        assert all(r[0] == "ok" for r in probe_results)

    async def test_stats_immediate_emit(self, svc):
        """Stats emits events synchronously via callback, not just polling."""

        reg = svc.connection_registry

        received = []

        def collector(etype, payload):
            received.append((etype, payload))

        reg.on_event(collector)
        reg.register_ws("immediate-test", "10.0.0.1:9999", "test")
        assert len(received) == 1
        assert received[0][0] == "conn" and received[0][1]["connected"] is True

        assert received[0][1]["kind"] == "ws"

        reg.unregister_ws("immediate-test")
        assert len(received) == 2
        assert received[1][1]["connected"] is False

        reg.register_zmq_sub("immediate-topic")
        assert len(received) == 3
        assert received[2][0] == "sub"

    async def test_ingest_with_subscribers(self, svc):
        """Data flows correctly when subscribers and ingestion overlap."""

        ingest_port = svc.settings.ingestion.port

        stream_port = svc.settings.streaming.port

        ctx = zmq.Context()

        async def subscriber():
            sub = ctx.socket(zmq.SUB)
            sub.setsockopt(zmq.LINGER, 0)
            sub.connect(f"tcp://127.0.0.1:{stream_port}")
            sub.setsockopt(zmq.SUBSCRIBE, b"")
            await asyncio.sleep(0.2)
            count = 0
            for _ in range(100):
                events = sub.poll(10, zmq.POLLIN)

                if events:
                    sub.recv_multipart()
                    count += 1
                else:
                    await asyncio.sleep(0.01)
            sub.close(linger=0)
            return count

        async def write():
            push = ctx.socket(zmq.PUSH)
            push.setsockopt(zmq.LINGER, 500)
            push.connect(f"tcp://127.0.0.1:{ingest_port}")
            for i in range(100):
                push.send(
                    orjson.dumps({"metric": "throughput.test", "value": float(i)})
                )
                await asyncio.sleep(0.002)
            push.close(linger=0)

        sub_count, _ = await asyncio.gather(subscriber(), write())
        await asyncio.sleep(0.3)
        assert sub_count > 0, "subscriber should receive published data"

    async def test_conncheck_returns_correctly(self, svc):
        """conncheck admin command returns the correct subset of present IDs."""

        reg = svc.connection_registry
        reg.register_ws("keep-a", "peer-a", "t")
        reg.register_ws("keep-b", "peer-b", "t")
        reply = svc._admin_handler(
            {"cmd": "conncheck", "ids": ["keep-a", "missing", "keep-b"]}
        )
        assert reply["present"] == ["keep-a", "keep-b"]

        reg.unregister_ws("keep-a")
        reply = svc._admin_handler({"cmd": "conncheck", "ids": ["keep-a", "keep-b"]})

        assert reply["present"] == ["keep-b"]

    async def test_stats_report_loop_runs(self, svc):
        """The stats publisher's report loop emits 'report' events periodically."""

        stats_port = svc.settings.stats.port

        ctx = zmq.Context()
        stats_sub = ctx.socket(zmq.SUB)
        stats_sub.setsockopt(zmq.LINGER, 0)
        stats_sub.connect(f"tcp://127.0.0.1:{stats_port}")
        stats_sub.setsockopt(zmq.SUBSCRIBE, b"report")
        await asyncio.sleep(0.5)

        # Wait for a report cycle (default 10s) — shorten to about 1s for test

        found = False
        for _ in range(40):
            events = stats_sub.poll(250, zmq.POLLIN)

            if events:
                found = True
                break
            await asyncio.sleep(0.25)

        stats_sub.close(linger=0)
        ctx.term()
        # At least one report should arrive within ~10s total wait
        assert found, "Stats publisher should emit at least one report event"


async def _pump(svc):
    while True:
        await svc.ingress.drain_many()
        await svc.ingress.flush()
        await svc.broker.run_once(block=False)
        await svc.admin_broker.run_once(block=False)
        await asyncio.sleep(0.005)
