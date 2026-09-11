"""Messaging edge cases: ROUTER broker, error replies, ingress limits, worker errors."""

import asyncio
import json
import time

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

    async def test_router_sheds_at_max_inflight(self, context):
        port = free_tcp_port()

        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(max_inflight=1),
            handler=lambda q: {"status": "ok"},
            context=context,
            use_router=True,
        )
        await broker.start()
        blocker = asyncio.Event()
        holder = asyncio.create_task(blocker.wait())
        broker._router_tasks.add(holder)
        try:
            assert await broker.run_once(block=False) is False
            assert broker.stats()["shed_total"] == 1
        finally:
            broker._router_tasks.discard(holder)
            holder.cancel()
            await asyncio.gather(holder, return_exceptions=True)
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

    async def test_drain_many_one_sink_call_per_batch(self, context):
        """A drained burst reaches the sink ONCE (one transaction per batch)."""
        port = free_tcp_port()
        calls: list[int] = []
        rows_seen: list[int] = []

        def sink(rows):
            calls.append(1)
            rows_seen.append(len(rows))

        ing = Ingress(
            f"tcp://127.0.0.1:{port}", IngestionSettings(), sink=sink, context=context
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            import orjson

            for i in range(5):
                pub.send(orjson.dumps({"metric": "m", "value": float(i)}))
            await asyncio.sleep(0.2)
            handled = await ing.drain_many()
            await ing.flush()
        finally:
            pub.close(linger=0)
            await ing.stop()
        assert handled == 5
        # One batch, not five per-row transactions.
        assert calls == [1]
        assert rows_seen == [5]

    async def test_invalid_rows_mid_batch_are_skipped(self, context):
        """Bad frames inside a burst are dropped and counted; valid rows in
        the SAME batch still reach the sink (no all-or-nothing parse)."""
        port = free_tcp_port()
        received: list[str] = []

        def sink(rows):
            received.extend(m for m, _t, _v, _ts in rows)

        ing = Ingress(
            f"tcp://127.0.0.1:{port}", IngestionSettings(), sink=sink, context=context
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            pub.send(b"{not json")
            pub.send(b'{"metric": "a", "value": 1.0}')
            # Invalid: no value.
            pub.send(b'{"metric": "b"}')
            pub.send(b'{"metric": "c", "value": 2.0}')
            pub.send(b'{"metric": "d", "value": 3.0}')
            await asyncio.sleep(0.2)
            handled = await ing.drain_many()
            await ing.flush()
        finally:
            pub.close(linger=0)
            await ing.stop()
        assert handled == 3
        assert received == ["a", "c", "d"]
        assert ing.invalid_count == 2
        assert ing.recv_count == 3

    async def test_sink_failure_isolated_and_drain_survives(self, context):
        """A raising sink must not kill the pipeline: the persister isolates
        the failed batch (logged + counted by the sink's own accounting) and
        keeps consuming — the WorkerPool depends on this to keep serving
        after a bad batch."""
        port = free_tcp_port()
        calls = 0

        def bad_sink(rows):
            nonlocal calls
            calls += 1
            raise RuntimeError("db exploded")

        ing = Ingress(
            f"tcp://127.0.0.1:{port}",
            IngestionSettings(),
            sink=bad_sink,
            context=context,
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            pub.send(b'{"metric": "a", "value": 1.0}')
            await asyncio.sleep(0.2)
            assert await ing.drain_many() == 1
            await ing.flush()
            assert calls == 1
            # socket healthy, counters still counted the receive
            assert ing.recv_count == 1
            pub.send(b'{"metric": "b", "value": 2.0}')
            await asyncio.sleep(0.2)
            assert await ing.drain_many() == 1
            await ing.flush()
            # the persister survived the first failure and consumed batch 2
            assert calls == 2
            assert ing.recv_count == 2
        finally:
            pub.close(linger=0)
            await ing.stop()


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
        # Wait for the second call with a deadline: CI/desktop machines can
        # be momentarily loaded, and 0.1 s wall clock is not a contract.
        deadline = time.monotonic() + 5.0
        while len(calls) < 2 and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        await pool.stop()
        # error didn't kill the worker
        assert len(calls) >= 2

    async def test_size_zero_raises(self):
        with pytest.raises(ValueError):
            WorkerPool(lambda: None, size=0)


class TestIngressAccounting:
    """End-to-end ingest accounting: recv == persisted + dropped + invalid.

    A sink failure must never vanish silently: every accepted row lands in
    exactly one of {persisted, dropped}.
    """

    def _pump(self, pub, metric: str, n: int, start: int) -> None:
        for i in range(n):
            pub.send(
                json.dumps(
                    {
                        "metric": metric,
                        "value": float(i),
                        "tags": {"h": "a"},
                        "timestamp": (start + i) / 1e9,
                    }
                ).encode()
            )

    async def test_persisted_count_matches_accepted_rows(self, context):
        """Every validated row reaches the sink and is reported persisted."""
        import time as _time

        persisted: list[int] = []
        port = free_tcp_port()
        ing = Ingress(
            f"tcp://127.0.0.1:{port}",
            IngestionSettings(),
            context=context,
            sink=lambda rows: persisted.append(len(rows)),
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            self._pump(pub, "acct.metric", 100, start=_time.time_ns())
            await asyncio.sleep(0.2)
            await ing.drain()
            await ing.flush()
        finally:
            pub.close(linger=0)
        assert ing.recv_count == 100
        assert sum(persisted) == 100
        assert ing.stats()["recv"] == 100
        await ing.stop()

    async def test_sink_failure_counts_dropped_not_silent(self, context):
        """A failing insert surfaces as dropped rows in the sink's own
        accounting (Service._sink tallies `dropped`) — never silent."""
        import time as _time

        def bad_sink(rows):
            raise RuntimeError("disk full (simulated)")

        port = free_tcp_port()
        ing = Ingress(
            f"tcp://127.0.0.1:{port}",
            IngestionSettings(),
            context=context,
            sink=bad_sink,
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            self._pump(pub, "drop.metric", 10, start=_time.time_ns())
            await asyncio.sleep(0.2)
            await ing.drain()
            # The persister isolates the failure (logged, batch counted as
            # dropped by the sink wrapper) and stays alive for the shutdown
            # flush — the pipeline outlives any single bad batch.
            await ing.flush()
        finally:
            pub.close(linger=0)
        assert ing.recv_count == 10
        assert ing.invalid_count == 0
        assert ing.stats()["recv"] == 10
        await ing.stop()

    async def test_server_timestamps_strictly_increasing_burst(self):
        """A burst of timestamp-less points (server time) must get strictly
        increasing ns per process: 50 frames land inside one wall-clock tick,
        and duplicate ns within a series would violate the PRIMARY KEY and
        roll the whole batch back (measured: 4.3M unaccounted points at
        10k pts/s before this fix)."""
        from sqtseries.messaging.protocol import IngestMessage

        msgs = [IngestMessage(metric="burst.m", value=float(i)) for i in range(200)]
        ns = [m.to_rows()[3] for m in msgs]
        assert len(set(ns)) == 200, "duplicate server-assigned timestamps"
        assert ns == sorted(ns), "server timestamps not strictly increasing"

    async def test_sink_runs_off_event_loop(self, context):
        """A sync sink must execute in a worker thread, never on the event
        loop (a blocking insert_many on the loop starved the query broker:
        606/2377 REQ timeouts at 10k pts/s)."""
        import threading
        import time as _time

        loop_threads: list[int] = []
        done = threading.Event()

        def sink(rows):
            loop_threads.append(threading.get_ident())
            done.set()
            return len(rows)

        port = free_tcp_port()
        ing = Ingress(
            f"tcp://127.0.0.1:{port}",
            IngestionSettings(),
            context=context,
            sink=sink,
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            self._pump(pub, "offloop.metric", 5, start=_time.time_ns())
            await asyncio.sleep(0.2)
            await asyncio.wait_for(ing.drain(), timeout=10)
        finally:
            pub.close(linger=0)
            await ing.stop()
        assert done.is_set()
        assert loop_threads, "sink was never called"
        main_loop_thread = threading.get_ident()
        # drain() awaited on this thread; the sink must have run elsewhere.
        assert loop_threads[0] != main_loop_thread


class TestDecoupledPersister:
    """Decoupled persister: receiver never awaits the sink.

    The 2026-09-09 10k pts/s run measured ~1,650 pts/s when drain_many
    awaited commits inline; these tests pin the redesigned contract.
    """

    async def test_drain_never_awaits_sink(self, context):
        """drain_many() returns while the sink is STILL RUNNING — commit
        latency cannot throttle reception (the 1,650 pts/s ceiling)."""
        import time as _time

        release = asyncio.Event()
        sink_calls: list[int] = []

        async def slow_sink(rows):
            sink_calls.append(len(rows))
            await release.wait()

        port = free_tcp_port()
        ing = Ingress(
            f"tcp://127.0.0.1:{port}",
            IngestionSettings(),
            context=context,
            sink=slow_sink,
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.connect(f"tcp://127.0.0.1:{port}")
            for i in range(10):
                pub.send(
                    json.dumps(
                        {
                            "metric": "m",
                            "value": float(i),
                            "timestamp": (_time.time_ns() + i) / 1e9,
                        }
                    ).encode()
                )
            await asyncio.sleep(0.2)
            assert await ing.drain_many() == 10
            # The sink has been STARTED by the persister but has not
            # returned: drain already came back.
            await asyncio.sleep(0.05)
            assert sink_calls, "persister never started the sink"
            assert ing._persisted < 1
        finally:
            release.set()
            pub.close(linger=0)
            await ing.stop()

    async def test_full_queue_backpressures_receiver(self, context):
        """pending_max=1: while the sink blocks, a second batch must remain
        unparsed in the socket (backpressure), never dropped."""
        import time as _time

        release = asyncio.Event()

        async def slow_sink(rows):
            await release.wait()

        port = free_tcp_port()
        ing = Ingress(
            f"tcp://127.0.0.1:{port}",
            IngestionSettings(pending_max=1),
            context=context,
            sink=slow_sink,
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.setsockopt(zmq.SNDHWM, 10_000)
            pub.setsockopt(zmq.LINGER, 0)
            pub.connect(f"tcp://127.0.0.1:{port}")
            for i in range(40):
                pub.send(
                    json.dumps(
                        {
                            "metric": "m",
                            "value": float(i),
                            "timestamp": (_time.time_ns() + i) / 1e9,
                        }
                    ).encode()
                )
            await asyncio.sleep(0.2)
            # Batch 1 (20 frames) fills the queue; the sink is stuck.
            assert await ing.drain_many(max_messages=20) == 20
            # Batch 2 parses fine (drain never awaits the sink) but its
            # _enqueue must suspend: the queue already holds batch 1.
            drain_task = asyncio.create_task(ing.drain_many())
            await asyncio.sleep(0.1)
            assert not drain_task.done(), "drain should suspend on a full queue"
            release.set()
            assert await asyncio.wait_for(drain_task, 5) == 20
        finally:
            pub.close(linger=0)
            await ing.stop()

    async def test_shutdown_with_full_queue_persists_everything(self, context):
        """Cancellation mid-drain enqueues parsed rows; drain_inflight then
        flushes the full queue — zero loss across a hostile shutdown."""
        import time as _time

        gate = asyncio.Event()
        seen: list[int] = []

        async def gated_sink(rows):
            if not gate.is_set():
                await gate.wait()
            seen.append(len(rows))

        port = free_tcp_port()
        ing = Ingress(
            f"tcp://127.0.0.1:{port}",
            IngestionSettings(pending_max=2),
            context=context,
            sink=gated_sink,
        )
        await ing.start()
        try:
            pub = context.socket(zmq.PUSH)
            pub.setsockopt(zmq.SNDHWM, 10_000)
            pub.connect(f"tcp://127.0.0.1:{port}")
            total = 0
            for _round in range(5):
                for i in range(50):
                    pub.send(
                        json.dumps(
                            {
                                "metric": "m",
                                "value": float(i),
                                "timestamp": (_time.time_ns() + total + i) / 1e9,
                            }
                        ).encode()
                    )
                total += 50
                await asyncio.sleep(0.02)
            # Cancel a drain mid-parse with the sink gated shut.
            worker = asyncio.create_task(ing.drain_many())
            await asyncio.sleep(0.05)
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            gate.set()
            await ing.flush()
        finally:
            pub.close(linger=0)
            await ing.drain_inflight()
            await ing.stop()
        assert sum(seen) == total, "rows lost across cancellation + shutdown"


class TestFanoutHub:
    """Bounded fan-out: weight-bounded queues, drop-new, loud
    counting, boundary-aware topic matching."""

    @pytest.mark.asyncio
    async def test_topic_boundary_matching(self):
        from sqtseries.messaging.fanout import topic_matches

        assert topic_matches("*", "anything")
        assert topic_matches("stress.m1", "stress.m1")
        assert topic_matches("stress.m1", "stress.m1.load")
        assert not topic_matches("stress.m1", "stress.m10")
        assert not topic_matches("cpu", "cpu2")
        # Explicit prefix contract.
        assert topic_matches("cpu.", "cpu.load")

    @pytest.mark.asyncio
    async def test_deliver_matches_and_counts(self):
        from sqtseries.messaging.fanout import FanoutHub

        class _PS:
            endpoint = "tcp://127.0.0.1:12999"

        hub = FanoutHub(_PS())
        hub.attach("m1", conn_id="a")
        hub.attach("m2", conn_id="b")
        hub.attach("*", conn_id="c")
        n = hub.deliver("m1", b"x" * 10)
        # a (exact) + c (star); NOT m2.
        assert n == 2
        assert hub.published_total == 1

    @pytest.mark.asyncio
    async def test_weight_bound_drops_newest_and_counts(self):
        from sqtseries.messaging.fanout import FanoutHub

        class _PS:
            endpoint = "tcp://127.0.0.1:12998"

        hub = FanoutHub(_PS(), queue_bytes=100)
        hub.attach("*", conn_id="s")
        # 3 x 60 B frames: first fits (60), second fits (120 > 100 -> drop),
        # so exactly one drop for the 2nd, then 3rd also dropped (60+60>100)
        hub.deliver("t", b"a" * 60)
        hub.deliver("t", b"b" * 60)
        hub.deliver("t", b"c" * 60)
        st = hub.subscriber_stats()["s"]
        assert st["dropped"] == 2
        assert hub.dropped_total == 2
        assert st["pending_bytes"] == 60

    @pytest.mark.asyncio
    async def test_oversized_single_frame_passes_through(self):
        from sqtseries.messaging.fanout import FanoutHub

        class _PS:
            endpoint = "tcp://127.0.0.1:12997"

        hub = FanoutHub(_PS(), queue_bytes=100)
        hub.attach("*", conn_id="s")
        n = hub.deliver("t", b"x" * 500)  # > budget, empty queue -> deliver
        assert n == 1
        kind, (_topic, payload) = await hub.recv("s")
        assert kind == "frame" and payload == b"x" * 500

    @pytest.mark.asyncio
    async def test_queue_bounded_under_stalled_consumer(self):
        """THE leak-regression test: publish 5k frames while the consumer
        never reads; total pending memory must stay <= budget (+1 frame)."""
        from sqtseries.messaging.fanout import FanoutHub

        class _PS:
            endpoint = "tcp://127.0.0.1:12996"

        hub = FanoutHub(_PS(), queue_bytes=64 * 1024)
        hub.attach("*", conn_id="s")
        frame = b"x" * 200
        for _ in range(5000):
            hub.deliver("t", frame)
        st = hub.subscriber_stats()["s"]
        # every frame is 200 B; budget 64 KiB => ~327 queued max
        assert st["dropped"] == 5000 - st["queue_depth"]
        assert st["pending_bytes"] <= 64 * 1024 + 200
        assert hub.dropped_total == st["dropped"]

    @pytest.mark.asyncio
    async def test_detach_wakes_recv(self):
        from sqtseries.messaging.fanout import FanoutHub

        class _PS:
            endpoint = "tcp://127.0.0.1:12995"

        hub = FanoutHub(_PS())
        hub.attach("*", conn_id="s")
        hub.detach("s")
        kind, item = await asyncio.wait_for(hub.recv("s"), timeout=2)
        assert kind == "closed" and item is None

    @pytest.mark.asyncio
    async def test_recv_timeout_returns_timeout(self):
        from sqtseries.messaging.fanout import FanoutHub

        class _PS:
            endpoint = "tcp://127.0.0.1:12994"

        hub = FanoutHub(_PS())
        hub.attach("*", conn_id="s")
        kind, item = await asyncio.wait_for(hub.recv("s", timeout=0.05), timeout=2)
        assert kind == "timeout" and item is None
        # queue still usable afterwards
        hub.deliver("t", b"hello")
        kind, (_topic, payload) = await hub.recv("s")
        assert kind == "frame" and payload == b"hello"
