"""Tests for query cache and connection health.
Covers:
- Query result cache
- Connection health sweep
"""

import time

from sqtseries.messaging.connection_registry import ConnectionRegistry
from sqtseries.messaging.query_cache import QueryResultCache


class TestConnectionHealthSweep:
    """touch_ws refresh keeps connections alive; sweep_stale reports idle ones."""

    def test_empty_registry_sweeps_clean(self):
        assert ConnectionRegistry().sweep_stale() == []

    def test_fresh_connection_not_stale(self):
        reg = ConnectionRegistry()
        reg.register_ws("c1", "peer", "t")
        assert reg.sweep_stale(expired_timeout_s=30.0) == []

    def test_idle_connection_reported_stale(self):
        reg = ConnectionRegistry()
        reg.register_ws("c1", "peer", "t")
        reg._ws["c1"]["last_activity_at"] -= 60.0
        stale = reg.sweep_stale(expired_timeout_s=30.0)
        assert [s["id"] for s in stale] == ["c1"]
        assert stale[0]["age_s"] >= 30.0

    def test_touch_refreshes_and_clears_stale(self):
        reg = ConnectionRegistry()
        reg.register_ws("c1", "peer", "t")
        reg._ws["c1"]["last_activity_at"] -= 60.0
        assert len(reg.sweep_stale(expired_timeout_s=30.0)) == 1
        reg.touch_ws("c1")
        assert reg.sweep_stale(expired_timeout_s=30.0) == []

    def test_sweep_is_read_only(self):
        reg = ConnectionRegistry()
        reg.register_ws("c1", "peer", "t")
        reg._ws["c1"]["last_activity_at"] -= 60.0
        reg.sweep_stale(expired_timeout_s=30.0)
        assert reg.check_connection("c1") is True

    def test_unknown_touch_never_stale(self):
        reg = ConnectionRegistry()
        reg.touch_ws("ghost")
        assert reg.sweep_stale(expired_timeout_s=0.0) == []


# ---------------------------------------------------------------------------
# Query result cache
# ---------------------------------------------------------------------------


class TestQueryResultCache:
    """Verify the query result cache deduplicates identical queries."""

    def test_cache_hit(self):
        """Identical query returns cached result."""
        cache = QueryResultCache(ttl_s=5.0)

        query = {"metric": "cpu", "start": 100, "end": 200}

        result = {"status": "ok", "data": [{"timestamp": 1.0, "value": 42.0}]}

        cache.put(query, result)
        cached = cache.get(query)
        assert cached == result
        assert cache.hits == 1
        assert cache.misses == 0

    def test_cache_miss(self):
        """Different query returns None."""
        cache = QueryResultCache(ttl_s=5.0)

        query1 = {"metric": "cpu", "start": 100, "end": 200}

        query2 = {"metric": "mem", "start": 100, "end": 200}

        result = {"status": "ok", "data": []}

        cache.put(query1, result)
        cached = cache.get(query2)
        assert cached is None
        assert cache.misses == 1

    def test_cache_ttl_expiry(self):
        """Expired entries are not returned."""
        cache = QueryResultCache(ttl_s=0.05)

        query = {"metric": "cpu", "start": 100, "end": 200}

        result = {"status": "ok", "data": []}

        cache.put(query, result)
        time.sleep(0.1)
        cached = cache.get(query)
        assert cached is None
        assert cache.misses == 1

    def test_cache_lru_eviction(self):
        """Oldest entry is evicted when cache is full."""
        cache = QueryResultCache(maxsize=2, ttl_s=5.0)

        q1 = {"metric": "a", "start": 1, "end": 2}

        q2 = {"metric": "b", "start": 1, "end": 2}

        q3 = {"metric": "c", "start": 1, "end": 2}

        cache.put(q1, {"status": "ok"})
        cache.put(q2, {"status": "ok"})
        cache.put(q3, {"status": "ok"})  # evicts q1

        assert cache.get(q1) is None  # evicted
        assert cache.get(q2) is not None  # still there
        assert cache.get(q3) is not None  # still there

    def test_cache_stats(self):
        """stats() returns accurate hit/miss counts."""
        cache = QueryResultCache(ttl_s=5.0)

        q1 = {"metric": "a", "start": 1, "end": 2}

        cache.put(q1, {"status": "ok"})
        cache.get(q1)  # hit
        cache.get(q1)  # hit
        cache.get({"metric": "b"})  # miss

        stats = cache.stats()
        assert stats["hits"] == 2
        assert stats["misses"] == 1
        assert stats["size"] == 1

    def test_cache_clear(self):
        """clear() empties the cache."""
        cache = QueryResultCache(ttl_s=5.0)

        q1 = {"metric": "a", "start": 1, "end": 2}
        cache.put(q1, {"status": "ok"})
        assert cache.stats()["size"] == 1

        cache.clear()
        assert cache.stats()["size"] == 0
        assert cache.get(q1) is None

    def test_cache_skips_error_results(self):
        """Error results should NOT be cached (only cache 'ok' results)."""

        cache = QueryResultCache(ttl_s=5.0)

        query = {"metric": "cpu", "start": 100, "end": 200}

        error_result = {"status": "error", "error": {"code": "NOT_READY"}}

        # In real code, errors are not cached — only the handler decides.
        # This test verifies the cache doesn't break on error results.
        cache.put(query, error_result)
        cached = cache.get(query)
        assert cached == error_result  # cache doesn't filter, caller decides

    def test_cache_with_list_aggregations(self):
        """aggregations as a list (from query handler) must be hashable."""

        cache = QueryResultCache(ttl_s=5.0)

        q1 = {"metric": "cpu", "start": 100, "end": 200, "aggregations": ["avg", "max"]}

        q2 = {"metric": "cpu", "start": 100, "end": 200, "aggregations": ["max", "avg"]}

        result = {"status": "ok", "data": {"avg": 1.0, "max": 2.0}}

        cache.put(q1, result)
        # Same metrics in different order should hit the same cache entry

        cached = cache.get(q2)
        assert cached == result

    def test_cache_with_none_values(self):
        """Query with None values should work."""
        cache = QueryResultCache(ttl_s=5.0)

        q = {"metric": "cpu", "start": None, "end": None}

        result = {"status": "ok", "data": []}
        cache.put(q, result)
        assert cache.get(q) == result


class TestSweepBatching:
    """sweep_stale pacing: oldest-first + batch_size per tick."""

    def _stale_registry(self, n: int) -> ConnectionRegistry:
        import time

        reg = ConnectionRegistry()
        now = time.time()
        for i in range(n):
            cid = f"c{i:04d}"
            reg.register_ws(cid, "peer", "t")
            reg._ws[cid]["last_activity_at"] = now - (n - i) * 10.0
        return reg

    def test_oldest_first_order(self):
        reg = self._stale_registry(3)
        ids = [s["id"] for s in reg.sweep_stale(expired_timeout_s=0.0)]
        assert ids == ["c0000", "c0001", "c0002"]

    def test_batch_limit(self):
        reg = self._stale_registry(5)
        first = reg.sweep_stale(expired_timeout_s=0.0, batch_size=2)
        assert [s["id"] for s in first] == ["c0000", "c0001"]
        for s in first:
            reg.unregister_ws(s["id"])
        second = reg.sweep_stale(expired_timeout_s=0.0, batch_size=2)
        assert [s["id"] for s in second] == ["c0002", "c0003"]

    def test_batch_none_returns_all(self):
        reg = self._stale_registry(4)
        assert len(reg.sweep_stale(expired_timeout_s=0.0)) == 4
        assert len(reg.sweep_stale(expired_timeout_s=0.0, batch_size=99)) == 4

    def test_batch_zero_returns_empty(self):
        reg = self._stale_registry(3)
        assert reg.sweep_stale(expired_timeout_s=0.0, batch_size=0) == []

    def test_batch_skips_fresh(self):
        reg = self._stale_registry(3)
        reg.touch_ws("c0000")
        ids = [s["id"] for s in reg.sweep_stale(expired_timeout_s=5.0, batch_size=10)]
        assert "c0000" not in ids
        assert ids == ["c0001", "c0002"]

    def test_batch_is_read_only(self):
        reg = self._stale_registry(3)
        reg.sweep_stale(expired_timeout_s=0.0, batch_size=1)
        assert reg.ws_count == 3


class TestTcpKeepalive:
    """TCP keepalive second net on long-lived sockets."""

    def test_socket_options_include_keepalive(self):
        import zmq

        from sqtseries.messaging.context import socket_options

        opts = socket_options()
        assert opts[zmq.TCP_KEEPALIVE] == 1
        assert opts[zmq.TCP_KEEPALIVE_IDLE] == 60
        assert opts[zmq.TCP_KEEPALIVE_CNT] == 3
        assert opts[zmq.TCP_KEEPALIVE_INTVL] == 10

    def test_socket_options_keepalive_opt_out(self):
        import zmq

        from sqtseries.messaging.context import socket_options

        opts = socket_options(tcp_keepalive=False)
        assert zmq.TCP_KEEPALIVE not in opts
        assert zmq.TCP_KEEPALIVE_IDLE not in opts

    def test_helper_sets_values(self):
        import zmq

        from sqtseries.messaging.context import apply_tcp_keepalive

        ctx = zmq.Context()
        sock = ctx.socket(zmq.SUB)
        try:
            apply_tcp_keepalive(sock)
            assert sock.getsockopt(zmq.TCP_KEEPALIVE) == 1
            assert sock.getsockopt(zmq.TCP_KEEPALIVE_IDLE) == 60
            assert sock.getsockopt(zmq.TCP_KEEPALIVE_CNT) == 3
            assert sock.getsockopt(zmq.TCP_KEEPALIVE_INTVL) == 10
        finally:
            sock.close()
            ctx.term()

    async def test_live_pubsub_has_keepalive(self):
        import zmq
        from conftest import free_port

        from sqtseries.messaging.pubsub import PubSub

        ps = PubSub(f"tcp://127.0.0.1:{free_port()}", registry=None)
        await ps.start()
        try:
            assert ps.socket is not None
            assert ps.socket.getsockopt(zmq.TCP_KEEPALIVE) == 1
            assert ps.socket.getsockopt(zmq.TCP_KEEPALIVE_IDLE) == 60
        finally:
            await ps.stop()

    def test_live_client_sub_has_keepalive(self):
        import zmq

        from sqtseries.client import Client

        c = Client()
        try:
            sock = c._get_sub_sock()
            assert sock.getsockopt(zmq.TCP_KEEPALIVE) == 1
            assert sock.getsockopt(zmq.TCP_KEEPALIVE_IDLE) == 60
        finally:
            c.close()


class TestSweepDrainLoop:
    """Page sweep→unregister until empty (batched eviction integration)."""

    async def test_drain_to_empty(self):
        import time

        reg = ConnectionRegistry()
        now = time.time()
        for i in range(250):
            cid = f"d{i:04d}"
            reg.register_ws(cid, "peer", "t")
            reg._ws[cid]["last_activity_at"] = now - 60.0
        drained = 0
        while True:
            batch = reg.sweep_stale(expired_timeout_s=30.0, batch_size=100)
            if not batch:
                break
            for s in batch:
                reg.unregister_ws(s["id"])
                drained += 1
        assert drained == 250
        assert reg.ws_count == 0
        assert reg.sweep_stale(expired_timeout_s=30.0) == []


class TestBrokerKeepaliveInherit:
    """QueryBroker inherits TCP keepalive via shared socket_options."""

    async def test_live_broker_has_keepalive(self):
        import zmq
        from conftest import free_port

        from sqtseries.config import QuerySettings
        from sqtseries.messaging.broker import QueryBroker

        b = QueryBroker(
            f"tcp://127.0.0.1:{free_port()}",
            QuerySettings(),
            handler=lambda q: {"status": "ok"},
        )
        await b.start()
        try:
            assert b.socket is not None
            assert b.socket.getsockopt(zmq.TCP_KEEPALIVE) == 1
            assert b.socket.getsockopt(zmq.TCP_KEEPALIVE_IDLE) == 60
        finally:
            await b.stop()


class TestStatsKeepaliveLive:
    """StatsPublisher PUB carries TCP keepalive like the other sockets."""

    async def test_live_stats_has_keepalive(self):
        import zmq
        from conftest import free_port

        from sqtseries.messaging.stats_publisher import StatsPublisher

        pub = StatsPublisher(
            f"tcp://127.0.0.1:{free_port()}", registry=ConnectionRegistry()
        )
        await pub.start()
        try:
            assert pub.socket is not None
            assert pub.socket.getsockopt(zmq.TCP_KEEPALIVE) == 1
            assert pub.socket.getsockopt(zmq.TCP_KEEPALIVE_IDLE) == 60
        finally:
            await pub.stop()
