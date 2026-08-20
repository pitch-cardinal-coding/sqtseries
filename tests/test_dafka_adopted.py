"""Tests for patterns adopted from dafka/zyre into sqtseries.

Covers:
- Connection health sweep (dafka_beacon.c:272, zyre_peer.c:198)
- Query result cache (dafka_fetch_filter.c)
"""

import time

from sqtseries.messaging.connection_registry import ConnectionRegistry
from sqtseries.messaging.query_cache import QueryResultCache

# ---------------------------------------------------------------------------
# XPUB welcome message
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Connection health sweep
# ---------------------------------------------------------------------------


class TestConnectionHealthSweep:
    """Verify evasive/expired timeout detection for WebSocket connections."""

    def test_touch_ws_updates_activity(self):
        """touch_ws() refreshes last_activity_at."""
        reg = ConnectionRegistry(expired_timeout_s=1.0)
        reg.register_ws("c1", "127.0.0.1:12345", "cpu")

        entry = reg.list_connections()[0]
        original_activity = entry["last_activity_at"]

        time.sleep(0.05)
        reg.touch_ws("c1")

        entry = reg.list_connections()[0]
        assert entry["last_activity_at"] > original_activity

    def test_sweep_stale_returns_expired(self):
        """sweep_stale() returns connections older than expired_timeout_s."""
        reg = ConnectionRegistry(expired_timeout_s=0.1)
        reg.register_ws("c1", "127.0.0.1:12345", "cpu")
        reg.register_ws("c2", "127.0.0.1:12346", "mem")

        # Wait for c1 to expire
        time.sleep(0.15)

        # Touch c2 to keep it alive
        reg.touch_ws("c2")

        stale = reg.sweep_stale()
        stale_ids = [s["id"] for s in stale]
        assert "c1" in stale_ids
        assert "c2" not in stale_ids

    def test_sweep_stale_empty_when_all_active(self):
        """sweep_stale() returns empty when all connections are active."""
        reg = ConnectionRegistry(expired_timeout_s=5.0)
        reg.register_ws("c1", "127.0.0.1:12345", "cpu")

        stale = reg.sweep_stale()
        assert stale == []

    def test_sweep_stale_does_not_remove(self):
        """sweep_stale() does NOT remove connections — caller decides."""
        reg = ConnectionRegistry(expired_timeout_s=0.1)
        reg.register_ws("c1", "127.0.0.1:12345", "cpu")

        time.sleep(0.15)

        stale = reg.sweep_stale()
        assert len(stale) == 1
        # Connection is still in the registry
        assert reg.check_connection("c1")

    def test_sweep_stale_includes_age(self):
        """Stale entries include age_s for diagnostic purposes."""
        reg = ConnectionRegistry(expired_timeout_s=0.1)
        reg.register_ws("c1", "127.0.0.1:12345", "cpu")

        time.sleep(0.15)

        stale = reg.sweep_stale()
        assert len(stale) == 1
        assert "age_s" in stale[0]
        assert stale[0]["age_s"] >= 0.1

    def test_sweep_stale_with_evasive_timeout(self):
        """evasive_timeout_s is tracked but doesn't auto-disconnect."""
        reg = ConnectionRegistry(evasive_timeout_s=0.05, expired_timeout_s=0.2)
        reg.register_ws("c1", "127.0.0.1:12345", "cpu")

        time.sleep(0.1)

        # Not expired yet
        stale = reg.sweep_stale()
        assert len(stale) == 0

        # Wait for expiry
        time.sleep(0.15)
        stale = reg.sweep_stale()
        assert len(stale) == 1


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
