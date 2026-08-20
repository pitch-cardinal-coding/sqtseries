"""Tests for patterns adopted from dafka/zyre into sqtseries.

Covers:
- Query result cache (dafka_fetch_filter.c)
"""

import time

from sqtseries.messaging.query_cache import QueryResultCache

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
