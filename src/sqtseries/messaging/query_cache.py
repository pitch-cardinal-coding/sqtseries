"""Query result cache with TTL for deduplicating identical queries.
When multiple WebSocket clients subscribe to the same metric and time range,
the query engine runs the same query N times.  This cache deduplicates those
identical queries by keying on ``(metric, start, end, aggregation, interval)``.
Inspired by dafka's fetch filter (dafka/src/dafka_fetch_filter.c) which
suppresses duplicate FETCH requests for the same partition.  Implemented as
a bounded LRU with per-entry TTL, similar to ``_LRUCache`` in engine/store.py.
"""

import time
from collections import OrderedDict
from typing import Any


class QueryResultCache:
    """Bounded LRU cache with per-entry TTL for query results.
    ``maxsize`` caps the number of cached entries (default 512 — enough for

    typical dashboard queries without excessive memory).  ``ttl_s`` is the

    time-to-live for each entry (default 5.0s — short enough that stale data

    from a newly ingested point is quickly visible, long enough to deduplicate

    a burst of identical subscribe-then-query calls).
    """

    __slots__ = ("_data", "_maxsize", "_ttl_s", "hits", "misses")

    def __init__(self, maxsize: int = 512, ttl_s: float = 5.0):
        self._data: OrderedDict[tuple, tuple[float, Any]] = OrderedDict()

        self._maxsize = maxsize
        self._ttl_s = ttl_s
        self.hits = 0
        self.misses = 0

    def _make_key(self, query: dict[str, Any]) -> tuple:
        """Build a hashable cache key from a query dict.
        ``aggregations`` may be a list (from the query handler) which is not

        hashable — convert to a frozenset for stable ordering and hashability.

        """
        aggs = query.get("aggregations")
        if isinstance(aggs, list):
            aggs = tuple(sorted(aggs))
        return (
            query.get("metric"),
            query.get("start"),
            query.get("end"),
            query.get("aggregation"),
            query.get("interval"),
            aggs,
            query.get("limit"),
            query.get("order", "asc"),
        )

    def get(self, query: dict[str, Any]) -> dict[str, Any] | None:
        """Return cached result if fresh, else None."""
        key = self._make_key(query)
        entry = self._data.get(key)

        if entry is None:
            self.misses += 1

            return None
        ts, result = entry
        if time.monotonic() - ts > self._ttl_s:
            # expired — remove and report miss
            del self._data[key]
            self.misses += 1
            return None
        # hit — move to end (most-recently-used)
        self._data.move_to_end(key)
        self.hits += 1
        return result

    def put(self, query: dict[str, Any], result: dict[str, Any]) -> None:
        """Cache a query result."""
        key = self._make_key(query)
        self._data[key] = (time.monotonic(), result)
        self._data.move_to_end(key)
        while len(self._data) > self._maxsize:
            self._data.popitem(last=False)

    def clear(self) -> None:
        """Drop all cached entries."""
        self._data.clear()

    def stats(self) -> dict[str, int]:
        """Return cache statistics."""
        return {
            "size": len(self._data),
            "maxsize": self._maxsize,
            "hits": self.hits,
            "misses": self.misses,
        }
