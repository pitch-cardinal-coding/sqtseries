"""Query result cache with TTL for deduplicating identical queries.
When multiple WebSocket clients subscribe to the same metric and time range,
the query engine runs the same query N times.  This cache deduplicates those
identical queries by keying on ``(metric, start, end, aggregation, interval,
aggregations, limit, order)``.
Implemented as a bounded LRU with per-entry TTL, similar to ``_LRUCache`` in
engine/store.py.

Bounded by WEIGHT, not just count (bounded-queue doctrine: bound the
memory, not the message count): each entry carries the number of data rows in
its result, and the cache evicts LRU entries until the total row weight fits
``max_weight``. A single result larger than ``max_weight`` is never cached at
all — one huge query cannot flush the whole cache or pin its memory here
(measured 2026-09-09: count-only bound let cached 100k-row results grow the
heap ~5 MB/s under a 10k pts/s pump with ``end=now`` queries).
"""

import time
from collections import OrderedDict
from typing import Any

# A result with more rows than this is never cached (it would occupy the
# entire weight budget alone).
MAX_CACHED_ROWS = 10_000


class QueryResultCache:
    """Bounded LRU cache with per-entry TTL and a total-weight bound.

    ``maxsize`` caps the number of cached entries (default 512).
    ``max_weight`` caps the total number of cached data rows across all
    entries (default 200_000) — the memory bound.
    ``ttl_s`` is the time-to-live for each entry (default 5.0s).
    """

    __slots__ = (
        "_data",
        "_max_weight",
        "_maxsize",
        "_ttl_s",
        "_weight",
        "evicted_weight",
        "hits",
        "misses",
        "oversize_skips",
    )

    def __init__(
        self,
        maxsize: int = 512,
        ttl_s: float = 5.0,
        max_weight: int = 200_000,
    ):
        self._data: OrderedDict[tuple, tuple[float, Any, int]] = OrderedDict()
        self._maxsize = maxsize
        self._max_weight = max_weight
        self._ttl_s = ttl_s
        self._weight = 0
        self.hits = 0
        self.misses = 0
        self.evicted_weight = 0
        self.oversize_skips = 0

    def _make_key(self, query: dict[str, Any]) -> tuple:
        """Build a hashable cache key from a query dict.
        ``aggregations`` may be a list (from the query handler) which is not
        hashable — convert to a sorted tuple for stable ordering and
        hashability.
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

    @staticmethod
    def _row_count(result: Any) -> int:
        """Rows in a result payload (the cache weight unit)."""
        if isinstance(result, dict):
            data = result.get("data")
            if isinstance(data, list):
                return len(data)
            aggs = result.get("aggregations")
            if isinstance(aggs, dict):
                return len(aggs)
        return 1

    def get(self, query: dict[str, Any]) -> dict[str, Any] | None:
        """Return cached result if fresh, else None."""
        key = self._make_key(query)
        entry = self._data.get(key)

        if entry is None:
            self.misses += 1

            return None
        ts, result, _w = entry
        if time.monotonic() - ts > self._ttl_s:
            # expired — remove and report miss
            del self._data[key]
            self._weight -= _w
            self.misses += 1
            return None
        # hit — move to end (most-recently-used)
        self._data.move_to_end(key)
        self.hits += 1
        return result

    def put(self, query: dict[str, Any], result: dict[str, Any]) -> None:
        """Cache a query result, honoring the weight bound."""
        key = self._make_key(query)
        weight = self._row_count(result)
        if weight > MAX_CACHED_ROWS or weight > self._max_weight:
            # Never cache: a single oversize result would dominate the
            # budget (and pin its memory) — bounded-work doctrine.
            self.oversize_skips += 1
            return
        # Drop any previous entry for this key (its weight must not count
        # twice).
        old = self._data.pop(key, None)
        if old is not None:
            self._weight -= old[2]
        self._data[key] = (time.monotonic(), result, weight)
        self._data.move_to_end(key)
        self._weight += weight
        # Evict LRU until both bounds hold.
        while self._data and (
            len(self._data) > self._maxsize or self._weight > self._max_weight
        ):
            _k, (_ts, _r, ew) = self._data.popitem(last=False)
            self._weight -= ew
            self.evicted_weight += ew

    def clear(self) -> None:
        """Drop all cached entries."""
        self._data.clear()
        self._weight = 0

    def stats(self) -> dict[str, int]:
        """Return cache statistics."""
        return {
            "size": len(self._data),
            "maxsize": self._maxsize,
            "weight": self._weight,
            "max_weight": self._max_weight,
            "hits": self.hits,
            "misses": self.misses,
            "oversize_skips": self.oversize_skips,
        }
