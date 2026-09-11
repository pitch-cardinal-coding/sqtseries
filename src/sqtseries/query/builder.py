"""High-level query API over the storage engine."""

import time
from collections.abc import Iterable, Iterator

from ..engine.store import StorageEngine
from ..partition.rollup import HOUR_NS, query_rollup_partial, rollup_watermark
from .agg import (
    AGGREGATORS,
    AggregationFunction,
    aggregate_series,
    downsample,
    gap_fill_linear,
    parse_interval,
)

# Aggregations exactly expressible from the rollup's count/sum/min/max columns.
ROLLUP_FUNCS = frozenset({"avg", "sum", "min", "max", "count"})

# Downsample functions computable from per-bucket count/sum/min/max partials
# without materializing any raw row (same set as ROLLUP_FUNCS by construction).
_SQL_DOWNSAMPLE_FUNCS = ROLLUP_FUNCS


class QueryError(Exception):
    """Base error for query failures."""


class MaxRowsExceededError(QueryError):
    """A query tried to materialize more raw rows than the configured cap."""


class TimeSeriesDB:
    """High-level query facade over a StorageEngine.
    Provides the spec's API: insert / query / query_stream / aggregate /

    downsample. Rollup-eligible queries read pre-aggregated hours;
    SQL-expressible downsamples group per bucket inside SQLite; only
    median/p95/p99/first/last and gap filling run in Python over streamed
    samples (partition tables are queried per-partition by the store).
    """

    def __init__(self, store: StorageEngine, max_rows: int = 10_000):
        """``max_rows`` caps raw rows materialized per query (0 = unbounded).

        Bounded-queue doctrine: bound the work per request. A windowless read must
        not materialize the whole partition set — transient allocations
        would grow with the table and RSS would climb with ingest rate
        (measured 2026-09-09: +27 MB/snapshot at 10k pts/s). Raises
        MaxRowsExceeded instead of silently truncating.
        """
        self.store = store
        self.max_rows = max_rows

    def insert(
        self,
        metric: str,
        value: float,
        tags: dict[str, str] | None = None,
        timestamp_ns: int | None = None,
    ) -> int:
        """Insert a single measurement (server-side timestamp by default)."""

        if timestamp_ns is None:
            timestamp_ns = time.time_ns()
        return self.store.insert_many([(metric, tags, value, timestamp_ns)])

    def query(
        self,
        metric: str | None = None,
        series_ids: Iterable[int] | None = None,
        start: int | None = None,
        end: int | None = None,
        *,
        aggregation: str | AggregationFunction | None = None,
        interval: str | None = None,
        limit: int | None = None,
        order: str = "asc",
        fill_gaps_ns: int | None = None,
    ) -> list[tuple[int, float]]:
        """Return (timestamp_ns, value) pairs.
        - ``start``/``end`` are nanosecond epoch.
        - With ``aggregation``+``interval``: downsample into buckets.
        - With ``aggregation`` alone: one value over the whole window.
        - ``fill_gaps_ns`` linearly interpolates gaps <= that size.
        """
        if aggregation is not None and _name(aggregation).lower() not in AGGREGATORS:
            raise ValueError(f"unsupported aggregation: {aggregation}")
        if self._rollup_eligible(metric, series_ids, start, end, aggregation, interval):
            return self._query_rollup(
                metric=metric,
                series_ids=series_ids,
                start=start,
                end=end,
                aggregation=aggregation,
                interval=interval,
                limit=limit,
                order=order,
            )
        # A caller limit only bounds the SQL fetch on the pure raw path:
        # with aggregation/downsample/gap-fill the transform needs every
        # row in the window, and `limit` is applied to the result instead.
        # SQL-expressible downsamples skip the fetch entirely: SQLite
        # aggregates per bucket and Python only ever sees one small partial
        # per bucket, however many raw points the window holds.
        use_sql_downsample = (
            aggregation is not None
            and interval is not None
            and _name(aggregation).lower() in _SQL_DOWNSAMPLE_FUNCS
        )
        raw_limited = aggregation is None and not fill_gaps_ns
        rows = (
            []
            if use_sql_downsample
            else self._fetch(
                metric=metric,
                series_ids=series_ids,
                start=start,
                end=end,
                order=order,
                caller_limit=limit if raw_limited else None,
            )
        )

        if use_sql_downsample:
            rows = self._downsample_sql(
                metric=metric,
                series_ids=series_ids,
                start=start,
                end=end,
                aggregation=aggregation,
                interval=interval,
            )
        elif aggregation is not None and interval is not None:
            rows = downsample(rows, interval, aggregation)
        elif aggregation is not None:
            if rows:
                val = aggregate_series(rows, aggregation)
                # anchor at the oldest sample in the window, matching the
                # rollup fast path (first_sample_ts), regardless of order

                anchor = rows[0][0] if order != "desc" else rows[-1][0]
                rows = [(anchor, val)]
            else:
                rows = []
        elif fill_gaps_ns:
            rows = gap_fill_linear(rows, fill_gaps_ns)

        if limit is not None:
            # keep the first N in the selected order
            rows = rows[:limit]
        return rows

    def query_stream(
        self,
        metric: str,
        start: int | None = None,
        end: int | None = None,
        *,
        order: str = "asc",
    ) -> Iterator[tuple[int, float]]:
        """Stream raw samples in bounded memory."""
        series_ids = self.store.series_ids_for_metric(metric)
        for sid in series_ids:
            yield from self.store.query_time_range(
                series_ids=[sid], start_ns=start, end_ns=end, order=order
            )

    def aggregate(
        self,
        metric: str,
        start: int | None = None,
        end: int | None = None,
        *,
        funcs: list[str | AggregationFunction] | None = None,
    ) -> dict[str, float]:
        """Compute aggregations over the whole window; returns {func: value}."""

        funcs = funcs or ["avg"]
        raw_rows: list[tuple[int, float]] | None = None
        result: dict[str, float] = {}
        for f in funcs:
            name = _name(f).lower()
            if self._rollup_eligible(metric, None, start, end, name, None):
                result[_name(f)] = self._rollup_whole_window(metric, start, end, name)
            else:
                if raw_rows is None:
                    # Enforce the cap with a cheap COUNT(*) before paying for
                    # a 10k-row fetch that only ends in MaxRowsExceeded: same
                    # trigger as _fetch, none of the churn.
                    sids = self._resolve_series_ids(metric, None)
                    cap = self.max_rows
                    if (
                        cap
                        and cap > 0
                        and self.store.count_in_range(sids, start, end) > cap
                    ):
                        raise MaxRowsExceededError(
                            f"query would materialize more than max_rows={cap} raw rows; "
                            "narrow the time window, use an aggregation+interval, or "
                            "raise query.max_rows"
                        )
                    raw_rows = self._fetch(metric=metric, start=start, end=end)
                result[_name(f)] = aggregate_series(raw_rows, f)
        return result

    def downsample(
        self,
        metric: str,
        start: int | None = None,
        end: int | None = None,
        *,
        interval: str = "5m",
        aggregation: str | AggregationFunction = "avg",
    ) -> list[tuple[int, float]]:
        return self.query(
            metric=metric,
            start=start,
            end=end,
            aggregation=aggregation,
            interval=interval,
        )

    def _downsample_sql(
        self,
        metric: str | None,
        series_ids: Iterable[int] | None,
        start: int | None,
        end: int | None,
        aggregation: str | AggregationFunction,
        interval: str,
    ) -> list[tuple[int, float]]:
        name = _name(aggregation).lower()
        bucket_ns = parse_interval(interval) * 1_000_000_000
        sids = self._resolve_series_ids(metric, series_ids)
        if not sids:
            return []
        partials = self.store.downsample_sqlite(sids, start, end, bucket_ns)
        cap = self.max_rows
        if cap and cap > 0 and len(partials) > cap:
            raise MaxRowsExceededError(
                f"query would return more than max_rows={cap} buckets; "
                "widen the interval, narrow the time window, or "
                "raise query.max_rows"
            )
        out = []
        for bkey in sorted(partials):
            cnt, total, mn, mx = partials[bkey]
            if name == "avg":
                value = total / cnt
            elif name == "sum":
                value = total
            elif name == "min":
                value = mn
            elif name == "max":
                value = mx
            else:
                value = float(cnt)
            out.append((bkey * bucket_ns, value))
        return out

    def _resolve_series_ids(
        self, metric: str | None, series_ids: Iterable[int] | None
    ) -> list[int]:
        if series_ids is not None:
            return list(series_ids)
        if metric is None:
            raise ValueError("either metric or series_ids required")
        return self.store.series_ids_for_metric(metric)

    def _fetch(
        self,
        metric: str | None = None,
        series_ids: Iterable[int] | None = None,
        start: int | None = None,
        end: int | None = None,
        order: str = "asc",
        caller_limit: int | None = None,
    ) -> list[tuple[int, float]]:
        series_ids = self._resolve_series_ids(metric, series_ids)
        if not series_ids:
            return []
        # Bound the materialization (bounded-queue doctrine: bounded work per
        # request). Fetch cap+1 so we can distinguish "exactly full" from
        # "over" without a second count query; the store enforces the limit
        # globally across partitions, so the cursor stops early — the excess
        # rows are never even read from SQLite. A caller-supplied limit
        # ("give me the newest point") stays O(limit): the cap exists to
        # bound unbounded windows, not to punish bounded asks.
        cap = self.max_rows
        fetch_limit = cap + 1 if cap and cap > 0 else None
        if caller_limit is not None and (
            fetch_limit is None or caller_limit < fetch_limit
        ):
            fetch_limit = caller_limit
        rows = list(
            self.store.query_time_range(
                series_ids=series_ids,
                start_ns=start,
                end_ns=end,
                limit=fetch_limit,
                order=order,
            )
        )
        if cap and cap > 0 and len(rows) > cap:
            raise MaxRowsExceededError(
                f"query would materialize more than max_rows={cap} raw rows; "
                "narrow the time window, use an aggregation+interval, or "
                "raise query.max_rows"
            )
        return rows

    def _rollup_eligible(
        self,
        metric: str | None,
        series_ids: Iterable[int] | None,
        start: int | None,
        end: int | None,
        aggregation: str | AggregationFunction | None,
        interval: str | None,
    ) -> bool:
        """Whether a query can be answered from the hourly rollup.
        Requires: an aggregation expressible from count/sum/min/max; a
        whole-window aggregate or a whole-hour-multiple interval; and rollup

        coverage such that every fully-inside hour is already rolled and the

        window's tail is at most the current hour.
        """
        if aggregation is None:
            return False
        name = _name(aggregation).lower()
        if name not in ROLLUP_FUNCS:
            return False
        if interval is not None:
            try:
                bucket_ns = parse_interval(interval) * 1_000_000_000
            except ValueError:
                return False
            if bucket_ns % HOUR_NS != 0:
                return False
        now_ns = time.time_ns()
        eff_end = end if end is not None else now_ns

        start_hour = (start // HOUR_NS) * HOUR_NS if start is not None else 0

        end_hour = (eff_end // HOUR_NS) * HOUR_NS

        watermark = rollup_watermark(self.store.db)

        if watermark <= 0:
            return False
        # Requires at least one fully-inside hour (span >= 2 hours) and a tail
        # within the rolled region — otherwise there is nothing to accelerate
        # and the raw path is faster (fewer queries).
        if end_hour > watermark or end_hour < start_hour + 2 * HOUR_NS:
            return False
        return not (start is not None and start >= watermark)

    def _query_rollup(
        self,
        *,
        metric: str | None,
        series_ids: Iterable[int] | None,
        start: int | None,
        end: int | None,
        aggregation: str | AggregationFunction,
        interval: str | None,
        limit: int | None,
        order: str,
    ) -> list[tuple[int, float]]:
        """Serve an eligible query from rollup_hourly plus the raw edges.

        Fully-inside hours come from the rollup; the partial edge hours (and

        any un-rolled tail up to ``end``) come from the raw partitions; results

        are merged by bucket. The window is split so the sources never overlap.

        """
        name = _name(aggregation).lower()

        series_ids = self._resolve_series_ids(metric, series_ids)

        if not series_ids:
            return []
        bucket_ns = (
            parse_interval(interval) * 1_000_000_000 if interval is not None else None
        )

        merged = self._rollup_window_partials(series_ids, start, end, bucket_ns)

        if bucket_ns is None:
            if not merged:
                return []
            cnt, sm, mn, mx = merged[None]
            first_ts = self.store.first_sample_ts(series_ids, start, end)
            if first_ts is None:
                first_ts = start if start is not None else 0
            return [(first_ts, _value(name, cnt, sm, mn, mx))]
        return _finalize_buckets(merged, name, limit)

    def _rollup_whole_window(
        self, metric: str, start: int | None, end: int | None, name: str
    ) -> float:
        """Whole-window aggregate value from rollup + raw edges."""
        series_ids = self._resolve_series_ids(metric, None)
        if not series_ids:
            return aggregate_series([], name)
        merged = self._rollup_window_partials(series_ids, start, end, None)

        if not merged:
            return aggregate_series([], name)
        cnt, sm, mn, mx = merged[None]
        return _value(name, cnt, sm, mn, mx)

    def _rollup_window_partials(
        self,
        series_ids: list[int],
        start: int | None,
        end: int | None,
        bucket_ns: int | None,
    ) -> dict[int | None, list[float]]:
        """Merged per-bucket partials for [start, end] from rollup + raw edges.

        The window is split into three disjoint sources so nothing is counted

        twice: the first edge hour and the last (partial) hour come from the

        raw partitions; the fully-inside, already-rolled hours come from the

        rollup table.
        """
        now_ns = time.time_ns()
        eff_end = end if end is not None else now_ns

        start_hour = (start // HOUR_NS) * HOUR_NS if start is not None else 0

        end_hour = (eff_end // HOUR_NS) * HOUR_NS

        left: list[tuple[int, float]] = []
        left_end_incl = min(start_hour + HOUR_NS - 1, eff_end)

        if left_end_incl >= (start if start is not None else 0):
            left = list(
                self.store.query_time_range(
                    series_ids=series_ids,
                    start_ns=start,
                    end_ns=left_end_incl,
                    order="asc",
                )
            )

        right: list[tuple[int, float]] = []
        if end_hour > start_hour and eff_end >= end_hour:
            right = list(
                self.store.query_time_range(
                    series_ids=series_ids,
                    start_ns=end_hour,
                    end_ns=eff_end,
                    order="asc",
                )
            )

        middle: list[tuple[int | None, int, float, float, float]] = []
        if end_hour >= start_hour + 2 * HOUR_NS:
            middle = query_rollup_partial(
                self.store.db,
                series_ids,
                start_hour + HOUR_NS,
                end_hour,
                bucket_ns=bucket_ns,
            )

        return _merge_partials(
            [
                _bucket_partials(left, bucket_ns),
                _bucket_partials(right, bucket_ns),
                middle,
            ]
        )


def _name(f: str | AggregationFunction) -> str:
    return f.value if isinstance(f, AggregationFunction) else str(f)


def _bucket_partials(
    samples: Iterable[tuple[int, float]], bucket_ns: int | None
) -> list[tuple[int | None, int, float, float, float]]:
    """Partial aggregates over raw samples, matching the rollup's shape.

    With ``bucket_ns`` None returns one whole-window partial; otherwise one

    (bucket_start_ns, count, sum, min, max) per epoch-aligned bucket.
    """
    if bucket_ns is None:
        vals = [v for _, v in samples]
        if not vals:
            return []
        return [(None, len(vals), sum(vals), min(vals), max(vals))]
    buckets: dict[int, list[float]] = {}
    for ts, val in samples:
        bucket_key = ts // bucket_ns
        bucket_stats = buckets.get(bucket_key)

        if bucket_stats is None:
            buckets[bucket_key] = [1, val, val, val]
        else:
            bucket_stats[0] += 1
            bucket_stats[1] += val
            bucket_stats[2] = min(bucket_stats[2], val)
            bucket_stats[3] = max(bucket_stats[3], val)
    return [
        (bucket_key * bucket_ns, *stats)
        for bucket_key, stats in sorted(buckets.items())
    ]


def _merge_partials(
    groups: list[list[tuple[int | None, int, float, float, float]]],
) -> dict[int | None, list[float]]:
    """Combine partial aggregates sharing the same bucket key."""
    merged: dict[int | None, list[float]] = {}
    for parts in groups:
        for bucket, row_count, row_sum, row_min, row_max in parts:
            bucket_stats = merged.get(bucket)
            if bucket_stats is None:
                merged[bucket] = [row_count, row_sum, row_min, row_max]
            else:
                bucket_stats[0] += row_count
                bucket_stats[1] += row_sum
                bucket_stats[2] = min(bucket_stats[2], row_min)
                bucket_stats[3] = max(bucket_stats[3], row_max)
    return merged


def _finalize_buckets(
    merged: dict[int | None, list[float]], name: str, limit: int | None
) -> list[tuple[int, float]]:
    """Emit (bucket_start_ns, value) ascending, honoring limit (first N)."""

    out: list[tuple[int, float]] = []
    for bucket in sorted(merged):
        cnt, sm, mn, mx = merged[bucket]
        out.append((bucket, _value(name, cnt, sm, mn, mx)))
    if limit is not None:
        out = out[:limit]
    return out


def _value(name: str, cnt: int, sm: float, mn: float, mx: float) -> float:
    if name == "avg":
        return sm / cnt
    if name == "sum":
        return sm
    if name == "count":
        return float(cnt)
    if name == "min":
        return mn
    if name == "max":
        return mx
    raise ValueError(f"unsupported rollup aggregation: {name}")
