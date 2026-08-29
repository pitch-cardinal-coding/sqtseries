"""Aggregation, downsampling, and gap-filling functions."""

import math
import statistics
from collections.abc import Callable, Iterable
from enum import StrEnum

# (timestamp_ns, value)
Sample = tuple[int, float]


class AggregationFunction(StrEnum):
    AVG = "avg"

    SUM = "sum"

    MIN = "min"

    MAX = "max"

    COUNT = "count"

    FIRST = "first"

    LAST = "last"

    MEDIAN = "median"

    P95 = "p95"

    P99 = "p99"


def median(values: list[float]) -> float:
    return statistics.median(values) if values else float("nan")


def p95(values: list[float]) -> float:
    return _percentile(values, 0.95)


def p99(values: list[float]) -> float:
    return _percentile(values, 0.99)


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    if len(values) == 1:
        return values[0]
    sorted_vals = sorted(values)
    rank = (len(sorted_vals) - 1) * q
    upper = math.ceil(rank)

    lower = math.floor(rank)

    if lower == upper:
        return sorted_vals[int(rank)]
    return sorted_vals[lower] * (upper - rank) + sorted_vals[upper] * (rank - lower)


AGGREGATORS: dict[str, Callable[[list[float]], float]] = {
    "avg": lambda v: statistics.mean(v),
    "sum": lambda v: sum(v),
    "min": lambda v: min(v),
    "max": lambda v: max(v),
    "count": lambda v: float(len(v)),
    "first": lambda v: v[0],
    "last": lambda v: v[-1],
    "median": median,
    "p95": p95,
    "p99": p99,
}


def aggregate_series(
    samples: Iterable[Sample],
    func: str | AggregationFunction,
) -> float:
    """Aggregate a list of (timestamp_ns, value) samples over a whole window."""

    name = func.value if isinstance(func, AggregationFunction) else func

    agg: Callable[[list[float]], float] = AGGREGATORS.get(name.lower())

    if agg is None:
        raise ValueError(f"unsupported aggregation: {name}")
    vals = [v for _, v in samples]
    if not vals:
        # Empty window: count/sum are naturally 0.0; the rest are undefined
        # (nan), consistent with min/max. Never raise StatisticsError/IndexError.

        if name in ("count", "sum"):
            return 0.0
        return float("nan")
    return agg(vals)


INTERVAL_UNITS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
}


def parse_interval(interval: str) -> int:
    """Parse an interval string (e.g. '5m', '10s', '1h', '1d') into seconds."""

    text = interval.strip().lower()
    if not text:
        raise ValueError("empty interval")
    unit = text[-1]

    if text[0].isdigit() and unit in INTERVAL_UNITS:
        num = int(text[:-1])

        if num < 1:
            raise ValueError(f"interval must be >= 1: {interval}")
        return num * INTERVAL_UNITS[unit]
    raise ValueError(f"unsupported interval: {interval}")


def downsample(
    samples: Iterable[Sample],
    interval: str,
    func: str | AggregationFunction = "avg",
) -> list[Sample]:
    """Bucket samples into ``interval`` bins and aggregate each bucket."""

    seconds = parse_interval(interval)
    bucket_ns = seconds * 1_000_000_000

    agg = _aggfn(func)

    buckets: dict[int, list[float]] = {}
    for ts, value in samples:
        bkey = ts // bucket_ns
        buckets.setdefault(bkey, []).append(value)

    out = []

    for bkey in sorted(buckets):
        vals = buckets[bkey]
        out.append((bkey * bucket_ns, agg(vals)))
    return out


def downsample_to_intervals(
    samples: list[Sample],
    interval: str,
    func: str | AggregationFunction = "avg",
    fill_missing: bool = False,
) -> list[tuple[int, float]]:
    """Downsample and optionally emit empty buckets (value=nan) for gaps.

    ``start_ns``/``end_ns`` semantics handled by caller when ``fill_missing``.

    """
    if not fill_missing:
        return downsample(samples, interval, func)
    seconds = parse_interval(interval)
    bucket_ns = seconds * 1_000_000_000

    agg = _aggfn(func)

    buckets: dict[int, list[float]] = {}
    bounds: list[int] = []
    for ts, value in samples:
        bkey = ts // bucket_ns
        buckets.setdefault(bkey, []).append(value)
        bounds.append(bkey)
    if not bounds:
        return []

    out = []
    for bkey in range(min(bounds), max(bounds) + 1):
        vals = buckets.get(bkey)
        result = agg(vals) if vals else float("nan")
        out.append((bkey * bucket_ns, result))
    return out


def _aggfn(func: str | AggregationFunction) -> Callable[[list[float]], float]:
    name = func.value if isinstance(func, AggregationFunction) else func
    agg = AGGREGATORS.get(name.lower())

    if agg is None:
        raise ValueError(f"unsupported aggregation: {func}")
    return agg


def gap_fill_linear(samples: list[Sample], max_gap_ns: int) -> list[Sample]:
    """Linearly interpolate between samples, filling gaps <= max_gap_ns.

    Samples must be sorted ascending by timestamp. Gaps larger than the

    threshold remain unfilled.
    """
    if len(samples) < 2:
        return list(samples)

    out: list[Sample] = []
    for i in range(len(samples)):
        ts, val = samples[i]
        out.append((ts, val))
        if i + 1 >= len(samples):
            continue
        nts, nval = samples[i + 1]
        gap = nts - ts
        if 0 < gap <= max_gap_ns:
            # insert halfway point (linear step)
            mid_ts = ts + gap // 2

            mid_val = (val + nval) / 2.0
            out.append((mid_ts, mid_val))
    return out
