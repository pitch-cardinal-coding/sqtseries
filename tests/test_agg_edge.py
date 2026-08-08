"""Aggregation edge cases: percentiles, intervals, gap-filling, error paths."""

import math

import pytest

from sqtseries.query.agg import (
    aggregate_series,
    downsample,
    downsample_to_intervals,
    gap_fill_linear,
    p95,
    p99,
    parse_interval,
)


class TestPercentiles:
    def test_single_value(self):
        assert p95([5.0]) == 5.0
        assert p99([5.0]) == 5.0

    def test_two_values(self):
        # k = (2-1)*0.95 = 0.95, f=0, c=1 -> interpolation
        assert p95([0.0, 10.0]) == pytest.approx(9.5)

    def test_exact_index(self):
        # (21-1)*0.95 = 19 -> integer index, no interpolation
        vals = list(range(1, 22))
        assert p95(vals) == 20.0
        assert p99(vals) == pytest.approx(20.8)

    def test_empty(self):
        assert math.isnan(p95([]))
        assert math.isnan(p99([]))


class TestAggregateSeries:
    def test_empty_min_max_nan(self):
        assert math.isnan(aggregate_series([], "min"))
        assert math.isnan(aggregate_series([], "max"))

    def test_empty_window_safe_values(self):
        # empty window must not raise: avg/min/max/... -> nan, count/sum -> 0
        assert math.isnan(aggregate_series([], "avg"))
        assert math.isnan(aggregate_series([], "median"))
        assert math.isnan(aggregate_series([], "first"))
        assert aggregate_series([], "count") == 0.0
        assert aggregate_series([], "sum") == 0.0

    def test_first_last(self):
        samples = [(0, 1.0), (1, 2.0), (2, 3.0)]
        assert aggregate_series(samples, "first") == 1.0
        assert aggregate_series(samples, "last") == 3.0

    def test_unsupported_func(self):
        with pytest.raises(ValueError):
            aggregate_series([(0, 1.0)], "bogus")


class TestParseInterval:
    def test_valid(self):
        assert parse_interval("5s") == 5
        assert parse_interval("1m") == 60
        assert parse_interval("2h") == 7200
        assert parse_interval("1d") == 86400
        # whitespace trimmed
        assert parse_interval(" 10s ") == 10

    def test_empty(self):
        with pytest.raises(ValueError):
            parse_interval("")

    def test_zero(self):
        with pytest.raises(ValueError):
            parse_interval("0s")

    def test_unsupported(self):
        with pytest.raises(ValueError):
            parse_interval("5x")


class TestDownsampleToIntervals:
    def test_fill_missing_emits_gap_buckets(self):
        # 10s apart
        samples = [(0, 1.0), (10_000_000_000, 2.0)]
        out = downsample_to_intervals(samples, "5s", "sum", fill_missing=True)
        # buckets: 0, 5, 10 (three buckets, middle empty -> nan)
        assert [b for b, _ in out] == [0, 5_000_000_000, 10_000_000_000]
        assert math.isnan(out[1][1])

    def test_fill_missing_no_gap(self):
        samples = [(0, 1.0), (1_000_000_000, 2.0)]
        out = downsample_to_intervals(samples, "1s", "sum", fill_missing=True)
        assert [b for b, _ in out] == [0, 1_000_000_000]
        assert [v for _, v in out] == [1.0, 2.0]

    def test_fill_missing_empty(self):
        assert downsample_to_intervals([], "5s", fill_missing=True) == []

    def test_no_fill_missing_delegates(self):
        samples = [(0, 1.0), (10_000_000_000, 2.0)]
        out = downsample_to_intervals(samples, "5s", "sum", fill_missing=False)
        # no empty bucket
        assert [b for b, _ in out] == [0, 10_000_000_000]


class TestGapFill:
    def test_gap_larger_than_max_unchanged(self):
        samples = [(0, 0.0), (100, 20.0)]
        filled = gap_fill_linear(samples, max_gap_ns=50)
        assert len(filled) == 2

    def test_short_samples_unchanged(self):
        assert gap_fill_linear([(0, 1.0)], max_gap_ns=100) == [(0, 1.0)]
        assert gap_fill_linear([], max_gap_ns=100) == []


class TestDownsampleEdge:
    def test_downsample_aggregates(self):
        samples = [(i * 1_000_000_000, float(i)) for i in range(10)]
        d = downsample(samples, "5s", "sum")
        assert [v for _, v in d] == [10.0, 35.0]

    def test_unsupported_func(self):
        with pytest.raises(ValueError):
            downsample([(0, 1.0)], "5s", "bogus")
