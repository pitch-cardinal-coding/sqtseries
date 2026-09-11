"""Query engine for sqtseries: aggregation, downsampling, gap filling."""

from .agg import (
    AggregationFunction,
    aggregate_series,
    downsample,
    downsample_to_intervals,
    gap_fill_linear,
    median,
    p95,
    p99,
    parse_interval,
)
from .builder import MaxRowsExceededError, QueryError, TimeSeriesDB

__all__ = [
    "AggregationFunction",
    "MaxRowsExceededError",
    "QueryError",
    "TimeSeriesDB",
    "aggregate_series",
    "downsample",
    "downsample_to_intervals",
    "gap_fill_linear",
    "median",
    "p95",
    "p99",
    "parse_interval",
]
