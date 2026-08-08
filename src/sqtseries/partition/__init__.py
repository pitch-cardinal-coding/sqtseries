"""Retention & partitions: automatic data lifecycle management."""

from .manager import PartitionManager
from .retention import RetentionManager, RetentionPolicy, parse_ttl
from .rollup import (
    RollupManager,
    completed_hour_ns,
    ensure_rollup_table,
    rollup_new_hours,
    rollup_partition,
    rollup_watermark,
)

__all__ = [
    "PartitionManager",
    "RetentionManager",
    "RetentionPolicy",
    "RollupManager",
    "completed_hour_ns",
    "ensure_rollup_table",
    "parse_ttl",
    "rollup_new_hours",
    "rollup_partition",
    "rollup_watermark",
]
