"""ZeroMQ messaging layer (externally facing + internal plumbing)."""

from .admin import AdminBroker
from .broker import QueryBroker
from .connection_registry import ConnectionRegistry, new_connection_id
from .context import DEFAULT_HWM, apply_options, socket_options
from .ingress import Ingress
from .protocol import (
    IngestMessage,
    ProtocolError,
    dumps,
    parse_admin,
    parse_ingest,
    parse_query,
)
from .pubsub import PubSub, SubscriptionTracker
from .query_cache import QueryResultCache
from .stats_publisher import StatsPublisher
from .worker import WorkerPool

__all__ = [
    "DEFAULT_HWM",
    "AdminBroker",
    "ConnectionRegistry",
    "IngestMessage",
    "Ingress",
    "ProtocolError",
    "PubSub",
    "QueryBroker",
    "QueryResultCache",
    "StatsPublisher",
    "SubscriptionTracker",
    "WorkerPool",
    "apply_options",
    "dumps",
    "new_connection_id",
    "parse_admin",
    "parse_ingest",
    "parse_query",
    "socket_options",
]
