"""Message protocol: framing and validation for ZMQ messages.
Messages are JSON via orjson. A single-part frame holds the whole object for
ingest/query/admin; pubsub uses two-part frames: [topic, json-payload].
"""

import math
from dataclasses import dataclass
from typing import Any

import orjson


class ProtocolError(ValueError):
    """Raised on malformed or invalid messages."""


@dataclass
class IngestMessage:
    """Validated ingest message."""

    metric: str
    value: float
    tags: dict[str, str] | None = None
    # Unix epoch seconds (optional; server time default)
    timestamp: float | None = None

    def to_rows(self) -> tuple[str, dict[str, str] | None, float, int]:
        import time

        ts = self.timestamp if self.timestamp is not None else time.time()
        ts_ns = int(ts * 1_000_000_000)

        return (self.metric, self.tags, self.value, ts_ns)


def dumps(obj: Any) -> bytes:
    """Serialize to JSON bytes via orjson."""
    return orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS)


def loads(data: bytes | bytearray | memoryview) -> Any:
    """Deserialize from JSON bytes."""
    try:
        return orjson.loads(bytes(data))
    except (orjson.JSONDecodeError, ValueError) as exc:
        raise ProtocolError(f"invalid JSON: {exc}") from None


def parse_ingest(
    raw: Any, reject_client_timestamp_skew_s: float | None = None
) -> IngestMessage:
    """Validate an ingest message dict into an IngestMessage.
    ``reject_client_timestamp_skew_s`` (seconds): when > 0, timestamps whose

    skew from the server clock exceeds this are rejected (clock-skew guard).

    """
    if not isinstance(raw, dict):
        raise ProtocolError("ingest message must be an object")
    metric = raw.get("metric")
    if not isinstance(metric, str) or not metric:
        raise ProtocolError("metric must be a non-empty string")
    value = raw.get("value")

    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ProtocolError("value must be a number")
    if isinstance(value, float) and not math.isfinite(value):
        raise ProtocolError("value must be finite")
    tags = raw.get("tags")
    if tags is not None:
        if not isinstance(tags, dict):
            raise ProtocolError("tags must be an object")
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in tags.items()):
            raise ProtocolError("tags must be string->string")
    timestamp = raw.get("timestamp")
    if timestamp is not None and (
        not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool)
    ):
        raise ProtocolError("timestamp must be a number")
    if (
        timestamp is not None
        and isinstance(timestamp, float)
        and not math.isfinite(timestamp)
    ):
        raise ProtocolError("timestamp must be finite")

    if reject_client_timestamp_skew_s and timestamp is not None:
        import time

        skew = abs(time.time() - float(timestamp))
        if skew > reject_client_timestamp_skew_s:
            raise ProtocolError(
                f"client timestamp skew {skew:.1f}s exceeds {reject_client_timestamp_skew_s}s"
            )

    return IngestMessage(
        metric=metric, value=float(value), tags=tags, timestamp=timestamp
    )


def parse_query(raw: Any) -> dict[str, Any]:
    """Validate a query message; returns a normalized dict."""
    if not isinstance(raw, dict):
        raise ProtocolError("query message must be an object")
    if raw.get("type") not in (None, "query"):
        raise ProtocolError("type must be 'query' or omitted")
    metric = raw.get("metric")
    if not isinstance(metric, str) or not metric:
        raise ProtocolError("query metric must be a non-empty string")
    for key in ("start", "end"):
        value = raw.get(key)
        if value is not None and not isinstance(value, int):
            raise ProtocolError(f"{key} must be an integer")
    return dict(raw)


def parse_admin(raw: Any) -> str:
    """Validate an admin command; returns the command name."""
    if not isinstance(raw, dict):
        raise ProtocolError("admin message must be an object")
    cmd = raw.get("cmd")
    if not isinstance(cmd, str) or not cmd:
        raise ProtocolError("admin cmd must be a non-empty string")
    return cmd
