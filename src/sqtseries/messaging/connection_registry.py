"""Connection and subscription registry for sqtseries.

Tracks every active WebSocket connection and ZMQ SUB subscriber so the service
can answer ``connections``, ``conncheck``, and ``subscribers`` queries at any
time. Events carry arrival/leaving timestamps: WebSocket ``conn`` events include
``connected_at`` and (on leave) ``left_at``; ZMQ ``sub`` events include
``arrived_at`` / ``left_at`` plus ``first_seen`` (when a topic first became
active).
"""

import contextlib
import time
import uuid
from collections.abc import Callable
from typing import Any


class ConnectionRegistry:
    """Central tracker for active WebSocket and ZMQ SUB connections.

    Thread-safe: all mutation methods are synchronous (called from the event
    loop). The registry emits ``conn`` events on every state change and can
    verify connection IDs on demand (``conncheck``).
    """

    def __init__(self):
        # keyed by connection_id (str)
        self._ws: dict[str, dict[str, Any]] = {}
        # topic -> subscriber_count
        self._zmq_subs: dict[str, int] = {}
        # topic -> epoch seconds when its count first went 0 -> 1
        self._zmq_first_seen: dict[str, float] = {}
        self._listeners: list[Callable[[str, dict[str, Any]], None]] = []

    def on_event(self, callback: Callable[[str, dict[str, Any]], None]) -> None:
        """Register a listener for ``(event_type, payload)`` pairs.

        Event types: ``"conn"``, ``"sub"``.
        """
        self._listeners.append(callback)

    def remove_listener(self, callback: Callable[[str, dict[str, Any]], None]) -> None:
        """Remove a previously registered listener (idempotent).

        Every ``on_event`` registration must be paired with a ``remove_listener``
        when the subscriber stops, otherwise repeated start/stop cycles grow
        ``_listeners`` (and keep dead subscriber objects alive) without bound.
        """
        with contextlib.suppress(ValueError):
            self._listeners.remove(callback)

    def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        for cb in self._listeners:
            with contextlib.suppress(Exception):
                cb(event_type, payload)

    def register_ws(self, conn_id: str, peer: str, topic: str) -> None:
        """Record a new WebSocket connection."""
        entry: dict[str, Any] = {
            "kind": "ws",
            "peer": peer,
            "topic": topic,
            "connected_at": time.time(),
        }
        self._ws[conn_id] = entry
        payload = {**entry, "id": conn_id, "connected": True, "ttl": 60}
        self._emit("conn", payload)

    def unregister_ws(self, conn_id: str) -> None:
        """Mark a WebSocket connection as gone.

        The leave event carries the arrival time (``connected_at``) and the
        leaving time (``left_at``) so consumers can show durations.
        """
        entry = self._ws.pop(conn_id, None)
        if entry is None:
            return
        payload = {
            "kind": "ws",
            "peer": entry["peer"],
            "topic": entry["topic"],
            "id": conn_id,
            "connected": False,
            "connected_at": entry["connected_at"],
            "left_at": time.time(),
        }
        self._emit("conn", payload)

    def register_zmq_sub(self, topic: str) -> None:
        """Record a ZMQ SUB subscriber joining ``topic``.

        Emits ``arrived_at`` on every count increase; ``first_seen`` is set
        when a topic goes 0 -> 1 so snapshots can show when it became active.
        """
        count = self._zmq_subs.get(topic, 0) + 1
        self._zmq_subs[topic] = count
        now = time.time()
        if count == 1:
            self._zmq_first_seen[topic] = now
        payload = {
            "kind": "zmq",
            "topic": topic,
            "subscribers": count,
            "arrived_at": now,
            "ttl": 30,
        }
        self._emit("sub", payload)

    def unregister_zmq_sub(self, topic: str) -> None:
        """Record a ZMQ SUB subscriber leaving ``topic``.

        Emits ``left_at`` (leaving time) and ``first_seen`` (when the topic
        first became active) so consumers can show durations.
        """
        count = self._zmq_subs.get(topic, 0) - 1
        first_seen = self._zmq_first_seen.get(topic)
        if count <= 0:
            self._zmq_subs.pop(topic, None)
            self._zmq_first_seen.pop(topic, None)
            count = 0
        else:
            self._zmq_subs[topic] = count
        payload = {
            "kind": "zmq",
            "topic": topic,
            "subscribers": count,
            "left_at": time.time(),
            "first_seen": first_seen,
        }
        self._emit("sub", payload)

    def check_connection(self, conn_id: str) -> bool:
        """Return ``True`` if ``conn_id`` is a currently-registered connection."""
        return conn_id in self._ws

    def list_connections(self) -> list[dict[str, Any]]:
        """Return every active connection entry."""
        return [
            {**entry, "id": cid}
            for cid, entry in sorted(
                self._ws.items(), key=lambda item: item[1]["connected_at"]
            )
        ]

    def subscriber_count(self, topic: str | None = None) -> int:
        """Number of ZMQ SUB subscribers for ``topic``, or total across all topics."""
        if topic:
            return self._zmq_subs.get(topic, 0)
        return sum(self._zmq_subs.values())

    def active_topics(self) -> list[str]:
        """Topics that have at least one ZMQ SUB subscriber."""
        return sorted(self._zmq_subs.keys())

    @property
    def ws_count(self) -> int:
        return len(self._ws)

    @property
    def zmq_sub_count(self) -> int:
        return sum(self._zmq_subs.values())

    def snapshot(self) -> dict[str, Any]:
        """Return a read-only summary of current state."""
        return {
            "ws_connections": self.ws_count,
            "zmq_subscribers": self.zmq_sub_count,
            "active_topics": len(self._zmq_subs),
            "connections": [
                {
                    "id": cid,
                    "kind": entry["kind"],
                    "peer": entry["peer"],
                    "topic": entry["topic"],
                }
                for cid, entry in self._ws.items()
            ],
            "subscriptions": [
                {
                    "topic": topic,
                    "subscribers": count,
                    "first_seen": self._zmq_first_seen.get(topic),
                }
                for topic, count in sorted(self._zmq_subs.items())
            ],
        }


def new_connection_id() -> str:
    return uuid.uuid4().hex[:12]
