"""High-level Python client for sqtseries (external access).
Wraps the ZeroMQ transports:
- PUSH (write) on 12501
- REQ (query) on 12502
- SUB (subscribe) on 12503
Same JSON wire format as the HTTP gateway (see dist/docs/clients.html).
"""

from collections.abc import Iterator
from typing import Any

import orjson
import zmq

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORTS = {"write": 12501, "query": 12502, "subscribe": 12503, "admin": 12504}
# Query/admin replies fail with ClientError instead of hanging
RECV_TIMEOUT_MS = 30_000
# The write PUSH socket flushes queued measurements for this long on close.
# Zero would drop a measurement written immediately before exit (verified:
# write-then-close delivered 0 of 1 messages). Matches examples/producer.py.
WRITE_LINGER_MS = 2000


class Client:
    """ZeroMQ client for a running sqtseries service.
    Usage::
        from sqtseries.client import Client
        client = Client()
        client.write("cpu.usage", 0.72, {"host": "web1"})
        # query start/end are epoch NANOSECONDS or ISO-8601 strings;
        # rows carry epoch-seconds timestamps

        rows = client.query(
            "cpu.usage",
            start=1_691_234_567_000_000_000,
            end=1_691_238_167_000_000_000,
        )
        for payload in client.subscribe("cpu."):
            print(payload)
    """

    def __init__(self, host: str = DEFAULT_HOST, ports: dict[str, int] | None = None):
        self.host = host
        self.ports = {**DEFAULT_PORTS, **(ports or {})}
        self._ctx = zmq.Context()
        self._write_sock: zmq.Socket | None = None
        self._query_sock: zmq.Socket | None = None
        self._sub_sock: zmq.Socket | None = None
        self._admin_sock: zmq.Socket | None = None

    def write(
        self,
        metric: str,
        value: float,
        tags: dict[str, str] | None = None,
        timestamp: float | str | None = None,
    ) -> None:
        """Send one measurement (PUSH, fire-and-forget). Timestamp is epoch seconds or ISO-8601."""
        payload: dict[str, Any] = {"metric": metric, "value": value}
        if tags:
            payload["tags"] = tags
        if timestamp is not None:
            payload["timestamp"] = timestamp
        sock = self._get_write_sock()
        sock.send(orjson.dumps(payload))

    def write_many(self, points: list[dict[str, Any]]) -> None:
        """Send multiple measurements in one loop (still one frame each)."""

        sock = self._get_write_sock()
        for point in points:
            sock.send(orjson.dumps(point))

    def query(
        self,
        metric: str,
        start: int | str | None = None,
        end: int | str | None = None,
        *,
        aggregation: str | None = None,
        interval: str | None = None,
        limit: int | None = None,
        order: str = "asc",
    ) -> list[dict[str, Any]]:
        """Run a query; returns [{timestamp, value}, ...] (seconds)."""

        req: dict[str, Any] = {"type": "query", "metric": metric}
        if start is not None:
            req["start"] = start
        if end is not None:
            req["end"] = end
        if aggregation:
            req["aggregation"] = aggregation
        if interval:
            req["interval"] = interval
        if limit is not None:
            req["limit"] = limit
        if order != "asc":
            req["order"] = order

        sock = self._get_query_sock()
        sock.send(orjson.dumps(req))
        try:
            reply = orjson.loads(sock.recv())
        except zmq.Again:
            # REQ enforces strict send/recv alternation: a timed-out recv
            # leaves the socket awaiting a reply, so the next send would raise
            # EFSM and permanently break it. Drop it; the next call recreates.

            self._reset_sock("_query_sock")
            raise ClientError("query timed out (no reply within 30s)") from None
        if reply.get("status") == "error":
            err = reply.get("error", {})
            raise ClientError(err.get("message", "query failed"))
        return reply.get("data", [])

    def aggregate(
        self,
        metric: str,
        start: int | str | None = None,
        end: int | str | None = None,
        *,
        funcs: list[str] | None = None,
    ) -> dict[str, float]:
        """Compute aggregations over a window; returns {func: value}."""

        wanted = ",".join(funcs or ["avg"])
        req: dict[str, Any] = {
            "type": "query",
            "metric": metric,
            "aggregations": wanted,
        }
        if start is not None:
            req["start"] = start
        if end is not None:
            req["end"] = end
        sock = self._get_query_sock()
        sock.send(orjson.dumps(req))
        try:
            reply = orjson.loads(sock.recv())
        except zmq.Again:
            self._reset_sock("_query_sock")
            raise ClientError("aggregate timed out (no reply within 30s)") from None
        if reply.get("status") == "error":
            err = reply.get("error", {})
            raise ClientError(err.get("message", "aggregate failed"))
        return reply.get("data", {})

    def admin(self, cmd: str, **kwargs: Any) -> dict[str, Any]:
        """Send an admin command (ping/health/stats/optimize/backup/vacuum,

        connections/conncheck/subscribers). Extra keyword arguments become

        part of the request, e.g. ``admin("conncheck", ids=["abc"])``."""

        sock = self._get_admin_sock()
        sock.send(orjson.dumps({"cmd": cmd, **kwargs}))
        try:
            reply = orjson.loads(sock.recv())
        except zmq.Again:
            self._reset_sock("_admin_sock")
            raise ClientError("admin command timed out (no reply within 30s)") from None
        if reply.get("status") == "error":
            err = reply.get("error", {})
            raise ClientError(err.get("message", "admin command failed"))
        return reply

    def subscribe(
        self, topic: str = "*", timeout: float = 0.5
    ) -> Iterator[dict[str, Any] | None]:
        """Yield live measurement dicts matching ``topic`` (SUB socket).

        Polls with a timeout and yields ``None`` as a heartbeat when idle, so

        callers can break out of the loop cleanly (e.g. on shutdown) without

        blocking forever in recv. Closing the client while the iterator is

        blocked would otherwise abort libzmq (context close is not thread-safe

        against active recv). Pattern per pyzmq docs: poll then recv NOBLOCK.

        """
        sock = self._get_sub_sock()
        if topic != "*":
            sock.setsockopt(zmq.SUBSCRIBE, topic.encode())
        else:
            sock.setsockopt(zmq.SUBSCRIBE, b"")
        while True:
            events = sock.poll(timeout=int(timeout * 1000), flags=zmq.POLLIN)
            if not events:
                yield None
                continue
            try:
                frames = sock.recv_multipart(flags=zmq.NOBLOCK)
            except zmq.Again:
                yield None
                continue
            if len(frames) < 2:
                continue
            yield orjson.loads(frames[1])

    def _get_write_sock(self) -> zmq.Socket:
        if self._write_sock is None:
            sock = self._ctx.socket(zmq.PUSH)
            sock.setsockopt(zmq.SNDHWM, 10000)
            sock.setsockopt(zmq.LINGER, WRITE_LINGER_MS)
            sock.connect(f"tcp://{self.host}:{self.ports['write']}")
            self._write_sock = sock
        return self._write_sock

    def _get_query_sock(self) -> zmq.Socket:
        if self._query_sock is None:
            sock = self._ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 500)
            sock.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)
            sock.connect(f"tcp://{self.host}:{self.ports['query']}")
            self._query_sock = sock
        return self._query_sock

    def _get_sub_sock(self) -> zmq.Socket:
        if self._sub_sock is None:
            sock = self._ctx.socket(zmq.SUB)
            sock.setsockopt(zmq.LINGER, 0)
            # Capped backoff spreads simultaneous reconnects after a restart.
            sock.setsockopt(zmq.RECONNECT_IVL, 100)
            sock.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)
            from sqtseries.messaging.context import apply_tcp_keepalive

            apply_tcp_keepalive(sock)
            sock.connect(f"tcp://{self.host}:{self.ports['subscribe']}")

            self._sub_sock = sock
        return self._sub_sock

    def _get_admin_sock(self) -> zmq.Socket:
        if self._admin_sock is None:
            sock = self._ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 500)
            sock.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)
            sock.connect(f"tcp://{self.host}:{self.ports['admin']}")
            self._admin_sock = sock
        return self._admin_sock

    def _reset_sock(self, attr: str) -> None:
        """Close a broken socket so the next call recreates it.
        Only REQ sockets need this (their strict send/recv alternation leaves

        them unusable after a recv timeout); it is harmless for the others.

        """
        sock = getattr(self, attr, None)
        if sock is not None:
            sock.close(linger=0)
            setattr(self, attr, None)

    def close(self) -> None:
        """Close all sockets and the context (idempotent)."""
        # The write socket keeps a flush linger so measurements queued but not
        # yet delivered are still sent; everything else can close immediately.

        if self._write_sock is not None:
            self._write_sock.close(linger=WRITE_LINGER_MS)
            self._write_sock = None
        for sock in (self._query_sock, self._sub_sock, self._admin_sock):
            if sock is not None:
                sock.close(linger=0)
        self._query_sock = self._sub_sock = self._admin_sock = None
        self._ctx.term()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class ClientError(Exception):
    """Raised when the service returns an error reply."""
