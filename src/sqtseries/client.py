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
        # Set when close() begins terminating the context. A subscribe()
        # iterator parked in poll() on another thread then wakes with
        # ENOTSOCK/ContextTerminated — that is the cooperative stop signal,
        # not an error, so subscribe() returns instead of raising.
        self._closing = False
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
        self,
        topic: str = "*",
        timeout: float = 0.5,
        resync_interval_s: float = 30.0,
    ) -> Iterator[dict[str, Any] | None]:
        """Yield live measurement dicts matching ``topic`` (SUB socket).

        Polls with a timeout and yields ``None`` as a heartbeat when idle, so

        callers can break out of the loop cleanly (e.g. on shutdown) without

        blocking forever in recv. Closing the client while the iterator is

        blocked would otherwise abort libzmq (context close is not thread-safe

        against active recv). Pattern per pyzmq docs: poll then recv NOBLOCK.

        A transparent ZMQ reconnect does not always re-deliver subscriptions
        after a server restart (observed: re-handshaked sessions staying
        invisible indefinitely), so every ``resync_interval_s`` the client
        compares the server uptime and recreates the socket when a restart
        is detected. Recreation is count-safe: closing drops at most our own
        registration, and the fresh SUBSCRIBE re-adds exactly one.
        """
        import time

        sock = self._get_sub_sock()
        sub_bytes = b"" if topic == "*" else topic.encode()
        sock.setsockopt(zmq.SUBSCRIBE, sub_bytes)
        last_resync = time.monotonic()
        last_uptime: float | None = None
        while True:
            if self._closing:
                # close() was called (possibly from another thread while this
                # iterator was parked in poll()): stop yielding, no raise.
                return
            try:
                events = sock.poll(timeout=int(timeout * 1000), flags=zmq.POLLIN)
            except zmq.ZMQError as exc:
                # ctx.term() interrupts the parked poll. Racing close():
                # _closing may not be set yet when the poll explodes.
                if self._closing or exc.errno in (zmq.ENOTSOCK, zmq.ETERM):
                    return
                raise
            if not events:
                yield None
            else:
                try:
                    frames = sock.recv_multipart(flags=zmq.NOBLOCK)
                except zmq.Again:
                    yield None
                    continue
                except zmq.ZMQError as exc:
                    # Same race as poll(): close() tore down the context between
                    # the poll and this recv. That is a stop signal, not an error.
                    if self._closing or exc.errno in (zmq.ENOTSOCK, zmq.ETERM):
                        return
                    raise
                if len(frames) < 2:
                    continue
                yield orjson.loads(frames[1])
            due = time.monotonic() - last_resync >= resync_interval_s
            if resync_interval_s > 0 and due and not self._closing:
                last_resync = time.monotonic()
                uptime = self._server_uptime()
                if uptime is not None:
                    if last_uptime is not None and uptime < last_uptime:
                        try:
                            sock.close(linger=0)
                            self._sub_sock = None
                            sock = self._get_sub_sock()
                            sock.setsockopt(zmq.SUBSCRIBE, sub_bytes)
                        except zmq.ZMQError as exc:
                            # close() tore down the context mid-recreation.
                            if self._closing or exc.errno in (zmq.ENOTSOCK, zmq.ETERM):
                                return
                            raise
                    last_uptime = uptime

    def _server_uptime(self) -> float | None:
        """Return the service uptime in seconds, or None when unreachable.

        Uses a throwaway REQ socket so a timeout here can never break the
        cached admin socket (REQ strict alternation) or any live iterator.
        """
        try:
            sock = self._ctx.socket(zmq.REQ)
        except zmq.ZMQError as exc:
            # close() destroyed the context before we could even open the probe.
            if self._closing or exc.errno in (zmq.ENOTSOCK, zmq.ETERM):
                return None
            raise
        try:
            sock.setsockopt(zmq.LINGER, 0)
            sock.setsockopt(zmq.RCVTIMEO, 5000)
            sock.connect(f"tcp://{self.host}:{self.ports['admin']}")
            sock.send(orjson.dumps({"cmd": "stats"}))
            reply = orjson.loads(sock.recv())
        except zmq.Again:
            return None
        except zmq.ZMQError as exc:
            # Context torn down mid-probe by close(): a stop signal, not an error.
            if self._closing or exc.errno in (zmq.ENOTSOCK, zmq.ETERM):
                return None
            raise
        finally:
            sock.close(linger=0)
        if reply.get("status") != "ok":
            return None
        uptime = reply.get("uptime_s")
        return float(uptime) if isinstance(uptime, (int, float)) else None

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
        # Flag first so a subscribe() iterator parked in poll() on another
        # thread treats the upcoming context-termination interrupt as a stop
        # signal rather than an error.
        self._closing = True

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
