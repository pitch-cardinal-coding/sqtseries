"""ZeroMQ context and socket helpers.
Socket defaults derived from measured benchmarks:
- LINGER must be set BEFORE bind/connect (pyzmq#1407).
- Server sockets: LINGER 500ms (clean shutdown drain).
- Client sockets: LINGER 0ms.
- HWM: 10,000 for high-traffic ingestion (backpressure before overflow).
"""

from typing import Any

import structlog
import zmq

log = structlog.get_logger(__name__)

DEFAULT_HWM = 10000
SERVER_LINGER_MS = 500
CLIENT_LINGER_MS = 0


def socket_options(
    *,
    hwm: int = DEFAULT_HWM,
    linger_ms: int = SERVER_LINGER_MS,
    sndhwm: int | None = None,
    rcvhwm: int | None = None,
    max_msg_size: int = 50 * 1024 * 1024,
    heartbeat_ivl: int = 1000,
    heartbeat_timeout: int = 5000,
    heartbeat_ttl: int = 5000,
    immediate: bool = True,
) -> dict[int, Any]:
    """Return socket options to apply before bind/connect.
    LINGER is set here (before bind) per pyzmq#1407 guidance.
    """
    opts: dict[int, Any] = {
        zmq.LINGER: linger_ms,
        zmq.MAXMSGSIZE: max_msg_size,
        zmq.HEARTBEAT_IVL: heartbeat_ivl,
        zmq.HEARTBEAT_TIMEOUT: heartbeat_timeout,
        zmq.HEARTBEAT_TTL: heartbeat_ttl,
        zmq.IMMEDIATE: int(bool(immediate)),
    }
    if hwm is not None:
        opts[zmq.SNDHWM] = sndhwm if sndhwm is not None else hwm
        opts[zmq.RCVHWM] = rcvhwm if rcvhwm is not None else hwm
    return opts


def apply_options(sock: zmq.Socket, options: dict[int, Any]) -> zmq.Socket:
    """Apply options, tolerating per-option failures (defensive)."""
    for opt, value in options.items():
        try:
            sock.setsockopt(opt, value)
        except zmq.ZMQError as exc:  # pragma: no cover - defensive
            log.warning("failed to set socket option %s: %s", opt, exc)
    return sock
