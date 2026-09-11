"""REST routes for the sqtseries HTTP gateway."""

import asyncio
import math
from typing import Any

import structlog
from fastapi import APIRouter, Body, HTTPException, Query, Request
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_413_CONTENT_TOO_LARGE,
    HTTP_504_GATEWAY_TIMEOUT,
)

from ..query import MaxRowsExceededError, TimeSeriesDB

router = APIRouter(prefix="/api/v1")
log = structlog.get_logger(__name__)

# Module-level sentinel so B008 is satisfied while keeping FastAPI body binding.
_WRITE_BODY = Body(...)

# Default cap on a single HTTP read/aggregate, matching query.timeout_s.
_DEFAULT_QUERY_TIMEOUT_S = 30.0


def _tsdb(request: Request) -> TimeSeriesDB:
    return request.app.state.tsdb


def _count_http(request: Request, key: str, n: int = 1) -> None:
    counters = getattr(request.app.state, "http_counters", None)
    if counters is not None:
        counters[key] = counters.get(key, 0) + n


def _to_ns(seconds: float | str | None) -> int | None:
    """Convert epoch seconds or ISO-8601 to nanoseconds; 400 on bad input."""
    if seconds is None:
        return None
    if isinstance(seconds, str):
        s = seconds.strip()
        if not s:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="timestamp string is empty"
            )
        try:
            f = float(s)
            seconds = f
        except ValueError:
            from ..messaging.protocol import iso_to_ns

            try:
                return iso_to_ns(s)
            except ValueError as exc:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST, detail=str(exc)
                ) from None
    try:
        return int(seconds * 1_000_000_000)
    except (OverflowError, ValueError) as exc:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=str(exc)) from None


@router.post("/write")
async def write(request: Request, payload: Any = _WRITE_BODY) -> dict[str, Any]:
    # Create ingress: validated rows converge on StorageEngine.insert_many
    # (the CUD Create funnel) via _insert_validated_batch below.
    """Ingest one or many measurements."""
    tsdb = _tsdb(request)

    ingestion = getattr(request.app.state, "ingestion", None)

    pubsub = getattr(request.app.state, "pubsub", None)

    max_skew = getattr(ingestion, "reject_client_timestamp_skew_s", None)

    if isinstance(payload, list):
        # validate the WHOLE batch first, so a bad item rejects the batch
        # without partially persisting the earlier ones (retry-safe)
        validated = [_validate_row(item, max_skew) for item in payload]
        # insert_many is a blocking BEGIN IMMEDIATE + commit — run it off the
        # event loop (same treatment as /read) so a sustained write load can
        # never stall WS streaming, pings, or other HTTP requests.
        written = await _run_query(
            request, lambda: _insert_validated_batch(tsdb, validated)
        )

        if pubsub is not None:
            await _broadcast(pubsub, validated)
        _count_http(request, "writes", written)
        return {
            "status": "ok",
            "written": written,
        }
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST, detail="body must be an object or array"
        )
    validated = _validate_row(payload, max_skew)
    written = await _run_query(request, lambda: _insert_validated(tsdb, validated))

    if pubsub is not None:
        await _broadcast(pubsub, [validated])
    _count_http(request, "writes", written)
    return {
        "status": "ok",
        "written": written,
    }


async def _broadcast(
    pubsub: Any, rows: list[tuple[str, float, dict | None, int | None]]
) -> None:
    """Republish HTTP writes to live subscribers, like the ZMQ ingest path.

    Best-effort: the write is already persisted; a failing broadcast (e.g.

    during shutdown) must not turn a successful write into a 500.
    """
    for metric, value, tags, _ts_ns in rows:
        try:
            await pubsub.publish(
                metric.encode(), {"metric": metric, "tags": tags, "value": value}
            )
        except Exception:
            log.warning("http publish failed", metric=metric)


def _validate_row(
    row: Any, max_skew: float | None
) -> tuple[str, float, dict | None, int | None]:
    """Validate a write body into (metric, value, tags, ts_ns); 400 on bad input."""

    import time

    if not isinstance(row, dict):
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST, detail="each write must be an object"
        )
    metric = row.get("metric")
    if not isinstance(metric, str) or not metric:
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST, detail="metric must be a non-empty string"
        )
    value = row.get("value")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST, detail="value must be a number"
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST, detail="value must be finite"
        )
    tags = row.get("tags")
    if tags is not None and (
        not isinstance(tags, dict)
        or not all(isinstance(k, str) and isinstance(v, str) for k, v in tags.items())
    ):
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail="tags must be string->string object",
        )
    ts = row.get("timestamp")
    if ts is not None:
        if isinstance(ts, str):
            ts_ns = _to_ns(ts)
            ts_f = ts_ns / 1_000_000_000
        else:
            if not isinstance(ts, (int, float)) or isinstance(ts, bool):
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail="timestamp must be a number or ISO-8601 string",
                )
            if isinstance(ts, float) and not math.isfinite(ts):
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST, detail="timestamp must be finite"
                )
            try:
                ts_f = float(ts)
            except OverflowError:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST, detail="timestamp out of range"
                ) from None
            ts_ns = _to_ns(ts_f)
        # the storage layer stores timestamps as signed 64-bit ns (SQLite
        # INTEGER), so reject anything unrepresentable instead of letting the
        # insert 500 on a sqlite OverflowError.
        if not -(2**63) <= ts_ns <= 2**63 - 1:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="timestamp out of range"
            ) from None
        # same clock-skew guard as the ZMQ ingest path: keeps the rollup's
        # in-order assumption (a late write must never land in a rolled hour)

        if max_skew and abs(time.time() - ts_f) > max_skew:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"client timestamp skew exceeds {max_skew}s",
            )
    else:
        ts_ns = None
    try:
        value_f = float(value)
    except OverflowError:
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST, detail="value out of range"
        ) from None
    return metric, value_f, tags, ts_ns


def _insert_validated(
    tsdb: TimeSeriesDB, v: tuple[str, float, dict | None, int | None]
) -> int:
    metric, value, tags, ts_ns = v
    return tsdb.insert(metric, value, tags, timestamp_ns=ts_ns)


def _insert_validated_batch(
    tsdb: TimeSeriesDB, rows: list[tuple[str, float, dict | None, int | None]]
) -> int:
    """Persist a validated batch in ONE transaction.
    Rows without a timestamp get server time. Timestamp-less rows must each

    get a distinct ns, or two rows for the same series would collide on the

    clustered PRIMARY KEY (series_id, timestamp_ns).
    """
    import time

    now = time.time_ns()

    prepared = [
        (metric, tags, value, now + i if ts_ns is None else ts_ns)
        for i, (metric, value, tags, ts_ns) in enumerate(rows)
    ]
    return tsdb.store.insert_many(prepared)


async def _run_query(request: Request, fn: Any) -> Any:
    """Run a (blocking) query off the event loop so a slow aggregate can't

    stall WS streaming, pings, admin, or other HTTP requests.
    Mirrors the ZMQ broker path (asyncio.to_thread + wait_for). The timeout

    comes from query.timeout_s (default 30s; None or <= 0 disables); the abandoned thread finishes in

    the background and its result is discarded.
    """
    timeout = getattr(request.app.state, "query_timeout_s", _DEFAULT_QUERY_TIMEOUT_S)

    try:
        if timeout is None or timeout <= 0:
            return await asyncio.to_thread(fn)
        return await asyncio.wait_for(asyncio.to_thread(fn), timeout=timeout)
    except TimeoutError:
        raise HTTPException(
            status_code=HTTP_504_GATEWAY_TIMEOUT,
            detail=f"query exceeded {timeout}s",
        ) from None


@router.get("/read")
async def read(
    request: Request,
    metric: str = Query(..., description="metric name"),
    start: str | float | None = Query(
        None, description="start, epoch seconds or ISO-8601"
    ),
    end: str | float | None = Query(None, description="end, epoch seconds or ISO-8601"),
    aggregation: str | None = Query(None),
    interval: str | None = Query(None),
    limit: int | None = Query(None, ge=1, le=100000),
    order: str = Query("asc", pattern="^(asc|desc)$"),
) -> dict[str, Any]:
    tsdb = _tsdb(request)
    try:
        data = await _run_query(
            request,
            lambda: tsdb.query(
                metric=metric,
                start=_to_ns(start),
                end=_to_ns(end),
                aggregation=aggregation,
                interval=interval,
                limit=limit,
                order=order,
            ),
        )
    except MaxRowsExceededError as exc:
        # 413: the request is valid, but its result size exceeds the server's
        # bounded-work policy (bounded-queue doctrine) — narrow the window or use
        # an aggregation.
        raise HTTPException(
            status_code=HTTP_413_CONTENT_TOO_LARGE, detail=str(exc)
        ) from None
    except (ValueError, OverflowError) as exc:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=str(exc)) from None
    _count_http(request, "queries")
    return {
        "status": "ok",
        "data": [{"timestamp": ts / 1e9, "value": v} for ts, v in data],
    }


@router.get("/aggregate")
async def aggregate(
    request: Request,
    metric: str = Query(...),
    start: str | float | None = Query(None),
    end: str | float | None = Query(None),
    funcs: str = Query("avg", description="comma-separated, e.g. avg,min,max,p95"),
) -> dict[str, Any]:
    tsdb = _tsdb(request)

    wanted = [f.strip() for f in funcs.split(",") if f.strip()]
    try:
        result = await _run_query(
            request,
            lambda: tsdb.aggregate(
                metric=metric, start=_to_ns(start), end=_to_ns(end), funcs=wanted
            ),
        )
    except MaxRowsExceededError as exc:
        raise HTTPException(
            status_code=HTTP_413_CONTENT_TOO_LARGE, detail=str(exc)
        ) from None
    except (ValueError, OverflowError) as exc:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=str(exc)) from None
    _count_http(request, "queries")
    return {"status": "ok", "aggregations": result}


@router.get("/stats")
async def stats(request: Request) -> dict[str, Any]:
    store: Any = request.app.state.store
    payload: dict[str, Any] = {"status": "ok"}
    if store is not None:
        # list_metrics/series_count are full scans; they must run off the
        # event loop (the dashboard polls /stats every second, and a slow
        # scan here would stall every other request and WS tick).
        metrics, series = await _run_query(request, lambda: _store_counts(store))
        payload["metrics"] = metrics
        payload["series"] = series
    return payload


def _store_counts(store: Any) -> tuple[int, int]:
    return len(store.list_metrics()), store.series_count()


@router.get("/connections")
async def connections(request: Request) -> dict[str, Any]:
    """Live list of active WebSocket connections (kind/peer/topic/connected_at)."""

    registry = getattr(request.app.state, "registry", None)
    if registry is None:
        return {"status": "ok", "data": []}
    return {"status": "ok", "data": registry.list_connections()}


@router.get("/subscribers")
async def subscribers(request: Request) -> dict[str, Any]:
    """Live ZMQ SUB subscriptions (topic -> subscriber count).

    ``topics`` lists every known topic with its live count (0 when idle);
    ``subscriptions`` keeps the active-only view.
    """
    registry = getattr(request.app.state, "registry", None)
    if registry is None:
        return {"status": "ok", "zmq_subscribers": 0, "subscriptions": []}
    snap = registry.snapshot()

    return {
        "status": "ok",
        "zmq_subscribers": snap["zmq_subscribers"],
        "subscriptions": snap["subscriptions"],
        "topics": [{**entry, "total": None} for entry in registry.known_topics()],
    }
