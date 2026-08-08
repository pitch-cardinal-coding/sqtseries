"""REST routes for the sqtseries HTTP gateway."""

import math
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query, Request
from starlette.status import HTTP_400_BAD_REQUEST

from ..query import TimeSeriesDB

router = APIRouter(prefix="/api/v1")

# Module-level sentinel so B008 is satisfied while keeping FastAPI body binding.
_WRITE_BODY = Body(...)


def _tsdb(request: Request) -> TimeSeriesDB:
    return request.app.state.tsdb


def _to_ns(seconds: float | None) -> int | None:
    """Convert epoch seconds to nanoseconds; raise 400 on overflow."""
    if seconds is None:
        return None
    try:
        return int(seconds * 1_000_000_000)
    except (OverflowError, ValueError) as exc:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=str(exc)) from None


@router.post("/write")
async def write(request: Request, payload: Any = _WRITE_BODY) -> dict[str, Any]:
    """Ingest one or many measurements."""
    tsdb = _tsdb(request)
    ingestion = getattr(request.app.state, "ingestion", None)
    max_skew = getattr(ingestion, "reject_client_timestamp_skew_s", None)
    if isinstance(payload, list):
        # validate the WHOLE batch first, so a bad item rejects the batch
        # without partially persisting the earlier ones (retry-safe)
        validated = [_validate_row(item, max_skew) for item in payload]
        return {
            "status": "ok",
            "written": sum(_insert_validated(tsdb, v) for v in validated),
        }
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST, detail="body must be an object or array"
        )
    return {
        "status": "ok",
        "written": _insert_validated(tsdb, _validate_row(payload, max_skew)),
    }


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
        if not isinstance(ts, (int, float)) or isinstance(ts, bool):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="timestamp must be a number"
            )
        if isinstance(ts, float) and not math.isfinite(ts):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="timestamp must be finite"
            )
        ts_ns = _to_ns(float(ts))
        # same clock-skew guard as the ZMQ ingest path: keeps the rollup's
        # in-order assumption (a late write must never land in a rolled hour)
        if max_skew and abs(time.time() - float(ts)) > max_skew:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"client timestamp skew exceeds {max_skew}s",
            )
    else:
        ts_ns = None
    return metric, float(value), tags, ts_ns


def _insert_validated(
    tsdb: TimeSeriesDB, v: tuple[str, float, dict | None, int | None]
) -> int:
    metric, value, tags, ts_ns = v
    return tsdb.insert(metric, value, tags, timestamp_ns=ts_ns)


@router.get("/read")
async def read(
    request: Request,
    metric: str = Query(..., description="metric name"),
    start: float | None = Query(None, description="start, epoch seconds"),
    end: float | None = Query(None, description="end, epoch seconds"),
    aggregation: str | None = Query(None),
    interval: str | None = Query(None),
    limit: int | None = Query(None, ge=1, le=100000),
    order: str = Query("asc", pattern="^(asc|desc)$"),
) -> dict[str, Any]:
    tsdb = _tsdb(request)
    try:
        data = tsdb.query(
            metric=metric,
            start=_to_ns(start),
            end=_to_ns(end),
            aggregation=aggregation,
            interval=interval,
            limit=limit,
            order=order,
        )
    except (ValueError, OverflowError) as exc:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=str(exc)) from None
    return {
        "status": "ok",
        "data": [{"timestamp": ts / 1e9, "value": v} for ts, v in data],
    }


@router.get("/aggregate")
async def aggregate(
    request: Request,
    metric: str = Query(...),
    start: float | None = Query(None),
    end: float | None = Query(None),
    funcs: str = Query("avg", description="comma-separated, e.g. avg,min,max,p95"),
) -> dict[str, Any]:
    tsdb = _tsdb(request)
    wanted = [f.strip() for f in funcs.split(",") if f.strip()]
    try:
        result = tsdb.aggregate(
            metric=metric, start=_to_ns(start), end=_to_ns(end), funcs=wanted
        )
    except (ValueError, OverflowError) as exc:
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=str(exc)) from None
    return {"status": "ok", "aggregations": result}


@router.get("/stats")
async def stats(request: Request) -> dict[str, Any]:
    store: Any = request.app.state.store
    payload: dict[str, Any] = {"status": "ok"}
    if store is not None:
        payload["metrics"] = len(store.list_metrics())
        payload["series"] = store.series_count()
    return payload
