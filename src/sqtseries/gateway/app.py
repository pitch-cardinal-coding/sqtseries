"""REST + WebSocket gateway over FastAPI.

Bridges HTTP to the engine directly (single-process architecture: same
write/query engine, no ZMQ hop). CORS + request-ID middleware.
"""

import uuid
from typing import Any

from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.middleware.cors import CORSMiddleware

from ..config import HttpSettings
from ..engine.store import StorageEngine
from ..query import TimeSeriesDB
from .ratelimit import RateLimitMiddleware
from .routes import router as api_router
from .websocket import subscribe_and_forward


def create_app(
    store: StorageEngine | None = None,
    tsdb: TimeSeriesDB | None = None,
    settings: HttpSettings | None = None,
    pubsub: Any | None = None,
    ingestion: Any | None = None,
    registry: Any | None = None,
) -> FastAPI:
    """Create the FastAPI application.

    Args:
        store: StorageEngine instance.
        tsdb: query facade (created from store if None).
        settings: HTTP settings.
        pubsub: PubSub instance for live WebSocket streaming (optional).
        ingestion: IngestionSettings — used to enforce the client timestamp
            skew guard on the HTTP write path (same as the ZMQ path).
    """
    settings = settings or HttpSettings()
    if tsdb is None:
        if store is None:
            raise ValueError("either store or tsdb is required")
        tsdb = TimeSeriesDB(store)

    # FastAPI serializes JSON directly via Pydantic (Rust-backed, same speed
    # class as orjson) when a response model / return type is declared — the
    # recommended path per FastAPI docs. No custom response class needed.
    app = FastAPI(title="sqtseries", version="0.1.0")

    app.state.tsdb = tsdb
    app.state.store = store
    app.state.pubsub = pubsub
    app.state.settings = settings
    app.state.ingestion = ingestion
    app.state.registry = registry

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(
        RateLimitMiddleware, limit_per_minute=settings.rate_limit_per_minute
    )

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next) -> Response:
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    app.include_router(api_router)

    @app.get("/api/v1/health")
    async def health() -> dict[str, str]:
        from ..health import is_ready

        store = app.state.store
        db_ok = store is not None and is_ready(store.db)
        return {"status": "ok" if db_ok else "degraded", "version": "0.1.0"}

    @app.websocket("/ws/subscribe")
    async def ws_subscribe(websocket: WebSocket, metric: str = "*"):
        await websocket.accept()
        pubsub = app.state.pubsub
        if pubsub is None:
            await websocket.close(code=1008, reason="streaming disabled")
            return
        registry = app.state.registry
        await subscribe_and_forward(
            websocket, pubsub=pubsub, topic=metric, registry=registry
        )

    return app
