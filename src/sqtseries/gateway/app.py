"""REST + WebSocket gateway over FastAPI.
Bridges HTTP to the engine directly (single-process architecture: same
write/query engine, no ZMQ hop). CORS + request-ID middleware.
"""

import asyncio
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ..config import HttpSettings
from ..engine.store import StorageEngine
from ..query import TimeSeriesDB
from .dashboard import dashboard_stream
from .ratelimit import RateLimitMiddleware
from .routes import router as api_router
from .websocket import subscribe_and_forward

STATIC_DIR = Path(__file__).resolve().parent / "static"


def _find_docs_dir() -> Path | None:
    """Documentation directory for the /docs mount, if shipped.

    Release wheels carry a docs_data copy (see scripts/build_wheel.sh);
    a source checkout falls back to dist/docs next to the repo root.
    None when neither exists — the dashboard Docs link then 404s instead
    of serving stale or missing pages.
    """
    packaged = Path(__file__).resolve().parent.parent / "docs_data"
    if (packaged / "index.html").is_file():
        return packaged
    repo = Path(__file__).resolve().parent.parent.parent.parent / "dist" / "docs"
    if (repo / "index.html").is_file():
        return repo
    return None


DOCS_DIR = _find_docs_dir()


class HeaderMiddleware:
    """Pure ASGI middleware: X-Request-ID + hardening response headers.

    Deliberately NOT ``@app.middleware("http")``: that decorator wraps every
    request in a ``BaseHTTPMiddleware`` task group + anyio memory stream —
    measurable per-request allocation that py-spy showed dominating stacks
    under load. This adds only the header values themselves.

    Security headers per COMPLIANCE.md; the gateway serves JSON only, so a
    strict CSP is safe; WebSockets (scope type != "http") pass untouched.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = None
        for key, value in scope.get("headers", []):
            if key == b"x-request-id":
                request_id = value.decode("latin-1")
                break
        if not request_id:
            request_id = uuid.uuid4().hex[:12]

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers["X-Request-ID"] = request_id
                headers["X-Frame-Options"] = "DENY"
                headers["X-Content-Type-Options"] = "nosniff"
                headers["X-XSS-Protection"] = "1; mode=block"
                headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
                headers["Permissions-Policy"] = (
                    "geolocation=(), microphone=(), camera=(), payment=(), usb=()"
                )
                # A route may set its own CSP (e.g. ReDoc needs inline
                # styles); the default stays tight everywhere else.
                if "content-security-policy" not in headers:
                    headers["Content-Security-Policy"] = (
                        "default-src 'self'; img-src 'self' data:"
                    )
            await send(message)

        await self.app(scope, receive, send_with_headers)


def create_app(
    store: StorageEngine | None = None,
    tsdb: TimeSeriesDB | None = None,
    settings: HttpSettings | None = None,
    pubsub: Any | None = None,
    ingestion: Any | None = None,
    registry: Any | None = None,
    query_timeout_s: float | None = None,
    query_max_rows: int = 10_000,
    stats_provider: Any | None = None,
) -> FastAPI:
    """Create the FastAPI application.
    Args:
        store: StorageEngine instance.
        tsdb: query facade (created from store if None).
        settings: HTTP settings.
        pubsub: PubSub instance for live WebSocket streaming (optional).

        ingestion: IngestionSettings — used to enforce the client timestamp
            skew guard on the HTTP write path (same as the ZMQ path).
        query_timeout_s: cap for one HTTP read/aggregate (None disables);
            the query runs off the event loop in a thread.
        query_max_rows: raw-row cap for queries run by the built-in tsdb
            (gateway-only mode); ignored when a ``tsdb`` is passed in.
        stats_provider: zero-arg callable returning the full service stats
            dict for the dashboard push channel (None = degraded snapshot
            from store + registry only).
    """
    settings = settings or HttpSettings()
    if tsdb is None:
        if store is None:
            raise ValueError("either store or tsdb is required")
        tsdb = TimeSeriesDB(store, max_rows=query_max_rows)

    # FastAPI serializes JSON directly via Pydantic (Rust-backed, same speed
    # class as orjson) when a response model / return type is declared — the
    # recommended path per FastAPI docs. No custom response class needed.

    app = FastAPI(
        title="sqtseries",
        version="0.1.0",
        # The /docs path serves the shipped user documentation instead, and
        # the gateway CSP blocks FastAPI's CDN-backed stock pages — so the
        # stock UI is off and /api-docs + /redoc below serve self-hosted
        # equivalents from vendored assets (no CDN, no inline scripts).
        # img-src allows data: for library icons (inert in image context).
        # Machine-readable schema: /openapi.json.
        docs_url=None,
        redoc_url=None,
        openapi_url="/openapi.json",
    )

    app.state.tsdb = tsdb
    app.state.store = store
    app.state.pubsub = pubsub
    app.state.settings = settings
    app.state.ingestion = ingestion
    app.state.registry = registry
    # Cap for one HTTP read/aggregate (None = no timeout). The query always
    # runs off the event loop in a thread regardless, so the loop stays
    # responsive even without a cap.
    app.state.query_timeout_s = query_timeout_s
    app.state.stats_provider = stats_provider
    # Shared HTTP counters: the ZMQ worker stats (ingress recv, broker
    # requests) never see HTTP traffic, so handlers tally here and the
    # service merges both into the admin stats counters.
    app.state.http_counters = {"writes": 0, "queries": 0}
    # Live WebSocket connection budget (checked in each ws endpoint).
    app.state.ws_count = 0
    app.state.ws_max_connections = settings.max_websocket_connections

    # Middleware is LIFO: the last one added runs outermost. HeaderMiddleware
    # (added below) is outermost, so even rate-limited 429 responses carry the
    # request-ID + hardening headers; then the rate limiter, then CORS nearest
    # the router. All three short-circuit non-HTTP scopes, so CORS does not
    # gate WebSockets: /ws/subscribe accepts before any CORS or rate-limit
    # check.

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(
        RateLimitMiddleware, limit_per_minute=settings.rate_limit_per_minute
    )
    app.add_middleware(HeaderMiddleware)

    app.include_router(api_router)

    # Dashboard assets live under their own prefix so GET /dashboard
    # always hits the page route below (a mount at /dashboard would
    # swallow it via slash-redirect).
    if STATIC_DIR.is_dir():
        app.mount(
            "/dashboard-assets",
            StaticFiles(directory=STATIC_DIR),
            name="dashboard-assets",
        )

        @app.get("/dashboard", include_in_schema=False)
        @app.get("/", include_in_schema=False)
        async def dashboard_page() -> FileResponse:
            return FileResponse(STATIC_DIR / "dashboard.html")

        @app.get("/api-docs", include_in_schema=False)
        async def swagger_page() -> HTMLResponse:
            return HTMLResponse(
                "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>"
                "<meta name='viewport' content='width=device-width, initial-scale=1'>"
                "<title>sqtseries - Swagger UI</title>"
                "<link rel='icon' href='/dashboard-assets/favicon.svg' type='image/svg+xml'>"
                "<link rel='stylesheet' href='/dashboard-assets/specs/swagger-ui.css'>"
                "</head><body><div id='swagger-ui'></div>"
                "<script src='/dashboard-assets/specs/swagger-ui-bundle.js'></script>"
                "<script src='/dashboard-assets/specs/swagger-init.js'></script>"
                "</body></html>"
            )

        @app.get("/redoc", include_in_schema=False)
        async def redoc_page() -> HTMLResponse:
            return HTMLResponse(
                "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>"
                "<meta name='viewport' content='width=device-width, initial-scale=1'>"
                "<title>sqtseries - ReDoc</title>"
                "<link rel='icon' href='/dashboard-assets/favicon.svg' type='image/svg+xml'>"
                "</head><body><redoc spec-url='/openapi.json'></redoc>"
                "<script src='/dashboard-assets/specs/redoc.standalone.js'></script>"
                "</body></html>",
                headers={
                    "Content-Security-Policy": (
                        "default-src 'self'; "
                        "img-src 'self' data: https://cdn.redoc.ly; "
                        "style-src 'self' 'unsafe-inline'; "
                        "worker-src 'self' blob:"
                    )
                },
            )

    # Shipped documentation (release wheel) or the repo copy in dev.
    if DOCS_DIR is not None and DOCS_DIR.is_dir():
        app.mount("/docs", StaticFiles(directory=DOCS_DIR, html=True), name="docs")

    @app.get("/api/v1/health")
    async def health() -> dict[str, str]:
        from ..health import is_ready

        store = app.state.store
        db_ok = store is not None and is_ready(store.db)

        return {"status": "ok" if db_ok else "degraded", "version": "0.1.0"}

    @app.websocket("/ws/subscribe")
    async def ws_subscribe(websocket: WebSocket, metric: str = "*"):
        await websocket.accept()
        if not _ws_slot_available(app):
            await websocket.close(code=1013, reason="too many connections")

            return
        try:
            pubsub = app.state.pubsub
            if pubsub is None:
                await websocket.close(code=1008, reason="streaming disabled")

                return
            registry = app.state.registry
            await subscribe_and_forward(
                websocket, pubsub=pubsub, topic=metric, registry=registry
            )
        finally:
            app.state.ws_count -= 1

    @app.websocket("/ws/connections")
    async def ws_connections(websocket: WebSocket):
        """Push live connection/subscription state to a monitoring client.

        Sends a ``{"type":"snapshot", ...}`` on connect, then one frame per

        registry change (``{"type":"conn", ...}`` / ``{"type":"sub", ...}``) as

        it happens — no polling. The registry's ``on_event`` callbacks may fire

        from any thread, so events are marshalled onto this loop via
        ``call_soon_threadsafe`` into a bounded queue drained by a sender task.

        """
        await websocket.accept()
        if not _ws_slot_available(app):
            await websocket.close(code=1013, reason="too many connections")

            return
        try:
            registry = app.state.registry
            if registry is None:
                await websocket.close(code=1008, reason="registry unavailable")

                return

            loop = asyncio.get_running_loop()
            queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

            def on_event(event_type: str, payload: dict[str, Any]) -> None:
                loop.call_soon_threadsafe(
                    queue.put_nowait, {"type": event_type, **payload}
                )

            registry.on_event(on_event)
            try:
                snap = registry.snapshot()
                await websocket.send_json(
                    {
                        "type": "snapshot",
                        "ws_connections": snap["ws_connections"],
                        "zmq_subscribers": snap["zmq_subscribers"],
                        "connections": registry.list_connections(),
                        "subscriptions": snap["subscriptions"],
                    }
                )

                async def sender() -> None:
                    while True:
                        item = await queue.get()
                        # A stalled client must not grow memory without bound:
                        # drop to a bounded backlog instead of accumulating.

                        while queue.qsize() > 500:
                            try:
                                queue.get_nowait()
                            except asyncio.QueueEmpty:
                                break
                        await websocket.send_json(item)

                async def watch_disconnect() -> None:
                    while True:
                        message = await websocket.receive()
                        if message["type"] == "websocket.disconnect":
                            return

                send_task = asyncio.create_task(sender())

                watch_task = asyncio.create_task(watch_disconnect())
                _done, pending = await asyncio.wait(
                    {send_task, watch_task}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(send_task, watch_task, return_exceptions=True)
            finally:
                registry.remove_listener(on_event)
        finally:
            app.state.ws_count -= 1

    @app.websocket("/ws/dashboard")
    async def ws_dashboard(websocket: WebSocket):
        """Push admin-dashboard stream: snapshot, live events, 1s ticks."""
        await websocket.accept()
        if not _ws_slot_available(app):
            await websocket.close(code=1013, reason="too many connections")

            return
        try:
            await dashboard_stream(
                websocket,
                provider=getattr(app.state, "stats_provider", None),
                store=app.state.store,
                registry=app.state.registry,
            )
        finally:
            app.state.ws_count -= 1

    return app


def _ws_slot_available(app: FastAPI) -> bool:
    """Reserve a WebSocket slot if the connection cap hasn't been hit.
    Single-threaded event loop: the check-and-increment is atomic (no await

    between them), so concurrent handlers cannot overshoot the cap.
    """
    if app.state.ws_count >= app.state.ws_max_connections:
        return False
    app.state.ws_count += 1
    return True
