"""HTTP gateway (FastAPI REST + WebSocket)."""

from .app import create_app
from .routes import router

__all__ = ["create_app", "router"]
