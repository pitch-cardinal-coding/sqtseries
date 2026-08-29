"""Structured logging via structlog.
- JSON output for production, pretty console for development.
- Optional rotating file output alongside stderr.
"""

import logging
import logging.handlers
import sys
from typing import Any

import structlog

from .config import LoggingSettings


def _json_serializer(obj: Any, **_: Any) -> str:
    import orjson

    return orjson.dumps(obj).decode()


def configure_logging(settings: LoggingSettings) -> structlog.stdlib.BoundLogger:
    """Configure structlog + stdlib logging; return the bound logger."""

    level = getattr(logging, settings.level.upper(), logging.INFO)

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        # format_exc_info -> plain string; works with both ConsoleRenderer and
        # JSONRenderer (dict_tracebacks produces dicts that this structlog's
        # ConsoleRenderer cannot render).
        structlog.processors.format_exc_info,
    ]

    if settings.format == "json":
        renderer: Any = structlog.processors.JSONRenderer(serializer=_json_serializer)
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    processors.append(renderer)

    logging.basicConfig(level=level, format="%(message)s", stream=sys.stderr)

    if settings.file:
        from pathlib import Path

        log_path = Path(settings.file).expanduser()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        root = logging.getLogger()
        # avoid duplicate handlers on repeated configure_logging (e.g. tests,
        # reloads): skip if a RotatingFileHandler already targets this file

        existing = any(
            isinstance(h, logging.handlers.RotatingFileHandler)
            and h.baseFilename == str(log_path)
            for h in root.handlers
        )
        if not existing:
            handler = logging.handlers.RotatingFileHandler(
                str(log_path),
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
            )
            root.addHandler(handler)
            root.setLevel(level)

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger("sqtseries")


__all__ = ["configure_logging"]
