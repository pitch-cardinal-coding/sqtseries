"""Logging configuration tests: console / json / file output.
structlog's config is process-global and loggers are cached on first use, so
these tests only assert the latest configure call's processors and never
mutate the stdlib logger registry.
"""

import logging

import structlog

from sqtseries.config import LoggingSettings
from sqtseries.logging import configure_logging


def _processor_names():
    return [type(p).__name__ for p in structlog.get_config()["processors"]]


def test_console_logger():
    configure_logging(LoggingSettings(level="INFO", format="console"))
    assert "ConsoleRenderer" in _processor_names()


def test_json_logger():
    configure_logging(LoggingSettings(level="DEBUG", format="json"))
    assert "JSONRenderer" in _processor_names()


def test_file_output(tmp_path):
    logfile = str(tmp_path / "sqtseries.log")
    configure_logging(LoggingSettings(level="INFO", format="json", file=logfile))

    root = logging.getLogger()
    handler = root.handlers[-1] if root.handlers else None
    assert isinstance(handler, logging.handlers.RotatingFileHandler)
    try:
        # the autouse conftest fixture silences the sqtseries logger; re-enable
        # it for this test so the message actually reaches the file handler

        logging.getLogger("sqtseries").setLevel(logging.INFO)
        logging.getLogger("sqtseries").info("hello world")
        handler.flush()
        assert "hello world" in (tmp_path / "sqtseries.log").read_text()
    finally:
        root.removeHandler(handler)


def test_logger_emits_structured_keywords():
    configure_logging(LoggingSettings(level="INFO", format="console"))
    logger = structlog.get_logger("sqtseries")
    # must not raise
    logger.info("event", metric="cpu", value=0.5)
