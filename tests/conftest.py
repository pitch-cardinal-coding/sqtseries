"""Shared pytest fixtures for sqtseries."""

import logging
import socket
from pathlib import Path

import pytest


def free_port() -> int:
    """Ask the OS for a currently-free port (never a hardcoded one).
    Hardcoded port ranges cause intermittent CI failures the moment any

    other process (a demo service, a parallel test run, a left-over
    instance) happens to hold one. Binding to port 0 hands the OS the
    choice, so fixtures can never clash.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def free_ports() -> dict[str, int]:
    """Six distinct OS-assigned free ports for one service instance."""
    names = ("ingest", "query", "streaming", "admin", "http", "stats")
    ports: dict[str, int] = {}
    for name in names:
        port = free_port()
        tries = 0
        # The kernel may hand back a just-released ephemeral port, so
        # two rapid free_port() calls can return the same value (seen
        # live: duplicate ingest/stats ports failing service startup).
        while port in ports.values():
            port = free_port()
            tries += 1
            if tries > 50:
                raise RuntimeError("cannot allocate 6 distinct free ports")
        ports[name] = port
    return ports


@pytest.fixture(autouse=True)
def _silence_sqtseries_logs():
    """Keep test output quiet: route sqtseries loggers to a null handler.

    Module loggers are structlog-backed proxies over stdlib logging; silencing

    the stdlib names prevents the background tasks (rollup, retention, etc.)

    from flooding test output.
    """
    logger = logging.getLogger("sqtseries")
    previous_level = logger.level
    logger.setLevel(logging.CRITICAL)
    yield
    logger.setLevel(previous_level)


@pytest.fixture
def sample_toml(tmp_path: Path) -> Path:
    """Write a sample TOML config file and return its path."""
    path = tmp_path / "config.toml"
    path.write_text(f"""[database]
path = "{tmp_path / "db.sqlite"}"
batch_size = 500

[ingestion]
port = 14101

[query]
port = 14102

[streaming]
port = 14103

[admin]
port = 14104

[http]
port = 14105
""")
    return path


@pytest.fixture
def sample_json(tmp_path: Path) -> Path:
    path = tmp_path / "config.json"
    path.write_text(
        '{"database": {"path": "%s"}, "ingestion": {"port": 14201}}'
        % (tmp_path / "db.json.sqlite")
    )
    return path
