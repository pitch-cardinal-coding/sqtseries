"""Runtime state file: PID, ports, start time, database path.

Written after startup, removed on shutdown. Clients use it to discover
the service's active ports.
"""

import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import orjson


class RuntimeState:
    """Persist/read runtime state in a JSON file."""

    def __init__(self, path: str):
        self.path = Path(path)

    def write(
        self,
        *,
        pid: int,
        ports: dict[str, int],
        db_path: str,
        version: str = "0.1.0",
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pid": pid,
            "ports": ports,
            "db_path": db_path,
            "version": version,
            "started_at": time.time(),
        }
        # atomic-ish write
        tmp = self.path.with_suffix(".tmp")
        tmp.write_bytes(orjson.dumps(payload))
        tmp.replace(self.path)

    def read(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            return orjson.loads(self.path.read_bytes())
        except orjson.JSONDecodeError, ValueError, OSError:
            return None

    def remove(self) -> None:
        with suppress(OSError):
            self.path.unlink(missing_ok=True)

    @property
    def exists(self) -> bool:
        return self.path.exists()
