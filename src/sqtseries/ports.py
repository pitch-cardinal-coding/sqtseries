"""Port allocation within a range (12500-12700 default, auto-detect).

Binds a socket to find a free port, then closes it. Race-tolerant.
"""

import socket
from dataclasses import dataclass, field

from .config import PortsSettings


class PortInUseError(Exception):
    """Raised when a requested port is unavailable."""


class PortExhaustedError(Exception):
    """Raised when no free port can be found in the range."""


@dataclass
class PortAllocator:
    """Find free ports in ``[start, end]``."""

    start: int = 12500
    end: int = 12700
    offset: int = 0
    reserved: set[int] = field(default_factory=set)

    @classmethod
    def from_settings(cls, settings: PortsSettings) -> PortAllocator:
        return cls(
            start=settings.port_range_start,
            end=settings.port_range_end,
            offset=settings.port_offset,
        )

    _used: set[int] = field(default_factory=set)

    def alloc(self, *, auto_detect: bool = True) -> int:
        """Allocate a free port. With ``auto_detect`` false, use ``start``.

        Returns the first free port found that isn't already taken and isn't in
        ``reserved`` (ports bound to other fixed services).
        """
        base = self.start + self.offset
        if not auto_detect:
            if self._is_available(base):
                self._used.add(base)
                return base
            raise PortInUseError(f"port {base} is unavailable")
        for candidate in range(base, self.end + 1 + self.offset):
            if candidate in self._used or candidate in self.reserved:
                continue
            if self._is_available(candidate):
                self._used.add(candidate)
                return candidate
        raise PortExhaustedError(f"no free port in [{base}, {self.end + self.offset}]")

    def release(self, port: int) -> None:
        self._used.discard(port)

    @staticmethod
    def _is_available(port: int) -> bool:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False
        finally:
            sock.close()
