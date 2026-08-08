"""Port management tests."""

import pytest

from sqtseries.ports import PortAllocator, PortExhaustedError, PortInUseError


def _grab(port: int):
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", port))
    return s


class TestPortAllocator:
    def test_alloc_returns_free_port(self):
        port = PortAllocator(start=20000, end=20010).alloc()
        assert isinstance(port, int)

    def test_alloc_skips_used(self, monkeypatch):
        held = _grab(20099)
        try:
            pa = PortAllocator(start=20099, end=20120)
            # 20099 taken; next free should be 20100
            port = pa.alloc()
            assert port == 20100
        finally:
            held.close()

    def test_alloc_deducts_internally(self):
        pa = PortAllocator(start=20200, end=20299)
        p1 = pa.alloc()
        p2 = pa.alloc()
        assert p1 != p2

    def test_release(self):
        pa = PortAllocator(start=20300, end=20399)
        p1 = pa.alloc()
        pa.release(p1)
        assert pa.alloc(auto_detect=False) == p1 or pa.alloc() == p1

    def test_exhausted(self):
        pa = PortAllocator(start=20400, end=20400)
        pa.alloc()
        with pytest.raises(PortExhaustedError):
            pa.alloc()

    def test_fixed_port_in_use(self):
        s = _grab(20420)
        try:
            pa = PortAllocator(start=20420, end=20420)
            with pytest.raises(PortInUseError):
                pa.alloc(auto_detect=False)
        finally:
            s.close()

    def test_offset(self):
        pa = PortAllocator(start=20500, end=20599, offset=100)
        assert pa.alloc() >= 20600
