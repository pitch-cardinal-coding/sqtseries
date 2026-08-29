"""Port management tests."""

import socket

import pytest
import zmq

from sqtseries.config import Settings
from sqtseries.ports import PortAllocator, PortExhaustedError, PortInUseError
from sqtseries.service import Service


def _grab(port: int):
    s = socket.socket()
    s.bind(("127.0.0.1", port))
    return s


def _hold_first_two_of_free_block(n: int = 8) -> tuple[int, list]:
    """Find ``n`` consecutive free ports; hold ONLY the first two.
    The held pair plays the role of "busy 12500/12501" that the ingest
    auto-detector must skip. The remaining ``n - 2`` ports are released so

    the service can bind them.
    """
    for base in range(30000, 60000, 16):
        probe = []

        ok = True

        for i in range(n):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.bind(("127.0.0.1", base + i))
                probe.append(s)
            except OSError:
                for x in probe:
                    x.close()
                ok = False
                break
        if not ok:
            continue
        for s in probe[2:]:
            s.close()
        return base, probe[:2]
    raise RuntimeError("could not find a free port block")


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


class TestServiceIngestReservation:
    async def test_auto_detect_never_picks_a_fixed_port(self, tmp_path):
        """Regression: the ingest auto-detect must skip ALL fixed ports.

        Before the fix the reserved set in Service._start omitted the stats

        port, so with 12500/12501 busy the detector picked 12506 (stats) for

        ingest and the StatsPublisher bind then failed — the service would not

        start. Verify it starts and the ingest port avoids every fixed port.

        """
        base, held = _hold_first_two_of_free_block()
        try:
            s = Settings(
                database={"path": str(tmp_path / "db.sqlite")},
                ingestion={"port": base},
                query={"port": base + 2},
                streaming={"port": base + 3},
                admin={"port": base + 4},
                http={"port": base + 5},
                stats={"port": base + 6},
                ports={
                    "auto_detect": True,
                    "port_range_start": base,
                    "port_range_end": base + 7,
                },
            )
            svc = Service(s)
            await svc.start()
            try:
                fixed = {base + 2, base + 3, base + 4, base + 5, base + 6}

                endpoint = svc.ingress.socket.getsockopt_string(zmq.LAST_ENDPOINT)
                ingest_port = int(endpoint.rsplit(":", 1)[1])
                assert ingest_port not in fixed
                assert svc.stats_publisher is not None
            finally:
                await svc.shutdown()
        finally:
            for sk in held:
                sk.close()
