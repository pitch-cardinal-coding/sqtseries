"""Tests for the camera pipeline: overlay source, feed flattening, and
the 17-question --ask catalog (including sparse-data resilience)."""

import asyncio
import subprocess
import sys
import time
from pathlib import Path

import pytest

from sqtseries.config import Settings
from sqtseries.service import Service

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = REPO_ROOT / "examples"

SAMPLE_MESSAGE = {
    "camera_id": "test_cam_001",
    "generated_at": "2026-08-08T16:26:30.333533+00:00",
    "current_visible_people": 21,
    "total_detected_people": 150,
    "current_visible_cars": 6,
    "total_detected_vehicles": 81,
    "people_last_minute": 20,
    "cars_last_minute": 6,
    "stats_footer_text": "ADVERTISE WITH US",
    "detection_overlay_enabled": True,
    "detection_overlay_stream_url": "/overlay/stream",
    "detection_overlay_updated_at": "2026-08-08T16:26:30.333550+00:00",
    "system_metrics": {
        "batteryPercent": 84.3,
        "batteryTemperature": 28.9,
        "batteryHealth": "Good",
        "cpuUsagePercent": 38.5,
        "cpuTemperature": 63.8,
        "timestamp": "2026-08-08T16:26:30.333563+00:00",
    },
}


def free_tcp_port() -> int:
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture
async def running_service(tmp_path):
    ports = {
        "ingest": free_tcp_port(),
        "query": free_tcp_port(),
        "streaming": free_tcp_port(),
        "admin": free_tcp_port(),
        "http": free_tcp_port(),
        "stats": free_tcp_port(),
    }
    s = Settings(
        database={"path": str(tmp_path / "cam.sqlite")},
        ingestion={"port": ports["ingest"]},
        query={"port": ports["query"]},
        streaming={"port": ports["streaming"]},
        admin={"port": ports["admin"]},
        http={"port": ports["http"]},
        stats={"port": ports["stats"]},
        ports={"auto_detect": False},
    )
    svc = Service(s)
    await svc.start()
    await svc.pool.stop()

    async def pump():
        while True:
            await svc.ingress.drain()
            await svc.broker.run_once(block=False)
            await svc.admin_broker.run_once(block=False)
            await asyncio.sleep(0.005)

    pump_task = asyncio.create_task(pump())
    try:
        yield svc, s, ports
    finally:
        pump_task.cancel()
        await asyncio.gather(pump_task, return_exceptions=True)
        await svc.shutdown()


async def run_example(script: str, *args: str) -> subprocess.CompletedProcess:
    return await asyncio.to_thread(
        subprocess.run,
        [sys.executable, str(EXAMPLES / script), *args],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=REPO_ROOT,
    )


class TestFlatten:
    def test_flatten_message(self):
        from examples.camera_feed import flatten

        points = flatten(SAMPLE_MESSAGE)
        assert len(points) == 11
        by_metric = {p["metric"]: p for p in points}
        assert by_metric["traffic.people.current"]["value"] == 21.0
        assert by_metric["traffic.people.total"]["value"] == 150.0
        assert by_metric["system.battery.percent"]["value"] == 84.3
        assert by_metric["system.cpu.usage"]["value"] == 38.5
        assert by_metric["status.detection_overlay"]["value"] == 1.0
        for p in points:
            assert p["tags"] == {"camera_id": "test_cam_001"}
            assert "timestamp" in p  # generated_at preserved

    def test_flatten_skips_strings_and_missing(self):
        from examples.camera_feed import flatten

        msg = dict(SAMPLE_MESSAGE)
        del msg["current_visible_people"]  # missing numeric -> skipped
        points = flatten(msg)
        metrics = {p["metric"] for p in points}
        assert "traffic.people.current" not in metrics
        assert "system.battery.percent" in metrics
        # string fields never become points
        assert not any(p["metric"].startswith("stats_footer") for p in points)

    def test_flatten_error_payload(self):
        from examples.camera_feed import flatten

        # simulator error messages have no camera_id / metrics -> empty
        points = flatten({"status": "error", "message": "simulated unavailable"})
        assert points == []


class TestAskSparseData:
    """All 17 questions must be safe against an empty or nearly-empty DB."""

    async def test_ask_on_empty_db_no_errors(self, running_service):
        _, _s, ports = running_service
        res = await run_example(
            "camera_feed.py",
            "--ask",
            "--host",
            "127.0.0.1",
            "--http-port",
            str(ports["http"]),
            "--hours",
            "720",
        )
        assert res.returncode == 0, res.stderr
        assert "error" not in res.stdout.lower()
        for q in range(1, 18):
            assert f"[Q{q}]" in res.stdout, f"Q{q} missing"

    async def test_ask_single_point(self, running_service):
        """One point: questions degrade gracefully, no tracebacks."""
        svc, _s, ports = running_service
        from examples.camera_feed import flatten

        now = time.time()
        for p in flatten(SAMPLE_MESSAGE):
            svc.ts.insert(
                p["metric"], p["value"], p["tags"], timestamp_ns=int(now * 1e9)
            )
        res = await run_example(
            "camera_feed.py",
            "--ask",
            "--host",
            "127.0.0.1",
            "--http-port",
            str(ports["http"]),
            "--hours",
            "24",
        )
        assert res.returncode == 0, res.stderr
        assert "not enough" in res.stdout or "no data" in res.stdout

    async def test_ask_after_real_pump(self, running_service):
        """Write a batch of realistic points, then ask: answers appear."""
        svc, _s, ports = running_service
        from examples.camera_feed import flatten

        base = time.time() - 3600
        for i in range(60):  # one message per minute for an hour
            msg = dict(SAMPLE_MESSAGE)
            msg["current_visible_people"] = 10 + (i % 20)
            msg["system_metrics"]["batteryPercent"] = 85.0 - i * 0.1
            for p in flatten(msg):
                svc.ts.insert(
                    p["metric"],
                    p["value"],
                    p["tags"],
                    timestamp_ns=int((base + i * 60) * 1e9),
                )
        res = await run_example(
            "camera_feed.py",
            "--ask",
            "--host",
            "127.0.0.1",
            "--http-port",
            str(ports["http"]),
            "--hours",
            "24",
        )
        assert res.returncode == 0, res.stderr
        assert "avg=" in res.stdout  # Q1 answered with real numbers
        assert "battery" in res.stdout  # Q8 reported

    async def test_ask_q8_recharge_message(self, running_service):
        """Battery that gains charge is reported as a recharge."""
        svc, _s, ports = running_service
        from examples.camera_feed import flatten

        base = time.time() - 7200
        for i in range(10):
            msg = dict(SAMPLE_MESSAGE)
            msg["system_metrics"]["batteryPercent"] = 50.0 + i * 5.0  # rising
            for p in flatten(msg):
                svc.ts.insert(
                    p["metric"],
                    p["value"],
                    p["tags"],
                    timestamp_ns=int((base + i * 600) * 1e9),
                )
        res = await run_example(
            "camera_feed.py",
            "--ask",
            "--host",
            "127.0.0.1",
            "--http-port",
            str(ports["http"]),
            "--hours",
            "24",
        )
        assert res.returncode == 0, res.stderr
        assert "gained charge" in res.stdout


class TestOverlayServer:
    async def test_random_port_mode(self, tmp_path):
        """overlay_server.py --port 0 picks a random free port and serves
        the WebSocket metrics endpoint."""
        import websockets

        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(EXAMPLES / "overlay_server.py"),
            "--port",
            "0",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            port = None
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=15)
                text = line.decode()
                if "WebSocket: ws://localhost:" in text:
                    port = int(
                        text.split("WebSocket: ws://localhost:")[1].split(
                            "/ws/metrics"
                        )[0]
                    )
                    break
            assert port is not None, "overlay did not report a port"
            assert 1024 <= port <= 65535

            import json as j

            async def probe():
                last_exc: Exception | None = None
                for _ in range(20):
                    try:
                        async with websockets.connect(
                            f"ws://localhost:{port}/ws/metrics", open_timeout=5
                        ) as ws:
                            return j.loads(
                                await asyncio.wait_for(ws.recv(), timeout=10)
                            )
                    except Exception as exc:
                        last_exc = exc
                        await asyncio.sleep(0.2)
                raise AssertionError(f"overlay not reachable: {last_exc}")

            msg = await probe()
            assert msg["camera_id"] == "test_cam_001"
            assert "current_visible_people" in msg
            assert "system_metrics" in msg
        finally:
            proc.terminate()
            await proc.wait()
