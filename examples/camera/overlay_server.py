#!/usr/bin/env python3
"""
Overlay HTTP server + WebSocket data generator using FastAPI + uvicorn.

Serves the HTML overlay on HTTP and provides simulated analytics data
via WebSocket at /ws/metrics on the same port.

Can run standalone (no test_server.py needed).

Usage:
    python3 overlay_server.py                    # random free port
    python3 overlay_server.py --port 9090        # custom port
    python3 overlay_server.py --port 0           # random free port
"""

import argparse
import asyncio
import contextlib
import logging
import os
import random
import socket
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

logger = logging.getLogger("overlay_server")

OVERLAY_DIR = Path(__file__).resolve().parent

CONFIG = {
    "host": os.environ.get("TEST_HOST", "0.0.0.0"),
    "port": None,  # resolved in main(): --port, else TEST_PORT, else random
    "push_interval": float(os.environ.get("TEST_PUSH_INTERVAL", "0.25")),
    "error_interval": float(os.environ.get("TEST_ERROR_INTERVAL", "0")),
    "camera_id": os.environ.get("TEST_CAMERA_ID", "test_cam_001"),
}


def free_port() -> int:
    """Ask the OS for a currently-free port (never clashes with anything)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class DataSimulator:
    def __init__(self) -> None:
        self.base_people = 15
        self.base_cars = 8
        self.total_people = 150
        self.total_vehicles = 80
        self._tick = 0
        self._last_error_at = 0.0

    def tick(self) -> int:
        self._tick += 1
        return self._tick

    def should_error(self) -> bool:
        interval = CONFIG["error_interval"]
        if interval <= 0:
            return False
        now = time.monotonic()
        if now - self._last_error_at >= interval:
            self._last_error_at = now
            return True
        return False

    def dashboard_metrics(self) -> dict:
        t = self.tick()
        visible_people = max(0, self.base_people + int(random.gauss(0, 3)) + (t % 10))
        visible_cars = max(0, self.base_cars + int(random.gauss(0, 2)) + (t % 5))
        self.total_people = max(
            self.total_people, self.total_people + int(random.gauss(0.5, 1))
        )
        self.total_vehicles = max(
            self.total_vehicles, self.total_vehicles + int(random.gauss(0.3, 0.5))
        )

        return {
            "camera_id": CONFIG["camera_id"],
            "generated_at": datetime.now(UTC).isoformat(),
            "current_visible_people": visible_people,
            "total_detected_people": self.total_people,
            "current_visible_cars": visible_cars,
            "total_detected_vehicles": self.total_vehicles,
            "people_last_minute": max(0, visible_people + int(random.gauss(0, 2))),
            "cars_last_minute": max(0, visible_cars + int(random.gauss(0, 1))),
            "stats_footer_text": (
                "ADVERTISE WITH US | CONTACT US: VISIT BITTONET.COM TO LEARN MORE"
            ),
            "detection_overlay_enabled": True,
            "detection_overlay_stream_url": "/overlay/stream",
            "detection_overlay_updated_at": datetime.now(UTC).isoformat(),
            "system_metrics": {
                "batteryPercent": round(85 - random.uniform(0, 5), 1),
                "batteryTemperature": round(28.5 + random.uniform(-2, 2), 1),
                "batteryHealth": "Good",
                "cpuUsagePercent": round(45.2 + random.uniform(-10, 10), 1),
                "cpuTemperature": round(62.1 + random.uniform(-3, 3), 1),
                "timestamp": datetime.now(UTC).isoformat(),
            },
        }


sim = DataSimulator()

app = FastAPI()


class ConnectionManager:
    def __init__(self) -> None:
        self._active: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._active.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self._active.discard(websocket)

    async def broadcast(self, payload: dict) -> None:
        if not self._active:
            return
        dead: list[WebSocket] = []
        results = await asyncio.gather(
            *(ws.send_json(payload) for ws in self._active),
            return_exceptions=True,
        )
        for ws, result in zip(self._active, results, strict=False):
            if isinstance(result, Exception):
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


@app.get("/overlay.html")
async def serve_overlay() -> FileResponse:
    # CSP note (COMPLIANCE.md): overlay.html intentionally ships as a single
    # self-contained file with inline CSS/JS — it is a trusted, developer-owned
    # artifact for LAN/OBS browser sources (no user input, no third-party
    # content), so an external-assets split would only add failure modes. The
    # sqtseries HTTP gateway (the real API surface) does send strict security
    # headers.
    return FileResponse(OVERLAY_DIR / "overlay.html")


@app.get("/")
async def root() -> FileResponse:
    return FileResponse(OVERLAY_DIR / "overlay.html")


@app.websocket("/ws/metrics")
async def ws_metrics(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        while True:
            if sim.should_error():
                payload = {
                    "status": "error",
                    "message": "simulated analytics unavailable",
                    "generated_at": datetime.now(UTC).isoformat(),
                }
            else:
                payload = sim.dashboard_metrics()

            await manager.broadcast(payload)

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    websocket.receive_text(), timeout=CONFIG["push_interval"]
                )
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as exc:
        logger.error("WebSocket error: %s", exc)
        manager.disconnect(websocket)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve ADS overlay HTML and generate simulated analytics data"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="HTTP/WebSocket port (default: TEST_PORT env, else a random "
        "free port; 0 also picks a random free port)",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> None:
    args = parse_args(argv)
    if args.port not in (None, 0):
        port = args.port
    elif os.environ.get("TEST_PORT") and int(os.environ["TEST_PORT"]) != 0:
        port = int(os.environ["TEST_PORT"])
    else:
        port = free_port()
    CONFIG["port"] = port
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    logger.info("Serving overlay at http://localhost:%d/overlay.html", port)
    logger.info("OBS Browser Source URL: http://localhost:%d/overlay.html", port)
    logger.info("WebSocket: ws://localhost:%d/ws/metrics", port)
    logger.info("Press Ctrl-C to stop")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        ws_max_size=2**20,
        ws_ping_interval=25,
        ws_ping_timeout=10,
        log_level="info",
        access_log=False,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
