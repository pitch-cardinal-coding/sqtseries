"""Playwright tests for the live camera dashboard page.

``examples/camera/overlay.html`` is served by the overlay simulator
(``examples/camera/overlay_server.py``) and streams camera metrics over the
same-origin WebSocket at ``/ws/metrics``. These tests boot the simulator as a
subprocess and drive a real headless Chromium through Playwright to verify the
page connects, renders live values, and recovers when the server restarts.

The module skips when ``playwright`` (or its Chromium browser) is unavailable,
so the rest of the suite never depends on it.
"""

import json
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlencode

import httpx
import pytest

pytest.importorskip("playwright")

REPO_ROOT = Path(__file__).resolve().parents[1]
CAMERA_DIR = REPO_ROOT / "examples" / "camera"
OVERLAY_SCRIPT = CAMERA_DIR / "overlay_server.py"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_http(url: str, timeout: float = 15) -> None:
    """Block until the URL returns 200, or raise."""
    deadline = time.monotonic() + timeout
    with httpx.Client(timeout=1.0) as client:
        while time.monotonic() < deadline:
            try:
                if client.get(url).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
    raise AssertionError(f"server did not become ready at {url}")


class OverlayProc:
    """A bootable overlay_server.py subprocess on a chosen port."""

    def __init__(self, port: int) -> None:
        self.port = port
        self.proc: subprocess.Popen | None = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        self.stop()
        self.proc = subprocess.Popen(
            [
                sys.executable,
                str(OVERLAY_SCRIPT),
                "--port",
                str(self.port),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=REPO_ROOT,
        )
        _wait_http(f"{self.base_url}/")

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.proc = None


class SqtseriesProc:
    """A restartable ``sqtseries run`` subprocess on fixed free ports."""

    def __init__(self, tmp_path) -> None:
        ports = {
            name: _free_port()
            for name in ("ingestion", "query", "streaming", "admin", "http", "stats")
        }
        self.http_port = ports["http"]
        self.stream_port = ports["streaming"]
        self.base_url = f"http://127.0.0.1:{self.http_port}"
        cfg = tmp_path / "sqtseries.toml"
        db = tmp_path / "sqtseries.sqlite"
        sections = "".join(f"[{name}]\nport = {ports[name]}\n\n" for name in ports)
        cfg.write_text(
            f'[database]\npath = "{db}"\nbatch_size = 500\n\n{sections}'
            "[ports]\nauto_detect = false\n"
        )
        self._cfg = cfg
        self.proc: subprocess.Popen | None = None
        self._client: httpx.Client | None = None

    def start(self) -> None:
        self.stop()
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "sqtseries", "--config", str(self._cfg), "run"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=REPO_ROOT,
        )
        _wait_http(f"{self.base_url}/api/v1/health")

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.proc = None

    def write(self, metric: str, value: float, tags: dict | None = None) -> None:
        if self._client is None:
            self._client = httpx.Client(timeout=5.0)
        r = self._client.post(
            f"{self.base_url}/api/v1/write",
            json={"metric": metric, "value": value, "tags": tags},
        )
        r.raise_for_status()


@pytest.fixture
def overlay():
    proc = OverlayProc(_free_port())
    proc.start()
    try:
        yield proc
    finally:
        proc.stop()


@pytest.fixture
def sqtseries(tmp_path):
    proc = SqtseriesProc(tmp_path)
    proc.start()
    try:
        yield proc
    finally:
        proc.stop()


def _db_page_url(overlay, sqtseries) -> str:
    return (
        overlay.base_url
        + "/overlay.html?"
        + urlencode(
            {
                "sqtseries": f"ws://127.0.0.1:{sqtseries.http_port}/ws/subscribe",
                "metric": "testdb.",
            }
        )
    )


@pytest.fixture(scope="module")
def page():
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            yield page
        finally:
            browser.close()


def _wait_connected(page, timeout: float = 15000) -> None:
    page.wait_for_function(
        "() => document.getElementById('conn-state-text').textContent === 'connected'",
        timeout=timeout,
    )


def _value(page, el: str) -> str:
    return page.text_content(f"#{el}")


class TestLiveDashboard:
    def test_connects_and_renders_live_values(self, overlay, page):
        console_errors: list[str] = []
        page.on(
            "console",
            lambda m: console_errors.append(m.text) if m.type == "error" else None,
        )

        page.goto(overlay.base_url + "/overlay.html")

        # connects to the same-origin metrics WebSocket
        _wait_connected(page)
        assert _value(page, "camera-id") == "camera: test_cam_001"
        assert _value(page, "overlay-status") == "ON"

        # a live value is present and keeps changing (0.25s push interval)
        first = _value(page, "people-current-n")
        assert first not in ("", "—")
        page.wait_for_function(
            "() => document.getElementById('people-current-n').textContent !== "
            + json.dumps(first),
            timeout=10000,
        )

        # battery / cpu render with units
        assert "%" in page.text_content("#battery-pct")
        assert "°C" in _value(page, "cpu-temp")

        # the raw-JSON viewer holds the last message
        assert _value(page, "last-message").startswith("{")

        # clean console: no 404 favicon, no JS errors on a fresh load
        assert console_errors == [], console_errors

    def test_reconnects_after_server_restart(self, overlay, page):
        page.goto(overlay.base_url + "/overlay.html")
        _wait_connected(page)

        # kill the server: the page must flip to reconnecting and the
        # staleness counter must start ticking
        overlay.stop()
        page.wait_for_function(
            "() => document.getElementById('conn-state-text').textContent "
            "=== 'reconnecting'",
            timeout=15000,
        )
        page.wait_for_function(
            "() => { const t = document.getElementById('cpu-age').textContent; "
            "return t.endsWith('s ago') && !t.startsWith('0s'); }",
            timeout=10000,
        )

        # restart on the same port: the page must recover and resume streaming
        overlay.start()
        _wait_connected(page, timeout=20000)
        before = _value(page, "people-current-n")
        page.wait_for_function(
            "() => document.getElementById('people-current-n').textContent !== "
            + json.dumps(before),
            timeout=10000,
        )

    def test_subscribes_to_sqtseries(self, overlay, sqtseries, page):
        """With ?sqtseries= the page also streams readings the time-series DB
        publishes — an HTTP write must appear in the DB card."""
        page.goto(_db_page_url(overlay, sqtseries))
        page.wait_for_function(
            "() => document.getElementById('db-conn-state-text').textContent "
            "=== 'connected'",
            timeout=15000,
        )
        # footer shows the db endpoint
        assert page.is_visible("#db-footer")

        # a write published by sqtseries over HTTP must show up in the card
        sqtseries.write("testdb.foo", 42.0, {"camera_id": "cam_x"})
        page.wait_for_function(
            "() => document.getElementById('db-body').textContent.includes('testdb.foo')",
            timeout=10000,
        )
        body = page.text_content("#db-body")
        assert "42" in body
        assert "camera_id=cam_x" in body
        assert int(page.text_content("#db-frames")) >= 1

        # without ?sqtseries= the DB card stays idle (no connection attempts)
        page.goto(overlay.base_url + "/overlay.html")
        page.wait_for_function(
            "() => document.getElementById('db-conn-state-text').textContent === 'idle'",
            timeout=5000,
        )
        assert "not configured" in page.text_content("#db-empty")

    def test_db_reconnects_after_sqtseries_restart(self, overlay, sqtseries, page):
        page.goto(_db_page_url(overlay, sqtseries))
        page.wait_for_function(
            "() => document.getElementById('db-conn-state-text').textContent "
            "=== 'connected'",
            timeout=15000,
        )

        # kill sqtseries: the DB badge must flip to reconnecting
        sqtseries.stop()
        page.wait_for_function(
            "() => document.getElementById('db-conn-state-text').textContent "
            "=== 'reconnecting'",
            timeout=15000,
        )

        # restart on the same ports: the page must recover and resume streaming
        sqtseries.start()
        page.wait_for_function(
            "() => document.getElementById('db-conn-state-text').textContent "
            "=== 'connected'",
            timeout=20000,
        )
        sqtseries.write("testdb.after", 7.0, {"camera_id": "cam_y"})
        page.wait_for_function(
            "() => document.getElementById('db-body').textContent.includes('testdb.after')",
            timeout=10000,
        )
        assert "7" in page.text_content("#db-body")

    def test_connected_clients_live(self, overlay, sqtseries, page):
        """The DB card's connected-clients react instantly on BOTH connection
        and disconnection, via the /ws/connections push (no polling), and the
        activity log records each join/leave."""
        import threading

        import websockets.sync.client as wsc

        page.goto(_db_page_url(overlay, sqtseries))
        page.wait_for_function(
            "() => document.getElementById('db-conn-state-text').textContent "
            "=== 'connected'",
            timeout=15000,
        )
        # the page's own /ws/subscribe connection must show up
        page.wait_for_function(
            "() => document.getElementById('clients-body').textContent.includes('ws')",
            timeout=10000,
        )
        before = int(page.text_content("#db-clients"))
        assert before >= 1

        # open a second subscriber that stays until told to close
        connected = threading.Event()
        close = threading.Event()

        def second_client():
            with wsc.connect(
                f"ws://127.0.0.1:{sqtseries.http_port}/ws/subscribe?metric=other."
            ):
                connected.set()
                close.wait(20)

        t = threading.Thread(target=second_client, daemon=True)
        t.start()
        assert connected.wait(10), "second subscriber did not connect"
        try:
            # CONNECTION: the count must rise live (no refresh)
            page.wait_for_function(
                "() => parseInt(document.getElementById('db-clients').textContent, 10) >= "
                + str(before + 1),
                timeout=10000,
            )
            page.wait_for_function(
                "() => document.getElementById('activity').textContent.includes('joined')",
                timeout=10000,
            )
        finally:
            close.set()
            t.join(timeout=5)

        # DISCONNECTION: the count must drop back live
        page.wait_for_function(
            "() => parseInt(document.getElementById('db-clients').textContent, 10) === "
            + str(before),
            timeout=10000,
        )
        page.wait_for_function(
            "() => document.getElementById('activity').textContent.includes('left')",
            timeout=10000,
        )

    def test_conn_validate_script(self, sqtseries):
        """examples/camera/conn_validate.py passes against a real instance:
        every join/leave is pushed live with the exact figures."""
        res = subprocess.run(
            [
                sys.executable,
                str(CAMERA_DIR / "conn_validate.py"),
                "--host",
                "127.0.0.1",
                "--http-port",
                str(sqtseries.http_port),
                "--stream-port",
                str(sqtseries.stream_port),
                "--clients",
                "2",
                "--zmq-subs",
                "1",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=REPO_ROOT,
        )
        assert res.returncode == 0, f"stdout:\n{res.stdout}\nstderr:\n{res.stderr}"
        assert "OK: every join/leave figure correct and pushed live" in res.stdout
