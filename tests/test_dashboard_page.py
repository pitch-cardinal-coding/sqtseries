"""Playwright tests for the admin dashboard page.

``GET /dashboard`` is served by the gateway itself (same origin as the
API), streaming ``/ws/dashboard`` (snapshot, live conn/sub events, 1s
ticks). These tests boot a real ``sqtseries run`` subprocess and drive a
real headless Chromium through Playwright to verify the page connects,
renders live values, recovers across a restart, stays usable on a mobile
viewport, and logs zero console errors. Skips when ``playwright`` (or its
Chromium browser) is unavailable, so the rest of the suite never depends
on it.
"""

import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

pytest.importorskip("playwright")

REPO_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_http(url: str, timeout: float = 20) -> None:
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


class DashboardProc:
    """A restartable ``sqtseries run`` subprocess on fixed free ports."""

    def __init__(self, tmp_path) -> None:
        ports = {
            name: _free_port()
            for name in ("ingestion", "query", "streaming", "admin", "http", "stats")
        }
        self.http_port = ports["http"]
        self.base_url = f"http://127.0.0.1:{self.http_port}"
        cfg = tmp_path / "dashboard.toml"
        db = tmp_path / "dashboard.sqlite"
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
        _wait_http(f"{self.base_url}/dashboard")

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.proc = None

    def write(self, metric: str, value: float) -> None:
        if self._client is None:
            self._client = httpx.Client(timeout=5.0)
        r = self._client.post(
            f"{self.base_url}/api/v1/write",
            json={"metric": metric, "value": value},
        )
        r.raise_for_status()


@pytest.fixture
def service(tmp_path):
    proc = DashboardProc(tmp_path)
    proc.start()
    try:
        yield proc
    finally:
        proc.stop()


@pytest.fixture(scope="module")
def dash_page():
    from playwright.sync_api import sync_playwright

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            pg = browser.new_page()
            try:
                yield pg
            finally:
                browser.close()
    except Exception as exc:
        pytest.skip(f"playwright browser unavailable: {exc}")


def _wait_connected(dash_page, timeout: float = 15000) -> None:
    dash_page.wait_for_function(
        "() => document.getElementById('conn-state-text').textContent === 'connected'",
        timeout=timeout,
    )


class TestDashboardPage:
    def test_connects_renders_snapshot_console_clean(self, service, dash_page):
        console_errors: list[str] = []
        failed: list[str] = []
        dash_page.on(
            "console",
            lambda m: console_errors.append(m.text) if m.type == "error" else None,
        )
        dash_page.on(
            "response",
            lambda r: failed.append(r.url) if r.status >= 400 else None,
        )
        dash_page.goto(service.base_url + "/dashboard")
        _wait_connected(dash_page)
        assert dash_page.text_content("#stat-status") == "ok"
        assert dash_page.text_content("#stat-version") == "0.1.0"
        assert "active" in (dash_page.text_content("#conn-note") or "")
        assert console_errors == [], console_errors
        assert failed == [], failed

    def test_live_values_move(self, service, dash_page):
        dash_page.goto(service.base_url + "/dashboard")
        _wait_connected(dash_page)
        before = dash_page.text_content("#stat-ingested")
        for _ in range(5):
            service.write("dash.live", 1.0)
        dash_page.wait_for_function(
            "() => document.getElementById('stat-ingested').textContent !== "
            + "'"
            + (before or "")
            + "'",
            timeout=15000,
        )
        assert dash_page.text_content("#stat-ingest-rate") != "0.0/s"

    def test_reconnects_after_restart(self, service, dash_page):
        dash_page.goto(service.base_url + "/dashboard")
        _wait_connected(dash_page)
        service.stop()
        dash_page.wait_for_function(
            "() => document.getElementById('conn-state-text').textContent "
            "=== 'reconnecting'",
            timeout=15000,
        )
        service.start()
        _wait_connected(dash_page, timeout=20000)

    def test_mobile_no_horizontal_scroll(self, service, dash_page):
        dash_page.set_viewport_size({"width": 390, "height": 844})
        try:
            dash_page.goto(service.base_url + "/dashboard")
            _wait_connected(dash_page)
            assert dash_page.evaluate(
                "() => document.documentElement.scrollWidth "
                "<= document.documentElement.clientWidth"
            )
            dash_page.click("#nav-toggle")
            dash_page.wait_for_function(
                "() => document.body.classList.contains('nav-open')",
                timeout=5000,
            )
            assert dash_page.evaluate(
                "() => document.documentElement.scrollWidth "
                "<= document.documentElement.clientWidth"
            )
        finally:
            dash_page.set_viewport_size({"width": 1280, "height": 800})
