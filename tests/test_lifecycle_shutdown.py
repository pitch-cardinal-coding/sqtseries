"""Process-lifecycle regression tests.
Covers the fixes for orphaned/hung sqtseries processes:
- ``sqtseries run`` must exit promptly after SIGTERM even when a WebSocket
  client connected and disconnected (the connection task used to linger up to

  the 30s keepalive window, blocking uvicorn's graceful shutdown).
- The camera overlay server must terminate on SIGHUP (it used to ignore
  SIGHUP, so ``tmux kill-session`` orphaned it).
- ``sqtseries stop`` waits for the service to actually exit, removes
  runtime.json, and leaves the ports immediately rebindable.
"""

import asyncio
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from sqtseries.config import Settings
from sqtseries.service import Service

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = REPO_ROOT / "examples"
CAMERA = EXAMPLES / "camera"
PYTHON = sys.executable


def free_tcp_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def write_config(tmp_path: Path, ports: dict[str, int]) -> Path:
    """A TOML config with six fixed free ports and auto_detect off."""
    cfg = tmp_path / "lifecycle.toml"
    cfg.write_text(f"""[database]
path = "{tmp_path / "lc.sqlite"}"

[ingestion]
port = {ports["ingest"]}

[query]
port = {ports["query"]}

[streaming]
port = {ports["streaming"]}

[admin]
port = {ports["admin"]}

[http]
port = {ports["http"]}

[stats]
port = {ports["stats"]}

[ports]
auto_detect = false
""")
    return cfg


def six_ports() -> dict[str, int]:
    return {
        n: free_tcp_port()
        for n in ("ingest", "query", "streaming", "admin", "http", "stats")
    }


async def wait_for_http(
    proc: asyncio.subprocess.Process, port: int, timeout: float = 15.0
) -> None:
    import httpx

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.returncode is not None:
            raise AssertionError(f"service exited early rc={proc.returncode}")
        try:
            async with httpx.AsyncClient(timeout=1) as c:
                r = await c.get(f"http://127.0.0.1:{port}/api/v1/health")
                if r.status_code == 200:
                    return
        except httpx.TransportError:
            pass
        await asyncio.sleep(0.05)
    raise AssertionError("service never became healthy")


class TestSigtermShutdownPromptness:
    """`sqtseries run` must exit promptly on SIGTERM in every lifecycle case."""

    async def _boot_service(self, tmp_path):
        ports = six_ports()
        cfg = write_config(tmp_path, ports)

        proc = await asyncio.create_subprocess_exec(
            PYTHON,
            "-m",
            "sqtseries",
            "--config",
            str(cfg),
            "run",
            cwd=REPO_ROOT,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.STDOUT,
        )
        await wait_for_http(proc, ports["http"])
        return proc, ports

    async def _sigterm_and_time(self, proc) -> float:
        start = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), timeout=8)
        except TimeoutError:
            elapsed = time.monotonic() - start
            pytest.fail(f"SIGTERM shutdown hung; no exit after {elapsed:.1f}s")
        return time.monotonic() - start

    async def test_clean_run_exits_promptly(self, tmp_path):
        proc, _ = await self._boot_service(tmp_path)
        try:
            elapsed = await self._sigterm_and_time(proc)
            assert elapsed < 3, f"expected prompt exit, took {elapsed:.1f}s"
        finally:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()

    async def test_exits_promptly_after_ws_disconnect(self, tmp_path):
        """Regression: a WS client that connected and disconnected left the

        connection task blocked in the ZMQ recv for up to 30s, which stalled

        uvicorn's graceful shutdown after SIGTERM."""
        import websockets

        proc, ports = await self._boot_service(tmp_path)
        try:
            async with websockets.connect(
                f"ws://127.0.0.1:{ports['http']}/ws/subscribe?metric=*",
                open_timeout=5,
            ) as ws:
                await ws.send("hello")
            # give the server a moment to notice the disconnect
            await asyncio.sleep(0.3)
            elapsed = await self._sigterm_and_time(proc)
            assert elapsed < 3, f"expected prompt exit, took {elapsed:.1f}s"
        finally:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()

    async def test_exits_promptly_with_ws_open_at_sigterm(self, tmp_path):
        """A still-connected client must not stall shutdown either; the server

        closes the connection and the watcher ends the task immediately."""

        import websockets

        proc, ports = await self._boot_service(tmp_path)
        try:
            async with websockets.connect(
                f"ws://127.0.0.1:{ports['http']}/ws/subscribe?metric=*",
                open_timeout=5,
            ) as ws:
                await ws.send("hello")
                await asyncio.sleep(0.3)
                elapsed = await self._sigterm_and_time(proc)
                assert elapsed < 3, f"expected prompt exit, took {elapsed:.1f}s"
        finally:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()


class TestStopCommand:
    """`sqtseries stop` is deterministic: waits for exit, removes runtime.json,

    leaves ports immediately rebindable."""

    async def test_stop_waits_removes_runtime_and_frees_ports(self, tmp_path):
        ports = six_ports()
        cfg = write_config(tmp_path, ports)

        proc = await asyncio.create_subprocess_exec(
            PYTHON,
            "-m",
            "sqtseries",
            "--config",
            str(cfg),
            "run",
            cwd=REPO_ROOT,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            await wait_for_http(proc, ports["http"])
            rt = tmp_path / "runtime.json"

            deadline = time.monotonic() + 5

            while time.monotonic() < deadline and not rt.exists():
                await asyncio.sleep(0.05)
            assert rt.exists(), "runtime.json never appeared"

            start = time.monotonic()

            stop = await asyncio.to_thread(
                subprocess.run,
                [PYTHON, "-m", "sqtseries", "--config", str(cfg), "stop"],
                capture_output=True,
                text=True,
                timeout=20,
            )
            elapsed = time.monotonic() - start
            assert stop.returncode == 0, stop.stderr
            assert "Sent SIGTERM" in stop.stdout
            assert not rt.exists(), "runtime.json not removed on clean stop"

            assert elapsed < 15, f"stop took too long: {elapsed:.1f}s"
            # the service must exit shortly after stop returns (stop waits on
            # runtime.json removal, which happens after every socket has closed)

            await asyncio.wait_for(proc.wait(), timeout=5)
            # all six ports must be immediately rebindable
            for port in ports.values():
                with socket.socket() as s:
                    s.bind(("127.0.0.1", port))
        finally:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()


class TestWsStreamAndRegistry:
    """The websocket handler still streams ZMQ-published frames, tolerates

    clients that send data, and unregisters the connection on disconnect."""

    def _settings(self, free_ports, tmp_path) -> Settings:
        return Settings(
            database={"path": str(tmp_path / "ws.sqlite")},
            ingestion={"port": free_ports["ingest"]},
            query={"port": free_ports["query"]},
            streaming={"port": free_ports["streaming"]},
            admin={"port": free_ports["admin"]},
            http={"port": free_ports["http"]},
            stats={"port": free_ports["stats"]},
            ports={"auto_detect": False},
        )

    async def test_streams_frames_and_cleans_registry(self, free_ports, tmp_path):
        import websockets

        svc = Service(self._settings(free_ports, tmp_path))
        await svc.start()
        try:
            async with websockets.connect(
                f"ws://127.0.0.1:{free_ports['http']}/ws/subscribe?metric=*",
                open_timeout=5,
            ) as ws:
                # client sends data — must not be dropped
                await ws.send("hello")
                # allow the SUB socket to finish connecting, then publish

                await asyncio.sleep(0.2)
                frame = None
                for _ in range(3):
                    svc._on_publish(b"cpu", {"metric": "cpu", "value": 1.0})

                    try:
                        frame = await asyncio.wait_for(ws.recv(), timeout=2)

                        break
                    except TimeoutError:
                        continue
                assert frame is not None, "no frame received from pubsub"

                assert '"cpu"' in frame
            # disconnect must promptly unregister the connection
            for _ in range(100):
                if svc.connection_registry.ws_count == 0:
                    break
                await asyncio.sleep(0.02)
            assert svc.connection_registry.ws_count == 0
        finally:
            await svc.shutdown()

    async def test_graceful_shutdown_timeout_configured(self, free_ports, tmp_path):
        """The belt-and-suspenders backstop: uvicorn must be told to cancel

        stragglers so a stop always completes."""
        svc = Service(self._settings(free_ports, tmp_path))
        await svc.start()
        try:
            assert svc.http_server.config.timeout_graceful_shutdown == 5
        finally:
            await svc.shutdown()


class TestOverlaySignals:
    """The camera overlay server must terminate like a normal server on SIGHUP

    (it used to ignore SIGHUP, which orphaned it under ``tmux kill-session``)."""

    async def _boot_overlay(self) -> tuple[asyncio.subprocess.Process, int]:
        proc = await asyncio.create_subprocess_exec(
            PYTHON,
            str(CAMERA / "overlay_server.py"),
            "--port",
            "0",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        port = None

        deadline = time.monotonic() + 15

        while time.monotonic() < deadline:
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=15)
            text = line.decode()

            if "WebSocket: ws://localhost:" in text:
                port = int(
                    text.split("WebSocket: ws://localhost:")[1].split("/ws/metrics")[0]
                )
                break
        assert port is not None, "overlay did not report a port"
        return proc, port

    async def test_sighup_terminates_overlay(self, tmp_path):
        """A direct SIGHUP (what tmux sends on kill-session) must terminate it."""

        proc, _ = await self._boot_overlay()
        try:
            proc.send_signal(signal.SIGHUP)
            await asyncio.wait_for(proc.wait(), timeout=5)
        finally:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()

    async def test_overlay_dies_when_tmux_session_killed(self, tmp_path):
        """End-to-end: killing the controlling tmux session stops the overlay

        instead of leaving it as an orphaned process."""
        if shutil.which("tmux") is None:
            pytest.skip("tmux not available")
        ss = shutil.which("ss")
        if ss is None:
            pytest.skip("ss not available")
        session = f"sqt-lc-{os.getpid()}"

        log = tmp_path / "overlay.log"

        tmux = shutil.which("tmux")
        subprocess.run(
            [
                tmux,
                "new-session",
                "-d",
                "-s",
                session,
                f"cd {REPO_ROOT} && exec {PYTHON} -u "
                f"examples/camera/overlay_server.py > {log} 2>&1",
            ],
            check=True,
            timeout=15,
        )
        try:
            port = None

            deadline = time.monotonic() + 15

            while time.monotonic() < deadline:
                if log.exists():
                    text = log.read_text()
                    if "WebSocket: ws://localhost:" in text:
                        port = int(
                            text.split("WebSocket: ws://localhost:")[1].split(
                                "/ws/metrics"
                            )[0]
                        )
                        break
                await asyncio.sleep(0.2)
            assert port is not None, "overlay did not report a port"
            # Under full-suite load the overlay can take a while to bind;
            # give it up to 20s (and tolerate a transient ss failure).
            pid = None
            for _ in range(100):
                out = subprocess.run(
                    [ss, "-ltnp"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout
                for line in out.splitlines():
                    if f":{port}" in line and "pid=" in line:
                        cand = line.split("pid=")[1].split(",")[0]
                        if cand.isdigit():
                            pid = int(cand)
                            break
                if pid:
                    break
                await asyncio.sleep(0.2)
            assert pid, f"no pid found listening on {port}"
            subprocess.run(
                [tmux, "kill-session", "-t", session],
                check=True,
                timeout=15,
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    break
                await asyncio.sleep(0.1)
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)
        finally:
            subprocess.run(
                [tmux, "kill-session", "-t", session],
                capture_output=True,
                timeout=10,
            )
