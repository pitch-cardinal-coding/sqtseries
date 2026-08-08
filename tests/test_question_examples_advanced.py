"""Tests for the advanced question catalog (question_examples_advanced.py)."""

import asyncio
import time

import pytest

from sqtseries.config import Settings
from sqtseries.service import Service


@pytest.fixture
def free_tcp_port():
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture
async def running_service(tmp_path):
    s = Settings(
        database={"path": str(tmp_path / "adv.sqlite"), "batch_size": 500},
        ingestion={"port": 26101, "reject_client_timestamp_skew_s": 864000},
        query={"port": 26102},
        streaming={"port": 26103},
        admin={"port": 26104},
        http={"port": 26105},
        stats={"port": 26106},
        ports={"auto_detect": False},
    )
    svc = Service(s)
    await svc.start()
    await svc.pool.stop()
    pump_task = asyncio.create_task(_pump(svc))
    try:
        yield svc, s
    finally:
        pump_task.cancel()
        await asyncio.gather(pump_task, return_exceptions=True)
        await svc.shutdown()


async def _pump(svc):
    while True:
        await svc.ingress.drain()
        await svc.broker.run_once(block=False)
        await svc.admin_broker.run_once(block=False)
        await asyncio.sleep(0.005)


class TestAdvancedQuestionExamples:
    @pytest.fixture(autouse=True)
    def setup(self, running_service):
        self.svc, self.settings = running_service
        self.ports = {
            "write": self.settings.ingestion.port,
            "query": self.settings.query.port,
            "http": self.settings.http.port,
            "admin": self.settings.admin.port,
        }
        self.db_path = str(self.svc.settings.db_path_expanded())

    async def run_advanced(self):
        """Run the advanced example script against the test service."""
        import sys
        from pathlib import Path

        script = (
            Path(__file__).parent / ".." / "examples" / "question_examples_advanced.py"
        )
        cmd = [
            sys.executable,
            str(script),
            "--host",
            "127.0.0.1",
            "--write-port",
            str(self.ports["write"]),
            "--query-port",
            str(self.ports["query"]),
            "--http-port",
            str(self.ports["http"]),
            "--admin-port",
            str(self.ports["admin"]),
            "--db",
            self.db_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=90)
        return proc.returncode, stdout.decode(), stderr.decode()

    async def test_advanced_catalog_runs(self):
        """All 10 advanced questions run and produce ANSWER lines."""
        rc, out, err = await self.run_advanced()
        assert rc == 0, f"exit {rc}: stderr={err}"

        # Check all 10 answer keys appear
        answers = {}
        for line in out.splitlines():
            if line.startswith("  ANSWER: "):
                key, val = line.split("ANSWER: ", 1)[1].split(" = ")
                answers[key.strip()] = val.strip()

        expected = [
            "busiest_host",
            "p95_page_1",
            "p95_page_2",
            "p95_page_3",
            "visitors_page_1",
            "visitors_page_2",
            "visitors_page_3",
            "top_page",
            "visitors_per_minute",
            "latency_ladder",
            "visitors_by_day",
            "busiest_visitors_hour",
            "readings_expected",
            "readings_received",
            "readings_missing",
            "points_per_day",
            "daily_trend",
            "latest_age_s",
        ]
        for key in expected:
            assert key in answers, f"Missing ANSWER: {key} in output"

        # Sanity checks on values
        assert float(answers["visitors_per_minute"]) >= 0.5
        assert (
            "shrinking" in answers["daily_trend"]
            or "growing" in answers["daily_trend"]
            or "flat" in answers["daily_trend"]
        )
        assert int(answers["latest_age_s"]) >= 0
        busiest = answers["busiest_host"].split(" ")[0]
        assert busiest in ("web1", "web2", "web3")

    async def test_per_series_questions_need_db(self):
        """Per-series questions (1-3) skip gracefully without --db."""
        import sys
        from pathlib import Path

        script = (
            Path(__file__).parent / ".." / "examples" / "question_examples_advanced.py"
        )
        cmd = [
            sys.executable,
            str(script),
            "--host",
            "127.0.0.1",
            "--write-port",
            str(self.ports["write"]),
            "--query-port",
            str(self.ports["query"]),
            "--http-port",
            str(self.ports["http"]),
            "--admin-port",
            str(self.ports["admin"]),
            # no --db flag
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        assert proc.returncode == 0, f"exit {proc.returncode}: {stderr.decode()}"

        out = stdout.decode()
        # Questions 1-3 should show "skipped (needs --db)"
        assert "skipped (needs --db)" in out or "needs --db" in out
        # Questions 4-10 should still run and have ANSWER lines
        answer_count = sum(1 for line in out.splitlines() if "ANSWER:" in line)
        assert answer_count >= 7  # questions 4-10 = 7 answers

    async def test_per_series_with_db_works(self):
        """Per-series questions produce answers when --db is provided."""
        rc, out, err = await self.run_advanced()
        assert rc == 0, f"exit {rc}: {err}"
        assert "busiest_host" in out
        assert "web3" in out or "web2" in out or "web1" in out
        assert "p95_page" in out
        assert "visitors_page" in out


class TestAdvancedQueriesUnit:
    """Unit tests for the advanced example functions (no service needed)."""

    def test_utc_day_start(self):
        """utc_day_start returns midnight UTC in seconds."""
        now = 1_700_000_000  # arbitrary timestamp
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(time, "time", lambda: now)
            from examples.question_examples_advanced import utc_day_start

            # midnight of that day
            expected = (now // 86400) * 86400
            assert utc_day_start(0) == expected
            assert utc_day_start(1) == expected - 86400
            assert utc_day_start(7) == expected - 7 * 86400


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
