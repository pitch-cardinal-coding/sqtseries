"""Run the example scripts for real, against a live in-process service.
The examples in examples/ are the canonical "how do I do X" reference,
so they must work against a running service, not just import. Each test
boots the service the way a user would (with the clock-skew guard raised,
since several examples backfill history) and executes the script as a
subprocess — exactly the command the README and docs tell users to run.
Ports are allocated dynamically (bind port 0) so parallel/leftover
processes can never break the run.
"""

import asyncio
import re
import subprocess
import sys
from pathlib import Path

import pytest

from sqtseries.config import Settings
from sqtseries.service import Service

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = REPO_ROOT / "examples"
ANSWER_RE = re.compile(r"ANSWER: (\S+) = ([\d.]+)")


@pytest.fixture
async def running_service(tmp_path, free_ports):
    s = Settings(
        database={"path": str(tmp_path / "c.sqlite"), "batch_size": 500},
        ingestion={
            "port": free_ports["ingest"],
            "reject_client_timestamp_skew_s": 864000,
        },
        query={"port": free_ports["query"]},
        streaming={"port": free_ports["streaming"]},
        admin={"port": free_ports["admin"]},
        http={"port": free_ports["http"]},
        stats={"port": free_ports["stats"]},
        ports={"auto_detect": False},
    )
    svc = Service(s)
    await svc.start()
    await svc.pool.stop()
    pump_task = asyncio.create_task(_pump(svc))
    try:
        yield svc, s, free_ports
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


async def run_example(script: str, *args: str) -> subprocess.CompletedProcess:
    """Run one example script exactly like the docs tell users to."""
    return await asyncio.to_thread(
        subprocess.run,
        [sys.executable, str(EXAMPLES / script), *args],
        capture_output=True,
        text=True,
        timeout=90,
        cwd=REPO_ROOT,
    )


def parse_answers(stdout: str) -> dict[str, float]:
    """Pull machine-readable 'ANSWER: key = value' lines from the output."""

    return {
        m.group(1): float(m.group(2))
        for line in stdout.splitlines()
        if (m := ANSWER_RE.search(line))
    }


def _args(running_service) -> list[str]:
    _, _, ports = running_service
    return [
        "--host",
        "127.0.0.1",
        "--write-port",
        str(ports["ingest"]),
        "--query-port",
        str(ports["query"]),
        "--http-port",
        str(ports["http"]),
    ]


class TestQuestionExamples:
    """The four README questions must be answerable, with real numbers."""

    async def test_answers_all_four_readme_questions(self, running_service):
        res = await run_example("question_examples.py", *_args(running_service))

        assert res.returncode == 0, (
            f"exit {res.returncode}\nstdout:\n{res.stdout}\nstderr:\n{res.stderr}"
        )
        answers = parse_answers(res.stdout)

        # 1. average CPU load last hour: seeded cycle 20.0..100.0
        assert 20 <= answers["avg_cpu_last_hour"] <= 100
        # 2. peak temperature yesterday: seeded sine 5.0..25.0
        assert 5 <= answers["peak_temp_yesterday"] <= 25
        # 3. p99 latency over the week: seeded uniform 10.0..109.0
        assert 10 <= answers["p99_latency_week"] <= 110
        # 4. count today: seeded visitor points always land in today
        assert answers["count_today"] > 0

        # 5. trend must be a real comparison result
        assert "trend = " in res.stdout

        # 9/10. visitors sums and busiest buckets must be positive
        assert answers["visitors_last_24h"] > 0
        assert "busiest_hour" in res.stdout
        assert "busiest_day" in res.stdout

        # 11. the staleness probe must be >= 0s (seed covers the window)

        assert answers["no_data_for_s"] >= 0

        # 12. all-time stats must be internally consistent
        m = re.search(r"ANSWER: alltime = (.*)", res.stdout)
        assert m, "alltime answer missing"
        stats = dict(p.split("=") for p in m.group(1).split())
        assert float(stats["min"]) <= float(stats["avg"]) <= float(stats["max"])

        assert float(stats["count"]) >= 24

        # multi-agg stats_3days must be internally consistent
        m = re.search(r"ANSWER: stats_3days = (.*)", res.stdout)
        assert m, "stats_3days answer missing"
        stats = dict(p.split("=") for p in m.group(1).split())
        assert float(stats["min"]) <= float(stats["avg"]) <= float(stats["max"])

        assert float(stats["median"]) <= float(stats["p99"])
        assert float(stats["p95"]) <= float(stats["p99"])

    async def test_all_answer_lines_present(self, running_service):
        res = await run_example("question_examples.py", *_args(running_service))

        assert res.returncode == 0, res.stderr
        for key in (
            "avg_cpu_last_hour",
            "peak_temp_yesterday",
            "p99_latency_week",
            "count_today",
        ):
            assert key in parse_answers(res.stdout), f"missing ANSWER: {key}"


class TestOtherExamples:
    async def test_producer_and_consumer(self, running_service):
        _, _, ports = running_service
        res = await run_example(
            "producer.py",
            "--host",
            "127.0.0.1",
            "--port",
            str(ports["ingest"]),
            "--count",
            "10",
            "--rate",
            "0",
        )
        assert res.returncode == 0, res.stderr

        res = await run_example(
            "consumer.py",
            "--host",
            "127.0.0.1",
            "--port",
            str(ports["query"]),
        )
        assert res.returncode == 0, res.stderr
        assert "ZMQ query reply" in res.stdout

    async def test_ingest_http_then_read(self, running_service):
        _, _, ports = running_service
        res = await run_example(
            "ingest_http.py",
            "--host",
            "127.0.0.1",
            "--port",
            str(ports["http"]),
            "--count",
            "5",
            "--batch",
            "3",
        )
        assert res.returncode == 0, res.stderr

        res = await run_example(
            "consumer.py",
            "--host",
            "127.0.0.1",
            "--http",
            f"http://127.0.0.1:{ports['http']}",
        )
        assert res.returncode == 0, res.stderr
        assert "HTTP query reply" in res.stdout

    async def test_admin_examples(self, running_service):
        _, _, ports = running_service
        res = await run_example(
            "admin_examples.py",
            "--host",
            "127.0.0.1",
            "--port",
            str(ports["admin"]),
        )
        assert res.returncode == 0, res.stderr
        assert "--- ping ---" in res.stdout

    async def test_query_examples_every_query_type(self, running_service):
        res = await run_example("query_examples.py", *_args(running_service))

        assert res.returncode == 0, (
            f"exit {res.returncode}\nstdout:\n{res.stdout}\nstderr:\n{res.stderr}"
        )
        for marker in (
            "1. Raw query",
            "3. Whole-window aggregation",
            "4. Downsampling",
            "5. Multi-aggregation",
            "6. HTTP query",
            "7. HTTP aggregate",
        ):
            assert marker in res.stdout

    async def test_tags_examples(self, running_service):
        """tags_examples.py: tagged writes, whole-metric merge, validation."""

        _, _, ports = running_service
        res = await run_example(
            "tags_examples.py",
            "--host",
            "127.0.0.1",
            "--write-port",
            str(ports["ingest"]),
            "--query-port",
            str(ports["query"]),
            "--http-port",
            str(ports["http"]),
        )
        assert res.returncode == 0, (
            f"exit {res.returncode}\nstdout:\n{res.stdout}\nstderr:\n{res.stderr}"
        )
        for marker in (
            "=== 1. Writing tagged measurements",
            "=== 3. Whole-metric query over the wire (merged)",
            "(all hosts merged)",
            "=== 5. Tag validation",
            "HTTP 400",
            "=== 6. Cardinality warning",
        ):
            assert marker in res.stdout, f"missing {marker!r}"

    async def test_run_custom_lifecycle(self, tmp_path):
        """run_custom.py starts its own instance (own ports, own db)."""

        res = await run_example("run_custom.py", "--workdir", str(tmp_path / "custom"))

        assert res.returncode == 0, (
            f"exit {res.returncode}\nstdout:\n{res.stdout}\nstderr:\n{res.stderr}"
        )
        assert "ping:    ok" in res.stdout
        assert "write+query: ok" in res.stdout
        assert "stop:    ok" in res.stdout
        # runtime file is removed on graceful stop
        assert not (tmp_path / "custom" / "runtime.json").exists()
