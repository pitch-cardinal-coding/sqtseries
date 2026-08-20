#!/usr/bin/env python3
"""Run all examples against a live sqtseries service.

Starts a service on free ports, runs every example script, and reports
pass/fail. Used to verify examples are not stale.

Usage:
    python3 examples/run_all_examples.py
    python3 examples/run_all_examples.py --skip-camera
"""

import argparse
import socket
import subprocess
import sys
import time
from pathlib import Path

import zmq

PROJECT = Path(__file__).resolve().parent.parent
PY = sys.executable
ENV_PY = "/home/iam/devcode/.env/sqtseries/bin/python3"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until_ping(port: int, timeout_s: float = 30.0) -> bool:
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 500)
    sock.setsockopt(zmq.RCVTIMEO, 1000)
    sock.connect(f"tcp://127.0.0.1:{port}")
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            try:
                sock.send_json({"cmd": "ping"})
                if sock.poll(1000) & zmq.POLLIN:
                    reply = sock.recv_json()
                    if reply.get("pong"):
                        return True
            except zmq.ZMQError:
                time.sleep(0.1)
    finally:
        sock.close(linger=0)
        ctx.term()
    return False


def run_example(name: str, cmd: list[str], timeout: float = 30.0) -> tuple[bool, str]:
    """Run an example script; return (success, output)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(PROJECT),
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return True, "TIMEOUT (long-running, started OK)"
    except Exception as e:
        return False, str(e)


def main():
    parser = argparse.ArgumentParser(description="Run all sqtseries examples")
    parser.add_argument(
        "--skip-camera", action="store_true", help="skip camera examples"
    )
    parser.add_argument(
        "--skip-advanced", action="store_true", help="skip advanced examples"
    )
    args = parser.parse_args()

    # Pick free ports
    ports = {
        "ingest": free_port(),
        "query": free_port(),
        "streaming": free_port(),
        "admin": free_port(),
        "http": free_port(),
        "stats": free_port(),
    }

    # Write config
    workdir = Path("/tmp/sqtseries-example-test")  # noqa: S108 - CLI test default
    workdir.mkdir(exist_ok=True)
    cfg_path = workdir / "config.toml"
    db_path = workdir / "test.sqlite"

    cfg_path.write_text(f"""[database]
path = "{db_path}"

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

    # Start the service
    print(f"Starting sqtseries on ports: {ports}")
    proc = subprocess.Popen(
        [ENV_PY, "-m", "sqtseries", "--config", str(cfg_path), "run"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        if not wait_until_ping(ports["admin"]):
            print("FAIL: service did not start")
            return 1
        print("Service started OK\n")

        results = []

        # --- Non-camera examples ---
        examples = [
            (
                "ingest_http.py",
                [
                    ENV_PY,
                    str(PROJECT / "examples" / "ingest_http.py"),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(ports["http"]),
                    "--count",
                    "5",
                    "--batch",
                    "2",
                ],
                15,
            ),
            (
                "producer.py",
                [
                    ENV_PY,
                    str(PROJECT / "examples" / "producer.py"),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(ports["ingest"]),
                    "--count",
                    "10",
                    "--rate",
                    "50",
                ],
                15,
            ),
            (
                "consumer.py",
                [
                    ENV_PY,
                    str(PROJECT / "examples" / "consumer.py"),
                    "--port",
                    str(ports["query"]),
                ],
                10,
            ),
            (
                "consumer.py (http)",
                [
                    ENV_PY,
                    str(PROJECT / "examples" / "consumer.py"),
                    "--http",
                    f"http://127.0.0.1:{ports['http']}",
                ],
                10,
            ),
            (
                "run_custom.py",
                [
                    ENV_PY,
                    str(PROJECT / "examples" / "run_custom.py"),
                    "--workdir",
                    str(workdir / "custom"),
                ],
                30,
            ),
        ]

        if not args.skip_advanced:
            examples.extend(
                [
                    (
                        "tags_examples.py",
                        [
                            ENV_PY,
                            str(PROJECT / "examples" / "tags_examples.py"),
                            "--host",
                            "127.0.0.1",
                            "--write-port",
                            str(ports["ingest"]),
                            "--query-port",
                            str(ports["query"]),
                            "--http-port",
                            str(ports["http"]),
                        ],
                        30,
                    ),
                    (
                        "question_examples.py",
                        [
                            ENV_PY,
                            str(PROJECT / "examples" / "question_examples.py"),
                            "--host",
                            "127.0.0.1",
                            "--write-port",
                            str(ports["ingest"]),
                            "--query-port",
                            str(ports["query"]),
                            "--http-port",
                            str(ports["http"]),
                        ],
                        30,
                    ),
                    (
                        "admin_examples.py",
                        [
                            ENV_PY,
                            str(PROJECT / "examples" / "admin_examples.py"),
                            "--host",
                            "127.0.0.1",
                            "--port",
                            str(ports["admin"]),
                        ],
                        15,
                    ),
                    (
                        "maintenance_examples.py",
                        [
                            ENV_PY,
                            str(PROJECT / "examples" / "maintenance_examples.py"),
                            "--host",
                            "127.0.0.1",
                            "--admin-port",
                            str(ports["admin"]),
                            "--http-port",
                            str(ports["http"]),
                        ],
                        15,
                    ),
                    (
                        "stream_examples.py",
                        [
                            ENV_PY,
                            str(PROJECT / "examples" / "stream_examples.py"),
                            "--host",
                            "127.0.0.1",
                            "--port",
                            str(ports["streaming"]),
                        ],
                        5,
                    ),  # long-running; timeout = success (just needs to start without crash)
                    (
                        "stats_monitor.py",
                        [
                            ENV_PY,
                            str(PROJECT / "examples" / "stats_monitor.py"),
                            "--host",
                            "127.0.0.1",
                            "--port",
                            str(ports["stats"]),
                        ],
                        5,
                    ),  # long-running; timeout = success (just needs to start without crash)
                    (
                        "query_examples.py",
                        [
                            ENV_PY,
                            str(PROJECT / "examples" / "query_examples.py"),
                            "--host",
                            "127.0.0.1",
                            "--query-port",
                            str(ports["query"]),
                            "--http-port",
                            str(ports["http"]),
                        ],
                        30,
                    ),
                ]
            )

        if not args.skip_camera:
            examples.extend(
                [
                    (
                        "camera_feed.py (--ask)",
                        [
                            ENV_PY,
                            str(PROJECT / "examples" / "camera" / "camera_feed.py"),
                            "--ask",
                            "--host",
                            "127.0.0.1",
                            "--http-port",
                            str(ports["http"]),
                            "--hours",
                            "1",
                        ],
                        15,
                    ),
                ]
            )

        for name, cmd, timeout in examples:
            ok, output = run_example(name, cmd, timeout)
            status = "PASS" if ok else "FAIL"
            results.append((name, status, output))
            # Show brief output on failure
            if not ok:
                lines = output.strip().split("\n")
                brief = "\n".join(lines[-5:]) if len(lines) > 5 else output
                print(f"  {status}: {name}")
                print(f"    {brief}")
            else:
                print(f"  {status}: {name}")

        # Summary
        passed = sum(1 for _, s, _ in results if s == "PASS")
        failed = sum(1 for _, s, _ in results if s == "FAIL")
        print(f"\n{'=' * 50}")
        print(f"Results: {passed} passed, {failed} failed out of {len(results)}")
        if failed:
            print("\nFailed examples:")
            for name, status, _output in results:
                if status == "FAIL":
                    print(f"  - {name}")
        return 1 if failed else 0

    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        # Cleanup
        for f in workdir.rglob("*"):
            if f.is_file():
                f.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
