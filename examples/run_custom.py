"""Run your own sqtseries instance with your own ports and database file.

The one command that powers all other examples, demonstrated end to end:

    python3 -m sqtseries --config /path/to/config.toml run

This script does exactly that: it writes a config that picks free ports and
a fresh database file, starts the service as a background process with
``--config``, waits until the admin socket pings, writes and queries one
metric over the custom ports, then stops the service gracefully. No package
code is imported for the lifecycle — only the client for write/query — so
the flow is exactly what you would do in production:

    python3 examples/run_custom.py

After it runs, look in ./sqt-custom-work/ for `config.toml`, the database
file, and `runtime.json` (which records the live ports).
"""

import argparse
import socket
import subprocess
import sys
import time
from pathlib import Path

import zmq

from sqtseries import Client


def free_port() -> int:
    """Ask the OS for a currently-free port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def write_config(workdir: Path) -> dict[str, int]:
    """Write a config with distinct free ports; return the port map."""
    ports = {}
    for name in ("ingest", "query", "streaming", "admin", "http", "stats"):
        ports[name] = free_port()
    (workdir / "config.toml").write_text(f"""[database]
path = "{workdir / 'custom.sqlite'}"

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
    return ports


def wait_until_ping(port: int, timeout_s: float = 20.0) -> None:
    """Poll the admin REP socket until the service answers 'ping'."""
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
                if sock.poll(1000) & zmq.POLLIN and sock.recv_json().get("pong"):
                    return True
            except zmq.ZMQError:
                time.sleep(0.1)
    finally:
        sock.close(linger=0)
        ctx.term()
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Start, verify, and stop your own sqtseries instance"
    )
    parser.add_argument(
        "--workdir",
        default="sqt-custom-work",
        help="directory for config.toml, db, runtime.json (created if needed)",
    )
    args = parser.parse_args()

    workdir = Path(args.workdir).expanduser()
    workdir.mkdir(parents=True, exist_ok=True)
    cfg_path = workdir / "config.toml"
    db_path = workdir / "custom.sqlite"
    ports = write_config(workdir)

    cmd = [sys.executable, "-m", "sqtseries", "--config", str(cfg_path), "run"]

    print(f"config:  {cfg_path}")
    print(f"db:      {db_path}")
    print(f"run:     {' '.join(cmd)}")
    print(
        f"ports:   ingest={ports['ingest']} query={ports['query']} "
        f"stream={ports['streaming']} admin={ports['admin']} "
        f"http={ports['http']} stats={ports['stats']}"
    )

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        if not wait_until_ping(ports["admin"]):
            print("ping: FAILED — service did not come up")
            return 1
        print(f"ping:    ok (admin port {ports['admin']})")

        client = Client(
            ports={
                "write": ports["ingest"],
                "query": ports["query"],
                "subscribe": ports["streaming"],
                "admin": ports["admin"],
            }
        )
        try:
            client.write("demo.cpu", 42.0)
            time.sleep(0.3)
            rows = client.query("demo.cpu")
            assert rows, "no rows returned"
            print(f"write+query: ok ({len(rows)} row(s), " f"value={rows[0]['value']})")
        finally:
            client.close()

        # stop via the stop command (reads runtime.json next to the db)
        subprocess.run(
            [sys.executable, "-m", "sqtseries", "--config", str(cfg_path), "stop"],
            check=True,
            timeout=20,
        )
        proc.wait(timeout=10)
        print("stop:    ok (SIGTERM, runtime file removed)")
        return 0
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=10)
            print("stop:    fallback SIGTERM (stop command did not finish)")


if __name__ == "__main__":
    raise SystemExit(main())
