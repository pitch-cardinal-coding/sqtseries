"""CLI edge cases: stop/status with missing or stale runtime state."""

import json

from click.testing import CliRunner

from sqtseries.cli import main


def _config(tmp_path, db_path):
    p = tmp_path / "config.toml"
    p.write_text(f'[database]\npath = "{db_path}"\n\n[ports]\nauto_detect = false\n')

    return str(p)


def _runtime(tmp_path, pid: int) -> str:
    rt = tmp_path / "runtime.json"
    rt.write_text(
        json.dumps(
            {
                "pid": pid,
                "version": "0.1.0",
                "db_path": str(tmp_path / "db.sqlite"),
                "ports": {"ingest": 12501},
            }
        )
    )
    return str(rt)


def test_stop_not_running(tmp_path):
    cfg = _config(tmp_path, str(tmp_path / "db.sqlite"))
    r = CliRunner().invoke(main, ["--config", cfg, "stop"])
    assert r.exit_code != 0
    assert "not running" in r.output


def test_stop_stale_pid(tmp_path):
    cfg = _config(tmp_path, str(tmp_path / "db.sqlite"))
    # pid almost certainly not alive
    _runtime(tmp_path, 999_999_999)
    r = CliRunner().invoke(main, ["--config", cfg, "stop"])
    assert r.exit_code != 0


def test_status_stale_runtime(tmp_path):
    cfg = _config(tmp_path, str(tmp_path / "db.sqlite"))
    _runtime(tmp_path, 999_999_999)
    r = CliRunner().invoke(main, ["--config", cfg, "status"])
    assert r.exit_code == 0
    assert "stale runtime file" in r.output
