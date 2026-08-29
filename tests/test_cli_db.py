"""Tests for --db flag and multi-database conflict detection in the CLI.
Covers:
- ``--db`` overriding the configured database path for direct DB commands
- ``run`` refusing to start when a live instance serves a different DB
  in the same directory (shared runtime.json)
- ``status`` flagging the mismatch
- ``stop`` refusing to kill the wrong instance
- systemd unit rendering the config file so custom ports survive install
"""

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

import sqtseries.cli as cli_mod
import sqtseries.systemd as sd
from sqtseries.cli import main


def _write_config(tmp_path, db_dir: str) -> str:
    """Config using free-ish fixed ports and a db path under db_dir."""

    cfg = tmp_path / "conf.toml"
    cfg.write_text(f"""[database]
path = "{db_dir}/main.sqlite"

[ingestion]
port = 26101

[query]
port = 26102

[streaming]
port = 26103

[admin]
port = 26104

[http]
port = 26105

[stats]
port = 26106

[ports]
auto_detect = false
""")
    return str(cfg)


def _write_runtime(db_dir: str, pid: int, db_path: str) -> None:
    Path(db_dir).mkdir(parents=True, exist_ok=True)
    (Path(db_dir) / "runtime.json").write_text(
        json.dumps(
            {
                "pid": pid,
                "ports": {"ingest": 26101},
                "db_path": db_path,
                "version": "0.1.0",
            }
        )
    )


@pytest.fixture
def db_env(tmp_path, monkeypatch):
    """Config + data dir; every pid pretends to be a live sqtseries."""

    monkeypatch.setattr(cli_mod, "_pid_is_sqtseries", lambda pid: True)

    db_dir = str(tmp_path / "data")
    cfg = _write_config(tmp_path, db_dir)

    return {"cfg": cfg, "db_dir": db_dir}


class TestDbFlag:
    """`--db` must redirect the resolved database the commands act on."""

    def test_stats_reads_flagged_db(self, tmp_path, monkeypatch):
        import time

        from sqtseries.engine import (
            StorageEngine,
            create_sqlite_engine,
            initialize_schema,
        )

        db_dir = str(tmp_path / "data")
        cfg = _write_config(tmp_path, db_dir)

        engine = create_sqlite_engine(f"{db_dir}/other.sqlite")
        initialize_schema(engine)
        store = StorageEngine(engine)
        store.insert_many([("cpu", None, 1.0, time.time_ns())])
        engine.dispose()

        r = CliRunner().invoke(
            main, ["--config", cfg, "--db", f"{db_dir}/other.sqlite", "stats"]
        )
        assert r.exit_code == 0, r.output
        assert "metrics: 1" in r.output

    def test_ports_show_flagged_db_when_not_running(self, tmp_path):
        db_dir = str(tmp_path / "data")
        cfg = _write_config(tmp_path, db_dir)

        r = CliRunner().invoke(
            main,
            ["--config", cfg, "--db", f"{db_dir}/other.sqlite", "ports"],
        )
        assert r.exit_code == 0, r.output
        assert "ingest=26101" in r.output


class TestConflictDetection:
    def test_status_flags_conflict(self, db_env, tmp_path):
        _write_runtime(
            db_env["db_dir"], os.getpid(), f"{db_env['db_dir']}/other.sqlite"
        )
        r = CliRunner().invoke(main, ["--config", db_env["cfg"], "status"])

        assert r.exit_code == 0
        assert "running" in r.output
        assert "Warning" in r.output
        assert "other.sqlite" in r.output
        assert "main.sqlite" in r.output

    def test_status_silent_when_dbs_match(self, db_env):
        _write_runtime(db_env["db_dir"], os.getpid(), f"{db_env['db_dir']}/main.sqlite")

        r = CliRunner().invoke(main, ["--config", db_env["cfg"], "status"])

        assert r.exit_code == 0
        assert "Warning" not in r.output

    def test_run_refuses_conflicting_db(self, db_env):
        _write_runtime(db_env["db_dir"], 9999, f"{db_env['db_dir']}/other.sqlite")

        r = CliRunner().invoke(main, ["--config", db_env["cfg"], "run"])

        assert r.exit_code != 0
        assert "Conflicting databases" in r.output

    def test_run_refuses_same_db_already_served(self, db_env):
        _write_runtime(db_env["db_dir"], 9999, f"{db_env['db_dir']}/main.sqlite")

        r = CliRunner().invoke(main, ["--config", db_env["cfg"], "run"])

        assert r.exit_code != 0
        assert "already being served" in r.output

    def test_stop_refuses_wrong_db(self, db_env):
        _write_runtime(db_env["db_dir"], 9999, f"{db_env['db_dir']}/other.sqlite")

        r = CliRunner().invoke(main, ["--config", db_env["cfg"], "stop"])

        assert r.exit_code != 0
        assert "Refusing" in r.output
        assert "other.sqlite" in r.output

    def test_stop_reaches_kill_with_matching_db(self, db_env):
        _write_runtime(db_env["db_dir"], 42424242, f"{db_env['db_dir']}/main.sqlite")

        r = CliRunner().invoke(
            main,
            [
                "--config",
                db_env["cfg"],
                "--db",
                f"{db_env['db_dir']}/main.sqlite",
                "stop",
            ],
        )
        # pid does not exist -> ProcessLookupError -> "not running" error

        assert r.exit_code != 0
        assert "not running" in r.output


class TestSystemdConfigPassthrough:
    def test_template_includes_config_when_passed(self):
        unit = sd.unit_template_contents(
            "/usr/bin/python3",
            "/data/db.sqlite",
            config_file="/etc/sqtseries/conf.toml",
        )
        assert (
            "ExecStart=/usr/bin/python3 -m sqtseries "
            "--config /etc/sqtseries/conf.toml run" in unit
        )

    def test_template_omits_config_when_absent(self):
        unit = sd.unit_template_contents("/usr/bin/python3", "/data/db.sqlite")

        assert "ExecStart=/usr/bin/python3 -m sqtseries run" in unit
        assert "--config" not in unit

    def test_install_writes_config_file(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(sd, "_unit_path", lambda system: tmp_path / sd.UNIT_NAME)

        monkeypatch.setattr(sd, "_systemctl", lambda args: calls.append(args))

        path = sd.install_systemd_unit(
            python="/usr/bin/python3",
            db_path="/data/db.sqlite",
            config_file="/etc/custom/conf.toml",
        )
        assert "--config /etc/custom/conf.toml" in path.read_text()
