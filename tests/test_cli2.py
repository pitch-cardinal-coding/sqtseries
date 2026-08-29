"""Phase 7 packaging & CLI tests."""

from click.testing import CliRunner

from sqtseries.cli import main
from sqtseries.systemd import UNIT_NAME, unit_template_contents


def _make_toml(tmp_path, db_path: str) -> str:
    p = tmp_path / "config.toml"
    p.write_text(f'[database]\npath = "{db_path}"\n\n[ports]\nauto_detect = false\n')

    return str(p)


def _fresh_db(tmp_path) -> tuple[str, str]:
    """Create a minimal config + initialized DB; returns (config, db)."""

    from sqtseries.engine import create_sqlite_engine, initialize_schema

    db = str(tmp_path / "db.sqlite")
    eng = create_sqlite_engine(db)
    initialize_schema(eng)
    eng.dispose()
    return _make_toml(tmp_path, db), db


class TestCliCommands:
    def test_all_commands_registered(self):
        runner = CliRunner()
        r = runner.invoke(main, ["--help"])

        for cmd in [
            "run",
            "stop",
            "status",
            "ports",
            "health",
            "stats",
            "vacuum",
            "optimize",
            "backup",
            "install",
            "uninstall",
        ]:
            assert cmd in r.output, f"missing {cmd}"

    def test_version(self):
        runner = CliRunner()
        r = runner.invoke(main, ["--version"])
        assert "0.1.0" in r.output

    def test_health_ok_on_fresh_db(self, tmp_path):
        cfg, _ = _fresh_db(tmp_path)
        r = CliRunner().invoke(main, ["--config", cfg, "health"])
        assert r.exit_code == 0
        assert "ok" in r.output

    def test_stats_empty(self, tmp_path):
        cfg, _ = _fresh_db(tmp_path)
        r = CliRunner().invoke(main, ["--config", cfg, "stats"])
        assert r.exit_code == 0
        assert "series" in r.output

    def test_vacuum(self, tmp_path):
        cfg, _ = _fresh_db(tmp_path)
        r = CliRunner().invoke(main, ["--config", cfg, "vacuum"])
        assert r.exit_code == 0
        assert "VACUUM" in r.output

    def test_optimize(self, tmp_path):
        cfg, _ = _fresh_db(tmp_path)
        r = CliRunner().invoke(main, ["--config", cfg, "optimize"])
        assert r.exit_code == 0

    def test_backup(self, tmp_path):
        cfg, _ = _fresh_db(tmp_path)
        r = CliRunner().invoke(main, ["--config", cfg, "backup"])
        assert r.exit_code == 0
        assert "backup" in r.output

    def test_status_not_running(self, tmp_path):
        cfg, _ = _fresh_db(tmp_path)
        r = CliRunner().invoke(main, ["--config", cfg, "status"])
        assert r.exit_code == 0
        assert "not running" in r.output

    def test_ports_not_running(self, tmp_path):
        cfg, _ = _fresh_db(tmp_path)
        r = CliRunner().invoke(main, ["--config", cfg, "ports"])
        assert r.exit_code == 0
        assert "ingest=" in r.output


def test_unit_template():
    unit = unit_template_contents(
        "/usr/bin/python3", "/home/u/.sqtseries/data/db.sqlite"
    )
    assert "[Unit]" in unit
    assert "ExecStart=/usr/bin/python3 -m sqtseries run" in unit
    assert "Restart=on-failure" in unit
    assert "ProtectSystem=strict" in unit
    assert "SQT_SERIES_DATABASE__PATH=/home/u/.sqtseries/data/db.sqlite" in unit

    assert "ReadWritePaths=/home/u/.sqtseries/data" in unit
    assert "StartLimitIntervalSec=60" in unit
    assert "StartLimitBurst=3" in unit


def test_unit_name_const():
    assert UNIT_NAME == "sqtseries.service"


def test_install_writes_unit_to_home(tmp_path):
    cfg, _ = _fresh_db(tmp_path)
    home = tmp_path / "home"

    runner = CliRunner()
    r = runner.invoke(main, ["--config", cfg, "install"], env={"HOME": str(home)})

    assert r.exit_code == 0, r.output
    unit = home / ".config/systemd/user/sqtseries.service"
    assert unit.exists()
    assert "ExecStart=" in unit.read_text()


def test_uninstall_removes_unit(tmp_path):
    cfg, _ = _fresh_db(tmp_path)
    home = tmp_path / "home"

    runner = CliRunner()
    runner.invoke(main, ["--config", cfg, "install"], env={"HOME": str(home)})

    r = runner.invoke(main, ["--config", cfg, "uninstall"], env={"HOME": str(home)})

    assert r.exit_code == 0, r.output
    assert not (home / ".config/systemd/user/sqtseries.service").exists()


def test_run_rejects_invalid_config(tmp_path):
    _ = _fresh_db(tmp_path)
    # duplicate ports -> validation fails
    bad = tmp_path / "bad.toml"
    bad.write_text("[ingestion]\nport = 21001\n\n[query]\nport = 21001\n")

    r = CliRunner().invoke(main, ["--config", str(bad), "run"])
    assert r.exit_code != 0
