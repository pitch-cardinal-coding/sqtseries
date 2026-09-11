"""systemd unit management edge cases (mocked systemctl + HOME)."""

import pytest

import sqtseries.systemd as sd


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    """Point unit paths + systemctl at a temp dir; record calls."""
    calls = []
    monkeypatch.setattr(
        sd,
        "_unit_path",
        lambda system: tmp_path / ("system" if system else "user") / sd.UNIT_NAME,
    )
    monkeypatch.setattr(sd, "_systemctl", lambda args: calls.append(args))

    return calls


def test_install_writes_and_enables(isolated, tmp_path):
    sd.install_systemd_unit(
        python="/usr/bin/python3",
        db_path="/data/db.sqlite",
        backup_path="/data/backups",
    )
    unit = (tmp_path / "user" / sd.UNIT_NAME).read_text()
    assert "ExecStart=/usr/bin/python3 -m sqtseries run" in unit
    assert "ReadWritePaths=/data /data/backups" in unit
    assert "SQT_SERIES_DATABASE__PATH=/data/db.sqlite" in unit
    assert isolated == [
        ["--user", "daemon-reload"],
        ["--user", "enable", "--now", sd.UNIT_NAME],
    ]


def test_install_system_uses_no_user_prefix(isolated, tmp_path):
    sd.install_systemd_unit(
        python="/usr/bin/python3", db_path="/data/db.sqlite", system=True
    )
    assert (tmp_path / "system" / sd.UNIT_NAME).exists()
    assert ["daemon-reload"] in isolated
    assert ["enable", "--now", sd.UNIT_NAME] in isolated


def test_uninstall_disables_and_removes(isolated, tmp_path):
    unit_path = tmp_path / "user" / sd.UNIT_NAME
    unit_path.parent.mkdir(parents=True)
    unit_path.write_text("[Unit]")
    sd.uninstall_systemd_unit()
    assert not unit_path.exists()
    assert ["--user", "disable", "--now", sd.UNIT_NAME] in isolated


def test_uninstall_system(isolated, tmp_path):
    unit_path = tmp_path / "system" / sd.UNIT_NAME
    unit_path.parent.mkdir(parents=True)
    unit_path.write_text("[Unit]")
    sd.uninstall_systemd_unit(system=True)
    assert not unit_path.exists()
    assert ["disable", "--now", sd.UNIT_NAME] in isolated


def test_template_quotes_python_with_spaces():
    unit = sd.unit_template_contents("/opt/my env/bin/python3", "/data/db.sqlite")

    assert "ExecStart='/opt/my env/bin/python3' -m sqtseries run" in unit


def test_template_defaults():
    unit = sd.unit_template_contents("/usr/bin/python3", "~/.sqtseries/data/db.sqlite")

    assert "ReadWritePaths=/home/" in unit or "/sqtseries/data" in unit


def test_template_system_runs_as_installing_user():
    unit = sd.unit_template_contents(
        "/usr/bin/python3",
        "/home/iam/.sqtseries/data/db.sqlite",
        system=True,
        user_home=("iam", "/home/iam"),
    )

    assert "User=iam" in unit
    assert "ProtectHome=false" in unit
    assert "/home/iam" in unit
    assert "WantedBy=multi-user.target" in unit


def test_template_system_without_user_stays_root():
    unit = sd.unit_template_contents("/usr/bin/python3", "/data/db.sqlite", system=True)

    assert "User=" not in unit
    assert "ProtectHome=read-only" in unit
    assert "WantedBy=multi-user.target" in unit


def test_template_user_scope_unchanged():
    unit = sd.unit_template_contents("/usr/bin/python3", "/data/db.sqlite")

    assert "User=" not in unit
    assert "WantedBy=default.target" in unit


def test_template_rejects_control_chars():
    import pytest

    with pytest.raises(ValueError):
        sd.unit_template_contents("/usr/bin/python3", "/data/db\r\n.sqlite")


def test_template_includes_jemalloc_when_present():
    unit = sd.unit_template_contents(
        "/usr/bin/python3",
        "/data/db.sqlite",
        jemalloc_path="/usr/lib/x86_64-linux-gnu/libjemalloc.so.2",
    )

    assert "Environment=LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2" in unit


def test_template_skips_jemalloc_when_absent(monkeypatch):
    monkeypatch.setattr(sd, "_find_jemalloc", lambda: None)
    unit = sd.unit_template_contents("/usr/bin/python3", "/data/db.sqlite")

    assert "LD_PRELOAD" not in unit
    assert "Environment=SQT_SERIES_DATABASE__PATH=/data/db.sqlite" in unit


def test_install_system_reanchors_home(monkeypatch, isolated, tmp_path):
    monkeypatch.setattr(sd, "_installing_user", lambda: ("iam", "/home/iam"))
    sd.install_systemd_unit(
        python="/usr/bin/python3",
        db_path="~/.sqtseries/data/db.sqlite",
        system=True,
    )
    unit = (tmp_path / "system" / sd.UNIT_NAME).read_text()
    assert "User=iam" in unit
    assert "/home/iam/.sqtseries/data/db.sqlite" in unit


def test_install_system_reanchors_root_expanded_default(
    monkeypatch, isolated, tmp_path
):
    """Under sudo, ~ already expanded to /root before reaching install —
    the unit must still serve the installing user's database, not /root's."""
    monkeypatch.setattr(sd, "_installing_user", lambda: ("iam", "/home/iam"))
    sd.install_systemd_unit(
        python="/usr/bin/python3",
        db_path="/root/.sqtseries/data/db.sqlite",
        backup_path="/root/.sqtseries/backups",
        system=True,
    )
    unit = (tmp_path / "system" / sd.UNIT_NAME).read_text()
    assert "/root/" not in unit
    assert "/home/iam/.sqtseries/data/db.sqlite" in unit
    assert "/home/iam/.sqtseries/backups" in unit
