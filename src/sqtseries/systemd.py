"""systemd unit management for sqtseries (user or system service)."""

import shlex
import subprocess
import sys
from contextlib import suppress
from pathlib import Path

UNIT_NAME = "sqtseries.service"


def unit_template_contents(
    python: str,
    db_path: str,
    backup_path: str | None = None,
    config_file: str | None = None,
) -> str:
    """Render the systemd unit file. Uses `python -m sqtseries run`."""

    cmd_parts = [python, "-m", "sqtseries"]
    if config_file:
        cmd_parts += ["--config", str(config_file)]
    cmd_parts.append("run")
    cmd = shlex.join(cmd_parts)

    writable = [str(Path(db_path).expanduser().parent)]

    if backup_path:
        writable.append(str(Path(backup_path).expanduser()))
    # unique, in order
    writable = list(dict.fromkeys(writable))
    read_write = " ".join(writable)

    return f"""\
[Unit]
Description=sqtseries time-series database
After=network.target
StartLimitIntervalSec=60
StartLimitBurst=3

[Service]
Type=simple
ExecStart={cmd}
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=sqtseries

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths={read_write}

# Environment
Environment=SQT_SERIES_DATABASE__PATH={db_path}
Environment=SQT_SERIES_LOGGING__LEVEL=INFO

[Install]
WantedBy=default.target
"""


def _unit_path(system: bool) -> Path:
    if system:
        return Path("/etc/systemd/system") / UNIT_NAME
    return Path.home() / ".config/systemd/user" / UNIT_NAME


def _systemctl(args: list[str]) -> None:
    """Run systemctl, ignoring failures (best-effort when testing)."""
    cmd: list[str] = ["systemctl", *args]
    subprocess.run(cmd, check=False, capture_output=True)


def install_systemd_unit(
    python: str | None = None,
    system: bool = False,
    db_path: str | None = None,
    backup_path: str | None = None,
    config_file: str | None = None,
) -> Path:
    """Write and enable the sqtseries unit; returns the unit file path."""

    python = python or sys.executable

    db_path = db_path or str(Path("~/.sqtseries/data/db.sqlite").expanduser())

    path = _unit_path(system)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(unit_template_contents(python, db_path, backup_path, config_file))

    prefix: list[str] = [] if system else ["--user"]
    _systemctl([*prefix, "daemon-reload"])
    _systemctl([*prefix, "enable", "--now", UNIT_NAME])
    return path


def uninstall_systemd_unit(*, system: bool = False) -> Path:
    """Disable and remove the sqtseries unit; returns the unit file path."""

    prefix: list[str] = [] if system else ["--user"]
    path = _unit_path(system)
    _systemctl([*prefix, "disable", "--now", UNIT_NAME])
    with suppress(OSError):
        path.unlink(missing_ok=True)
    return path
