"""systemd unit management for sqtseries (user or system service)."""

import os
import shlex
import subprocess
import sys
from contextlib import suppress
from pathlib import Path

UNIT_NAME = "sqtseries.service"


def _installing_user() -> tuple[str, str] | None:
    """Resolve (name, home dir) of the human installing a system unit.

    A system unit runs as root by default, which would put the database
    under /root instead of the operator's home. Returns None when no
    non-root installing user can be determined (then the unit runs as
    root with its explicit paths).
    """
    sudo_user = os.environ.get("SUDO_USER")
    sudo_uid = os.environ.get("SUDO_UID")
    # sudo make → sudo python: the second sudo sets SUDO_USER=root /
    # SUDO_UID=0, losing the original user. Fall back to logname.
    if (not sudo_user or sudo_user == "root") and sudo_uid:
        if sudo_uid == "0":
            try:
                logname = subprocess.check_output(
                    ["logname"], text=True, timeout=5
                ).strip()
                sudo_user = logname or None
            except Exception:
                sudo_user = None
        else:
            try:
                import pwd

                sudo_user = pwd.getpwuid(int(sudo_uid)).pw_name
            except Exception:
                sudo_user = None
    if not sudo_user or sudo_user == "root":
        return None
    try:
        import pwd

        return sudo_user, pwd.getpwnam(sudo_user).pw_dir
    except Exception:
        return None


def _against_home(path: str | None, home: str) -> str | None:
    """Expand a leading ~ against an explicit home dir (not the euid's)."""
    if path is None:
        return None
    if path == "~":
        return home
    if path.startswith("~/"):
        return home + path[1:]
    return path


def unit_template_contents(
    python: str,
    db_path: str,
    backup_path: str | None = None,
    config_file: str | None = None,
    system: bool = False,
    user_home: str | None = None,
) -> str:
    """Render the systemd unit file. Uses `python -m sqtseries run`."""

    for _p in (db_path, backup_path, config_file):
        if _p and any(ch in _p for ch in "\r\n"):
            raise ValueError("paths must not contain control characters")
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

    user_line = ""
    protect_home = "read-only"
    wanted_by = "default.target"
    if system:
        wanted_by = "multi-user.target"
        if user_home:
            user_line = f"User={user_home[0]}\n"
            # Full home access for the installing user: the database,
            # runtime file and backups all live under $HOME by default,
            # and ProtectHome=read-only would block writes.
            protect_home = "false"
            if user_home[1] not in writable:
                writable.append(user_home[1])
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
{user_line}\
# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome={protect_home}
ReadWritePaths={read_write}

# Environment
Environment=SQT_SERIES_DATABASE__PATH={db_path}
Environment=SQT_SERIES_LOGGING__LEVEL=INFO

[Install]
WantedBy={wanted_by}
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

    user_home = _installing_user() if system else None
    if user_home:
        # Under sudo "~" expands to /root: re-anchor explicit home paths
        # to the installing user's home so the unit serves their database.
        db_path = _against_home(db_path, user_home[1]) or db_path
        backup_path = _against_home(backup_path, user_home[1])
        config_file = _against_home(config_file, user_home[1])

    path = _unit_path(system)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        unit_template_contents(
            python,
            db_path,
            backup_path,
            config_file,
            system=system,
            user_home=user_home,
        )
    )

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
