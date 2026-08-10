"""sqtseries command-line interface."""

import asyncio
import contextlib
import os
import time
from pathlib import Path
from typing import Any

import click

from . import __version__
from .config import Settings, validate_settings
from .logging import configure_logging


def _load(config_file: str | None, db_path: str | None = None) -> Settings:
    settings = Settings.load(config_file)
    if db_path:
        settings = settings.model_copy(
            update={
                "database": settings.database.model_copy(
                    update={"path": str(Path(db_path).expanduser())}
                )
            }
        )
    errs = validate_settings(settings)
    if errs:
        raise click.ClickException("; ".join(errs))
    return settings


def _runtime_path(settings: Settings) -> str:
    return str(Path(settings.db_path_expanded()).parent / "runtime.json")


def _load_runtime(
    config_file: str | None, db_path: str | None = None
) -> dict[str, Any] | None:
    from .runtime import RuntimeState

    return RuntimeState(_runtime_path(_load(config_file, db_path))).read()


def _pid_is_sqtseries(pid: int) -> bool:
    """Whether ``/proc/<pid>`` belongs to a live sqtseries process."""
    try:
        with Path(f"/proc/{pid}/cmdline").open("rb") as f:
            cmd = f.read()
    except OSError:
        return False
    return b"sqtseries" in cmd


@click.group()
@click.version_option(version=__version__, prog_name="sqtseries")
@click.option(
    "--config",
    "config_file",
    type=click.Path(exists=False, dir_okay=False),
    default=None,
    help="Path to config file (TOML/YAML/JSON).",
)
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=False, dir_okay=False),
    default=None,
    help="Database file to use (overrides config file and env vars).",
)
@click.pass_context
def main(ctx: click.Context, config_file: str | None, db_path: str | None) -> None:
    """sqtseries — embedded time-series database on SQLite + ZeroMQ."""
    ctx.ensure_object(dict)
    ctx.obj["config_file"] = config_file
    ctx.obj["db_path"] = db_path


@main.command()
@click.pass_context
def run(ctx: click.Context) -> None:
    """Start the sqtseries service (foreground)."""
    config_file, db_path = ctx.obj["config_file"], ctx.obj["db_path"]
    settings = _load(config_file, db_path)
    db = settings.db_path_expanded()

    rt_state = _load_runtime(config_file, db_path)
    if (
        rt_state is not None
        and "pid" in rt_state
        and _pid_is_sqtseries(rt_state["pid"])
    ):
        rt_db = rt_state.get("db_path")
        if rt_db and Path(rt_db).expanduser() == Path(db):
            raise click.ClickException(
                f"Database {db} is already being served (pid {rt_state['pid']})"
            )
        raise click.ClickException(
            f"Conflicting databases: pid {rt_state['pid']} is serving {rt_db!r}, "
            f"but this instance is configured for {db!r}. Refusing to start "
            "— stop the other instance or point --db at its database."
        )

    other = (
        [p for p in Path(db).parent.glob("*.sqlite") if str(p) != db]
        if Path(db).parent.is_dir()
        else []
    )
    if other:
        click.echo(
            f"Note: other databases found in {Path(db).parent}: "
            + ", ".join(sorted(str(p) for p in other))
        )
        click.echo(
            "  Use --db to pick one to open / write to, or --config to "
            "point at the right one."
        )

    configure_logging(settings.logging)

    from .service import Service

    svc = Service(settings)

    async def _run() -> None:
        await svc.start()
        try:
            await svc.run()
        finally:
            await svc.shutdown()

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_run())


@main.command()
@click.pass_context
def stop(ctx: click.Context) -> None:
    """Stop the sqtseries service."""
    rt_state = _load_runtime(ctx.obj["config_file"], ctx.obj["db_path"])
    if rt_state is None or "pid" not in rt_state:
        raise click.ClickException("Service is not running")
    pid = rt_state["pid"]
    if not _pid_is_sqtseries(pid):
        raise click.ClickException(f"pid {pid} is not a sqtseries process")
    rt_db = rt_state.get("db_path")
    if rt_db:
        settings = _load(ctx.obj["config_file"], ctx.obj["db_path"])
        if Path(rt_db).expanduser() != Path(settings.db_path_expanded()):
            raise click.ClickException(
                f"Runtime points to database {rt_db!r} but this invocation "
                f"resolves to {settings.db_path_expanded()!r}. Refusing to "
                "stop the wrong instance — pass --db <that database> instead."
            )
    try:
        os.kill(pid, 15)
    except ProcessLookupError:
        raise click.ClickException(f"pid {pid} not running") from None
    click.echo(f"Sent SIGTERM to pid {pid}")
    # Wait (bounded) for the service to actually exit. A restart script that
    # immediately rebinds the same ports must not race a still-dying process.
    # Clean shutdown removes runtime.json as its last step, so watch that file;
    # os.kill(pid, 0) alone is not enough because a not-yet-reaped zombie still
    # answers to signal 0. Also bail early if the pid disappears outright.
    rt_path = Path(rt_db).parent / "runtime.json" if rt_db else None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if rt_path is not None and not rt_path.exists():
            return
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    click.echo(
        f"Warning: pid {pid} still running 10s after SIGTERM; " "check the service log",
        err=True,
    )


@main.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show service status."""
    rt_state = _load_runtime(ctx.obj["config_file"], ctx.obj["db_path"])
    if rt_state is None:
        click.echo("Service: not running")
        return
    pid = rt_state["pid"]
    try:
        os.kill(pid, 0)
    except OSError:
        click.echo("Service: not running (stale runtime file)")
        return
    if not _pid_is_sqtseries(pid):
        click.echo("Service: not running (stale runtime file)")
        return
    click.echo(f"Service: running (pid {pid})")
    click.echo(f"Version:  {rt_state.get('version', '?')}")
    click.echo(f"DB:       {rt_state.get('db_path', '?')}")
    rt_db = rt_state.get("db_path")
    if rt_db:
        settings = _load(ctx.obj["config_file"], ctx.obj["db_path"])
        if Path(rt_db).expanduser() != Path(settings.db_path_expanded()):
            click.echo(
                f"Warning: running instance uses {rt_db!r}, but this config "
                f"resolves to {settings.db_path_expanded()!r}"
            )
    for k, v in rt_state.get("ports", {}).items():
        click.echo(f"  {k}: {v}")


@main.command()
@click.pass_context
def ports(ctx: click.Context) -> None:
    """Show active ports."""
    rt_state = _load_runtime(ctx.obj["config_file"], ctx.obj["db_path"])
    if rt_state is None:
        settings = _load(ctx.obj["config_file"], ctx.obj["db_path"])
        click.echo(
            f"not running; configured: ingest={settings.ingestion.port} "
            f"query={settings.query.port} stream={settings.streaming.port} "
            f"admin={settings.admin.port} http={settings.http.port}"
        )
        return
    for k, v in rt_state.get("ports", {}).items():
        click.echo(f"{k}: {v}")


@main.command()
@click.pass_context
def health(ctx: click.Context) -> None:
    """Health check (read-only DB probe)."""
    settings = _load(ctx.obj["config_file"], ctx.obj["db_path"])
    from .engine import create_sqlite_engine, quick_check

    engine = create_sqlite_engine(settings.db_path_expanded())
    try:
        status = quick_check(engine)
    finally:
        engine.dispose()
    ok = status == "ok"
    click.echo("ok" if ok else f"degraded: {status}")
    raise click.exceptions.Exit(0 if ok else 1)


@main.command()
@click.pass_context
def stats(ctx: click.Context) -> None:
    """Show database statistics."""
    settings = _load(ctx.obj["config_file"], ctx.obj["db_path"])
    from .engine import StorageEngine, create_sqlite_engine

    engine = create_sqlite_engine(settings.db_path_expanded())
    try:
        store = StorageEngine(engine)
        click.echo(f"metrics: {len(store.list_metrics())}")
        click.echo(f"series:  {store.series_count()}")
    finally:
        engine.dispose()


@main.command()
@click.pass_context
def vacuum(ctx: click.Context) -> None:
    """Database maintenance: full VACUUM."""
    settings = _load(ctx.obj["config_file"], ctx.obj["db_path"])
    from .engine import create_sqlite_engine

    engine = create_sqlite_engine(settings.db_path_expanded())
    try:
        # VACUUM cannot run inside a transaction
        with engine.connect() as conn:
            conn.exec_driver_sql("VACUUM")
        click.echo("VACUUM complete")
    finally:
        engine.dispose()


@main.command()
@click.pass_context
def optimize(ctx: click.Context) -> None:
    """Optimize query-planner statistics."""
    settings = _load(ctx.obj["config_file"], ctx.obj["db_path"])
    from .engine import create_sqlite_engine, run_optimize

    engine = create_sqlite_engine(settings.db_path_expanded())
    try:
        run_optimize(engine)
        click.echo("optimize complete")
    finally:
        engine.dispose()


@main.command()
@click.pass_context
def backup(ctx: click.Context) -> None:
    """Create a consistent VACUUM INTO backup."""
    settings = _load(ctx.obj["config_file"], ctx.obj["db_path"])
    from .engine import backup_database, create_sqlite_engine

    engine = create_sqlite_engine(settings.db_path_expanded())
    try:
        path = backup_database(engine, settings.backup.path)
        click.echo(f"backup written to {path}")
    finally:
        engine.dispose()


@main.command()
@click.option(
    "--system", is_flag=True, help="Install a system-level unit (needs root)."
)
@click.pass_context
def install(ctx: click.Context, system: bool) -> None:
    """Install the systemd service unit."""
    import sys

    from .systemd import install_systemd_unit

    settings = _load(ctx.obj["config_file"], ctx.obj["db_path"])
    install_systemd_unit(
        sys.executable,
        system=system,
        db_path=settings.db_path_expanded(),
        backup_path=settings.backup.path,
        config_file=ctx.obj["config_file"],
    )
    click.echo("systemd unit installed")


@main.command()
@click.option("--system", is_flag=True)
@click.pass_context
def uninstall(ctx: click.Context, system: bool) -> None:
    """Remove the systemd service unit."""
    from .systemd import uninstall_systemd_unit

    uninstall_systemd_unit(system=system)
    click.echo("systemd unit removed")


if __name__ == "__main__":
    main()
