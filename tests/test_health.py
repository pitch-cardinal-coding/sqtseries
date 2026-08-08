"""Health-check helpers edge cases."""

from sqtseries.engine import create_sqlite_engine, initialize_schema
from sqtseries.health import collect_health, is_ready


def test_collect_health_ok(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "h.sqlite"))
    initialize_schema(eng)
    payload = collect_health(eng, started_at=1000.0)
    assert payload["status"] == "ok"
    assert payload["database"] is True
    assert payload["version"] == "0.1.0"
    assert payload["uptime"] >= 0
    eng.dispose()


def test_collect_health_degraded_on_corrupt(tmp_path):
    from pathlib import Path

    path = Path(tmp_path / "bad.sqlite")
    eng = create_sqlite_engine(str(path))
    initialize_schema(eng)
    eng.dispose()
    with path.open("r+b") as f:
        f.seek(0)
        f.write(b"\x00" * 64)
    eng2 = create_sqlite_engine(str(path))
    payload = collect_health(eng2, started_at=1000.0)
    assert payload["status"] == "degraded"
    assert payload["database"] is False
    eng2.dispose()


def test_is_ready(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "r.sqlite"))
    initialize_schema(eng)
    assert is_ready(eng) is True
    eng.dispose()
