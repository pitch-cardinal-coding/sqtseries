"""Regression tests for bugs found in the QA audit (2026-08-07)."""

import os
import signal
import sqlite3
import subprocess
import sys
import time

import pytest

from sqtseries.config import Settings
from sqtseries.engine import (
    StorageEngine,
    create_sqlite_engine,
    initialize_schema,
)
from sqtseries.query import TimeSeriesDB


@pytest.fixture
def store(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "r.sqlite"))
    initialize_schema(eng)
    s = StorageEngine(eng)
    yield s
    s.close()


def test_sigterm_exits_zero(tmp_path):
    """SIGTERM must produce exit code 0 (not 1), so systemd Restart=on-failure

    doesn't restart a deliberately stopped service."""
    from conftest import free_port

    db = tmp_path / "db.sqlite"

    env = dict(
        os.environ,
        SQT_SERIES_DATABASE__PATH=str(db),
        SQT_SERIES_INGESTION__PORT=str(free_port()),
        SQT_SERIES_QUERY__PORT=str(free_port()),
        SQT_SERIES_STREAMING__PORT=str(free_port()),
        SQT_SERIES_ADMIN__PORT=str(free_port()),
        SQT_SERIES_HTTP__PORT=str(free_port()),
        SQT_SERIES_STATS__PORT=str(free_port()),
        SQT_SERIES_PORTS__AUTO_DETECT="false",
    )
    p = subprocess.Popen(
        [sys.executable, "-m", "sqtseries", "run"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    time.sleep(2)
    p.send_signal(signal.SIGTERM)
    out, _ = p.communicate(timeout=20)
    assert p.returncode == 0, out
    assert not (tmp_path / "runtime.json").exists()


class TestNonFinite:
    def test_nan_value_rejected(self, store):

        from sqtseries.messaging.protocol import ProtocolError, parse_ingest

        with pytest.raises(ProtocolError, match="finite"):
            parse_ingest({"metric": "m", "value": float("nan")})
        with pytest.raises(ProtocolError, match="finite"):
            parse_ingest({"metric": "m", "value": float("inf")})


class TestDescOrderAndLimit:
    def test_desc_is_globally_descending(self, store):
        from datetime import UTC, datetime

        def ns(dt):
            return int(dt.timestamp() * 1_000_000_000)

        store.insert_many(
            [
                ("m", None, 1.0, ns(datetime(2026, 1, 5, tzinfo=UTC))),
                ("m", None, 2.0, ns(datetime(2026, 2, 5, tzinfo=UTC))),
            ]
        )
        rows = list(store.query_time_range(metric="m", order="desc"))
        # newest (Feb) first, globally
        assert [v for _, v in rows] == [2.0, 1.0]

    def test_desc_limit_returns_newest(self, store):
        from datetime import UTC, datetime

        def ns(dt):
            return int(dt.timestamp() * 1_000_000_000)

        store.insert_many(
            [
                ("m", None, float(i), ns(datetime(2026, 1, i + 1, tzinfo=UTC)))
                for i in range(3)
            ]
            + [
                ("m", None, float(100 + i), ns(datetime(2026, 2, i + 1, tzinfo=UTC)))
                for i in range(3)
            ]
        )
        rows = list(store.query_time_range(metric="m", order="desc", limit=3))
        # newest 3, not oldest
        assert [v for _, v in rows] == [102.0, 101.0, 100.0]


class TestDroppedPartitionResilience:
    def test_query_after_drop_no_error(self, store, tmp_path):
        from datetime import UTC, datetime

        from sqtseries.partition import PartitionManager

        def ns(dt):
            return int(dt.timestamp() * 1_000_000_000)

        store.insert_many(
            [
                ("m", None, 1.0, ns(datetime(2026, 1, 5, tzinfo=UTC))),
                ("m", None, 2.0, ns(datetime(2026, 2, 5, tzinfo=UTC))),
            ]
        )
        store._parts_cache = [
            t
            for t in store.db.get_table_names()
            if t.startswith("measurements")
            # warm a stale cache
        ]
        pm = PartitionManager(store.db, on_change=store.invalidate_partitions)

        pm.drop_partition(2026, 1)
        # querying the full range must not raise "no such table"
        rows = list(store.query_time_range(metric="m"))
        assert len(rows) == 1


class TestAtomicInsert:
    def test_failed_insert_leaves_no_orphan_series(self, tmp_path):
        from datetime import UTC, datetime

        eng = create_sqlite_engine(str(tmp_path / "a.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)

        ts = int(datetime(2020, 5, 1, tzinfo=UTC).timestamp() * 1_000_000_000)

        with pytest.raises(sqlite3.OperationalError):
            store.insert_many(
                [("newmetric", None, 1.0, ts)],
                auto_create_partition=False,
            )
        # no orphan series row
        assert store.series_count() == 0
        store.close()


class TestRollupFreshness:
    """A write arriving within the clock-skew window must never land in an

    already-rolled hour, so the rollup fast path stays exact."""

    def test_late_write_within_skew_is_served_correctly(self, tmp_path):
        from datetime import UTC, datetime

        from sqtseries.partition import rollup_new_hours, rollup_watermark

        def ns(dt):
            return int(dt.timestamp() * 1_000_000_000)

        eng = create_sqlite_engine(str(tmp_path / "rf.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)

        HOUR = 3_600_000_000_000

        base = ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        store.insert_many(
            [("m", None, 1.0, base), ("m", None, 2.0, base + HOUR)]
            # hours 10 and 11
        )
        # 1h window for unambiguous timing
        skew_s = 3600.0

        # first rollup at 12:00 with skew 1h -> only hours < 11:00 are safe

        rollup_new_hours(
            eng, now_ns=ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC)), skew_s=skew_s
        )
        wm = rollup_watermark(eng)
        # hour 11 NOT rolled
        assert wm == ns(datetime(2026, 1, 1, 11, 0, tzinfo=UTC))
        # hour 11 is NOT rolled yet -> a late write to it (arriving 11:59,
        # lateness 29min < 1h skew) lands in an un-rolled hour
        store.insert_many(
            [("m", None, 5.0, base + HOUR + 30 * 60_000_000_000)]
            # 11:30
        )

        # second rollup at 13:00 -> hours < 12:00 now safe; hour 11 rolled,
        # including the 11:30 late write
        rollup_new_hours(
            eng, now_ns=ns(datetime(2026, 1, 1, 13, 0, tzinfo=UTC)), skew_s=skew_s
        )

        tsdb = TimeSeriesDB(store)

        end_ns = ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC))
        stats = tsdb.aggregate("m", end=end_ns, funcs=["count", "sum"])
        # 10:00, 11:00, 11:30
        assert stats["count"] == 3
        assert stats["sum"] == pytest.approx(8.0)
        store.close()

    def test_skew_cutoff_excludes_fresh_hours(self, tmp_path):
        """With skew, hours that haven't been safely closed are not rolled."""

        from datetime import UTC, datetime

        from sqtseries.partition import rollup_new_hours, rollup_watermark

        def ns(dt):
            return int(dt.timestamp() * 1_000_000_000)

        eng = create_sqlite_engine(str(tmp_path / "rf2.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)

        base = ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        store.insert_many([("m", None, 1.0, base)])
        # hour 11 is current
        now = ns(datetime(2026, 1, 1, 11, 30, tzinfo=UTC))
        rollup_new_hours(eng, now_ns=now, skew_s=300.0)
        wm = rollup_watermark(eng)
        # hour 11 (in progress, or fresh-ended within skew) must NOT be rolled

        assert wm <= ns(datetime(2026, 1, 1, 11, 0, tzinfo=UTC))
        store.close()


class TestBatchWriteAtomicity:
    def test_invalid_item_rejects_whole_batch(self, tmp_path):
        """A bad item in /write array must not persist the valid ones."""

        from fastapi.testclient import TestClient

        from sqtseries.gateway import create_app

        eng = create_sqlite_engine(str(tmp_path / "b.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        c = TestClient(create_app(store=store))
        r = c.post(
            "/api/v1/write",
            json=[
                {"metric": "m", "value": 1.0},
                # invalid
                {"metric": "", "value": 2.0},
            ],
        )
        assert r.status_code == 400
        # nothing persisted
        assert store.series_count() == 0


class TestAggregateAnchor:
    def test_anchor_consistent_asc_desc(self, tmp_path):
        from datetime import UTC, datetime

        from sqtseries.partition import ensure_rollup_table, rollup_new_hours

        def ns(dt):
            return int(dt.timestamp() * 1_000_000_000)

        eng = create_sqlite_engine(str(tmp_path / "a.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)

        base = ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))

        HOUR = 3_600_000_000_000
        store.insert_many([("m", None, 1.0, base), ("m", None, 2.0, base + HOUR)])

        ensure_rollup_table(eng)
        rollup_new_hours(eng, now_ns=ns(datetime(2026, 1, 1, 13, 0, tzinfo=UTC)))

        tsdb = TimeSeriesDB(store)
        # raw path: anchor is the OLDEST sample regardless of order
        asc = tsdb.query("m", aggregation="sum", order="asc")

        desc = tsdb.query("m", aggregation="sum", order="desc")
        assert asc[0][0] == base
        # same anchor, not the newest
        assert desc[0][0] == base
        assert asc[0][1] == pytest.approx(3.0)


class TestConfigEnvEdge:
    def test_empty_env_var_treated_unset(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SQT_SERIES_HTTP__PORT", "")
        s = Settings.load(None)
        # default, not a validation crash
        assert s.http.port == 12505

    def test_case_variant_env_wins(self, tmp_path, monkeypatch):
        cfg = tmp_path / "c.toml"
        cfg.write_text("[database]\npath = '/file/value.sqlite'\n")
        monkeypatch.setenv("SQT_SERIES_CONFIG_FILE", str(cfg))
        monkeypatch.setenv("sqt_series_database__path", "/env/value.sqlite")

        s = Settings.load(None)
        # env wins
        assert s.db_path_expanded() == "/env/value.sqlite"

    def test_config_file_with_config_file_key(self, tmp_path, monkeypatch):
        cfg = tmp_path / "c.toml"
        cfg.write_text("config_file = 'nested.toml'\n[ingestion]\nport = 15002\n")
        # must not TypeError
        s = Settings.load(str(cfg))
        assert s.ingestion.port == 15002


class TestPortAllocatorReserved:
    def test_alloc_skips_reserved(self, tmp_path, monkeypatch):
        from sqtseries.ports import PortAllocator

        def fake_available(port):
            # everything looks free
            return True

        monkeypatch.setattr(
            PortAllocator, "_is_available", staticmethod(fake_available)
        )
        allocator = PortAllocator(start=12500, end=12700, reserved={12500, 12502})

        assert allocator.alloc(auto_detect=True) == 12501


class TestLoggingFile:
    def test_file_expands_home_and_creates_dir(self, tmp_path, monkeypatch):
        from sqtseries.config import LoggingSettings
        from sqtseries.logging import configure_logging

        target = tmp_path / "nested" / "logs" / "app.log"
        configure_logging(
            LoggingSettings(level="INFO", format="json", file=str(target))
        )
        # parent dir created
        assert target.exists()


class TestHealthChecksDb:
    def test_health_degraded_on_corrupt(self, tmp_path):
        from pathlib import Path

        from fastapi.testclient import TestClient

        from sqtseries.gateway import create_app

        path = Path(tmp_path / "bad.sqlite")
        eng = create_sqlite_engine(str(path))
        initialize_schema(eng)
        eng.dispose()
        with path.open("r+b") as f:
            f.seek(0)
            f.write(b"\x00" * 64)
        eng2 = create_sqlite_engine(str(path))
        app = create_app(store=StorageEngine(eng2))
        r = TestClient(app).get("/api/v1/health")
        assert r.json()["status"] == "degraded"
        eng2.dispose()


def test_no_update_statements_in_service():
    """Append-only contract: no UPDATE may exist in shipped service code.

    User data is written once via StorageEngine.insert_many (CUD Create)
    and removed only whole-partition by TTL retention (CUD Delete).
    Rollup maintenance uses INSERT OR REPLACE on derived tables, never
    UPDATE on user rows. Comments are ignored (only string literals —
    where SQL lives — are searched)."""
    import io
    import re
    import tokenize
    from pathlib import Path

    src = Path(__file__).resolve().parent.parent / "src" / "sqtseries"
    hits = []
    for path in sorted(src.rglob("*.py")):
        tokens = tokenize.generate_tokens(io.StringIO(path.read_text()).readline)
        hits.extend(
            f"{path.name}:{tok.start[0]}"
            for tok in tokens
            if tok.type == tokenize.STRING
            and re.search(r"(?<![A-Za-z_])UPDATE(?![A-Za-z_])", tok.string)
        )
    assert hits == [], f"UPDATE statements found in service code: {hits}"
