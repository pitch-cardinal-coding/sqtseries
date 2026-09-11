"""Edge cases: pump dated points over a timeframe with mixed precision,
read back ranges and verify exact data."""

import pytest

from sqtseries.engine import create_sqlite_engine
from sqtseries.engine.db import Database
from sqtseries.engine.migrations import run_migrations
from sqtseries.engine.store import StorageEngine
from sqtseries.messaging.protocol import (
    ProtocolError,
    iso_to_ns,
    parse_ingest,
    parse_query,
)
from sqtseries.query import TimeSeriesDB


@pytest.fixture
def tsdb(tmp_path):
    db = str(tmp_path / "edge.sqlite")
    run_migrations(Database(db))
    eng = create_sqlite_engine(db)
    try:
        yield TimeSeriesDB(StorageEngine(eng))
    finally:
        eng.dispose()


def _pump_6h(tsdb, metric="edge.pump"):
    """One point every 10 min over 6h; value is the point index."""
    points = []
    for i in range(37):
        hh = i // 6
        mm = (i % 6) * 10
        ts = f"2026-09-11T{hh:02d}:{mm:02d}:00Z"
        msg = parse_ingest({"metric": metric, "value": float(i), "timestamp": ts})
        points.append((iso_to_ns(ts), float(i)))
        tsdb.store.insert_many([msg.to_rows()])
    return points


class TestPumpTimeframe:
    def test_full_range_read_back(self, tsdb):
        points = _pump_6h(tsdb)
        rows = tsdb.query(
            "edge.pump", start="2026-09-11T00:00:00Z", end="2026-09-11T06:00:00Z"
        )
        assert rows == points

    def test_subrange_slicing(self, tsdb):
        _pump_6h(tsdb)
        rows = tsdb.query(
            "edge.pump", start="2026-09-11T02:00:00Z", end="2026-09-11T03:00:00Z"
        )
        assert [v for _, v in rows] == [float(i) for i in range(12, 19)]
        assert all(
            iso_to_ns("2026-09-11T02:00:00Z") <= ts <= iso_to_ns("2026-09-11T03:00:00Z")
            for ts, _ in rows
        )

    def test_boundaries_inclusive(self, tsdb):
        _pump_6h(tsdb)
        ts = iso_to_ns("2026-09-11T02:00:00Z")
        rows = tsdb.query("edge.pump", start=ts, end=ts)
        assert rows == [(ts, 12.0)]

    def test_iso_matches_epoch_window(self, tsdb):
        points = _pump_6h(tsdb)
        s, e = iso_to_ns("2026-09-11T01:00:00Z"), iso_to_ns("2026-09-11T05:00:00Z")
        assert (
            tsdb.query(
                "edge.pump", start="2026-09-11T01:00:00Z", end="2026-09-11T05:00:00Z"
            )
            == tsdb.query("edge.pump", start=s, end=e)
            == [p for p in points if s <= p[0] <= e]
        )


class TestPrecision:
    FRACS = (
        ("2026-09-11T00:28:51.1Z", 100_000_000),
        ("2026-09-11T00:28:51.12Z", 120_000_000),
        ("2026-09-11T00:28:51.123Z", 123_000_000),
        ("2026-09-11T00:28:51.1234Z", 123_400_000),
        ("2026-09-11T00:28:51.12345Z", 123_450_000),
        ("2026-09-11T00:28:51.123456Z", 123_456_000),
        ("2026-09-11T00:28:51.1234567Z", 123_456_700),
        ("2026-09-11T00:28:51.12345678Z", 123_456_780),
        ("2026-09-11T00:28:51.123456789Z", 123_456_789),
        ("2026-09-11T00:28:51.9250386Z", 925_038_600),
    )

    def test_fraction_widths(self):
        base = iso_to_ns("2026-09-11T00:28:51Z")
        for text, delta in self.FRACS:
            assert iso_to_ns(text) - base == delta, text

    def test_pump_precision_ordering(self, tsdb):
        base = iso_to_ns("2026-09-11T00:28:51Z")
        for i, (text, _) in enumerate(self.FRACS):
            msg = parse_ingest(
                {"metric": "edge.prec", "value": float(i), "timestamp": text}
            )
            tsdb.store.insert_many([msg.to_rows()])
        rows = tsdb.query(
            "edge.prec", start="2026-09-11T00:28:51Z", end="2026-09-11T00:28:52Z"
        )
        assert [ts for ts, _ in rows] == sorted(ts for ts, _ in rows)
        assert rows[0][0] - base == 100_000_000
        assert rows[-1][0] - base == 925_038_600
        assert [v for _, v in rows] == [float(i) for i in range(len(self.FRACS))]

    def test_beyond_9_digits_truncates(self):
        assert iso_to_ns("2026-09-11T00:28:51.1234567899Z") == iso_to_ns(
            "2026-09-11T00:28:51.123456789Z"
        )

    def test_offset_and_naive_equal_zulu(self):
        assert iso_to_ns("2026-09-11T02:28:51+02:00") == iso_to_ns(
            "2026-09-11T00:28:51Z"
        )
        assert iso_to_ns("2026-09-11T00:28:51") == iso_to_ns("2026-09-11T00:28:51Z")
        assert iso_to_ns("2026-09-11T00:28:51.123456789") == iso_to_ns(
            "2026-09-11T00:28:51.123456789Z"
        )


class TestIsoRejection:
    def test_bad_strings_rejected(self):
        for bad in ("now", "", "2026-13-01T00:00:00Z", "2026-09-11"):
            if bad == "2026-09-11":
                assert iso_to_ns(bad) == iso_to_ns("2026-09-11T00:00:00Z")
            else:
                with pytest.raises(ProtocolError):
                    iso_to_ns(bad)

    def test_ingest_bad_iso_rejected(self):
        with pytest.raises(ProtocolError):
            parse_ingest({"metric": "m", "value": 1.0, "timestamp": "yesterday"})

    def test_query_bad_iso_rejected(self):
        with pytest.raises(ProtocolError):
            parse_query({"metric": "m", "start": "soon"})

    def test_skew_guard_applies_to_iso(self):
        with pytest.raises(ProtocolError):
            parse_ingest(
                {"metric": "m", "value": 1.0, "timestamp": "2000-01-01T00:00:00Z"},
                reject_client_timestamp_skew_s=300,
            )

    def test_gateway_bad_iso_is_400(self):
        from fastapi import HTTPException

        from sqtseries.gateway.routes import _to_ns

        with pytest.raises(HTTPException) as exc:
            _to_ns("not-a-time")
        assert exc.value.status_code == 400
