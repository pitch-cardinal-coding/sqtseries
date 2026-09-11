"""ISO-8601 timestamp input (beside epoch)."""

import pytest

from sqtseries.messaging.protocol import (
    ProtocolError,
    iso_to_ns,
    parse_ingest,
    parse_query,
)


class TestIsoToNs:
    def test_zulu_seconds(self):
        assert iso_to_ns("2026-09-11T14:04:00Z") == iso_to_ns(
            "2026-09-11T14:04:00+00:00"
        )

    def test_millis(self):
        base = iso_to_ns("2026-09-11T00:28:51Z")
        assert iso_to_ns("2026-09-11T00:28:51.740Z") - base == 740_000_000

    def test_micros_six_digits(self):
        base = iso_to_ns("2026-09-11T00:28:51Z")
        assert iso_to_ns("2026-09-11T00:28:51.123456Z") - base == 123_456_000

    def test_nanos_nine_digits(self):
        base = iso_to_ns("2026-09-11T00:28:51Z")
        assert iso_to_ns("2026-09-11T00:28:51.123456789Z") - base == 123_456_789

    def test_seven_digits_like_return(self):
        base = iso_to_ns("2026-09-11T00:28:51Z")
        assert iso_to_ns("2026-09-11T00:28:51.9250386Z") - base == 925_038_600

    def test_naive_is_utc(self):
        assert iso_to_ns("2026-09-11T14:04:00") == iso_to_ns("2026-09-11T14:04:00Z")

    def test_offset(self):
        assert iso_to_ns("2026-09-11T16:04:00+02:00") == iso_to_ns(
            "2026-09-11T14:04:00Z"
        )

    def test_invalid(self):
        with pytest.raises(ProtocolError):
            iso_to_ns("now")
        with pytest.raises(ProtocolError):
            iso_to_ns("")


class TestParseIngestIso:
    def test_iso_accepted_exact_ns(self):
        msg = parse_ingest(
            {"metric": "m", "value": 1.0, "timestamp": "2026-09-11T00:28:51.123456789Z"}
        )
        _, _, _, ts_ns = msg.to_rows()
        assert ts_ns == iso_to_ns("2026-09-11T00:28:51.123456789Z")

    def test_epoch_still_works(self):
        msg = parse_ingest({"metric": "m", "value": 1.0, "timestamp": 1700000000.5})
        _, _, _, ts_ns = msg.to_rows()
        assert ts_ns == int(1700000000.5 * 1_000_000_000)


class TestParseQueryIso:
    def test_iso_window(self):
        q = parse_query(
            {
                "metric": "m",
                "start": "2026-09-11T14:00:00Z",
                "end": "2026-09-11T15:00:00Z",
            }
        )
        assert q["start"] == iso_to_ns("2026-09-11T14:00:00Z")
        assert q["end"] == iso_to_ns("2026-09-11T15:00:00Z")

    def test_int_still_works(self):
        q = parse_query({"metric": "m", "start": 100, "end": 200})
        assert (q["start"], q["end"]) == (100, 200)


class TestBuilderIsoRoundTrip:
    def test_insert_query_iso(self, tmp_path):
        from sqtseries.engine import create_sqlite_engine
        from sqtseries.engine.db import Database
        from sqtseries.engine.migrations import run_migrations
        from sqtseries.query import TimeSeriesDB

        db = str(tmp_path / "iso.sqlite")
        run_migrations(Database(db))
        eng = create_sqlite_engine(db)
        try:
            from sqtseries.engine.store import StorageEngine

            tsdb = TimeSeriesDB(StorageEngine(eng))
            ts_ns = iso_to_ns("2026-09-11T00:28:51.123456789Z")
            tsdb.store.insert_many([("m.iso", None, 42.0, ts_ns)])
            rows = tsdb.query(
                "m.iso", start="2026-09-11T00:00:00Z", end="2026-09-11T01:00:00Z"
            )
            assert rows == [(ts_ns, 42.0)]
        finally:
            eng.dispose()


class TestGatewayToNs:
    def test_numeric_string_and_iso(self):
        from sqtseries.gateway.routes import _to_ns

        assert _to_ns("1700000000.5") == int(1700000000.5 * 1_000_000_000)
        assert _to_ns("2026-09-11T00:28:51.740Z") == iso_to_ns(
            "2026-09-11T00:28:51.740Z"
        )
