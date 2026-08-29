"""Protocol serialization/validation tests."""

import pytest

from sqtseries.messaging.protocol import (
    IngestMessage,
    ProtocolError,
    dumps,
    loads,
    parse_admin,
    parse_ingest,
    parse_query,
)


class TestFraming:
    def test_roundtrip(self):
        payload = {"metric": "cpu", "value": 0.5}
        assert loads(dumps(payload)) == payload

    def test_invalid_json(self):
        with pytest.raises(ProtocolError):
            loads(b"{not json")


class TestParseIngest:
    def test_valid(self):
        msg = parse_ingest(
            {"metric": "cpu.usage", "value": 0.72, "tags": {"host": "web1"}}
        )
        assert msg.metric == "cpu.usage"
        assert msg.value == 0.72
        assert msg.tags == {"host": "web1"}

    def test_missing_metric(self):
        with pytest.raises(ProtocolError):
            parse_ingest({"value": 1.0})

    def test_empty_metric(self):
        with pytest.raises(ProtocolError):
            parse_ingest({"metric": "", "value": 1.0})

    def test_bad_value(self):
        with pytest.raises(ProtocolError):
            parse_ingest({"metric": "m", "value": "high"})

    def test_bad_tags(self):
        with pytest.raises(ProtocolError):
            parse_ingest({"metric": "m", "value": 1.0, "tags": {"host": 42}})

    def test_bad_timestamp(self):
        with pytest.raises(ProtocolError):
            parse_ingest({"metric": "m", "value": 1.0, "timestamp": "now"})

    def test_to_rows_uses_server_time(self):
        msg = IngestMessage(metric="m", value=1.0)
        _, _, _, ts_ns = msg.to_rows()
        import time

        assert abs(ts_ns / 1e9 - time.time()) < 2

    def test_skew_rejected_when_configured(self):
        import time

        # > 5s skew rejected
        bad_ts = time.time() - 10
        with pytest.raises(ProtocolError):
            parse_ingest(
                {"metric": "m", "value": 1.0, "timestamp": bad_ts},
                reject_client_timestamp_skew_s=5,
            )
        # future skew also rejected
        with pytest.raises(ProtocolError):
            parse_ingest(
                {"metric": "m", "value": 1.0, "timestamp": time.time() + 1000},
                reject_client_timestamp_skew_s=5,
            )
        # sane ts passes
        msg = parse_ingest(
            {"metric": "m", "value": 1.0, "timestamp": time.time()},
            reject_client_timestamp_skew_s=5,
        )
        assert msg.timestamp is not None


class TestParseQuery:
    def test_valid(self):
        q = parse_query({"type": "query", "metric": "cpu", "start": 1, "end": 2})

        assert q["metric"] == "cpu"

    def test_type_required_value(self):
        with pytest.raises(ProtocolError):
            parse_query({"type": "write", "metric": "cpu"})

    def test_missing_metric(self):
        with pytest.raises(ProtocolError):
            parse_query({"start": 1})

    def test_bad_start(self):
        with pytest.raises(ProtocolError):
            parse_query({"metric": "cpu", "start": "1"})


class TestParseAdmin:
    def test_valid(self):
        assert parse_admin({"cmd": "health"}) == "health"

    def test_missing_cmd(self):
        with pytest.raises(ProtocolError):
            parse_admin({})
