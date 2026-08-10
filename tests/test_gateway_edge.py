"""HTTP gateway validation edge cases (400/422 responses)."""

import pytest
from fastapi.testclient import TestClient

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.gateway import create_app

pytest.importorskip("fastapi")


@pytest.fixture
def client(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "g.sqlite"))
    initialize_schema(eng)
    store = StorageEngine(eng)
    app = create_app(store=store)
    return TestClient(app)


class TestWriteValidation:
    def test_empty_metric(self, client):
        r = client.post("/api/v1/write", json={"metric": "", "value": 1.0})
        assert r.status_code == 400

    def test_missing_metric(self, client):
        r = client.post("/api/v1/write", json={"value": 1.0})
        assert r.status_code == 400

    def test_non_numeric_value(self, client):
        r = client.post("/api/v1/write", json={"metric": "a", "value": "x"})
        assert r.status_code == 400

    def test_bool_value(self, client):
        r = client.post("/api/v1/write", json={"metric": "a", "value": True})
        assert r.status_code == 400

    def test_non_dict_tags(self, client):
        r = client.post(
            "/api/v1/write", json={"metric": "a", "value": 1.0, "tags": "x"}
        )
        assert r.status_code == 400

    def test_bad_tag_value_type(self, client):
        r = client.post(
            "/api/v1/write", json={"metric": "a", "value": 1.0, "tags": {"k": 5}}
        )
        assert r.status_code == 400

    def test_invalid_timestamp(self, client):
        r = client.post(
            "/api/v1/write", json={"metric": "a", "value": 1.0, "timestamp": "now"}
        )
        assert r.status_code == 400

    def test_non_object_body(self, client):
        r = client.post("/api/v1/write", json="nope")
        assert r.status_code == 400

    def test_array_with_invalid_item(self, client):
        r = client.post(
            "/api/v1/write", json=[{"metric": "a", "value": 1.0}, {"metric": ""}]
        )
        assert r.status_code == 400

    def test_huge_integer_value_400(self, client):
        # stdlib JSON accepts integers beyond 2^63, but the value can't be
        # stored as a float — must be a 400, never a 500 (OverflowError)
        r = client.post("/api/v1/write", json={"metric": "a", "value": 10**400})
        assert r.status_code == 400

    def test_huge_integer_value_in_batch_400(self, client):
        r = client.post(
            "/api/v1/write",
            json=[{"metric": "a", "value": 1.0}, {"metric": "a", "value": 10**400}],
        )
        assert r.status_code == 400
        # nothing persisted (batch rejected as a whole)
        r = client.get("/api/v1/stats")
        assert r.json()["series"] == 0

    def test_huge_integer_timestamp_400(self, client):
        # a timestamp beyond float range must be a 400, never a 500
        # (float(10**400) used to raise OverflowError before validation)
        r = client.post(
            "/api/v1/write", json={"metric": "a", "value": 1.0, "timestamp": 10**400}
        )
        assert r.status_code == 400

    def test_float_huge_integer_timestamp_400(self, client):
        # fits in a float but overflows the signed 64-bit ns storage bound
        r = client.post(
            "/api/v1/write", json={"metric": "a", "value": 1.0, "timestamp": 10**100}
        )
        assert r.status_code == 400

    def test_negative_huge_integer_timestamp_400(self, client):
        r = client.post(
            "/api/v1/write", json={"metric": "a", "value": 1.0, "timestamp": -(10**400)}
        )
        assert r.status_code == 400

    def test_int64_boundary_timestamp_rejected_400(self, client):
        # 2**63 seconds is far beyond the int64-ns storage range
        r = client.post(
            "/api/v1/write", json={"metric": "a", "value": 1.0, "timestamp": 2**63}
        )
        assert r.status_code == 400

    def test_huge_integer_timestamp_in_batch_400(self, client):
        r = client.post(
            "/api/v1/write",
            json=[
                {"metric": "a", "value": 1.0},
                {"metric": "a", "value": 1.0, "timestamp": 10**400},
            ],
        )
        assert r.status_code == 400
        # nothing persisted (batch rejected as a whole)
        r = client.get("/api/v1/stats")
        assert r.json()["series"] == 0

    def test_batch_no_timestamps_single_transaction(self, client):
        """Timestamp-less batch rows must not collide on the PK."""
        r = client.post(
            "/api/v1/write",
            json=[{"metric": "a", "value": 1.0}, {"metric": "a", "value": 2.0}],
        )
        assert r.status_code == 200
        assert r.json()["written"] == 2
        r = client.get("/api/v1/read", params={"metric": "a", "aggregation": "count"})
        assert r.json()["data"][0]["value"] == 2


class TestReadValidation:
    def test_bad_aggregation_400(self, client):
        r = client.get(
            "/api/v1/read", params={"metric": "cpu.usage", "aggregation": "bogus"}
        )
        assert r.status_code == 400

    def test_invalid_order_422(self, client):
        r = client.get(
            "/api/v1/read", params={"metric": "cpu.usage", "order": "sideways"}
        )
        assert r.status_code == 422

    def test_missing_metric_422(self, client):
        r = client.get("/api/v1/read")
        assert r.status_code == 422

    def test_limit_over_max_422(self, client):
        r = client.get("/api/v1/read", params={"metric": "a", "limit": 999999999})
        assert r.status_code == 422


class TestAggregateValidation:
    def test_bad_func_400(self, client):
        r = client.get("/api/v1/aggregate", params={"metric": "a", "funcs": "avg,nope"})
        assert r.status_code == 400


class TestSkewGuard:
    def test_http_write_rejects_far_past_timestamp(self, tmp_path):
        """HTTP writes enforce the same clock-skew guard as ZMQ ingest."""
        import time

        from sqtseries.config import IngestionSettings
        from sqtseries.engine import (
            StorageEngine,
            create_sqlite_engine,
            initialize_schema,
        )

        eng = create_sqlite_engine(str(tmp_path / "g.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        app = create_app(
            store=store,
            ingestion=IngestionSettings(reject_client_timestamp_skew_s=5),
        )
        c = TestClient(app)
        r = c.post(
            "/api/v1/write",
            json={"metric": "m", "value": 1.0, "timestamp": time.time() - 100_000},
        )
        assert r.status_code == 400
        assert "skew" in r.text
        # a near-now timestamp passes
        r = c.post(
            "/api/v1/write",
            json={"metric": "m", "value": 1.0, "timestamp": time.time()},
        )
        assert r.status_code == 200


class TestStatsHealth:
    def test_stats(self, client):
        r = client.get("/api/v1/stats")
        assert r.status_code == 200
        assert "series" in r.json()

    def test_health(self, client):
        r = client.get("/api/v1/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"
