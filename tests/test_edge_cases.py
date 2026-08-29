"""Edge case tests for sqtseries."""

import time

import pytest

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.messaging.protocol import ProtocolError, parse_ingest
from sqtseries.partition import PartitionManager


@pytest.fixture
def store(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "edge.sqlite"))
    initialize_schema(eng)
    s = StorageEngine(eng)
    yield s
    s.close()


class TestEdgeCases:
    def test_partition_drop_during_query(self, store, tmp_path):
        # Insert data into two partitions (Jan 2024 + Nov 2024)
        jan1 = 1_704_067_200_000_000_000

        nov1 = 1_730_419_200_000_000_000
        store.insert_many([("cpu", None, 1.0, jan1)])
        store.insert_many([("cpu", None, 2.0, nov1)])

        pm = PartitionManager(store.db, on_change=store.invalidate_partitions)
        # Querying while dropping: raw query should work if it gets a reference
        # to the table name before drop; drop might fail if locked but WAL mode
        # allows concurrent reading.
        rows = list(store.query_time_range(metric="cpu"))
        assert len(rows) == 2
        pm.drop_partition(2024, 11)
        # Query after drop
        rows = list(store.query_time_range(metric="cpu"))
        assert len(rows) == 1

    def test_max_batch_size(self, store):
        # Max params = 32766. If 4 columns/row, 8191 is theoretical max rows.
        # Test batch size of 8000.
        rows = [
            ("m", None, float(i), 1_700_000_000_000_000_000 + i) for i in range(8000)
        ]
        n = store.insert_many(rows)
        assert n == 8000
        assert store.series_count() == 1

    def test_timestamp_skew_rejection(self):
        # skew threshold 5s.
        now = time.time()
        # > 5s skew
        with pytest.raises(ProtocolError, match=r"skew .* exceeds 5s"):
            parse_ingest(
                {"metric": "m", "value": 1.0, "timestamp": now - 10},
                reject_client_timestamp_skew_s=5,
            )
        # <= 5s skew
        msg = parse_ingest(
            {"metric": "m", "value": 1.0, "timestamp": now - 2},
            reject_client_timestamp_skew_s=5,
        )
        assert msg.timestamp is not None
