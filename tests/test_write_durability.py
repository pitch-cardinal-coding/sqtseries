"""Durability regression tests for the two write-path fixes.

1. A transient sqlite3.OperationalError must NOT lose a batch. The ingest
   socket is PULL with no acks: once drained, the producer can never re-send,
   so dropping on the first error destroys data the producer already handed
   over. Before the fix, ANY exception dropped the batch immediately.

2. Retrying must not duplicate rows. Safe only because Database.begin()
   rolls the failed transaction back AND commit() is the last thing that can
   fail, so an exception reaching the sink always means "not committed".

3. A deterministic (non-OperationalError) failure must still drop immediately —
   retrying it 4x would only delay the accounting.
"""

import sqlite3

from sqtseries.service import SINK_MAX_ATTEMPTS, Service


class FlakyStore:
    """Store whose insert_many fails N times with a transient error."""

    def __init__(self, exc: BaseException, fail_times: int):
        self.exc = exc
        self.fail_times = fail_times
        self.calls: list[int] = []

    def insert_many(self, rows, *, auto_create_partition: bool = True):
        self.calls.append(len(rows))
        if len(self.calls) <= self.fail_times:
            raise self.exc
        return len(rows)


class AlwaysFails:
    def __init__(self, exc: BaseException):
        self.exc = exc
        self.calls = 0

    def insert_many(self, rows, *, auto_create_partition: bool = True):
        self.calls += 1
        raise self.exc


def _svc() -> Service:
    s = Service.__new__(Service)  # no sockets, no start()
    s._persisted_count = 0
    s._dropped_count = 0
    s._sink_retry_count = 0
    s.store = None
    return s


ROWS = [("m", None, 1.0, 1_700_000_000_000_000_000)]


def test_transient_error_is_retried_and_batch_survives():
    svc = _svc()
    svc.store = FlakyStore(sqlite3.OperationalError("database is locked"), fail_times=2)
    svc._sink(ROWS)
    assert svc._persisted_count == 1, (
        "transient failure lost a batch that was retryable"
    )
    assert svc._dropped_count == 0
    assert svc._sink_retry_count == 2
    assert len(svc.store.calls) == 3


def test_retry_exhaustion_drops_and_counts():
    svc = _svc()
    svc.store = AlwaysFails(sqlite3.OperationalError("database is locked"))
    svc._sink(ROWS)
    assert svc._dropped_count == 1, "exhausted retries must be counted, not vanish"
    assert svc._persisted_count == 0
    assert svc.store.calls == SINK_MAX_ATTEMPTS, "must not retry forever"


def test_deterministic_failure_drops_without_retrying():
    svc = _svc()
    svc.store = AlwaysFails(ValueError("deterministic: bad row"))
    svc._sink(ROWS)
    assert svc._dropped_count == 1
    assert svc.store.calls == 1, "a deterministic failure must not be retried"
    assert svc._sink_retry_count == 0


def test_no_double_count_when_batch_eventually_persists():
    """A retried-then-succeeded batch counts once as persisted, never as dropped."""
    svc = _svc()
    svc.store = FlakyStore(sqlite3.OperationalError("disk I/O error"), fail_times=1)
    svc._sink(ROWS)
    assert (svc._persisted_count, svc._dropped_count) == (1, 0)
