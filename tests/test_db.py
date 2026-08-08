"""Database/Connection/Result wrapper edge cases (raw sqlite3 layer)."""

import sqlite3

import pytest

from sqtseries.engine.db import Database, create_sqlite_engine


@pytest.fixture
def db(tmp_path):
    return create_sqlite_engine(str(tmp_path / "db.sqlite"))


def test_result_fetch_helpers(db):
    with db.connect() as conn:
        conn.executescript(
            "CREATE TABLE t(x INTEGER, y TEXT);"
            "INSERT INTO t VALUES (1, 'a'), (2, 'b');"
        )
        assert conn.execute("SELECT x, y FROM t ORDER BY x").first() == (1, "a")
        assert conn.execute("SELECT x FROM t WHERE x = 2").scalar() == 2
        assert conn.execute("SELECT x FROM t WHERE x = 99").first() is None
        assert conn.execute("SELECT x FROM t WHERE x = 99").scalar() is None
        res = conn.execute("SELECT x FROM t ORDER BY x")
        # __getitem__ fetches the next row and returns row[idx]
        # row 1, column 0
        assert res[0] == 1
        # advances: row 2, column 0
        assert res[0] == 2
        with pytest.raises(IndexError):
            # cursor exhausted
            res[5]


def test_result_iter(db):
    with db.connect() as conn:
        conn.executescript("CREATE TABLE t(x); INSERT INTO t VALUES (1),(2),(3)")
        assert list(conn.execute("SELECT x FROM t ORDER BY x")) == [(1,), (2,), (3,)]


def test_connection_commit_rollback_close(db):
    with db.connect() as conn:
        conn.executescript("CREATE TABLE t(x)")
        conn.commit()
        # no active txn; must not raise
        conn.rollback()
        conn.close()
    # connect() always opens a fresh connection
    db.execute("SELECT 1")


def test_connection_execute_error_closes_cursor(db):
    with db.connect() as conn:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("THIS IS NOT SQL")
        # connection still usable after an error
        assert conn.execute("SELECT 1").scalar() == 1


def test_executemany_error_path(db):
    with db.connect() as conn:
        conn.executescript("CREATE TABLE t(x INTEGER NOT NULL)")
        with pytest.raises(sqlite3.IntegrityError):
            conn.executemany("INSERT INTO t VALUES (?)", [(1,), (None,)])
        assert conn.execute("SELECT COUNT(*) FROM t").scalar() == 1


def test_connect_is_reader_writes_dont_persist(db):
    """connect() is a reader connection: DML is rolled back on close."""
    with db.connect() as conn:
        conn.executescript("CREATE TABLE t(x)")
        conn.execute("INSERT INTO t VALUES (1)")
    with db.connect() as conn:
        # DDL autocommitted, but the INSERT (implicit txn) was rolled back
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='t'").first()
        assert conn.execute("SELECT COUNT(*) FROM t").scalar() == 0


def test_begin_commits_on_success(db):
    with db.begin() as conn:
        conn.execute("CREATE TABLE t(x)")
        conn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    assert db.scalar("SELECT COUNT(*) FROM t") == 2


def test_begin_rolls_back_on_exception(db):
    with pytest.raises(RuntimeError), db.begin() as conn:
        conn.execute("CREATE TABLE t(x)")
        conn.execute("INSERT INTO t VALUES (1)")
        raise RuntimeError("boom")
    with db.connect() as conn:
        assert (
            conn.execute("SELECT name FROM sqlite_master WHERE name='t'").first()
            is None
        )


def test_database_execute_persists_writes(db):
    db.execute("CREATE TABLE t(x)")
    db.execute("INSERT INTO t VALUES (5)")
    assert db.scalar("SELECT x FROM t") == 5
    assert db.scalar("SELECT x FROM t WHERE x = 99") is None


def test_connection_executescript(db):
    with db.begin() as conn:
        conn.executescript("CREATE TABLE t(x)")
        conn.executescript("INSERT INTO t VALUES (1)")
    assert db.scalar("SELECT COUNT(*) FROM t") == 1


def test_connection_dbapi(db):
    with db.connect() as conn:
        assert conn.dbapi_connection is not None


def test_database_missing_parent_dir(tmp_path):
    path = str(tmp_path / "nested" / "deep" / "db.sqlite")
    db = Database(path)
    assert db.path == path
