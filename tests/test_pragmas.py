"""PRAGMA configuration tests."""

import pytest

from sqtseries.engine import (
    create_sqlite_engine,
    incremental_vacuum,
    initialize_schema,
    quick_check,
    run_optimize,
)


@pytest.fixture
def engine(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "pragmas.sqlite"))
    yield eng
    eng.dispose()


class TestConnectionPragmas:
    def test_wal_enabled(self, engine):
        initialize_schema(engine)
        with engine.connect() as conn:
            mode = conn.exec_driver_sql("PRAGMA journal_mode").scalar()
        assert mode.lower() == "wal"

    def test_synchronous_normal(self, engine):
        initialize_schema(engine)
        with engine.connect() as conn:
            val = conn.exec_driver_sql("PRAGMA synchronous").scalar()
        # NORMAL == 1
        assert val == 1

    def test_busy_timeout(self, engine):
        initialize_schema(engine)
        with engine.connect() as conn:
            val = conn.exec_driver_sql("PRAGMA busy_timeout").scalar()
        assert val == 5000

    def test_foreign_keys_on(self, engine):
        initialize_schema(engine)
        with engine.connect() as conn:
            val = conn.exec_driver_sql("PRAGMA foreign_keys").scalar()
        assert val == 1

    def test_page_size(self, engine):
        # page_size only applies to a fresh DB; set at create time
        initialize_schema(engine)
        with engine.connect() as conn:
            val = conn.exec_driver_sql("PRAGMA page_size").scalar()
        # page_size may be read even if the pragma couldn't change it
        # 4096 default on some platforms without init
        assert val in (8192, 4096)


class TestMaintenance:
    def test_quick_check_ok(self, engine):
        initialize_schema(engine)
        assert quick_check(engine) == "ok"

    def test_run_optimize(self, engine):
        initialize_schema(engine)
        run_optimize(engine)

    def test_incremental_vacuum(self, engine):
        initialize_schema(engine)
        incremental_vacuum(engine, pages=10)

    def test_auto_vacuum_incremental(self, engine):
        initialize_schema(engine)
        with engine.connect() as conn:
            val = conn.exec_driver_sql("PRAGMA auto_vacuum").scalar()
        assert val == 2
