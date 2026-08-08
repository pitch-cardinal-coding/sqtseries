"""Runtime state file tests."""

from sqtseries.runtime import RuntimeState


class TestRuntimeState:
    def test_write_read_roundtrip(self, tmp_path):
        rt = RuntimeState(str(tmp_path / "runtime.json"))
        db = str(tmp_path / "db.sqlite")
        rt.write(pid=1234, ports={"ingest": 12501}, db_path=db)
        data = rt.read()
        assert data is not None
        assert data["pid"] == 1234
        assert data["ports"]["ingest"] == 12501
        assert data["db_path"] == db

    def test_read_missing(self, tmp_path):
        rt = RuntimeState(str(tmp_path / "none.json"))
        assert rt.read() is None

    def test_remove(self, tmp_path):
        rt = RuntimeState(str(tmp_path / "rt.json"))
        rt.write(pid=1, ports={}, db_path="x")
        assert rt.exists
        rt.remove()
        assert not rt.exists

    def test_corrupt_file(self, tmp_path):
        p = tmp_path / "corrupt.json"
        p.write_text("{not json")
        rt = RuntimeState(str(p))
        assert rt.read() is None
