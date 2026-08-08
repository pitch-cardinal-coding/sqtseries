"""Config edge cases: file formats, validation bounds, error paths."""

import pytest

from sqtseries.config import Settings, validate_settings


@pytest.fixture
def sample_toml(tmp_path):
    p = tmp_path / "config.toml"
    p.write_text("[ingestion]\nport = 13001\n")
    return str(p)


class TestFileFormats:
    def test_unsupported_extension_raises(self, tmp_path):
        p = tmp_path / "config.ini"
        p.write_text("[x]")
        with pytest.raises(ValueError, match="Unsupported config"):
            Settings.load(str(p))

    def test_json_non_mapping_raises(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text("[1, 2, 3]")
        with pytest.raises(ValueError, match="must be an object"):
            Settings.load(str(p))

    def test_json_invalid_raises(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text("{not json")
        with pytest.raises(ValueError, match="invalid JSON"):
            Settings.load(str(p))

    def test_yaml_requires_pyyaml(self, tmp_path):
        p = tmp_path / "config.yaml"
        p.write_text("ingestion:\n  port: 13001\n")
        import importlib.util

        if importlib.util.find_spec("yaml") is None:
            with pytest.raises(ValueError, match="pyyaml"):
                Settings.load(str(p))
        else:
            s = Settings.load(str(p))
            assert s.ingestion.port == 13001


class TestValidation:
    def test_batch_size_over_limit(self):
        errs = validate_settings(Settings(database={"batch_size": 10000}))
        assert any("batch_size" in e for e in errs)

    def test_hwm_below_one(self):
        errs = validate_settings(Settings(ingestion={"hwm": 0}))
        assert any("hwm" in e for e in errs)

    def test_pending_max_below_one(self):
        errs = validate_settings(Settings(ingestion={"pending_max": 0}))
        assert any("pending_max" in e for e in errs)

    def test_invalid_rollup_interval(self):
        errs = validate_settings(Settings(rollup={"interval": "nope"}))
        assert any("rollup" in e for e in errs)

    def test_invalid_retention_ttl(self):
        errs = validate_settings(Settings(retention={"default_ttl": "nope"}))
        assert any("retention" in e for e in errs)

    def test_invalid_maintenance_interval(self):
        errs = validate_settings(Settings(maintenance={"analyze_interval": "x"}))
        assert any("maintenance" in e for e in errs)


def test_db_path_expanded(sample_toml):
    s = Settings(database={"path": "~/data/x.sqlite"})
    assert s.db_path_expanded().endswith("data/x.sqlite")
