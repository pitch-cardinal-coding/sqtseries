"""Tests for configuration loading and precedence."""

import pytest

from sqtseries.config import Settings, validate_settings


class TestDefaultSettings:
    def test_defaults_apply(self):
        s = Settings()
        assert s.database.path == "~/.sqtseries/data/db.sqlite"
        assert s.database.batch_size == 2000
        assert s.ingestion.port == 12501
        assert s.query.port == 12502
        assert s.streaming.port == 12503
        assert s.admin.port == 12504
        assert s.http.port == 12505

    def test_version_accessible(self):
        import sqtseries

        assert sqtseries.__version__ == "0.1.0"


class TestFileLoading:
    def test_load_from_toml(self, sample_toml):
        s = Settings.load(str(sample_toml))
        assert s.ingestion.port == 14101
        assert s.database.batch_size == 500
        # untouched values keep defaults
        assert s.logging.level == "INFO"
        assert s.http.host == "127.0.0.1"

    def test_load_from_json(self, sample_json):
        s = Settings.load(str(sample_json))
        assert s.ingestion.port == 14201

    def test_load_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            Settings.load("/nonexistent/config.toml")

    def test_load_default(self):
        s = Settings.load()
        assert s.ingestion.port == 12501

    def test_path_expands_home(self):
        s = Settings()
        expanded = s.db_path_expanded()
        assert expanded == s.database.path.replace(
            "~", __import__("os").path.expanduser("~")
        )


class TestEnvPrecedence:
    def test_env_overrides_defaults(self, monkeypatch):
        monkeypatch.setenv("SQT_SERIES_INGESTION__PORT", "13001")
        s = Settings()
        assert s.ingestion.port == 13001

    def test_env_overrides_file(self, monkeypatch, sample_toml):
        monkeypatch.setenv("SQT_SERIES_INGESTION__PORT", "13005")
        s = Settings.load(str(sample_toml))
        # env beats file
        assert s.ingestion.port == 13005

    def test_env_nested_value(self, monkeypatch):
        monkeypatch.setenv("SQT_SERIES_DATABASE__BATCH_SIZE", "900")
        s = Settings()
        assert s.database.batch_size == 900

    def test_env_leaf_excluded_from_file_override(self, monkeypatch, sample_toml):
        # The file sets ingestion.port=14101; env sets it to 13005.
        monkeypatch.setenv("SQT_SERIES_INGESTION__PORT", "13005")
        s = Settings.load(str(sample_toml))
        assert s.ingestion.port == 13005
        # The file's non-conflicting value still applies:
        assert s.database.batch_size == 500


class TestValidation:
    def test_valid_settings_have_no_errors(self):
        s = Settings()
        assert validate_settings(s) == []

    def test_invalid_port_range(self):
        s = Settings(ingestion={"port": 0})
        errors = validate_settings(s)
        assert any("ingestion.port" in e for e in errors)

    def test_duplicate_ports(self):
        s = Settings(ingestion={"port": 12505})
        errors = validate_settings(s)
        assert any("unique" in e for e in errors)

    def test_zero_rate_limit_rejected(self):
        s = Settings(http={"rate_limit_per_minute": 0})
        errors = validate_settings(s)
        assert any("rate_limit_per_minute" in e for e in errors)

    def test_zero_max_inflight_rejected(self):
        s = Settings(query={"max_inflight": 0})
        errors = validate_settings(s)
        assert any("max_inflight" in e for e in errors)
