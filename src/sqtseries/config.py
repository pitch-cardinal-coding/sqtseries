"""Configuration management for sqtseries.

Supports TOML, YAML, JSON files and environment variables.
Environment variables take precedence over file values.
"""

import os
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PORT_RANGE_START = 12500
PORT_RANGE_END = 12700


class DatabaseSettings(BaseModel):
    """SQLite storage configuration."""

    path: str = "~/.sqtseries/data/db.sqlite"
    page_size: int = 8192
    cache_size: int = -64000
    mmap_size: int = 268435456
    busy_timeout: int = 5000
    journal_size_limit: int = 67108864
    threads: int = 4
    batch_size: int = 2000
    flush_interval: float = 1.0


class IngestionSettings(BaseModel):
    """Ingestion pipeline configuration."""

    port: int = 12501
    hwm: int = 10000
    pending_max: int = 100
    max_message_size: int = 50 * 1024 * 1024
    reject_client_timestamp_skew_s: float = 300.0


class QuerySettings(BaseModel):
    """Query engine configuration."""

    port: int = 12502
    timeout_s: float = 30.0


class StreamingSettings(BaseModel):
    """Live subscription configuration."""

    port: int = 12503
    linger_seconds: float = 30.0


class StatsSettings(BaseModel):
    """Stats PUB socket configuration (connection/subscription events)."""

    enabled: bool = True
    port: int = 12506


class AdminSettings(BaseModel):
    """Admin command socket configuration."""

    port: int = 12504


class HttpSettings(BaseModel):
    """HTTP gateway configuration."""

    port: int = 12505
    host: str = "127.0.0.1"
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])
    rate_limit_per_minute: int = 600


class LoggingSettings(BaseModel):
    """Structured logging configuration."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    format: Literal["json", "console"] = "console"
    file: str | None = None


class RetentionSettings(BaseModel):
    """Retention policy configuration."""

    enabled: bool = True
    default_ttl: str = "30d"
    check_interval: str = "1h"
    # Only "month" is supported
    partition_interval: str = "month"


class RollupSettings(BaseModel):
    """Hourly rollup pre-aggregation configuration.

    ``interval`` is how often the background task rolls newly-completed hours
    into ``rollup_hourly`` (a duration like ``5m`` or ``1h``).
    """

    enabled: bool = True
    interval: str = "5m"


class MaintenanceSettings(BaseModel):
    """Periodic maintenance configuration.

    ``analyze_interval`` is how often ``ANALYZE`` refreshes query-planner
    statistics (a duration like ``1h`` or ``6h``).
    """

    enabled: bool = True
    analyze_interval: str = "1h"


class BackupSettings(BaseModel):
    """Backup configuration."""

    enabled: bool = True
    interval: str = "24h"
    path: str = "~/.sqtseries/backups/"


class PortsSettings(BaseModel):
    """Port allocation and auto-detection."""

    port_range_start: int = PORT_RANGE_START
    port_range_end: int = PORT_RANGE_END
    auto_detect: bool = True
    port_offset: int = 0


class Settings(BaseSettings):
    """Top-level settings. Env prefix: SQT_SERIES_.

    Load order (lowest to highest precedence):
    1. Defaults
    2. Config file (TOML/YAML/JSON) — ``SQT_SERIES_CONFIG_FILE`` or ``--config``
    3. Environment variables (``SQT_SERIES_*``)
    """

    model_config = SettingsConfigDict(
        env_prefix="SQT_SERIES_",
        env_nested_delimiter="__",
        # VAR= should mean "unset", not a validation error
        env_ignore_empty=True,
        extra="ignore",
        case_sensitive=False,
    )

    config_file: str | None = None

    database: DatabaseSettings = DatabaseSettings()
    ingestion: IngestionSettings = IngestionSettings()
    query: QuerySettings = QuerySettings()
    streaming: StreamingSettings = StreamingSettings()
    stats: StatsSettings = StatsSettings()
    admin: AdminSettings = AdminSettings()
    http: HttpSettings = HttpSettings()
    logging: LoggingSettings = LoggingSettings()
    maintenance: MaintenanceSettings = MaintenanceSettings()
    retention: RetentionSettings = RetentionSettings()
    rollup: RollupSettings = RollupSettings()
    backup: BackupSettings = BackupSettings()
    ports: PortsSettings = PortsSettings()

    @field_validator("config_file", mode="before")
    @classmethod
    def _expand_config_file(cls, v: Any) -> Any:
        if isinstance(v, str):
            return str(Path(v).expanduser())
        return v

    @classmethod
    def load(cls, config_file: str | None = None) -> Settings:
        """Load settings from file (if given) + environment + defaults.

        Precedence (lowest to highest):
        1. Defaults
        2. Config file values (TOML/YAML/JSON)
        3. Environment variables (SQT_SERIES_*)
        4. Explicit overrides passed as keyword args (CLI)

        Implementation note: pydantic-settings gives init kwargs precedence over
        env, so we cannot pass file values as init args (that would let a file
        shadow an env var). Instead we instantiate twice: first with env applied
        (no init kwargs), then merge only the file keys that the environment did
        **not** set, and rebuild with those as init kwargs. Env values always win.
        """
        if config_file is None:
            env_config_file = os.environ.get("SQT_SERIES_CONFIG_FILE")
            if env_config_file:
                config_file = str(Path(env_config_file).expanduser())

        file_values: dict[str, Any] = {}
        if config_file:
            path = Path(config_file)
            if not path.exists():
                raise FileNotFoundError(f"Config file not found: {config_file}")
            file_values = _read_config_file(path)

        # Instance with env applied (env source active, no init overrides).
        env_settings = cls(config_file=config_file)

        # Determine which leaf config paths the environment actually set, by
        # scanning the env prefix. File values for those paths are skipped.
        env_paths = _enviro_leaf_paths()

        # Only the file values whose paths were NOT set by the environment survive.
        file_override: dict[str, Any] = {}
        for path, value in _iter_leaf_paths(file_values):
            # A top-level "config_file" key would collide as a kwarg
            if path == ("config_file",):
                continue
            if path not in env_paths:
                _set_path(file_override, path, value)

        if not file_override:
            return env_settings

        return cls(**file_override, config_file=config_file)

    def db_path_expanded(self) -> str:
        return str(Path(self.database.path).expanduser())


def _enviro_leaf_paths() -> set[tuple[str, ...]]:
    """Return the set of leaf config paths set via ``SQT_SERIES_*`` env vars.

    Converts ``SQT_SERIES_DATABASE__BATCH_SIZE`` -> ``("database", "batch_size")``.
    Also normalizes the root ``SQT_SERIES_CONFIG_FILE`` special key out.
    Matching is case-insensitive, mirroring pydantic-settings
    ``case_sensitive=False`` (so ``sqt_series_database__path`` is honored).
    """
    paths: set[tuple[str, ...]] = set()
    prefix = "SQT_SERIES_"
    for key in os.environ:
        upper = key.upper()
        if not upper.startswith(prefix):
            continue
        rest = upper[len(prefix) :]
        if not rest or rest == "CONFIG_FILE":
            continue
        parts = tuple(p.lower() for p in rest.split("__") if p)
        if parts:
            paths.add(parts)
    return paths


def _iter_leaf_paths(
    mapping: dict[str, Any], prefix: tuple[str, ...] = ()
) -> Iterator[tuple[tuple[str, ...], Any]]:
    """Yield (path, value) for every leaf in a nested mapping."""
    for key, value in mapping.items():
        path = (*prefix, key)
        if isinstance(value, dict) and value:
            yield from _iter_leaf_paths(value, path)
        else:
            yield path, value


def _set_path(target: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    """Set target[path] = value, creating intermediate dicts."""
    node = target
    for part in path[:-1]:
        node = node.setdefault(part, {})
    node[path[-1]] = value


def _read_config_file(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    if suffix == ".toml":
        with path.open("rb") as f:
            return tomllib.load(f)
    if suffix in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore[import-not-found]
        except ImportError:
            raise ValueError(
                "YAML config requires 'pyyaml' — install it or use TOML/JSON"
            ) from None
        with path.open() as f:
            data = yaml.safe_load(f)
            if data is None:
                return {}
            if not isinstance(data, dict):
                raise ValueError("YAML config must be a mapping at top level")
            return data
    if suffix == ".json":
        import orjson

        raw = path.read_bytes()
        try:
            data = orjson.loads(raw)
        except (orjson.JSONDecodeError, ValueError) as exc:
            raise ValueError(f"invalid JSON config: {exc}") from None
        if not isinstance(data, dict):
            raise ValueError("JSON config must be an object at top level")
        return data
    raise ValueError(
        f"Unsupported config file format: {suffix} (use .toml, .yaml, .json)"
    )


def validate_settings(settings: Settings) -> list[str]:
    """Return a list of configuration errors (empty when valid)."""
    errors: list[str] = []

    for name, port in (
        ("ingestion.port", settings.ingestion.port),
        ("query.port", settings.query.port),
        ("streaming.port", settings.streaming.port),
        ("admin.port", settings.admin.port),
        ("http.port", settings.http.port),
    ):
        if not (1 <= port <= 65535):
            errors.append(f"{name} must be 1-65535, got {port}")

    ports = [
        settings.ingestion.port,
        settings.query.port,
        settings.streaming.port,
        settings.admin.port,
        settings.http.port,
    ]
    if len(set(ports)) != len(ports):
        errors.append(f"ports must be unique, got {ports}")

    if settings.ingestion.hwm < 1:
        errors.append(f"ingestion.hwm must be >= 1, got {settings.ingestion.hwm}")
    if settings.ingestion.pending_max < 1:
        errors.append(
            f"ingestion.pending_max must be >= 1, got {settings.ingestion.pending_max}"
        )
    if settings.database.batch_size < 1:
        errors.append(
            f"database.batch_size must be >= 1, got {settings.database.batch_size}"
        )
    if settings.database.batch_size > 8191:
        errors.append(
            f"database.batch_size {settings.database.batch_size} exceeds "
            "SQLITE_MAX_VARIABLE_NUMBER safe limit of 8191"
        )

    if settings.rollup.enabled:
        try:
            from .query.agg import parse_interval

            parse_interval(settings.rollup.interval)
        except ValueError as exc:
            errors.append(f"rollup.interval invalid: {exc}")

    if settings.maintenance.enabled:
        try:
            from .query.agg import parse_interval

            parse_interval(settings.maintenance.analyze_interval)
        except ValueError as exc:
            errors.append(f"maintenance.analyze_interval invalid: {exc}")

    if settings.retention.enabled:
        from .partition.retention import parse_ttl

        try:
            parse_ttl(settings.retention.default_ttl)
            parse_ttl(settings.retention.check_interval)
        except ValueError as exc:
            errors.append(f"retention invalid: {exc}")

    if settings.backup.enabled:
        try:
            from .query.agg import parse_interval

            parse_interval(settings.backup.interval)
        except ValueError as exc:
            errors.append(f"backup.interval invalid: {exc}")

    return errors


__all__ = [
    "AdminSettings",
    "BackupSettings",
    "DatabaseSettings",
    "HttpSettings",
    "IngestionSettings",
    "LoggingSettings",
    "MaintenanceSettings",
    "PortsSettings",
    "QuerySettings",
    "RetentionSettings",
    "RollupSettings",
    "Settings",
    "StreamingSettings",
    "validate_settings",
]
