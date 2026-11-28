"""Configuration loading and validation with explicit errors on missing/invalid settings."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from .errors import ConfigError

SUPPORTED_BACKENDS = frozenset({"sqlite", "memory"})
SUPPORTED_CONFLICT_POLICIES = frozenset({"quarantine_all"})
SUPPORTED_SCHEMA_VERSIONS = frozenset({"1.0.0"})


@dataclass(frozen=True)
class StorageConfig:
    backend: str = "sqlite"
    sqlite_path: str = ".runtime/security_master.db"
    postgres_dsn: Optional[str] = None


@dataclass(frozen=True)
class LicensingConfig:
    licensed_namespaces: FrozenSet[str] = field(
        default_factory=lambda: frozenset(
            {
                "OPENFIGI",
                "COMPOSITE_FIGI",
                "ISIN",
                "LEI",
                "MIC_TICKER",
                "TICKER",
                "VENDOR_INTERNAL",
            }
        )
    )
    restricted_namespaces: FrozenSet[str] = field(
        default_factory=lambda: frozenset({"CUSIP", "SEDOL"})
    )
    enforce_licensing: bool = True


@dataclass(frozen=True)
class ConflictResolutionConfig:
    policy: str = "quarantine_all"
    allow_same_source_correction: bool = True
    block_lookup_on_open_conflict: bool = True
    source_priority: Dict[str, int] = field(
        default_factory=lambda: {
            "EXCHANGE_OFFICIAL": 100,
            "REGULATORY_FILING": 95,
            "VENDOR_ALPHA": 80,
            "VENDOR_BETA": 80,
            "REVIEWER_WORKFLOW": 110,
        }
    )


@dataclass(frozen=True)
class QualityConfig:
    required_instrument_namespaces: Tuple[str, ...] = ("OPENFIGI", "ISIN")
    max_freshness_lag_seconds: int = 86400


@dataclass(frozen=True)
class ReproducibilityConfig:
    deterministic_seed: int = 20260101
    default_timezone: str = "UTC"


@dataclass(frozen=True)
class HttpConfig:
    host: str = "127.0.0.1"
    port: int = 8080


@dataclass(frozen=True)
class PlatformConfig:
    config_version: str = "1.0.0"
    schema_version: str = "1.0.0"
    environment: str = "prototype-offline"
    storage: StorageConfig = field(default_factory=StorageConfig)
    licensing: LicensingConfig = field(default_factory=LicensingConfig)
    conflict_resolution: ConflictResolutionConfig = field(default_factory=ConflictResolutionConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)
    reproducibility: ReproducibilityConfig = field(default_factory=ReproducibilityConfig)
    http: HttpConfig = field(default_factory=HttpConfig)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config_version": self.config_version,
            "schema_version": self.schema_version,
            "environment": self.environment,
            "storage": {
                "backend": self.storage.backend,
                "sqlite_path": self.storage.sqlite_path,
                "postgres_dsn": self.storage.postgres_dsn,
            },
            "licensing": {
                "licensed_namespaces": sorted(self.licensing.licensed_namespaces),
                "restricted_namespaces": sorted(self.licensing.restricted_namespaces),
                "enforce_licensing": self.licensing.enforce_licensing,
            },
            "conflict_resolution": {
                "policy": self.conflict_resolution.policy,
                "allow_same_source_correction": self.conflict_resolution.allow_same_source_correction,
                "block_lookup_on_open_conflict": self.conflict_resolution.block_lookup_on_open_conflict,
                "source_priority": dict(self.conflict_resolution.source_priority),
            },
            "quality": {
                "required_instrument_namespaces": list(self.quality.required_instrument_namespaces),
                "max_freshness_lag_seconds": self.quality.max_freshness_lag_seconds,
            },
            "reproducibility": {
                "deterministic_seed": self.reproducibility.deterministic_seed,
                "default_timezone": self.reproducibility.default_timezone,
            },
            "http": {
                "host": self.http.host,
                "port": self.http.port,
            },
        }


def _require_section(raw: Dict[str, Any], key: str) -> Dict[str, Any]:
    val = raw.get(key)
    if not isinstance(val, dict):
        raise ConfigError(
            f"Missing or invalid required configuration section: '{key}'",
            details={"missing_section": key},
        )
    return val


def load_config(path: Optional[Path] = None) -> PlatformConfig:
    """Load and validate a JSON configuration file.

    Raises ``ConfigError`` explicitly when the path is missing, unreadable,
    malformed JSON, or declares unsupported backends/policies.
    """
    if path is None:
        raise ConfigError(
            "Configuration file path is required; refusing silent fallback to defaults.",
            details={"hint": "Pass config/default.json or an explicit PlatformConfig."},
        )
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(
            f"Configuration file not found: {config_path}",
            details={"path": str(config_path)},
        )
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(
            f"Failed to parse configuration file: {config_path}",
            details={"path": str(config_path), "cause": str(exc)},
        ) from exc

    if not isinstance(raw, dict):
        raise ConfigError("Configuration root must be a JSON object.")

    schema_version = raw.get("schema_version")
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ConfigError(
            f"Unsupported schema_version '{schema_version}'.",
            details={
                "schema_version": schema_version,
                "supported": sorted(SUPPORTED_SCHEMA_VERSIONS),
            },
        )

    storage_raw = _require_section(raw, "storage")
    backend = storage_raw.get("backend")
    if backend == "postgres":
        raise ConfigError(
            "PostgreSQL live backend is not enabled in this offline prototype runtime; "
            "use backend='sqlite' or 'memory' (see config/postgres_schema.sql and ADR-001).",
            details={"backend": backend, "supported_runtime_backends": sorted(SUPPORTED_BACKENDS)},
        )
    if backend not in SUPPORTED_BACKENDS:
        raise ConfigError(
            f"Unsupported storage backend: '{backend}'",
            details={"backend": backend, "supported": sorted(SUPPORTED_BACKENDS)},
        )

    licensing_raw = _require_section(raw, "licensing")
    conflict_raw = _require_section(raw, "conflict_resolution")
    policy = conflict_raw.get("policy")
    if policy not in SUPPORTED_CONFLICT_POLICIES:
        raise ConfigError(
            f"Unsupported conflict resolution policy: '{policy}'. "
            "Ambiguous mappings must never be silently resolved.",
            details={"policy": policy, "supported": sorted(SUPPORTED_CONFLICT_POLICIES)},
        )

    quality_raw = _require_section(raw, "quality")
    repro_raw = _require_section(raw, "reproducibility")
    http_raw = _require_section(raw, "http")

    if repro_raw.get("default_timezone") != "UTC":
        raise ConfigError(
            "Only 'UTC' default_timezone is supported for bitemporal determinism.",
            details={"default_timezone": repro_raw.get("default_timezone")},
        )

    max_lag = quality_raw.get("max_freshness_lag_seconds")
    if not isinstance(max_lag, int) or max_lag <= 0:
        raise ConfigError(
            "quality.max_freshness_lag_seconds must be a positive integer.",
            details={"value": max_lag},
        )

    port = http_raw.get("port")
    if not isinstance(port, int) or not (1 <= port <= 65535):
        raise ConfigError(
            "http.port must be an integer between 1 and 65535.",
            details={"value": port},
        )

    return PlatformConfig(
        config_version=str(raw.get("config_version", "1.0.0")),
        schema_version=str(schema_version),
        environment=str(raw.get("environment", "prototype-offline")),
        storage=StorageConfig(
            backend=str(backend),
            sqlite_path=str(storage_raw.get("sqlite_path", ".runtime/security_master.db")),
            postgres_dsn=storage_raw.get("postgres_dsn"),
        ),
        licensing=LicensingConfig(
            licensed_namespaces=frozenset(licensing_raw.get("licensed_namespaces", [])),
            restricted_namespaces=frozenset(licensing_raw.get("restricted_namespaces", [])),
            enforce_licensing=bool(licensing_raw.get("enforce_licensing", True)),
        ),
        conflict_resolution=ConflictResolutionConfig(
            policy=str(policy),
            allow_same_source_correction=bool(
                conflict_raw.get("allow_same_source_correction", True)
            ),
            block_lookup_on_open_conflict=bool(
                conflict_raw.get("block_lookup_on_open_conflict", True)
            ),
            source_priority={
                str(k): int(v) for k, v in conflict_raw.get("source_priority", {}).items()
            },
        ),
        quality=QualityConfig(
            required_instrument_namespaces=tuple(
                str(ns) for ns in quality_raw.get("required_instrument_namespaces", [])
            ),
            max_freshness_lag_seconds=max_lag,
        ),
        reproducibility=ReproducibilityConfig(
            deterministic_seed=int(repro_raw.get("deterministic_seed", 20260101)),
            default_timezone="UTC",
        ),
        http=HttpConfig(
            host=str(http_raw.get("host", "127.0.0.1")),
            port=port,
        ),
    )
