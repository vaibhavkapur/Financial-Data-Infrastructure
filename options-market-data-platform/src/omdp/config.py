"""Typed configuration loading and validation for OMDP."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Tuple


class ConfigurationError(ValueError):
    """Raised when configuration is missing, malformed, or out of valid bounds."""


@dataclass(frozen=True)
class StorageConfig:
    backend: str = "sqlite"
    sqlite_path: str = "data/omdp.sqlite3"


@dataclass(frozen=True)
class ValidationConfig:
    max_quote_staleness_seconds: int = 300
    max_ingestion_delay_seconds: int = 60
    max_underlying_skew_seconds: int = 60
    max_future_clock_skew_seconds: int = 5
    allow_locked_quotes: bool = True
    exclude_stale_quotes_from_iv: bool = True
    supported_exercise_styles: Tuple[str, ...] = ("EUROPEAN",)
    supported_schema_versions: Tuple[str, ...] = ("1.0.0",)


@dataclass(frozen=True)
class PricingConfig:
    day_count_convention: str = "ACT/365F"
    default_expiry_utc_hour: int = 20
    default_expiry_utc_minute: int = 0
    iv_min_vol: float = 1e-6
    iv_max_vol: float = 5.0
    iv_price_tolerance: float = 1e-10
    iv_vol_tolerance: float = 1e-9
    iv_max_iterations: int = 100


@dataclass(frozen=True)
class PlatformConfig:
    config_version: str = "1.0.0"
    schema_version: str = "1.0.0"
    storage: StorageConfig = field(default_factory=StorageConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    pricing: PricingConfig = field(default_factory=PricingConfig)
    random_seed: int = 20260619

    def to_canonical_dict(self) -> Dict[str, Any]:
        return {
            "config_version": self.config_version,
            "schema_version": self.schema_version,
            "storage": {
                "backend": self.storage.backend,
                "sqlite_path": self.storage.sqlite_path,
            },
            "validation": {
                "max_quote_staleness_seconds": self.validation.max_quote_staleness_seconds,
                "max_ingestion_delay_seconds": self.validation.max_ingestion_delay_seconds,
                "max_underlying_skew_seconds": self.validation.max_underlying_skew_seconds,
                "max_future_clock_skew_seconds": self.validation.max_future_clock_skew_seconds,
                "allow_locked_quotes": self.validation.allow_locked_quotes,
                "exclude_stale_quotes_from_iv": self.validation.exclude_stale_quotes_from_iv,
                "supported_exercise_styles": list(self.validation.supported_exercise_styles),
                "supported_schema_versions": list(self.validation.supported_schema_versions),
            },
            "pricing": {
                "day_count_convention": self.pricing.day_count_convention,
                "default_expiry_utc_hour": self.pricing.default_expiry_utc_hour,
                "default_expiry_utc_minute": self.pricing.default_expiry_utc_minute,
                "iv_min_vol": self.pricing.iv_min_vol,
                "iv_max_vol": self.pricing.iv_max_vol,
                "iv_price_tolerance": self.pricing.iv_price_tolerance,
                "iv_vol_tolerance": self.pricing.iv_vol_tolerance,
                "iv_max_iterations": self.pricing.iv_max_iterations,
            },
            "reproducibility": {
                "random_seed": self.random_seed,
            },
        }

    def config_hash(self) -> str:
        payload = json.dumps(self.to_canonical_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _require_key(mapping: Dict[str, Any], key: str, section: str) -> Any:
    if key not in mapping:
        raise ConfigurationError(f"Missing required config key '{section}.{key}'")
    return mapping[key]


def load_config(path: str | Path) -> PlatformConfig:
    """Load and validate a PlatformConfig JSON file."""
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise ConfigurationError(f"Configuration file not found: {cfg_path}")
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"Invalid JSON in configuration file {cfg_path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigurationError("Root configuration must be a JSON object")

    return parse_config_dict(raw)


def parse_config_dict(raw: Dict[str, Any]) -> PlatformConfig:
    """Validate and construct PlatformConfig from a dictionary."""
    config_version = str(_require_key(raw, "config_version", "root"))
    schema_version = str(_require_key(raw, "schema_version", "root"))

    storage_raw = _require_key(raw, "storage", "root")
    val_raw = _require_key(raw, "validation", "root")
    pricing_raw = _require_key(raw, "pricing", "root")
    repro_raw = _require_key(raw, "reproducibility", "root")

    if not isinstance(storage_raw, dict):
        raise ConfigurationError("'storage' section must be an object")
    if not isinstance(val_raw, dict):
        raise ConfigurationError("'validation' section must be an object")
    if not isinstance(pricing_raw, dict):
        raise ConfigurationError("'pricing' section must be an object")
    if not isinstance(repro_raw, dict):
        raise ConfigurationError("'reproducibility' section must be an object")

    backend = str(_require_key(storage_raw, "backend", "storage")).lower()
    if backend not in ("sqlite",):
        raise ConfigurationError(
            f"Unsupported storage backend '{backend}'. Supported runtime backends: ['sqlite']. "
            "PostgreSQL/ClickHouse DDL templates are documented in config/ but not runnable in offline core."
        )
    sqlite_path = str(_require_key(storage_raw, "sqlite_path", "storage"))
    if not sqlite_path.strip():
        raise ConfigurationError("'storage.sqlite_path' must be a non-empty string")

    max_stale = int(_require_key(val_raw, "max_quote_staleness_seconds", "validation"))
    max_delay = int(_require_key(val_raw, "max_ingestion_delay_seconds", "validation"))
    max_skew = int(_require_key(val_raw, "max_underlying_skew_seconds", "validation"))
    max_future = int(_require_key(val_raw, "max_future_clock_skew_seconds", "validation"))
    if min(max_stale, max_delay, max_skew, max_future) < 0:
        raise ConfigurationError("Validation time thresholds must be non-negative")

    styles = tuple(str(s).upper() for s in _require_key(val_raw, "supported_exercise_styles", "validation"))
    schema_versions = tuple(str(v) for v in _require_key(val_raw, "supported_schema_versions", "validation"))
    if not styles:
        raise ConfigurationError("'validation.supported_exercise_styles' must be non-empty")
    if schema_version not in schema_versions:
        raise ConfigurationError(
            f"Active schema_version '{schema_version}' is not in supported_schema_versions {list(schema_versions)}"
        )

    day_count = str(_require_key(pricing_raw, "day_count_convention", "pricing")).upper()
    if day_count not in ("ACT/365F",):
        raise ConfigurationError(f"Unsupported day_count_convention '{day_count}'. Supported: ['ACT/365F']")

    iv_min = float(_require_key(pricing_raw, "iv_min_vol", "pricing"))
    iv_max = float(_require_key(pricing_raw, "iv_max_vol", "pricing"))
    iv_ptol = float(_require_key(pricing_raw, "iv_price_tolerance", "pricing"))
    iv_vtol = float(_require_key(pricing_raw, "iv_vol_tolerance", "pricing"))
    iv_iters = int(_require_key(pricing_raw, "iv_max_iterations", "pricing"))
    if not (0 < iv_min < iv_max):
        raise ConfigurationError("pricing.iv_min_vol must be > 0 and < pricing.iv_max_vol")
    if iv_ptol <= 0 or iv_vtol <= 0 or iv_iters <= 0:
        raise ConfigurationError("Pricing tolerances and max iterations must be positive")

    exp_hour = int(_require_key(pricing_raw, "default_expiry_utc_hour", "pricing"))
    exp_min = int(_require_key(pricing_raw, "default_expiry_utc_minute", "pricing"))
    if not (0 <= exp_hour <= 23 and 0 <= exp_min <= 59):
        raise ConfigurationError("default_expiry_utc_hour/minute out of range")

    seed = int(_require_key(repro_raw, "random_seed", "reproducibility"))

    return PlatformConfig(
        config_version=config_version,
        schema_version=schema_version,
        storage=StorageConfig(backend=backend, sqlite_path=sqlite_path),
        validation=ValidationConfig(
            max_quote_staleness_seconds=max_stale,
            max_ingestion_delay_seconds=max_delay,
            max_underlying_skew_seconds=max_skew,
            max_future_clock_skew_seconds=max_future,
            allow_locked_quotes=bool(_require_key(val_raw, "allow_locked_quotes", "validation")),
            exclude_stale_quotes_from_iv=bool(_require_key(val_raw, "exclude_stale_quotes_from_iv", "validation")),
            supported_exercise_styles=styles,
            supported_schema_versions=schema_versions,
        ),
        pricing=PricingConfig(
            day_count_convention=day_count,
            default_expiry_utc_hour=exp_hour,
            default_expiry_utc_minute=exp_min,
            iv_min_vol=iv_min,
            iv_max_vol=iv_max,
            iv_price_tolerance=iv_ptol,
            iv_vol_tolerance=iv_vtol,
            iv_max_iterations=iv_iters,
        ),
        random_seed=seed,
    )
