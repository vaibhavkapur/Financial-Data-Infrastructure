"""Versioned runtime configuration for the corporate actions engine."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Union

from corp_actions.errors import ConfigurationError

SUPPORTED_ROUNDING_MODES = frozenset(
    {"ROUND_HALF_EVEN", "ROUND_HALF_UP", "ROUND_DOWN", "ROUND_UP"}
)
SUPPORTED_DIVIDEND_BASES = frozenset({"post_split", "pre_split"})
SUPPORTED_FX_POLICIES = frozenset({"ex_date", "pay_date"})
SUPPORTED_STORAGE_BACKENDS = frozenset({"sqlite", "postgres"})


@dataclass(frozen=True)
class EngineConfig:
    """Immutable, versioned configuration controlling math, policies and storage."""

    config_version: str = "1.0.0"
    schema_version: str = "1.0.0"
    engine_version: str = "1.0.0"
    random_seed: int = 42
    decimal_precision: int = 28
    rounding_mode: str = "ROUND_HALF_EVEN"
    price_quantize: Decimal = Decimal("0.000001")
    cash_quantize: Decimal = Decimal("0.01")
    quantity_quantize: Decimal = Decimal("0.000001")
    factor_quantize: Decimal = Decimal("0.0000000001")
    reconciliation_tolerance: Decimal = Decimal("0.000001")
    same_day_dividend_basis: str = "post_split"
    fx_conversion_date_policy: str = "ex_date"
    freshness_sla_seconds: int = 86400
    storage_backend: str = "sqlite"
    sqlite_path: str = ":memory:"
    strict_unsupported_actions: bool = False

    def __post_init__(self) -> None:
        if self.decimal_precision < 12 or self.decimal_precision > 64:
            raise ConfigurationError(
                "decimal_precision must be between 12 and 64",
                details={"decimal_precision": self.decimal_precision},
            )
        if self.rounding_mode not in SUPPORTED_ROUNDING_MODES:
            raise ConfigurationError(
                f"Unsupported rounding_mode: {self.rounding_mode}",
                details={"supported": sorted(SUPPORTED_ROUNDING_MODES)},
            )
        if self.same_day_dividend_basis not in SUPPORTED_DIVIDEND_BASES:
            raise ConfigurationError(
                f"Unsupported same_day_dividend_basis: {self.same_day_dividend_basis}",
                details={"supported": sorted(SUPPORTED_DIVIDEND_BASES)},
            )
        if self.fx_conversion_date_policy not in SUPPORTED_FX_POLICIES:
            raise ConfigurationError(
                f"Unsupported fx_conversion_date_policy: {self.fx_conversion_date_policy}",
                details={"supported": sorted(SUPPORTED_FX_POLICIES)},
            )
        if self.storage_backend not in SUPPORTED_STORAGE_BACKENDS:
            raise ConfigurationError(
                f"Unsupported storage_backend: {self.storage_backend}",
                details={"supported": sorted(SUPPORTED_STORAGE_BACKENDS)},
            )
        if self.reconciliation_tolerance < Decimal("0"):
            raise ConfigurationError("reconciliation_tolerance must be non-negative")
        if self.freshness_sla_seconds <= 0:
            raise ConfigurationError("freshness_sla_seconds must be positive")

    @classmethod
    def from_file(cls, path: Union[str, Path]) -> "EngineConfig":
        cfg_path = Path(path)
        if not cfg_path.exists():
            raise ConfigurationError(
                f"Configuration file not found: {cfg_path}",
                details={"path": str(cfg_path)},
            )
        try:
            raw = json.loads(cfg_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ConfigurationError(
                f"Invalid JSON in configuration file: {cfg_path}",
                details={"error": str(exc)},
            ) from exc
        if not isinstance(raw, dict):
            raise ConfigurationError("Configuration file root must be a JSON object")
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EngineConfig":
        decimal_fields = {
            "price_quantize",
            "cash_quantize",
            "quantity_quantize",
            "factor_quantize",
            "reconciliation_tolerance",
        }
        parsed: Dict[str, Any] = {}
        valid_fields = set(cls.__dataclass_fields__.keys())
        unknown = set(data.keys()) - valid_fields
        if unknown:
            raise ConfigurationError(
                f"Unknown configuration keys: {sorted(unknown)}",
                details={"unknown_keys": sorted(unknown)},
            )
        for key, value in data.items():
            if key in decimal_fields:
                try:
                    parsed[key] = Decimal(str(value))
                except Exception as exc:
                    raise ConfigurationError(
                        f"Invalid Decimal value for {key}: {value!r}"
                    ) from exc
            else:
                parsed[key] = value
        return cls(**parsed)

    def to_dict(self) -> Dict[str, Any]:
        raw = asdict(self)
        for key, value in list(raw.items()):
            if isinstance(value, Decimal):
                raw[key] = str(value)
        return raw
