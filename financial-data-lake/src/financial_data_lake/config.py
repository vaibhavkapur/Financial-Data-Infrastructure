"""Versioned configuration models and loader (strictly no secrets)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StorageConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    raw_prefix: str = "raw"
    normalized_prefix: str = "normalized"
    curated_prefix: str = "curated"
    snapshots_prefix: str = "snapshots"
    quarantine_prefix: str = "quarantine"
    parquet_compression: Literal["snappy", "zstd", "gzip", "none"] = "snappy"
    checksum_algorithm: Literal["sha256"] = "sha256"


class QualityConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_null_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    require_utc_timestamps: bool = True
    require_availability_ge_event: bool = True
    max_price_spike_ratio: float = Field(default=5.0, gt=1.0)
    reject_on_critical_failure: bool = True


class RetentionPolicyConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    raw_retention_days: int = Field(default=2555, ge=1)
    normalized_retention_days: int = Field(default=2555, ge=1)
    curated_retention_days: int = Field(default=2555, ge=1)
    snapshot_retention_days: int = Field(default=1825, ge=1)
    allow_destructive_history_rewrite: bool = False
    archival_mode: Literal["cold_tier_immutable"] = "cold_tier_immutable"

    @field_validator("allow_destructive_history_rewrite")
    @classmethod
    def forbid_destructive_rewrite(cls, v: bool) -> bool:
        if v:
            raise ValueError("Destructive history rewrite is explicitly prohibited.")
        return v


class ExecutionControlsConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_execution_enabled: bool = False
    allow_redistribution: bool = False

    @field_validator("order_execution_enabled")
    @classmethod
    def keep_execution_disabled(cls, v: bool) -> bool:
        if v:
            raise ValueError("Order execution must remain disabled in the data lake.")
        return v

    @field_validator("allow_redistribution")
    @classmethod
    def forbid_redistribution(cls, v: bool) -> bool:
        if v:
            raise ValueError("Redistribution of market data outside entitlement is prohibited.")
        return v


class LakeConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    config_version: str = "1.0.0"
    lake_name: str = "saat-financial-data-lake-proto"
    environment: str = "local-prototype"
    storage: StorageConfig = Field(default_factory=StorageConfig)
    quality: QualityConfig = Field(default_factory=QualityConfig)
    retention: RetentionPolicyConfig = Field(default_factory=RetentionPolicyConfig)
    execution_controls: ExecutionControlsConfig = Field(default_factory=ExecutionControlsConfig)

    @classmethod
    def load(cls, path: Path | str) -> LakeConfig:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.model_validate(raw)
