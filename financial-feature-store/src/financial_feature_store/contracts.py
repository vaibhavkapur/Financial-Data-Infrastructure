"""Canonical domain contracts, UTC time semantics, and version fingerprints."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

from financial_feature_store.errors import TimestampContractError

SEMVER_PATTERN = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,63}$")


def ensure_utc(dt: datetime, field_name: str = "timestamp") -> datetime:
    """Validate that a datetime is timezone-aware and normalize it to UTC."""
    if not isinstance(dt, datetime):
        raise TimestampContractError(
            f"{field_name} must be a datetime instance",
            details={"field": field_name, "received_type": type(dt).__name__},
        )
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise TimestampContractError(
            f"{field_name} must be timezone-aware UTC (naive datetimes are forbidden)",
            details={"field": field_name, "value": dt.isoformat()},
        )
    return dt.astimezone(UTC)


def canonical_sha256(payload: Any) -> str:
    """Deterministic SHA-256 hex digest of a JSON-serializable object."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def parse_semver(version: str) -> tuple[int, int, int]:
    """Parse a strict MAJOR.MINOR.PATCH string."""
    match = SEMVER_PATTERN.match(version)
    if not match:
        raise ValueError(f"Version '{version}' must follow strict SemVer MAJOR.MINOR.PATCH")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


class ValueType(StrEnum):
    FLOAT64 = "float64"
    INT64 = "int64"
    BOOL = "bool"
    STRING = "string"


class NullPolicy(StrEnum):
    """Explicit null handling policies. Future/backward fill is intentionally unsupported."""

    KEEP_NULL = "keep_null"
    REJECT = "reject"
    DEFAULT_VALUE = "default_value"


class StalePolicy(StrEnum):
    """Policy applied when an online feature's age exceeds its configured TTL."""

    REJECT = "reject"
    WARN = "warn"
    FALLBACK_NULL = "fallback_null"


class FreshnessStatus(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    MISSING = "missing"


class LifecycleStage(StrEnum):
    DRAFT = "draft"
    STAGING = "staging"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class EntityKey(BaseModel):
    model_config = ConfigDict(frozen=True)

    entity_type: str = Field(..., min_length=2, max_length=64)
    entity_id: str = Field(..., min_length=1, max_length=128)


class SourceDatasetContract(BaseModel):
    """Provenance and entitlement metadata for an upstream data source."""

    model_config = ConfigDict(frozen=True)

    source_id: str = Field(..., min_length=2, max_length=128)
    dataset_name: str = Field(..., min_length=2, max_length=128)
    source_version: str = Field(..., min_length=1, max_length=64)
    schema_hash: str = Field(..., min_length=8, max_length=128)
    entitlement: str = Field(default="synthetic-internal", min_length=2, max_length=128)
    license: str = Field(default="internal-synthetic", min_length=2, max_length=128)
    provenance_uri: str = Field(..., min_length=1, max_length=512)
    currency: str | None = Field(default=None, max_length=16)


class TransformSpec(BaseModel):
    """Deterministic causal transformation specification."""

    model_config = ConfigDict(frozen=True)

    transform_name: str = Field(..., min_length=2, max_length=128)
    source_columns: tuple[str, ...] = Field(..., min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    lookback_seconds: float = Field(default=0.0, ge=0.0)
    causal_guarantee: Literal["strict_causal"] = "strict_causal"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def transform_hash(self) -> str:
        return canonical_sha256(
            {
                "transform_name": self.transform_name,
                "source_columns": list(self.source_columns),
                "params": self.params,
                "lookback_seconds": self.lookback_seconds,
                "causal_guarantee": self.causal_guarantee,
            }
        )[:16]


class QualityRuleSpec(BaseModel):
    """Data quality thresholds that gate promotion to ACTIVE / online serving."""

    model_config = ConfigDict(frozen=True)

    min_row_count: int = Field(default=1, ge=1)
    max_null_ratio: float = Field(default=0.05, ge=0.0, le=1.0)
    min_value: float | None = None
    max_value: float | None = None
    max_late_arrival_seconds: float = Field(default=60.0, ge=0.0)
    allow_nan_or_inf: bool = False

    @model_validator(mode="after")
    def _validate_bounds(self) -> QualityRuleSpec:
        if self.min_value is not None and self.max_value is not None and self.min_value > self.max_value:
            raise ValueError("min_value must be <= max_value")
        return self


class FeatureDefinition(BaseModel):
    """Immutable versioned contract for a single causal financial feature."""

    model_config = ConfigDict(frozen=True)

    name: str
    feature_view: str
    version: str
    entity_type: str = Field(..., min_length=2, max_length=64)
    value_type: ValueType
    unit: str = Field(..., min_length=1, max_length=32)
    currency: str | None = Field(default=None, max_length=16)
    description: str = Field(..., min_length=5, max_length=512)
    owner: str = Field(..., min_length=2, max_length=128)
    source: SourceDatasetContract
    transform: TransformSpec
    null_policy: NullPolicy = NullPolicy.KEEP_NULL
    default_value: float | int | bool | str | None = None
    ttl_seconds: float = Field(..., gt=0.0)
    stale_policy: StalePolicy = StalePolicy.REJECT
    quality_rules: QualityRuleSpec = Field(default_factory=QualityRuleSpec)
    stage: LifecycleStage = LifecycleStage.DRAFT
    tags: tuple[str, ...] = Field(default_factory=tuple)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("name", "feature_view")
    @classmethod
    def _validate_identifier(cls, value: str) -> str:
        if not IDENTIFIER_PATTERN.match(value):
            raise ValueError(
                f"Identifier '{value}' must match ^[a-z][a-z0-9_]{{1,63}}$"
            )
        return value

    @field_validator("version")
    @classmethod
    def _validate_semver(cls, value: str) -> str:
        parse_semver(value)
        return value

    @field_validator("created_at")
    @classmethod
    def _validate_utc(cls, value: datetime) -> datetime:
        return ensure_utc(value, "created_at")

    @model_validator(mode="after")
    def _validate_null_policy(self) -> FeatureDefinition:
        if self.null_policy == NullPolicy.DEFAULT_VALUE and self.default_value is None:
            raise ValueError("default_value must be provided when null_policy is 'default_value'")
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def version_id(self) -> str:
        """Content-addressed version fingerprint tying feature version to exact transform and source."""
        digest = canonical_sha256(
            {
                "name": self.name,
                "feature_view": self.feature_view,
                "version": self.version,
                "entity_type": self.entity_type,
                "value_type": self.value_type.value,
                "unit": self.unit,
                "currency": self.currency,
                "null_policy": self.null_policy.value,
                "default_value": self.default_value,
                "transform_hash": self.transform.transform_hash,
                "source_id": self.source.source_id,
                "source_version": self.source.source_version,
                "source_schema_hash": self.source.schema_hash,
            }
        )[:16]
        return f"{self.name}@{self.version}+{digest}"

    def with_stage(self, stage: LifecycleStage) -> FeatureDefinition:
        """Return a copy with an updated lifecycle stage (identity & version_id preserved)."""
        return self.model_copy(update={"stage": stage})


class FeatureRef(BaseModel):
    """Explicit reference to a feature name and version (optional pinned version_id)."""

    model_config = ConfigDict(frozen=True)

    name: str
    version: str | None = None
    expected_version_id: str | None = None


class FeatureRecord(BaseModel):
    """Single point-in-time feature observation with strict bi-temporal semantics."""

    model_config = ConfigDict(frozen=True)

    entity_id: str
    feature_name: str
    feature_version: str
    version_id: str
    event_time: datetime
    availability_time: datetime
    materialized_at: datetime
    value: float | int | bool | str | None
    unit: str
    source_version: str
    run_id: str
    is_late_arrival: bool = False
    late_arrival_seconds: float = 0.0

    @field_validator("event_time", "availability_time", "materialized_at")
    @classmethod
    def _validate_timestamps(cls, value: datetime) -> datetime:
        return ensure_utc(value)

    @model_validator(mode="after")
    def _validate_causality(self) -> FeatureRecord:
        if self.availability_time < self.event_time:
            raise TimestampContractError(
                "availability_time cannot precede event_time",
                details={
                    "entity_id": self.entity_id,
                    "feature_name": self.feature_name,
                    "event_time": self.event_time.isoformat(),
                    "availability_time": self.availability_time.isoformat(),
                },
            )
        return self


class OnlineFeatureValue(BaseModel):
    """Online served feature value with explicit freshness and version metadata."""

    model_config = ConfigDict(frozen=True)

    entity_id: str
    feature_name: str
    feature_version: str
    version_id: str
    value: float | int | bool | str | None
    event_time: datetime | None
    availability_time: datetime | None
    served_at: datetime
    age_seconds: float | None
    availability_age_seconds: float | None
    ttl_seconds: float
    freshness_status: FreshnessStatus
    is_stale: bool
    unit: str
    warnings: tuple[str, ...] = Field(default_factory=tuple)


class OnlineFeatureVector(BaseModel):
    """Collection of served features for a single entity at a point in time."""

    model_config = ConfigDict(frozen=True)

    entity_id: str
    requested_at: datetime
    features: dict[str, OnlineFeatureValue]
    all_fresh: bool
    stale_features: tuple[str, ...] = Field(default_factory=tuple)
    missing_features: tuple[str, ...] = Field(default_factory=tuple)


class QualityCheckResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    check_name: str
    passed: bool
    observed: Any
    threshold: Any
    message: str


class QualityReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    feature_name: str
    feature_version: str
    version_id: str
    run_id: str
    evaluated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    passed: bool
    row_count: int
    null_count: int
    null_ratio: float
    late_arrival_count: int
    max_late_arrival_seconds: float
    checks: tuple[QualityCheckResult, ...]


class MaterializationManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    feature_name: str
    feature_version: str
    version_id: str
    transform_hash: str
    source_id: str
    source_version: str
    source_schema_hash: str
    parquet_path: str
    sha256_checksum: str
    row_count: int
    entity_count: int
    min_event_time: datetime | None
    max_event_time: datetime | None
    min_availability_time: datetime | None
    max_availability_time: datetime | None
    materialized_at: datetime
    is_backfill: bool = False
    backfill_window_start: datetime | None = None
    backfill_window_end: datetime | None = None


class LineageEdge(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_node: str
    target_node: str
    relation: str
    recorded_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)


class LineageGraph(BaseModel):
    model_config = ConfigDict(frozen=True)

    nodes: dict[str, dict[str, Any]]
    edges: tuple[LineageEdge, ...]


class ParityDiffDetail(BaseModel):
    model_config = ConfigDict(frozen=True)

    entity_id: str
    as_of_time: datetime
    feature_name: str
    offline_value: Any
    online_value: Any
    offline_version_id: str | None
    online_version_id: str | None
    abs_diff: float | None
    reason: str


class ParityReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    passed: bool
    total_comparisons: int
    matched_count: int
    mismatched_count: int
    null_mismatches: int
    version_mismatches: int
    max_abs_diff: float
    atol: float
    rtol: float
    evaluated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    diffs: tuple[ParityDiffDetail, ...] = Field(default_factory=tuple)


class BackfillReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    feature_name: str
    feature_version: str
    version_id: str
    window_start: datetime
    window_end: datetime
    rows_written: int
    rows_deduplicated: int
    manifest: MaterializationManifest


class TrainingDatasetManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    export_name: str
    exported_at: datetime
    parquet_path: str
    sha256_checksum: str
    row_count: int
    entity_count: int
    feature_versions: dict[str, str]
    feature_transform_hashes: dict[str, str]
    source_provenance: dict[str, dict[str, str]]
    min_as_of_time: datetime | None
    max_as_of_time: datetime | None
    split_boundaries: dict[str, dict[str, Any]] = Field(default_factory=dict)
    null_counts: dict[str, int] = Field(default_factory=dict)
    zero_leakage_verified: bool = True
    seed: int | None = None


class GovernanceConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_active_features_per_view: int = Field(default=25, ge=1)
    max_total_active_features: int = Field(default=100, ge=1)
    require_owner: bool = True
    require_source_provenance: bool = True
    allow_order_execution: bool = False


class StoreConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    config_version: str = "1.0.0"
    environment: str = "local-prototype"
    governance: GovernanceConfig = Field(default_factory=GovernanceConfig)
    default_ttl_seconds: float = 300.0
    default_stale_policy: StalePolicy = StalePolicy.REJECT
    default_parity_atol: float = 1e-9
    default_parity_rtol: float = 1e-9
