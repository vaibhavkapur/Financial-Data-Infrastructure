"""Canonical domain contracts with UTC event/availability timestamps, units, and provenance."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def ensure_utc(value: datetime) -> datetime:
    """Validate and normalize a datetime to timezone-aware UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamp must be timezone-aware UTC; naive timestamps are rejected.")
    return value.astimezone(UTC)


class BitemporalMixin(BaseModel):
    """Shared bitemporal timestamps: when the event happened vs. when it became known."""

    model_config = ConfigDict(frozen=True)

    event_ts: datetime
    available_at: datetime
    revision_number: int = Field(default=1, ge=1)

    @field_validator("event_ts", "available_at")
    @classmethod
    def _validate_utc(cls, value: datetime) -> datetime:
        return ensure_utc(value)

    @model_validator(mode="after")
    def _validate_bitemporal_order(self) -> BitemporalMixin:
        if self.available_at < self.event_ts:
            raise ValueError(
                f"available_at ({self.available_at.isoformat()}) cannot precede "
                f"event_ts ({self.event_ts.isoformat()})."
            )
        return self


class InstrumentRecord(BitemporalMixin):
    instrument_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    name: str = Field(min_length=1)
    asset_class: Literal["equity", "etf", "future", "fx", "index"]
    exchange: str = Field(min_length=1)
    currency: str = Field(min_length=3, max_length=3)
    lot_size: int = Field(gt=0)
    status: Literal["ACTIVE", "DELISTED", "HALTED"]
    effective_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")


class TickRecord(BitemporalMixin):
    tick_id: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    event_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    price: float = Field(gt=0.0)
    size: float = Field(gt=0.0)
    currency: str = Field(min_length=3, max_length=3)
    venue: str = Field(min_length=1)
    side: Literal["BUY", "SELL", "UNKNOWN"]
    correction_of: str | None = None
    source_system: str = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_correction(self) -> TickRecord:
        if self.revision_number > 1 and not self.correction_of:
            raise ValueError("Revisions with revision_number > 1 must specify correction_of.")
        return self


class PriceBarRecord(BitemporalMixin):
    instrument_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    trade_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    open: float = Field(gt=0.0)
    high: float = Field(gt=0.0)
    low: float = Field(gt=0.0)
    close: float = Field(gt=0.0)
    volume: float = Field(ge=0.0)
    currency: str = Field(min_length=3, max_length=3)
    unit: str = Field(min_length=1)
    correction_reason: str | None = None

    @model_validator(mode="after")
    def _validate_ohlc(self) -> PriceBarRecord:
        if self.high < max(self.open, self.close, self.low):
            raise ValueError("high must be >= open, close, and low.")
        if self.low > min(self.open, self.close, self.high):
            raise ValueError("low must be <= open, close, and high.")
        return self


class CorporateActionRecord(BitemporalMixin):
    action_id: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    action_type: Literal["SPLIT", "CASH_DIVIDEND"]
    ex_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    record_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    pay_date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    split_numerator: float | None = Field(default=None, gt=0.0)
    split_denominator: float | None = Field(default=None, gt=0.0)
    cash_amount: float | None = Field(default=None, gt=0.0)
    currency: str = Field(min_length=3, max_length=3)

    @model_validator(mode="after")
    def _validate_action_fields(self) -> CorporateActionRecord:
        if self.action_type == "SPLIT":
            if self.split_numerator is None or self.split_denominator is None:
                raise ValueError("SPLIT action requires split_numerator and split_denominator.")
        elif self.action_type == "CASH_DIVIDEND":
            if self.cash_amount is None:
                raise ValueError("CASH_DIVIDEND action requires cash_amount.")
        return self


class FundamentalRecord(BitemporalMixin):
    filing_id: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    fiscal_period: str = Field(pattern=r"^\d{4}Q[1-4]$")
    period_end_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    metric_name: str = Field(min_length=1)
    metric_value: float
    unit: str = Field(min_length=1)
    currency: str = Field(min_length=3, max_length=3)
    is_restatement: bool = False


class AlternativeSentimentRecord(BitemporalMixin):
    signal_id: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    sentiment_score: float = Field(ge=-1.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    source_feed: str = Field(min_length=1)
    event_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")


class AccessContext(BaseModel):
    """Caller identity and purpose checked against dataset entitlements."""

    model_config = ConfigDict(frozen=True)

    principal: str = Field(min_length=1)
    role: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    requested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("requested_at")
    @classmethod
    def _validate_utc(cls, value: datetime) -> datetime:
        return ensure_utc(value)


class EntitlementPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset_id: str = Field(min_length=1)
    source_vendor: str = Field(min_length=1)
    license_id: str = Field(min_length=1)
    license_terms: str = Field(min_length=1)
    permitted_roles: list[str]
    permitted_purposes: list[str]
    redistribution_allowed: bool = False
    effective_from: datetime
    expires_at: datetime
    classification: str = "internal_synthetic"

    @field_validator("effective_from", "expires_at")
    @classmethod
    def _validate_utc(cls, value: datetime) -> datetime:
        return ensure_utc(value)


class PartitionManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    partition_id: str
    dataset_id: str
    layer: Literal["raw", "normalized", "curated"]
    partition_key: str
    partition_value: str
    version: int = Field(ge=1)
    schema_version: int = Field(ge=1)
    object_path: str
    sha256: str
    byte_size: int = Field(ge=0)
    row_count: int = Field(ge=0)
    min_event_ts: datetime | None = None
    max_event_ts: datetime | None = None
    min_available_at: datetime | None = None
    max_available_at: datetime | None = None
    ingestion_run_id: str
    source_checksum: str
    license_id: str
    provenance: str
    created_at: datetime
    status: Literal["ACTIVE", "QUARANTINED", "ARCHIVED"] = "ACTIVE"


class QualityCheckResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    rule_name: str
    severity: Literal["CRITICAL", "WARNING", "INFO"]
    passed: bool
    message: str
    affected_rows: int = 0
    details: dict[str, Any] = Field(default_factory=dict)


class QualityReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset_id: str
    layer: Literal["raw", "normalized", "curated"]
    ingestion_run_id: str
    checked_at: datetime
    total_rows: int
    passed: bool
    checks: list[QualityCheckResult]


class LineageEdge(BaseModel):
    model_config = ConfigDict(frozen=True)

    edge_id: str
    ingestion_run_id: str
    source_dataset: str
    source_layer: str
    source_path: str
    source_sha256: str
    target_dataset: str
    target_layer: str
    target_path: str
    target_sha256: str
    transformation: str
    created_at: datetime


class SnapshotManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    snapshot_id: str
    name: str
    as_of_ts: datetime
    datasets: list[str]
    partition_ids: list[str]
    partition_checksums: dict[str, str]
    manifest_sha256: str
    created_by: str
    created_at: datetime
    license_ids: dict[str, str]


DATASET_MODELS: dict[str, type[BitemporalMixin]] = {
    "market_ticks": TickRecord,
    "reference_instruments": InstrumentRecord,
    "prices_eod": PriceBarRecord,
    "corporate_actions": CorporateActionRecord,
    "fundamentals": FundamentalRecord,
    "alt_sentiment": AlternativeSentimentRecord,
}
