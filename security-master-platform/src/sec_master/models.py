"""Canonical data contracts, identifier namespaces, and bitemporal domain models."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


SCHEMA_VERSION = "1.0.0"


class AssetClass(str, Enum):
    EQUITY = "EQUITY"
    ETF = "ETF"
    FIXED_INCOME = "FIXED_INCOME"
    ADR = "ADR"


class IdentifierNamespace(str, Enum):
    OPENFIGI = "OPENFIGI"
    COMPOSITE_FIGI = "COMPOSITE_FIGI"
    ISIN = "ISIN"
    LEI = "LEI"
    MIC_TICKER = "MIC_TICKER"
    TICKER = "TICKER"
    CUSIP = "CUSIP"
    SEDOL = "SEDOL"
    VENDOR_INTERNAL = "VENDOR_INTERNAL"


class EventType(str, Enum):
    INSTRUMENT_UPSERT = "INSTRUMENT_UPSERT"
    LISTING_UPSERT = "LISTING_UPSERT"
    IDENTIFIER_MAP = "IDENTIFIER_MAP"
    CORPORATE_ACTION_LINK = "CORPORATE_ACTION_LINK"
    REVIEW_CORRECTION = "REVIEW_CORRECTION"


class CorporateActionType(str, Enum):
    TICKER_CHANGE = "TICKER_CHANGE"
    MERGER = "MERGER"
    SPINOFF = "SPINOFF"
    SPLIT = "SPLIT"
    REVERSE_SPLIT = "REVERSE_SPLIT"


class ConflictStatus(str, Enum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


class ReviewAction(str, Enum):
    ACCEPT_INCOMING = "ACCEPT_INCOMING"
    KEEP_EXISTING = "KEEP_EXISTING"
    CUSTOM_OVERRIDE = "CUSTOM_OVERRIDE"


def format_ts(dt: Optional[datetime]) -> Optional[str]:
    """Serialize a UTC datetime to canonical ISO-8601 ``Z`` string."""
    if dt is None:
        return None
    utc_dt = dt.astimezone(timezone.utc)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 UTC timestamp string into a timezone-aware UTC datetime."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Timestamp must be a non-empty ISO-8601 string.")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        raise ValueError(f"Timestamp '{value}' is missing timezone offset; UTC is required.")
    return dt.astimezone(timezone.utc)


def deterministic_hash(parts: Tuple[str, ...], length: int = 12) -> str:
    """Create a deterministic uppercase hex digest from canonical tuple parts."""
    payload = "|".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:length].upper()


def make_canonical_instrument_id(issuer_key: str, asset_class: str, seed_tag: str = "v1") -> str:
    """Create a stable canonical instrument ID (``SEC-<12HEX>``).

    Never derived from mutable exchange tickers.
    """
    digest = deterministic_hash((seed_tag, asset_class.upper(), issuer_key.strip().upper()), 12)
    return f"SEC-{digest}"


def make_canonical_listing_id(instrument_id: str, mic: str, currency: str) -> str:
    """Create a stable canonical listing ID (``LST-<12HEX>``)."""
    digest = deterministic_hash(
        (instrument_id.strip().upper(), mic.strip().upper(), currency.strip().upper()), 12
    )
    return f"LST-{digest}"


def make_deterministic_event_id(
    source_system: str, source_event_id: str, event_type: str
) -> str:
    digest = deterministic_hash((source_system, source_event_id, event_type), 16)
    return f"EVT-{digest}"


def make_version_id(prefix: str, entity_key: str, observed_at: datetime, source_event_id: str) -> str:
    digest = deterministic_hash(
        (prefix, entity_key, format_ts(observed_at) or "", source_event_id), 14
    )
    return f"{prefix}-{digest}"


def payload_sha256(payload: Dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


@dataclass(frozen=True)
class Lineage:
    source_system: str
    source_event_id: str
    schema_version: str = SCHEMA_VERSION
    supersedes_version_id: Optional[str] = None
    reviewer_id: Optional[str] = None
    review_rationale: Optional[str] = None
    conflict_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_system": self.source_system,
            "source_event_id": self.source_event_id,
            "schema_version": self.schema_version,
            "supersedes_version_id": self.supersedes_version_id,
            "reviewer_id": self.reviewer_id,
            "review_rationale": self.review_rationale,
            "conflict_id": self.conflict_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Lineage":
        return cls(
            source_system=str(data["source_system"]),
            source_event_id=str(data["source_event_id"]),
            schema_version=str(data.get("schema_version", SCHEMA_VERSION)),
            supersedes_version_id=data.get("supersedes_version_id"),
            reviewer_id=data.get("reviewer_id"),
            review_rationale=data.get("review_rationale"),
            conflict_id=data.get("conflict_id"),
        )


@dataclass(frozen=True)
class EventEnvelope:
    event_id: str
    event_type: EventType
    schema_version: str
    source_system: str
    source_event_id: str
    valid_from: datetime
    valid_to: Optional[datetime]
    observed_at: datetime
    payload: Dict[str, Any]
    supersedes_event_id: Optional[str] = None

    @property
    def sha256(self) -> str:
        return payload_sha256(self.payload)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "schema_version": self.schema_version,
            "source_system": self.source_system,
            "source_event_id": self.source_event_id,
            "valid_from": format_ts(self.valid_from),
            "valid_to": format_ts(self.valid_to),
            "observed_at": format_ts(self.observed_at),
            "payload": self.payload,
            "payload_sha256": self.sha256,
            "supersedes_event_id": self.supersedes_event_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EventEnvelope":
        return cls(
            event_id=str(data["event_id"]),
            event_type=EventType(data["event_type"]),
            schema_version=str(data["schema_version"]),
            source_system=str(data["source_system"]),
            source_event_id=str(data["source_event_id"]),
            valid_from=parse_ts(data["valid_from"]),  # type: ignore[arg-type]
            valid_to=parse_ts(data.get("valid_to")),
            observed_at=parse_ts(data["observed_at"]),  # type: ignore[arg-type]
            payload=dict(data["payload"]),
            supersedes_event_id=data.get("supersedes_event_id"),
        )


@dataclass(frozen=True)
class InstrumentVersion:
    version_id: str
    instrument_id: str
    name: str
    asset_class: AssetClass
    issuer_name: str
    country_of_risk: str
    status: str
    valid_from: datetime
    valid_to: Optional[datetime]
    observed_at: datetime
    superseded_at: Optional[datetime]
    lineage: Lineage

    def is_active_at(self, as_of_valid: datetime, as_of_observed: datetime) -> bool:
        valid_ok = self.valid_from <= as_of_valid and (
            self.valid_to is None or as_of_valid < self.valid_to
        )
        obs_ok = self.observed_at <= as_of_observed and (
            self.superseded_at is None or as_of_observed < self.superseded_at
        )
        return valid_ok and obs_ok

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version_id": self.version_id,
            "instrument_id": self.instrument_id,
            "name": self.name,
            "asset_class": self.asset_class.value,
            "issuer_name": self.issuer_name,
            "country_of_risk": self.country_of_risk,
            "status": self.status,
            "valid_from": format_ts(self.valid_from),
            "valid_to": format_ts(self.valid_to),
            "observed_at": format_ts(self.observed_at),
            "superseded_at": format_ts(self.superseded_at),
            "lineage": self.lineage.to_dict(),
        }


@dataclass(frozen=True)
class ListingVersion:
    version_id: str
    listing_id: str
    instrument_id: str
    mic: str
    ticker: str
    currency: str
    is_primary: bool
    status: str
    valid_from: datetime
    valid_to: Optional[datetime]
    observed_at: datetime
    superseded_at: Optional[datetime]
    lineage: Lineage

    def is_active_at(self, as_of_valid: datetime, as_of_observed: datetime) -> bool:
        valid_ok = self.valid_from <= as_of_valid and (
            self.valid_to is None or as_of_valid < self.valid_to
        )
        obs_ok = self.observed_at <= as_of_observed and (
            self.superseded_at is None or as_of_observed < self.superseded_at
        )
        return valid_ok and obs_ok

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version_id": self.version_id,
            "listing_id": self.listing_id,
            "instrument_id": self.instrument_id,
            "mic": self.mic,
            "ticker": self.ticker,
            "currency": self.currency,
            "is_primary": self.is_primary,
            "status": self.status,
            "valid_from": format_ts(self.valid_from),
            "valid_to": format_ts(self.valid_to),
            "observed_at": format_ts(self.observed_at),
            "superseded_at": format_ts(self.superseded_at),
            "lineage": self.lineage.to_dict(),
        }


@dataclass(frozen=True)
class IdentifierMapping:
    version_id: str
    instrument_id: str
    listing_id: Optional[str]
    namespace: IdentifierNamespace
    identifier_value: str
    mic: Optional[str]
    valid_from: datetime
    valid_to: Optional[datetime]
    observed_at: datetime
    superseded_at: Optional[datetime]
    lineage: Lineage

    def is_active_at(self, as_of_valid: datetime, as_of_observed: datetime) -> bool:
        valid_ok = self.valid_from <= as_of_valid and (
            self.valid_to is None or as_of_valid < self.valid_to
        )
        obs_ok = self.observed_at <= as_of_observed and (
            self.superseded_at is None or as_of_observed < self.superseded_at
        )
        return valid_ok and obs_ok

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version_id": self.version_id,
            "instrument_id": self.instrument_id,
            "listing_id": self.listing_id,
            "namespace": self.namespace.value,
            "identifier_value": self.identifier_value,
            "mic": self.mic,
            "valid_from": format_ts(self.valid_from),
            "valid_to": format_ts(self.valid_to),
            "observed_at": format_ts(self.observed_at),
            "superseded_at": format_ts(self.superseded_at),
            "lineage": self.lineage.to_dict(),
        }


@dataclass(frozen=True)
class CorporateActionLink:
    link_id: str
    action_type: CorporateActionType
    from_instrument_id: str
    to_instrument_id: str
    from_listing_id: Optional[str]
    to_listing_id: Optional[str]
    ratio: Decimal
    effective_at: datetime
    observed_at: datetime
    superseded_at: Optional[datetime]
    lineage: Lineage
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_active_at(self, as_of_valid: datetime, as_of_observed: datetime) -> bool:
        obs_ok = self.observed_at <= as_of_observed and (
            self.superseded_at is None or as_of_observed < self.superseded_at
        )
        return self.effective_at <= as_of_valid and obs_ok

    def to_dict(self) -> Dict[str, Any]:
        return {
            "link_id": self.link_id,
            "action_type": self.action_type.value,
            "from_instrument_id": self.from_instrument_id,
            "to_instrument_id": self.to_instrument_id,
            "from_listing_id": self.from_listing_id,
            "to_listing_id": self.to_listing_id,
            "ratio": str(self.ratio),
            "effective_at": format_ts(self.effective_at),
            "observed_at": format_ts(self.observed_at),
            "superseded_at": format_ts(self.superseded_at),
            "lineage": self.lineage.to_dict(),
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class ReviewDecision:
    conflict_id: str
    reviewer_id: str
    action: ReviewAction
    rationale: str
    observed_at: datetime
    override_payload: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "conflict_id": self.conflict_id,
            "reviewer_id": self.reviewer_id,
            "action": self.action.value,
            "rationale": self.rationale,
            "observed_at": format_ts(self.observed_at),
            "override_payload": self.override_payload,
        }


@dataclass(frozen=True)
class ConflictTicket:
    conflict_id: str
    conflict_type: str
    entity_key: str
    status: ConflictStatus
    created_at: datetime
    existing_claim: Dict[str, Any]
    incoming_claim: Dict[str, Any]
    resolved_at: Optional[datetime] = None
    resolution: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "conflict_id": self.conflict_id,
            "conflict_type": self.conflict_type,
            "entity_key": self.entity_key,
            "status": self.status.value,
            "created_at": format_ts(self.created_at),
            "resolved_at": format_ts(self.resolved_at),
            "existing_claim": self.existing_claim,
            "incoming_claim": self.incoming_claim,
            "resolution": self.resolution,
        }


@dataclass(frozen=True)
class LookupResult:
    instrument: InstrumentVersion
    listing: Optional[ListingVersion]
    matched_identifier: IdentifierMapping
    identifiers: Tuple[IdentifierMapping, ...]
    corporate_actions: Tuple[CorporateActionLink, ...]
    as_of_valid: datetime
    as_of_observed: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "instrument": self.instrument.to_dict(),
            "listing": self.listing.to_dict() if self.listing else None,
            "matched_identifier": self.matched_identifier.to_dict(),
            "identifiers": [item.to_dict() for item in self.identifiers],
            "corporate_actions": [item.to_dict() for item in self.corporate_actions],
            "as_of_valid": format_ts(self.as_of_valid),
            "as_of_observed": format_ts(self.as_of_observed),
        }


@dataclass(frozen=True)
class QualityMetrics:
    as_of_observed: datetime
    total_events: int
    active_instruments: int
    active_listings: int
    active_identifier_mappings: int
    corporate_action_links: int
    open_conflicts: int
    resolved_conflicts: int
    completeness_ratio: float
    incomplete_instruments: Tuple[str, ...]
    lineage_coverage_ratio: float
    max_observation_lag_seconds: float
    stale_sources: Tuple[str, ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "as_of_observed": format_ts(self.as_of_observed),
            "total_events": self.total_events,
            "active_instruments": self.active_instruments,
            "active_listings": self.active_listings,
            "active_identifier_mappings": self.active_identifier_mappings,
            "corporate_action_links": self.corporate_action_links,
            "open_conflicts": self.open_conflicts,
            "resolved_conflicts": self.resolved_conflicts,
            "completeness_ratio": round(self.completeness_ratio, 6),
            "incomplete_instruments": list(self.incomplete_instruments),
            "lineage_coverage_ratio": round(self.lineage_coverage_ratio, 6),
            "max_observation_lag_seconds": round(self.max_observation_lag_seconds, 3),
            "stale_sources": list(self.stale_sources),
        }
