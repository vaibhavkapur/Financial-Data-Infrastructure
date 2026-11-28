"""Strict schema, identifier check-digit, temporal window, and licensing validation."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

from .config import LicensingConfig, SUPPORTED_SCHEMA_VERSIONS
from .errors import LicensingError, UnsupportedEntityError, ValidationError
from .models import (
    AssetClass,
    CorporateActionType,
    EventEnvelope,
    EventType,
    IdentifierNamespace,
    ReviewAction,
    parse_ts,
)

_CANONICAL_SEC_RE = re.compile(r"^SEC-[A-F0-9]{12}$")
_CANONICAL_LST_RE = re.compile(r"^LST-[A-F0-9]{12}$")
_MIC_RE = re.compile(r"^[A-Z0-9]{4}$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_COUNTRY_RE = re.compile(r"^[A-Z]{2}$")
_TICKER_RE = re.compile(r"^[A-Z0-9.\-]{1,16}$")
_OPENFIGI_RE = re.compile(r"^BBG[A-Z0-9]{9}$")
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
_LEI_RE = re.compile(r"^[A-Z0-9]{20}$")

VALID_ENTITY_STATUSES = frozenset({"ACTIVE", "INACTIVE", "DELISTED", "SUSPENDED"})


def validate_isin_check_digit(isin: str) -> bool:
    """Validate an ISO 6166 ISIN using the standard Luhn double-add-double check digit."""
    if not _ISIN_RE.match(isin):
        return False
    expanded = []
    for ch in isin[:-1]:
        if ch.isdigit():
            expanded.append(ch)
        else:
            expanded.append(str(ord(ch) - ord("A") + 10))
    digits_str = "".join(expanded)
    total = 0
    # Starting from rightmost payload digit, alternate multiplying by 2 and 1.
    for idx, ch in enumerate(reversed(digits_str)):
        d = int(ch)
        if idx % 2 == 0:
            d *= 2
            if d > 9:
                d = (d // 10) + (d % 10)
        total += d
    expected_check = (10 - (total % 10)) % 10
    return expected_check == int(isin[-1])


def compute_isin_check_digit(isin_body_11: str) -> str:
    """Compute the ISO 6166 check digit for an 11-char ISIN prefix (helper for synthetic fixtures)."""
    if not re.match(r"^[A-Z]{2}[A-Z0-9]{9}$", isin_body_11):
        raise ValidationError(
            "ISIN prefix must be 2 uppercase letters followed by 9 alphanumerics.",
            details={"isin_body_11": isin_body_11},
        )
    expanded = []
    for ch in isin_body_11:
        if ch.isdigit():
            expanded.append(ch)
        else:
            expanded.append(str(ord(ch) - ord("A") + 10))
    digits_str = "".join(expanded)
    total = 0
    for idx, ch in enumerate(reversed(digits_str)):
        d = int(ch)
        if idx % 2 == 0:
            d *= 2
            if d > 9:
                d = (d // 10) + (d % 10)
        total += d
    return str((10 - (total % 10)) % 10)


def validate_namespace_licensing(namespace: IdentifierNamespace, licensing: LicensingConfig) -> None:
    """Reject identifiers in restricted/unlicensed namespaces when enforcement is enabled."""
    if not licensing.enforce_licensing:
        return
    if namespace.value not in licensing.licensed_namespaces:
        raise LicensingError(
            f"Identifier namespace '{namespace.value}' is not enabled in licensed_namespaces.",
            details={
                "namespace": namespace.value,
                "licensed_namespaces": sorted(licensing.licensed_namespaces),
            },
        )


def validate_identifier_value(
    namespace: IdentifierNamespace, value: str, mic: Optional[str] = None
) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(
            "Identifier value must be a non-empty string.",
            details={"namespace": namespace.value},
        )
    if namespace == IdentifierNamespace.ISIN:
        if not validate_isin_check_digit(value):
            raise ValidationError(
                f"Invalid ISIN '{value}': failed ISO 6166 format or Luhn check digit.",
                details={"namespace": "ISIN", "value": value},
            )
    elif namespace in (IdentifierNamespace.OPENFIGI, IdentifierNamespace.COMPOSITE_FIGI):
        if not _OPENFIGI_RE.match(value):
            raise ValidationError(
                f"Invalid OpenFIGI '{value}': expected 12-char 'BBG...' identifier.",
                details={"namespace": namespace.value, "value": value},
            )
    elif namespace == IdentifierNamespace.LEI:
        if not _LEI_RE.match(value):
            raise ValidationError(
                f"Invalid LEI '{value}': expected 20 uppercase alphanumeric characters.",
                details={"namespace": "LEI", "value": value},
            )
    elif namespace == IdentifierNamespace.MIC_TICKER:
        if ":" not in value:
            raise ValidationError(
                f"Invalid MIC_TICKER '{value}': expected '<MIC>:<TICKER>' format.",
                details={"namespace": "MIC_TICKER", "value": value},
            )
        part_mic, part_ticker = value.split(":", 1)
        if not _MIC_RE.match(part_mic) or not _TICKER_RE.match(part_ticker):
            raise ValidationError(
                f"Invalid MIC_TICKER '{value}': malformed MIC or ticker segment.",
                details={"namespace": "MIC_TICKER", "value": value},
            )
        if mic is not None and mic != part_mic:
            raise ValidationError(
                f"MIC_TICKER prefix '{part_mic}' disagrees with explicit mic '{mic}'.",
                details={"value": value, "mic": mic},
            )
    elif namespace == IdentifierNamespace.TICKER:
        if not _TICKER_RE.match(value):
            raise ValidationError(
                f"Invalid ticker '{value}'.",
                details={"namespace": "TICKER", "value": value},
            )


def _require_str(payload: Dict[str, Any], field_name: str) -> str:
    val = payload.get(field_name)
    if not isinstance(val, str) or not val.strip():
        raise ValidationError(
            f"Missing or empty required string field '{field_name}'.",
            details={"field": field_name},
        )
    return val.strip()


def validate_time_window(valid_from: datetime, valid_to: Optional[datetime]) -> None:
    if valid_from.tzinfo is None:
        raise ValidationError("valid_from must be timezone-aware (UTC).")
    if valid_to is not None:
        if valid_to.tzinfo is None:
            raise ValidationError("valid_to must be timezone-aware (UTC).")
        if valid_to <= valid_from:
            raise ValidationError(
                "valid_to must be strictly greater than valid_from.",
                details={
                    "valid_from": valid_from.isoformat(),
                    "valid_to": valid_to.isoformat(),
                },
            )


def validate_event(envelope: EventEnvelope, licensing: LicensingConfig) -> None:
    """Validate an EventEnvelope against canonical contracts and licensing policy."""
    if envelope.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ValidationError(
            f"Unsupported event schema_version '{envelope.schema_version}'.",
            details={
                "schema_version": envelope.schema_version,
                "supported": sorted(SUPPORTED_SCHEMA_VERSIONS),
            },
        )
    if not envelope.event_id.strip():
        raise ValidationError("event_id must be non-empty.")
    if not envelope.source_system.strip() or not envelope.source_event_id.strip():
        raise ValidationError(
            "source_system and source_event_id are required for lineage.",
            details={
                "source_system": envelope.source_system,
                "source_event_id": envelope.source_event_id,
            },
        )
    if envelope.observed_at.tzinfo is None:
        raise ValidationError("observed_at must be timezone-aware (UTC).")
    validate_time_window(envelope.valid_from, envelope.valid_to)

    p = envelope.payload
    if not isinstance(p, dict):
        raise ValidationError("Event payload must be a JSON object.")

    if envelope.event_type == EventType.INSTRUMENT_UPSERT:
        inst_id = _require_str(p, "instrument_id")
        if not _CANONICAL_SEC_RE.match(inst_id):
            raise ValidationError(
                f"Invalid canonical instrument_id '{inst_id}'; expected SEC-<12HEX>.",
                details={"instrument_id": inst_id},
            )
        _require_str(p, "name")
        _require_str(p, "issuer_name")
        raw_ac = _require_str(p, "asset_class")
        try:
            AssetClass(raw_ac)
        except ValueError as exc:
            raise UnsupportedEntityError(
                f"Unsupported asset_class '{raw_ac}'.",
                details={
                    "asset_class": raw_ac,
                    "supported": [ac.value for ac in AssetClass],
                },
            ) from exc
        country = _require_str(p, "country_of_risk")
        if not _COUNTRY_RE.match(country):
            raise ValidationError(
                f"Invalid ISO 3166-1 alpha-2 country_of_risk '{country}'.",
                details={"country_of_risk": country},
            )
        status = _require_str(p, "status")
        if status not in VALID_ENTITY_STATUSES:
            raise ValidationError(
                f"Invalid instrument status '{status}'.",
                details={"status": status, "allowed": sorted(VALID_ENTITY_STATUSES)},
            )

    elif envelope.event_type == EventType.LISTING_UPSERT:
        listing_id = _require_str(p, "listing_id")
        if not _CANONICAL_LST_RE.match(listing_id):
            raise ValidationError(
                f"Invalid canonical listing_id '{listing_id}'; expected LST-<12HEX>.",
                details={"listing_id": listing_id},
            )
        inst_id = _require_str(p, "instrument_id")
        if not _CANONICAL_SEC_RE.match(inst_id):
            raise ValidationError(
                f"Invalid canonical instrument_id '{inst_id}'.",
                details={"instrument_id": inst_id},
            )
        mic = _require_str(p, "mic")
        if not _MIC_RE.match(mic):
            raise ValidationError(
                f"Invalid ISO 10383 MIC '{mic}'; expected 4 uppercase alphanumerics.",
                details={"mic": mic},
            )
        ticker = _require_str(p, "ticker")
        if not _TICKER_RE.match(ticker):
            raise ValidationError(
                f"Invalid ticker '{ticker}'.",
                details={"ticker": ticker},
            )
        currency = _require_str(p, "currency")
        if not _CURRENCY_RE.match(currency):
            raise ValidationError(
                f"Invalid ISO 4217 currency '{currency}'.",
                details={"currency": currency},
            )
        status = _require_str(p, "status")
        if status not in VALID_ENTITY_STATUSES:
            raise ValidationError(
                f"Invalid listing status '{status}'.",
                details={"status": status, "allowed": sorted(VALID_ENTITY_STATUSES)},
            )
        if not isinstance(p.get("is_primary", False), bool):
            raise ValidationError("is_primary must be a boolean.")

    elif envelope.event_type == EventType.IDENTIFIER_MAP:
        inst_id = _require_str(p, "instrument_id")
        if not _CANONICAL_SEC_RE.match(inst_id):
            raise ValidationError(
                f"Invalid canonical instrument_id '{inst_id}'.",
                details={"instrument_id": inst_id},
            )
        listing_id = p.get("listing_id")
        if listing_id is not None and not _CANONICAL_LST_RE.match(str(listing_id)):
            raise ValidationError(
                f"Invalid canonical listing_id '{listing_id}'.",
                details={"listing_id": listing_id},
            )
        raw_ns = _require_str(p, "namespace")
        try:
            ns = IdentifierNamespace(raw_ns)
        except ValueError as exc:
            raise UnsupportedEntityError(
                f"Unsupported identifier namespace '{raw_ns}'.",
                details={
                    "namespace": raw_ns,
                    "supported": [item.value for item in IdentifierNamespace],
                },
            ) from exc
        validate_namespace_licensing(ns, licensing)
        val = _require_str(p, "identifier_value")
        mic = p.get("mic")
        if mic is not None and not _MIC_RE.match(str(mic)):
            raise ValidationError(f"Invalid MIC '{mic}'.", details={"mic": mic})
        validate_identifier_value(ns, val, mic=mic)

    elif envelope.event_type == EventType.CORPORATE_ACTION_LINK:
        raw_at = _require_str(p, "action_type")
        try:
            CorporateActionType(raw_at)
        except ValueError as exc:
            raise UnsupportedEntityError(
                f"Unsupported corporate action type '{raw_at}'.",
                details={
                    "action_type": raw_at,
                    "supported": [item.value for item in CorporateActionType],
                },
            ) from exc
        from_inst = _require_str(p, "from_instrument_id")
        to_inst = _require_str(p, "to_instrument_id")
        if not _CANONICAL_SEC_RE.match(from_inst) or not _CANONICAL_SEC_RE.match(to_inst):
            raise ValidationError(
                "from_instrument_id and to_instrument_id must be valid SEC-<12HEX> IDs.",
                details={"from_instrument_id": from_inst, "to_instrument_id": to_inst},
            )
        ratio_raw = p.get("ratio", "1")
        try:
            ratio = Decimal(str(ratio_raw))
        except InvalidOperation as exc:
            raise ValidationError(
                f"Invalid corporate action ratio '{ratio_raw}'.",
                details={"ratio": ratio_raw},
            ) from exc
        if ratio <= 0:
            raise ValidationError(
                "Corporate action ratio must be strictly positive.",
                details={"ratio": str(ratio)},
            )

    elif envelope.event_type == EventType.REVIEW_CORRECTION:
        _require_str(p, "conflict_id")
        _require_str(p, "reviewer_id")
        _require_str(p, "rationale")
        raw_action = _require_str(p, "action")
        try:
            ReviewAction(raw_action)
        except ValueError as exc:
            raise ValidationError(
                f"Invalid review action '{raw_action}'.",
                details={"action": raw_action, "allowed": [a.value for a in ReviewAction]},
            ) from exc

    else:
        raise UnsupportedEntityError(
            f"Unsupported event_type '{envelope.event_type}'.",
            details={"event_type": str(envelope.event_type)},
        )


def parse_required_ts(value: str, field_name: str) -> datetime:
    try:
        parsed = parse_ts(value)
    except ValueError as exc:
        raise ValidationError(
            f"Invalid timestamp for '{field_name}': {exc}",
            details={"field": field_name, "value": value},
        ) from exc
    if parsed is None:
        raise ValidationError(
            f"Timestamp '{field_name}' is required.",
            details={"field": field_name},
        )
    return parsed
