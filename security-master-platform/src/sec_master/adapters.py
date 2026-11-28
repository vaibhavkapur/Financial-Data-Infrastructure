"""Source adapters translating raw vendor, exchange, and corporate-action payloads
into canonical, schema-versioned EventEnvelopes with deterministic IDs and lineage.
"""

from __future__ import annotations

from typing import Any, Dict, List

from .errors import UnsupportedEntityError, ValidationError
from .models import (
    EventEnvelope,
    EventType,
    SCHEMA_VERSION,
    make_deterministic_event_id,
)
from .validation import parse_required_ts


def raw_record_to_envelope(raw: Dict[str, Any]) -> EventEnvelope:
    """Translate a canonical JSON fixture/API record into an EventEnvelope."""
    if not isinstance(raw, dict):
        raise ValidationError("Raw event record must be a JSON object.")

    raw_type = raw.get("event_type")
    if not isinstance(raw_type, str):
        raise ValidationError("Missing or non-string 'event_type'.")
    try:
        event_type = EventType(raw_type)
    except ValueError as exc:
        raise UnsupportedEntityError(
            f"Unsupported event_type '{raw_type}'.",
            details={"event_type": raw_type, "supported": [e.value for e in EventType]},
        ) from exc

    source_system = raw.get("source_system")
    source_event_id = raw.get("source_event_id")
    if not isinstance(source_system, str) or not source_system.strip():
        raise ValidationError("Missing or empty 'source_system'.")
    if not isinstance(source_event_id, str) or not source_event_id.strip():
        raise ValidationError("Missing or empty 'source_event_id'.")

    valid_from = parse_required_ts(raw.get("valid_from", ""), "valid_from")
    valid_to_raw = raw.get("valid_to")
    valid_to = parse_required_ts(valid_to_raw, "valid_to") if valid_to_raw is not None else None
    observed_at = parse_required_ts(raw.get("observed_at", ""), "observed_at")

    payload = raw.get("payload")
    if not isinstance(payload, dict):
        raise ValidationError("Event 'payload' must be a JSON object.")

    schema_version = str(raw.get("schema_version", SCHEMA_VERSION))
    event_id = str(
        raw.get("event_id")
        or make_deterministic_event_id(source_system.strip(), source_event_id.strip(), event_type.value)
    )
    return EventEnvelope(
        event_id=event_id,
        event_type=event_type,
        schema_version=schema_version,
        source_system=source_system.strip(),
        source_event_id=source_event_id.strip(),
        valid_from=valid_from,
        valid_to=valid_to,
        observed_at=observed_at,
        payload=dict(payload),
        supersedes_event_id=raw.get("supersedes_event_id"),
    )


class VendorReferenceAdapter:
    """Adapter translating external vendor reference batches into canonical EventEnvelopes."""

    def __init__(self, source_system: str, schema_version: str = SCHEMA_VERSION) -> None:
        self.source_system = source_system
        self.schema_version = schema_version

    def adapt_batch(self, records: List[Dict[str, Any]]) -> List[EventEnvelope]:
        envelopes: List[EventEnvelope] = []
        for rec in records:
            normalized = dict(rec)
            normalized.setdefault("source_system", self.source_system)
            normalized.setdefault("schema_version", self.schema_version)
            envelopes.append(raw_record_to_envelope(normalized))
        return envelopes


class CorporateActionFeedAdapter:
    """Adapter for sibling `financial-corporate-actions-engine` event contracts."""

    def __init__(self, source_system: str = "CORP_ACTION_ENGINE") -> None:
        self.source_system = source_system

    def adapt_action(self, action_payload: Dict[str, Any]) -> EventEnvelope:
        """Convert a corporate-action announcement into a canonical CORPORATE_ACTION_LINK event."""
        required = ("source_event_id", "action_type", "from_instrument_id", "to_instrument_id", "effective_at", "observed_at")
        for key in required:
            if key not in action_payload:
                raise ValidationError(
                    f"Corporate action feed missing required field '{key}'.",
                    details={"missing_field": key},
                )
        raw = {
            "event_type": EventType.CORPORATE_ACTION_LINK.value,
            "schema_version": action_payload.get("schema_version", SCHEMA_VERSION),
            "source_system": action_payload.get("source_system", self.source_system),
            "source_event_id": action_payload["source_event_id"],
            "valid_from": action_payload["effective_at"],
            "valid_to": None,
            "observed_at": action_payload["observed_at"],
            "supersedes_event_id": action_payload.get("supersedes_event_id"),
            "payload": {
                "action_type": action_payload["action_type"],
                "from_instrument_id": action_payload["from_instrument_id"],
                "to_instrument_id": action_payload["to_instrument_id"],
                "from_listing_id": action_payload.get("from_listing_id"),
                "to_listing_id": action_payload.get("to_listing_id"),
                "ratio": str(action_payload.get("ratio", "1")),
                "metadata": dict(action_payload.get("metadata", {})),
            },
        }
        return raw_record_to_envelope(raw)
