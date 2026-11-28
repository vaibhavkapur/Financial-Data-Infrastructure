"""Pure, I/O-free bitemporal Security Master domain state machine.

All state transitions (ingestion, bitemporal valid/system-time slicing,
conflict detection & quarantine, as-of symbol lookup, corporate-action lineage
traversal, and reviewer correction resolution) are completely isolated from
storage and transport I/O so they can be unit-tested and replayed deterministically.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from .config import PlatformConfig
from .errors import (
    AmbiguousMappingError,
    ConflictBlockedError,
    NotFoundError,
    PreObservationLookupError,
    ValidationError,
)
from .models import (
    AssetClass,
    ConflictStatus,
    ConflictTicket,
    CorporateActionLink,
    CorporateActionType,
    EventEnvelope,
    EventType,
    IdentifierMapping,
    IdentifierNamespace,
    InstrumentVersion,
    Lineage,
    ListingVersion,
    LookupResult,
    QualityMetrics,
    ReviewAction,
    ReviewDecision,
    deterministic_hash,
    format_ts,
    make_version_id,
)
from .validation import validate_event

SINGLE_VALUE_INSTRUMENT_NAMESPACES = frozenset(
    {
        IdentifierNamespace.ISIN,
        IdentifierNamespace.OPENFIGI,
        IdentifierNamespace.COMPOSITE_FIGI,
        IdentifierNamespace.LEI,
    }
)

GLOBALLY_UNIQUE_NAMESPACES = frozenset(
    {
        IdentifierNamespace.ISIN,
        IdentifierNamespace.OPENFIGI,
        IdentifierNamespace.COMPOSITE_FIGI,
        IdentifierNamespace.MIC_TICKER,
    }
)


@dataclass(frozen=True)
class IngestOutcome:
    event_id: str
    status: str  # APPLIED | CORROBORATED | DUPLICATE_SKIPPED | QUARANTINED | RESOLVED
    version_ids: Tuple[str, ...] = ()
    conflict_id: Optional[str] = None
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "status": self.status,
            "version_ids": list(self.version_ids),
            "conflict_id": self.conflict_id,
            "message": self.message,
        }


def ranges_overlap(
    start_a: datetime,
    end_a: Optional[datetime],
    start_b: datetime,
    end_b: Optional[datetime],
) -> bool:
    """Return True iff half-open intervals [start_a, end_a) and [start_b, end_b) overlap."""
    a_before_b = end_a is not None and end_a <= start_b
    b_before_a = end_b is not None and end_b <= start_a
    return not (a_before_b or b_before_a)


def _subtract_interval(
    old_start: datetime,
    old_end: Optional[datetime],
    cut_start: datetime,
    cut_end: Optional[datetime],
) -> List[Tuple[datetime, Optional[datetime]]]:
    """Return non-overlapping valid-time remainders of [old_start, old_end) minus [cut_start, cut_end)."""
    remainders: List[Tuple[datetime, Optional[datetime]]] = []
    if old_start < cut_start:
        prefix_end = cut_start if (old_end is None or cut_start < old_end) else old_end
        if old_start < prefix_end:
            remainders.append((old_start, prefix_end))
    if cut_end is not None and (old_end is None or cut_end < old_end):
        suffix_start = cut_end if cut_end > old_start else old_start
        if old_end is None or suffix_start < old_end:
            remainders.append((suffix_start, old_end))
    return remainders


class BitemporalStateEngine:
    """Pure in-memory bitemporal state machine for Security Master entities."""

    def __init__(self, config: PlatformConfig) -> None:
        self.config = config
        self._events_by_source: Dict[Tuple[str, str], EventEnvelope] = {}
        self._events_by_id: Dict[str, EventEnvelope] = {}
        self._ordered_events: List[EventEnvelope] = []

        self.instruments: List[InstrumentVersion] = []
        self.listings: List[ListingVersion] = []
        self.identifiers: List[IdentifierMapping] = []
        self.corporate_actions: List[CorporateActionLink] = []
        self.conflicts: Dict[str, ConflictTicket] = {}

    def apply_event(self, envelope: EventEnvelope) -> IngestOutcome:
        """Validate and deterministically apply one canonical EventEnvelope."""
        validate_event(envelope, self.config.licensing)

        source_key = (envelope.source_system, envelope.source_event_id)
        if source_key in self._events_by_source:
            existing = self._events_by_source[source_key]
            if existing.sha256 == envelope.sha256 and existing.event_type == envelope.event_type:
                return IngestOutcome(
                    event_id=existing.event_id,
                    status="DUPLICATE_SKIPPED",
                    message="Idempotent duplicate event skipped.",
                )
            raise ValidationError(
                "Conflicting payload for already-ingested (source_system, source_event_id).",
                details={
                    "source_system": envelope.source_system,
                    "source_event_id": envelope.source_event_id,
                    "existing_sha256": existing.sha256,
                    "incoming_sha256": envelope.sha256,
                },
            )

        self._events_by_source[source_key] = envelope
        self._events_by_id[envelope.event_id] = envelope
        self._ordered_events.append(envelope)

        if envelope.event_type == EventType.INSTRUMENT_UPSERT:
            return self._apply_instrument_upsert(envelope)
        if envelope.event_type == EventType.LISTING_UPSERT:
            return self._apply_listing_upsert(envelope)
        if envelope.event_type == EventType.IDENTIFIER_MAP:
            return self._apply_identifier_map(envelope)
        if envelope.event_type == EventType.CORPORATE_ACTION_LINK:
            return self._apply_corporate_action_link(envelope)
        if envelope.event_type == EventType.REVIEW_CORRECTION:
            return self._apply_review_correction(envelope)
        raise ValidationError(f"Unhandled event type: {envelope.event_type}")

    def _is_authorized_supersede(
        self,
        envelope: EventEnvelope,
        existing_lineage: Lineage,
    ) -> bool:
        if envelope.source_system == "REVIEWER_WORKFLOW":
            return True
        if not self.config.conflict_resolution.allow_same_source_correction:
            return False
        if existing_lineage.source_system == envelope.source_system:
            return True
        if envelope.supersedes_event_id and envelope.supersedes_event_id == existing_lineage.source_event_id:
            return True
        return False

    def _open_conflict(
        self,
        conflict_type: str,
        entity_key: str,
        created_at: datetime,
        existing_claim: Dict[str, Any],
        incoming_envelope: EventEnvelope,
    ) -> ConflictTicket:
        cid_hash = deterministic_hash(
            (conflict_type, entity_key, incoming_envelope.event_id, format_ts(created_at) or ""),
            12,
        )
        conflict_id = f"CNF-{cid_hash}"
        ticket = ConflictTicket(
            conflict_id=conflict_id,
            conflict_type=conflict_type,
            entity_key=entity_key,
            status=ConflictStatus.OPEN,
            created_at=created_at,
            existing_claim=existing_claim,
            incoming_claim=incoming_envelope.to_dict(),
        )
        self.conflicts[conflict_id] = ticket
        return ticket

    def _build_lineage(
        self,
        envelope: EventEnvelope,
        supersedes_version_id: Optional[str] = None,
        review_meta: Optional[Dict[str, Any]] = None,
    ) -> Lineage:
        meta = review_meta or {}
        return Lineage(
            source_system=envelope.source_system,
            source_event_id=envelope.source_event_id,
            schema_version=envelope.schema_version,
            supersedes_version_id=supersedes_version_id,
            reviewer_id=meta.get("reviewer_id"),
            review_rationale=meta.get("rationale"),
            conflict_id=meta.get("conflict_id"),
        )

    # ------------------------------------------------------------------
    # Instrument Upsert
    # ------------------------------------------------------------------
    def _apply_instrument_upsert(
        self,
        envelope: EventEnvelope,
        review_meta: Optional[Dict[str, Any]] = None,
        force_supersede: bool = False,
    ) -> IngestOutcome:
        p = envelope.payload
        inst_id = p["instrument_id"]
        overlapping = [
            (idx, rec)
            for idx, rec in enumerate(self.instruments)
            if rec.instrument_id == inst_id
            and rec.superseded_at is None
            and ranges_overlap(rec.valid_from, rec.valid_to, envelope.valid_from, envelope.valid_to)
        ]

        if overlapping:
            # Check exact corroboration
            all_identical = all(
                rec.name == p["name"]
                and rec.asset_class.value == p["asset_class"]
                and rec.issuer_name == p["issuer_name"]
                and rec.country_of_risk == p["country_of_risk"]
                and rec.status == p["status"]
                and rec.valid_from == envelope.valid_from
                and rec.valid_to == envelope.valid_to
                for _, rec in overlapping
            )
            if all_identical:
                return IngestOutcome(
                    event_id=envelope.event_id,
                    status="CORROBORATED",
                    version_ids=tuple(rec.version_id for _, rec in overlapping),
                    message="Incoming instrument attributes match active belief.",
                )

            if not force_supersede and not all(
                self._is_authorized_supersede(envelope, rec.lineage) for _, rec in overlapping
            ):
                ticket = self._open_conflict(
                    conflict_type="INSTRUMENT_ATTRIBUTE_CONFLICT",
                    entity_key=inst_id,
                    created_at=envelope.observed_at,
                    existing_claim={"versions": [rec.to_dict() for _, rec in overlapping]},
                    incoming_envelope=envelope,
                )
                return IngestOutcome(
                    event_id=envelope.event_id,
                    status="QUARANTINED",
                    conflict_id=ticket.conflict_id,
                    message="Cross-source instrument conflict quarantined for reviewer resolution.",
                )

        superseded_ids: List[str] = []
        created_ids: List[str] = []
        for idx, rec in overlapping:
            self.instruments[idx] = replace(rec, superseded_at=envelope.observed_at)
            superseded_ids.append(rec.version_id)
            for rem_idx, (rem_start, rem_end) in enumerate(
                _subtract_interval(rec.valid_from, rec.valid_to, envelope.valid_from, envelope.valid_to)
            ):
                slice_vid = make_version_id(
                    "INS",
                    f"{inst_id}:slice:{rem_idx}",
                    envelope.observed_at,
                    f"{envelope.source_event_id}:carry:{rec.version_id}",
                )
                self.instruments.append(
                    replace(
                        rec,
                        version_id=slice_vid,
                        valid_from=rem_start,
                        valid_to=rem_end,
                        observed_at=envelope.observed_at,
                        superseded_at=None,
                        lineage=replace(rec.lineage, supersedes_version_id=rec.version_id),
                    )
                )
                created_ids.append(slice_vid)

        vid = make_version_id("INS", inst_id, envelope.observed_at, envelope.source_event_id)
        new_ver = InstrumentVersion(
            version_id=vid,
            instrument_id=inst_id,
            name=p["name"],
            asset_class=AssetClass(p["asset_class"]),
            issuer_name=p["issuer_name"],
            country_of_risk=p["country_of_risk"],
            status=p["status"],
            valid_from=envelope.valid_from,
            valid_to=envelope.valid_to,
            observed_at=envelope.observed_at,
            superseded_at=None,
            lineage=self._build_lineage(
                envelope,
                supersedes_version_id=superseded_ids[0] if superseded_ids else None,
                review_meta=review_meta,
            ),
        )
        self.instruments.append(new_ver)
        created_ids.append(vid)
        return IngestOutcome(
            event_id=envelope.event_id,
            status="APPLIED",
            version_ids=tuple(created_ids),
            message="Instrument version applied.",
        )

    # ------------------------------------------------------------------
    # Listing Upsert (with ticker collision & derived MIC_TICKER sync)
    # ------------------------------------------------------------------
    def _apply_listing_upsert(
        self,
        envelope: EventEnvelope,
        review_meta: Optional[Dict[str, Any]] = None,
        force_supersede: bool = False,
    ) -> IngestOutcome:
        p = envelope.payload
        listing_id = p["listing_id"]
        inst_id = p["instrument_id"]
        mic = p["mic"]
        ticker = p["ticker"]
        currency = p["currency"]
        is_primary = bool(p.get("is_primary", False))
        status = p["status"]

        # 1. Check same-listing overlapping versions
        overlapping_same = [
            (idx, rec)
            for idx, rec in enumerate(self.listings)
            if rec.listing_id == listing_id
            and rec.superseded_at is None
            and ranges_overlap(rec.valid_from, rec.valid_to, envelope.valid_from, envelope.valid_to)
        ]

        # 2. Check cross-listing (MIC, TICKER) collision over overlapping valid time
        colliding_other = [
            (idx, rec)
            for idx, rec in enumerate(self.listings)
            if rec.listing_id != listing_id
            and rec.mic == mic
            and rec.ticker == ticker
            and rec.superseded_at is None
            and ranges_overlap(rec.valid_from, rec.valid_to, envelope.valid_from, envelope.valid_to)
        ]

        if colliding_other and not force_supersede:
            ticket = self._open_conflict(
                conflict_type="LISTING_SYMBOL_COLLISION",
                entity_key=f"{mic}:{ticker}",
                created_at=envelope.observed_at,
                existing_claim={"listings": [rec.to_dict() for _, rec in colliding_other]},
                incoming_envelope=envelope,
            )
            return IngestOutcome(
                event_id=envelope.event_id,
                status="QUARANTINED",
                conflict_id=ticket.conflict_id,
                message=(
                    f"Overlapping listing collision for {mic}:{ticker} across distinct listings "
                    "quarantined for reviewer resolution."
                ),
            )

        if overlapping_same:
            all_identical = all(
                rec.instrument_id == inst_id
                and rec.mic == mic
                and rec.ticker == ticker
                and rec.currency == currency
                and rec.is_primary == is_primary
                and rec.status == status
                and rec.valid_from == envelope.valid_from
                and rec.valid_to == envelope.valid_to
                for _, rec in overlapping_same
            )
            if all_identical:
                return IngestOutcome(
                    event_id=envelope.event_id,
                    status="CORROBORATED",
                    version_ids=tuple(rec.version_id for _, rec in overlapping_same),
                    message="Incoming listing attributes match active belief.",
                )

            if not force_supersede and not all(
                self._is_authorized_supersede(envelope, rec.lineage) for _, rec in overlapping_same
            ):
                ticket = self._open_conflict(
                    conflict_type="LISTING_ATTRIBUTE_CONFLICT",
                    entity_key=listing_id,
                    created_at=envelope.observed_at,
                    existing_claim={"listings": [rec.to_dict() for _, rec in overlapping_same]},
                    incoming_envelope=envelope,
                )
                return IngestOutcome(
                    event_id=envelope.event_id,
                    status="QUARANTINED",
                    conflict_id=ticket.conflict_id,
                    message="Cross-source listing attribute conflict quarantined for reviewer resolution.",
                )

        # If reviewer forced supersede over a colliding other listing, supersede that too
        to_supersede = list(overlapping_same)
        if force_supersede and colliding_other:
            to_supersede.extend(colliding_other)

        superseded_ids: List[str] = []
        created_ids: List[str] = []
        for idx, rec in to_supersede:
            self.listings[idx] = replace(rec, superseded_at=envelope.observed_at)
            superseded_ids.append(rec.version_id)
            for rem_idx, (rem_start, rem_end) in enumerate(
                _subtract_interval(rec.valid_from, rec.valid_to, envelope.valid_from, envelope.valid_to)
            ):
                slice_vid = make_version_id(
                    "LSTV",
                    f"{rec.listing_id}:slice:{rem_idx}",
                    envelope.observed_at,
                    f"{envelope.source_event_id}:carry:{rec.version_id}",
                )
                self.listings.append(
                    replace(
                        rec,
                        version_id=slice_vid,
                        valid_from=rem_start,
                        valid_to=rem_end,
                        observed_at=envelope.observed_at,
                        superseded_at=None,
                        lineage=replace(rec.lineage, supersedes_version_id=rec.version_id),
                    )
                )
                created_ids.append(slice_vid)

        vid = make_version_id("LSTV", listing_id, envelope.observed_at, envelope.source_event_id)
        new_listing = ListingVersion(
            version_id=vid,
            listing_id=listing_id,
            instrument_id=inst_id,
            mic=mic,
            ticker=ticker,
            currency=currency,
            is_primary=is_primary,
            status=status,
            valid_from=envelope.valid_from,
            valid_to=envelope.valid_to,
            observed_at=envelope.observed_at,
            superseded_at=None,
            lineage=self._build_lineage(
                envelope,
                supersedes_version_id=superseded_ids[0] if superseded_ids else None,
                review_meta=review_meta,
            ),
        )
        self.listings.append(new_listing)
        created_ids.append(vid)

        # Synchronize derived MIC_TICKER and TICKER identifier mappings for this listing
        self._sync_listing_symbol_mappings(
            envelope=envelope,
            listing=new_listing,
            superseded_listings=[rec for _, rec in to_supersede],
            review_meta=review_meta,
        )

        return IngestOutcome(
            event_id=envelope.event_id,
            status="APPLIED",
            version_ids=tuple(created_ids),
            message="Listing version applied.",
        )

    def _sync_listing_symbol_mappings(
        self,
        envelope: EventEnvelope,
        listing: ListingVersion,
        superseded_listings: Sequence[ListingVersion],
        review_meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        affected_listing_ids = {listing.listing_id} | {rec.listing_id for rec in superseded_listings}
        for idx, mapping in enumerate(self.identifiers):
            if (
                mapping.superseded_at is None
                and mapping.listing_id in affected_listing_ids
                and mapping.namespace in (IdentifierNamespace.MIC_TICKER, IdentifierNamespace.TICKER)
                and ranges_overlap(
                    mapping.valid_from, mapping.valid_to, listing.valid_from, listing.valid_to
                )
            ):
                self.identifiers[idx] = replace(mapping, superseded_at=envelope.observed_at)
                for rem_idx, (rem_start, rem_end) in enumerate(
                    _subtract_interval(
                        mapping.valid_from, mapping.valid_to, listing.valid_from, listing.valid_to
                    )
                ):
                    carry_vid = make_version_id(
                        "IDM",
                        f"{mapping.listing_id}:{mapping.namespace.value}:slice:{rem_idx}",
                        envelope.observed_at,
                        f"{envelope.source_event_id}:carry:{mapping.version_id}",
                    )
                    self.identifiers.append(
                        replace(
                            mapping,
                            version_id=carry_vid,
                            valid_from=rem_start,
                            valid_to=rem_end,
                            observed_at=envelope.observed_at,
                            superseded_at=None,
                            lineage=replace(mapping.lineage, supersedes_version_id=mapping.version_id),
                        )
                    )

        mic_ticker_val = f"{listing.mic}:{listing.ticker}"
        for ns, val in (
            (IdentifierNamespace.MIC_TICKER, mic_ticker_val),
            (IdentifierNamespace.TICKER, listing.ticker),
        ):
            vid = make_version_id(
                "IDM",
                f"{listing.listing_id}:{ns.value}:{val}",
                envelope.observed_at,
                envelope.source_event_id,
            )
            self.identifiers.append(
                IdentifierMapping(
                    version_id=vid,
                    instrument_id=listing.instrument_id,
                    listing_id=listing.listing_id,
                    namespace=ns,
                    identifier_value=val,
                    mic=listing.mic,
                    valid_from=listing.valid_from,
                    valid_to=listing.valid_to,
                    observed_at=envelope.observed_at,
                    superseded_at=None,
                    lineage=self._build_lineage(envelope, review_meta=review_meta),
                )
            )

    # ------------------------------------------------------------------
    # Identifier Mapping (with forward + reverse conflict detection)
    # ------------------------------------------------------------------
    def _apply_identifier_map(
        self,
        envelope: EventEnvelope,
        review_meta: Optional[Dict[str, Any]] = None,
        force_supersede: bool = False,
    ) -> IngestOutcome:
        p = envelope.payload
        inst_id = p["instrument_id"]
        listing_id = p.get("listing_id")
        ns = IdentifierNamespace(p["namespace"])
        val = p["identifier_value"]
        mic = p.get("mic")

        # 1. Exact corroboration check
        exact = [
            rec
            for rec in self.identifiers
            if rec.superseded_at is None
            and rec.instrument_id == inst_id
            and rec.listing_id == listing_id
            and rec.namespace == ns
            and rec.identifier_value == val
            and rec.mic == mic
            and rec.valid_from == envelope.valid_from
            and rec.valid_to == envelope.valid_to
        ]
        if exact:
            return IngestOutcome(
                event_id=envelope.event_id,
                status="CORROBORATED",
                version_ids=tuple(rec.version_id for rec in exact),
                message="Incoming identifier mapping matches active belief.",
            )

        # 2. Reverse collision: globally unique (namespace, value) already points to a DIFFERENT instrument/listing
        reverse_conflicts: List[Tuple[int, IdentifierMapping]] = []
        if ns in GLOBALLY_UNIQUE_NAMESPACES:
            for idx, rec in enumerate(self.identifiers):
                if (
                    rec.superseded_at is None
                    and rec.namespace == ns
                    and rec.identifier_value == val
                    and ranges_overlap(
                        rec.valid_from, rec.valid_to, envelope.valid_from, envelope.valid_to
                    )
                ):
                    diff_target = rec.instrument_id != inst_id or (
                        ns == IdentifierNamespace.MIC_TICKER and rec.listing_id != listing_id
                    )
                    if diff_target:
                        reverse_conflicts.append((idx, rec))

        if reverse_conflicts and not force_supersede:
            ticket = self._open_conflict(
                conflict_type="IDENTIFIER_IDENTITY_COLLISION",
                entity_key=f"{ns.value}:{val}",
                created_at=envelope.observed_at,
                existing_claim={"mappings": [rec.to_dict() for _, rec in reverse_conflicts]},
                incoming_envelope=envelope,
            )
            return IngestOutcome(
                event_id=envelope.event_id,
                status="QUARANTINED",
                conflict_id=ticket.conflict_id,
                message=(
                    f"Identifier {ns.value}={val} already maps to another canonical identity "
                    "in an overlapping valid window; quarantined."
                ),
            )

        # 3. Forward conflict: same instrument+namespace already has a DIFFERENT value over overlapping valid time
        forward_conflicts: List[Tuple[int, IdentifierMapping]] = []
        if ns in SINGLE_VALUE_INSTRUMENT_NAMESPACES or ns == IdentifierNamespace.MIC_TICKER:
            for idx, rec in enumerate(self.identifiers):
                if (
                    rec.superseded_at is None
                    and rec.instrument_id == inst_id
                    and rec.listing_id == listing_id
                    and rec.namespace == ns
                    and ranges_overlap(
                        rec.valid_from, rec.valid_to, envelope.valid_from, envelope.valid_to
                    )
                ):
                    forward_conflicts.append((idx, rec))

        if forward_conflicts and not force_supersede:
            if not all(
                self._is_authorized_supersede(envelope, rec.lineage)
                for _, rec in forward_conflicts
            ):
                ticket = self._open_conflict(
                    conflict_type="CONFLICTING_IDENTIFIER_VALUE",
                    entity_key=f"{inst_id}:{ns.value}",
                    created_at=envelope.observed_at,
                    existing_claim={"mappings": [rec.to_dict() for _, rec in forward_conflicts]},
                    incoming_envelope=envelope,
                )
                return IngestOutcome(
                    event_id=envelope.event_id,
                    status="QUARANTINED",
                    conflict_id=ticket.conflict_id,
                    message=(
                        f"Conflicting {ns.value} values for {inst_id} across sources; "
                        "quarantined for reviewer resolution."
                    ),
                )

        to_supersede = list(forward_conflicts)
        if force_supersede:
            for item in reverse_conflicts:
                if item[0] not in {i for i, _ in to_supersede}:
                    to_supersede.append(item)

        superseded_ids: List[str] = []
        created_ids: List[str] = []
        for idx, rec in to_supersede:
            self.identifiers[idx] = replace(rec, superseded_at=envelope.observed_at)
            superseded_ids.append(rec.version_id)
            for rem_idx, (rem_start, rem_end) in enumerate(
                _subtract_interval(
                    rec.valid_from, rec.valid_to, envelope.valid_from, envelope.valid_to
                )
            ):
                carry_vid = make_version_id(
                    "IDM",
                    f"{rec.instrument_id}:{rec.namespace.value}:slice:{rem_idx}",
                    envelope.observed_at,
                    f"{envelope.source_event_id}:carry:{rec.version_id}",
                )
                self.identifiers.append(
                    replace(
                        rec,
                        version_id=carry_vid,
                        valid_from=rem_start,
                        valid_to=rem_end,
                        observed_at=envelope.observed_at,
                        superseded_at=None,
                        lineage=replace(rec.lineage, supersedes_version_id=rec.version_id),
                    )
                )
                created_ids.append(carry_vid)

        vid = make_version_id(
            "IDM", f"{inst_id}:{ns.value}:{val}", envelope.observed_at, envelope.source_event_id
        )
        self.identifiers.append(
            IdentifierMapping(
                version_id=vid,
                instrument_id=inst_id,
                listing_id=listing_id,
                namespace=ns,
                identifier_value=val,
                mic=mic,
                valid_from=envelope.valid_from,
                valid_to=envelope.valid_to,
                observed_at=envelope.observed_at,
                superseded_at=None,
                lineage=self._build_lineage(
                    envelope,
                    supersedes_version_id=superseded_ids[0] if superseded_ids else None,
                    review_meta=review_meta,
                ),
            )
        )
        created_ids.append(vid)
        return IngestOutcome(
            event_id=envelope.event_id,
            status="APPLIED",
            version_ids=tuple(created_ids),
            message="Identifier mapping applied.",
        )

    # ------------------------------------------------------------------
    # Corporate Action Linkage
    # ------------------------------------------------------------------
    def _apply_corporate_action_link(self, envelope: EventEnvelope) -> IngestOutcome:
        p = envelope.payload
        action_type = CorporateActionType(p["action_type"])
        from_inst = p["from_instrument_id"]
        to_inst = p["to_instrument_id"]
        from_listing = p.get("from_listing_id")
        to_listing = p.get("to_listing_id")
        ratio = Decimal(str(p.get("ratio", "1")))
        metadata = dict(p.get("metadata", {}))

        # Supersede prior same-source revision of the same action if specified
        superseded_id: Optional[str] = None
        if envelope.supersedes_event_id:
            for idx, existing in enumerate(self.corporate_actions):
                if (
                    existing.superseded_at is None
                    and existing.lineage.source_event_id == envelope.supersedes_event_id
                ):
                    self.corporate_actions[idx] = replace(
                        existing, superseded_at=envelope.observed_at
                    )
                    superseded_id = existing.link_id

        link_id = make_version_id(
            "ACT",
            f"{action_type.value}:{from_inst}:{to_inst}",
            envelope.observed_at,
            envelope.source_event_id,
        )
        link = CorporateActionLink(
            link_id=link_id,
            action_type=action_type,
            from_instrument_id=from_inst,
            to_instrument_id=to_inst,
            from_listing_id=from_listing,
            to_listing_id=to_listing,
            ratio=ratio,
            effective_at=envelope.valid_from,
            observed_at=envelope.observed_at,
            superseded_at=None,
            lineage=self._build_lineage(envelope, supersedes_version_id=superseded_id),
            metadata=metadata,
        )
        self.corporate_actions.append(link)
        return IngestOutcome(
            event_id=envelope.event_id,
            status="APPLIED",
            version_ids=(link_id,),
            message=f"Corporate action link {action_type.value} applied.",
        )

    # ------------------------------------------------------------------
    # Reviewer Correction Workflow
    # ------------------------------------------------------------------
    def _apply_review_correction(self, envelope: EventEnvelope) -> IngestOutcome:
        p = envelope.payload
        conflict_id = p["conflict_id"]
        ticket = self.conflicts.get(conflict_id)
        if ticket is None:
            raise NotFoundError(
                f"Conflict ticket '{conflict_id}' not found.",
                details={"conflict_id": conflict_id},
            )
        if ticket.status != ConflictStatus.OPEN:
            raise ValidationError(
                f"Conflict ticket '{conflict_id}' is already {ticket.status.value}.",
                details={"conflict_id": conflict_id, "status": ticket.status.value},
            )

        action = ReviewAction(p["action"])
        reviewer_id = p["reviewer_id"]
        rationale = p["rationale"]
        review_meta = {
            "reviewer_id": reviewer_id,
            "rationale": rationale,
            "conflict_id": conflict_id,
        }

        applied_versions: Tuple[str, ...] = ()
        if action in (ReviewAction.ACCEPT_INCOMING, ReviewAction.CUSTOM_OVERRIDE):
            incoming = EventEnvelope.from_dict(ticket.incoming_claim)
            override_payload = p.get("override_payload") if action == ReviewAction.CUSTOM_OVERRIDE else None
            effective_payload = dict(incoming.payload)
            if override_payload is not None:
                if not isinstance(override_payload, dict):
                    raise ValidationError("override_payload must be a JSON object.")
                effective_payload.update(override_payload)

            synthetic = EventEnvelope(
                event_id=f"{envelope.event_id}:APPLY",
                event_type=incoming.event_type,
                schema_version=envelope.schema_version,
                source_system="REVIEWER_WORKFLOW",
                source_event_id=f"{envelope.source_event_id}:APPLY",
                valid_from=envelope.valid_from if p.get("use_review_valid_window") else incoming.valid_from,
                valid_to=envelope.valid_to if p.get("use_review_valid_window") else incoming.valid_to,
                observed_at=envelope.observed_at,
                payload=effective_payload,
                supersedes_event_id=incoming.event_id,
            )
            validate_event(synthetic, self.config.licensing)

            if synthetic.event_type == EventType.INSTRUMENT_UPSERT:
                sub = self._apply_instrument_upsert(
                    synthetic, review_meta=review_meta, force_supersede=True
                )
            elif synthetic.event_type == EventType.LISTING_UPSERT:
                sub = self._apply_listing_upsert(
                    synthetic, review_meta=review_meta, force_supersede=True
                )
            elif synthetic.event_type == EventType.IDENTIFIER_MAP:
                sub = self._apply_identifier_map(
                    synthetic, review_meta=review_meta, force_supersede=True
                )
            else:
                raise ValidationError(
                    f"Cannot apply review resolution for event type '{synthetic.event_type.value}'."
                )
            applied_versions = sub.version_ids

        resolution_record = {
            "action": action.value,
            "reviewer_id": reviewer_id,
            "rationale": rationale,
            "review_event_id": envelope.event_id,
            "applied_version_ids": list(applied_versions),
        }
        self.conflicts[conflict_id] = replace(
            ticket,
            status=ConflictStatus.RESOLVED,
            resolved_at=envelope.observed_at,
            resolution=resolution_record,
        )
        return IngestOutcome(
            event_id=envelope.event_id,
            status="RESOLVED",
            version_ids=applied_versions,
            conflict_id=conflict_id,
            message=f"Conflict {conflict_id} resolved with {action.value} by {reviewer_id}.",
        )

    # ------------------------------------------------------------------
    # As-of Bitemporal Queries & Corporate-Action Lineage
    # ------------------------------------------------------------------
    def _find_blocking_conflicts(
        self,
        namespace: IdentifierNamespace,
        value: str,
        mic: Optional[str],
        candidate_instrument_ids: Set[str],
        candidate_listing_ids: Set[str],
    ) -> List[ConflictTicket]:
        if not self.config.conflict_resolution.block_lookup_on_open_conflict:
            return []
        blocking: List[ConflictTicket] = []
        lookup_keys = {f"{namespace.value}:{value}"}
        if mic and namespace == IdentifierNamespace.TICKER:
            lookup_keys.add(f"{mic}:{value}")
            lookup_keys.add(f"MIC_TICKER:{mic}:{value}")
        if namespace == IdentifierNamespace.MIC_TICKER:
            lookup_keys.add(value)
        for inst_id in candidate_instrument_ids:
            lookup_keys.add(inst_id)
            lookup_keys.add(f"{inst_id}:{namespace.value}")
        lookup_keys.update(candidate_listing_ids)

        for ticket in self.conflicts.values():
            if ticket.status != ConflictStatus.OPEN:
                continue
            if ticket.entity_key in lookup_keys:
                blocking.append(ticket)
                continue
            # Also check if ticket's entity_key is "<inst_id>:<any_ns>" for a matched instrument
            if any(ticket.entity_key.startswith(f"{inst_id}:") for inst_id in candidate_instrument_ids):
                blocking.append(ticket)
        return blocking

    def lookup_as_of(
        self,
        namespace: IdentifierNamespace,
        value: str,
        as_of_valid: datetime,
        as_of_observed: datetime,
        mic: Optional[str] = None,
    ) -> LookupResult:
        """Resolve an identifier to its canonical instrument & listing as-of (valid, observed)."""
        normalized_value = value.strip()
        if namespace == IdentifierNamespace.TICKER and mic:
            # Explicit MIC disambiguates a bare ticker into MIC_TICKER lookup
            effective_ns = IdentifierNamespace.MIC_TICKER
            effective_val = f"{mic.strip().upper()}:{normalized_value}"
            effective_mic = mic.strip().upper()
        else:
            effective_ns = namespace
            effective_val = normalized_value
            effective_mic = mic.strip().upper() if mic else None

        # Gather all mappings matching the requested identifier key (across all time)
        key_matches = [
            rec
            for rec in self.identifiers
            if rec.namespace == effective_ns
            and rec.identifier_value == effective_val
            and (effective_mic is None or rec.mic == effective_mic)
        ]

        active_matches = [
            rec for rec in key_matches if rec.is_active_at(as_of_valid, as_of_observed)
        ]

        # Check conflict blocking BEFORE returning a potentially disputed active match
        candidate_insts = {rec.instrument_id for rec in active_matches}
        candidate_listings = {rec.listing_id for rec in active_matches if rec.listing_id}
        blocking = self._find_blocking_conflicts(
            effective_ns, effective_val, effective_mic, candidate_insts, candidate_listings
        )
        if blocking:
            raise ConflictBlockedError(
                f"Lookup for {effective_ns.value}={effective_val} is blocked by "
                f"{len(blocking)} open conflict ticket(s).",
                details={
                    "namespace": effective_ns.value,
                    "value": effective_val,
                    "conflict_ids": [t.conflict_id for t in blocking],
                    "conflict_types": [t.conflict_type for t in blocking],
                },
            )

        if not active_matches:
            # Boundary check: did a valid record exist for as_of_valid, but only observed AFTER as_of_observed?
            future_observed = [
                rec
                for rec in key_matches
                if rec.valid_from <= as_of_valid
                and (rec.valid_to is None or as_of_valid < rec.valid_to)
                and rec.observed_at > as_of_observed
            ]
            if future_observed:
                earliest = min(rec.observed_at for rec in future_observed)
                raise PreObservationLookupError(
                    f"Historical lookup for {effective_ns.value}={effective_val} at "
                    f"as_of_observed={format_ts(as_of_observed)} precedes earliest observation "
                    f"time {format_ts(earliest)}.",
                    details={
                        "namespace": effective_ns.value,
                        "value": effective_val,
                        "as_of_valid": format_ts(as_of_valid),
                        "as_of_observed": format_ts(as_of_observed),
                        "earliest_observed_at": format_ts(earliest),
                    },
                )
            raise NotFoundError(
                f"No active mapping found for {effective_ns.value}={effective_val} "
                f"at valid={format_ts(as_of_valid)}, observed={format_ts(as_of_observed)}.",
                details={
                    "namespace": effective_ns.value,
                    "value": effective_val,
                    "mic": effective_mic,
                    "as_of_valid": format_ts(as_of_valid),
                    "as_of_observed": format_ts(as_of_observed),
                },
            )

        # Deduplicate distinct canonical targets: (instrument_id, listing_id)
        distinct_targets: Dict[Tuple[str, Optional[str]], IdentifierMapping] = {}
        for rec in active_matches:
            distinct_targets[(rec.instrument_id, rec.listing_id)] = rec

        if len(distinct_targets) > 1:
            candidates = []
            for (cid_inst, cid_lst), rec in sorted(
                distinct_targets.items(), key=lambda kv: (kv[0][0], kv[0][1] or "")
            ):
                lst_ver = self._active_listing(cid_lst, as_of_valid, as_of_observed) if cid_lst else None
                candidates.append(
                    {
                        "instrument_id": cid_inst,
                        "listing_id": cid_lst,
                        "mic": rec.mic or (lst_ver.mic if lst_ver else None),
                        "currency": lst_ver.currency if lst_ver else None,
                    }
                )
            raise AmbiguousMappingError(
                f"Ambiguous lookup for {effective_ns.value}={effective_val}: matched "
                f"{len(distinct_targets)} distinct listings/instruments. Specify MIC or canonical ID.",
                details={
                    "namespace": effective_ns.value,
                    "value": effective_val,
                    "as_of_valid": format_ts(as_of_valid),
                    "as_of_observed": format_ts(as_of_observed),
                    "candidates": candidates,
                },
            )

        (target_inst_id, target_listing_id), matched_mapping = next(iter(distinct_targets.items()))

        inst_ver = self._active_instrument(target_inst_id, as_of_valid, as_of_observed)
        if inst_ver is None:
            # Check if instrument itself was only observed later
            future_inst = [
                rec
                for rec in self.instruments
                if rec.instrument_id == target_inst_id
                and rec.valid_from <= as_of_valid
                and (rec.valid_to is None or as_of_valid < rec.valid_to)
                and rec.observed_at > as_of_observed
            ]
            if future_inst:
                earliest = min(rec.observed_at for rec in future_inst)
                raise PreObservationLookupError(
                    f"Instrument {target_inst_id} was not yet observed at {format_ts(as_of_observed)}.",
                    details={
                        "instrument_id": target_inst_id,
                        "as_of_observed": format_ts(as_of_observed),
                        "earliest_observed_at": format_ts(earliest),
                    },
                )
            raise NotFoundError(
                f"Canonical instrument {target_inst_id} has no active version at "
                f"valid={format_ts(as_of_valid)}, observed={format_ts(as_of_observed)}."
            )

        listing_ver: Optional[ListingVersion] = None
        if target_listing_id is not None:
            listing_ver = self._active_listing(target_listing_id, as_of_valid, as_of_observed)
        else:
            # Instrument-level identifier (e.g. ISIN/OPENFIGI): return primary listing if unique, else None
            active_listings = self.get_listings_for_instrument(
                target_inst_id, as_of_valid, as_of_observed
            )
            primaries = [item for item in active_listings if item.is_primary]
            if len(primaries) == 1:
                listing_ver = primaries[0]
            elif len(active_listings) == 1:
                listing_ver = active_listings[0]

        all_ids = tuple(
            sorted(
                (
                    rec
                    for rec in self.identifiers
                    if rec.instrument_id == target_inst_id
                    and rec.is_active_at(as_of_valid, as_of_observed)
                ),
                key=lambda r: (r.namespace.value, r.identifier_value, r.listing_id or ""),
            )
        )
        related_actions = tuple(
            sorted(
                (
                    act
                    for act in self.corporate_actions
                    if (
                        act.from_instrument_id == target_inst_id
                        or act.to_instrument_id == target_inst_id
                    )
                    and act.is_active_at(as_of_valid, as_of_observed)
                ),
                key=lambda a: (a.effective_at, a.link_id),
            )
        )
        return LookupResult(
            instrument=inst_ver,
            listing=listing_ver,
            matched_identifier=matched_mapping,
            identifiers=all_ids,
            corporate_actions=related_actions,
            as_of_valid=as_of_valid,
            as_of_observed=as_of_observed,
        )

    def _active_instrument(
        self, instrument_id: str, as_of_valid: datetime, as_of_observed: datetime
    ) -> Optional[InstrumentVersion]:
        matches = [
            rec
            for rec in self.instruments
            if rec.instrument_id == instrument_id and rec.is_active_at(as_of_valid, as_of_observed)
        ]
        if not matches:
            return None
        matches.sort(key=lambda r: r.observed_at)
        return matches[-1]

    def _active_listing(
        self, listing_id: str, as_of_valid: datetime, as_of_observed: datetime
    ) -> Optional[ListingVersion]:
        matches = [
            rec
            for rec in self.listings
            if rec.listing_id == listing_id and rec.is_active_at(as_of_valid, as_of_observed)
        ]
        if not matches:
            return None
        matches.sort(key=lambda r: r.observed_at)
        return matches[-1]

    def get_listings_for_instrument(
        self, instrument_id: str, as_of_valid: datetime, as_of_observed: datetime
    ) -> List[ListingVersion]:
        """Return all active exchange listings for an instrument (supports dual listings)."""
        results = [
            rec
            for rec in self.listings
            if rec.instrument_id == instrument_id and rec.is_active_at(as_of_valid, as_of_observed)
        ]
        results.sort(key=lambda r: (not r.is_primary, r.mic, r.currency, r.listing_id))
        return results

    def trace_corporate_action_lineage(
        self,
        instrument_id: str,
        as_of_valid: datetime,
        as_of_observed: datetime,
    ) -> Dict[str, Any]:
        """Traverse predecessor and successor corporate-action links as-of (valid, observed)."""
        active_links = [
            act for act in self.corporate_actions if act.is_active_at(as_of_valid, as_of_observed)
        ]
        # Walk forward successors (BFS, cycle-safe)
        successors: List[CorporateActionLink] = []
        visited_fwd: Set[str] = {instrument_id}
        frontier = [instrument_id]
        while frontier:
            curr = frontier.pop(0)
            for act in sorted(active_links, key=lambda a: (a.effective_at, a.link_id)):
                if act.from_instrument_id == curr:
                    if act not in successors:
                        successors.append(act)
                    if act.to_instrument_id not in visited_fwd:
                        visited_fwd.add(act.to_instrument_id)
                        frontier.append(act.to_instrument_id)

        # Walk backward predecessors (BFS, cycle-safe)
        predecessors: List[CorporateActionLink] = []
        visited_bwd: Set[str] = {instrument_id}
        frontier_bwd = [instrument_id]
        while frontier_bwd:
            curr = frontier_bwd.pop(0)
            for act in sorted(active_links, key=lambda a: (a.effective_at, a.link_id)):
                if act.to_instrument_id == curr:
                    if act not in predecessors:
                        predecessors.append(act)
                    if act.from_instrument_id not in visited_bwd:
                        visited_bwd.add(act.from_instrument_id)
                        frontier_bwd.append(act.from_instrument_id)

        # Terminal canonical successor(s) for mergers/renames
        terminal_ids: List[str] = []
        outgoing_map: Dict[str, List[str]] = {}
        for act in successors:
            if act.action_type in (CorporateActionType.MERGER, CorporateActionType.TICKER_CHANGE):
                outgoing_map.setdefault(act.from_instrument_id, []).append(act.to_instrument_id)
        curr_nodes = [instrument_id]
        seen_term: Set[str] = set()
        while curr_nodes:
            node = curr_nodes.pop(0)
            if node in seen_term:
                continue
            seen_term.add(node)
            next_nodes = [n for n in outgoing_map.get(node, []) if n != node]
            if not next_nodes:
                terminal_ids.append(node)
            else:
                curr_nodes.extend(next_nodes)

        return {
            "instrument_id": instrument_id,
            "as_of_valid": format_ts(as_of_valid),
            "as_of_observed": format_ts(as_of_observed),
            "terminal_instrument_ids": sorted(set(terminal_ids)),
            "predecessors": [act.to_dict() for act in predecessors],
            "successors": [act.to_dict() for act in successors],
        }

    # ------------------------------------------------------------------
    # Quality, Completeness & Freshness Metrics
    # ------------------------------------------------------------------
    def compute_quality_metrics(self, as_of_observed: datetime) -> QualityMetrics:
        """Compute completeness, freshness, conflict queue health, and lineage coverage."""
        active_insts = [
            rec
            for rec in self.instruments
            if rec.observed_at <= as_of_observed
            and (rec.superseded_at is None or as_of_observed < rec.superseded_at)
        ]
        inst_ids = sorted({rec.instrument_id for rec in active_insts})

        active_listings = [
            rec
            for rec in self.listings
            if rec.observed_at <= as_of_observed
            and (rec.superseded_at is None or as_of_observed < rec.superseded_at)
        ]
        active_ids = [
            rec
            for rec in self.identifiers
            if rec.observed_at <= as_of_observed
            and (rec.superseded_at is None or as_of_observed < rec.superseded_at)
        ]
        active_actions = [
            rec
            for rec in self.corporate_actions
            if rec.observed_at <= as_of_observed
            and (rec.superseded_at is None or as_of_observed < rec.superseded_at)
        ]

        # Completeness: every active instrument has all configured required namespaces
        req_ns = set(self.config.quality.required_instrument_namespaces)
        incomplete: List[str] = []
        for iid in inst_ids:
            present = {rec.namespace.value for rec in active_ids if rec.instrument_id == iid}
            if not req_ns.issubset(present):
                incomplete.append(iid)
        completeness = (
            1.0 if not inst_ids else (len(inst_ids) - len(incomplete)) / float(len(inst_ids))
        )

        # Lineage coverage across all materialized versions
        all_lineages: List[Lineage] = (
            [r.lineage for r in self.instruments]
            + [r.lineage for r in self.listings]
            + [r.lineage for r in self.identifiers]
            + [r.lineage for r in self.corporate_actions]
        )
        valid_lineage_count = sum(
            1
            for lin in all_lineages
            if bool(lin.source_system and lin.source_event_id and lin.schema_version)
        )
        lineage_cov = (
            1.0 if not all_lineages else valid_lineage_count / float(len(all_lineages))
        )

        # Freshness: max observation lag (observed_at - valid_from) and stale sources vs as_of_observed
        max_lag = 0.0
        latest_by_source: Dict[str, datetime] = {}
        for ev in self._ordered_events:
            if ev.observed_at > as_of_observed:
                continue
            lag = max(0.0, (ev.observed_at - ev.valid_from).total_seconds())
            if lag > max_lag:
                max_lag = lag
            prev = latest_by_source.get(ev.source_system)
            if prev is None or ev.observed_at > prev:
                latest_by_source[ev.source_system] = ev.observed_at

        stale_threshold = float(self.config.quality.max_freshness_lag_seconds)
        stale_sources = sorted(
            src
            for src, last_ts in latest_by_source.items()
            if src != "REVIEWER_WORKFLOW"
            and (as_of_observed - last_ts).total_seconds() > stale_threshold
        )

        open_conflicts = sum(
            1 for t in self.conflicts.values() if t.status == ConflictStatus.OPEN
        )
        resolved_conflicts = sum(
            1 for t in self.conflicts.values() if t.status == ConflictStatus.RESOLVED
        )

        return QualityMetrics(
            as_of_observed=as_of_observed,
            total_events=len(self._ordered_events),
            active_instruments=len(inst_ids),
            active_listings=len({r.listing_id for r in active_listings}),
            active_identifier_mappings=len(active_ids),
            corporate_action_links=len(active_actions),
            open_conflicts=open_conflicts,
            resolved_conflicts=resolved_conflicts,
            completeness_ratio=completeness,
            incomplete_instruments=tuple(incomplete),
            lineage_coverage_ratio=lineage_cov,
            max_observation_lag_seconds=max_lag,
            stale_sources=tuple(stale_sources),
        )

    def snapshot_summary(self) -> Dict[str, Any]:
        """Return a deterministic logical snapshot for reproducibility & baseline comparisons."""
        return {
            "event_count": len(self._ordered_events),
            "event_ids": [e.event_id for e in self._ordered_events],
            "instruments": [r.to_dict() for r in self.instruments],
            "listings": [r.to_dict() for r in self.listings],
            "identifiers": [r.to_dict() for r in self.identifiers],
            "corporate_actions": [r.to_dict() for r in self.corporate_actions],
            "conflicts": [
                self.conflicts[cid].to_dict() for cid in sorted(self.conflicts.keys())
            ],
        }
