"""Application service wiring the pure bitemporal domain engine, durable store,
structured logging, diagnostic counters, replay, and reviewer correction workflow.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from .adapters import raw_record_to_envelope
from .config import PlatformConfig
from .domain import BitemporalStateEngine, IngestOutcome
from .errors import SecurityMasterError
from .models import (
    ConflictStatus,
    ConflictTicket,
    EventEnvelope,
    EventType,
    IdentifierNamespace,
    ListingVersion,
    LookupResult,
    QualityMetrics,
    ReviewAction,
    SCHEMA_VERSION,
    format_ts,
    make_deterministic_event_id,
)
from .storage import EventStore, InMemoryEventStore, SQLiteBitemporalStore


class SecurityMasterService:
    """High-level typed service interface for ingestion, queries, replay, and review."""

    def __init__(
        self,
        config: PlatformConfig,
        store: Optional[EventStore] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        if store is not None:
            self.store: EventStore = store
        elif config.storage.backend == "memory":
            self.store = InMemoryEventStore()
        else:
            self.store = SQLiteBitemporalStore(config.storage.sqlite_path)

        self.logger = logger or logging.getLogger("sec_master")
        self.engine = BitemporalStateEngine(config)
        self.counters: Dict[str, int] = {
            "events_ingested": 0,
            "events_applied": 0,
            "events_corroborated": 0,
            "events_duplicate_skipped": 0,
            "events_quarantined": 0,
            "events_rejected": 0,
            "lookups_succeeded": 0,
            "lookups_failed": 0,
            "conflicts_resolved": 0,
            "replays_executed": 0,
        }
        # Rehydrate state automatically on startup from durable event journal
        self._rehydrate_from_store()

    def _emit_log(self, event_name: str, fields: Dict[str, Any]) -> None:
        record = {"event": event_name, **fields}
        self.logger.info(json.dumps(record, sort_keys=True))

    def _rehydrate_from_store(self) -> int:
        events = self.store.load_events()
        for ev in events:
            self.engine.apply_event(ev)
        if events:
            self.store.sync_materialized_state(self.engine)
        return len(events)

    def ingest_event(self, envelope: EventEnvelope) -> IngestOutcome:
        """Validate, apply, durably persist, and emit structured diagnostics for one event."""
        self.counters["events_ingested"] += 1
        try:
            outcome = self.engine.apply_event(envelope)
        except SecurityMasterError as exc:
            self.counters["events_rejected"] += 1
            self._emit_log(
                "ingest_rejected",
                {
                    "event_id": envelope.event_id,
                    "source_system": envelope.source_system,
                    "error_code": exc.code,
                    "message": exc.message,
                },
            )
            raise

        if outcome.status != "DUPLICATE_SKIPPED":
            self.store.append_event(envelope)
            self.store.sync_materialized_state(self.engine)

        if outcome.status == "APPLIED":
            self.counters["events_applied"] += 1
        elif outcome.status == "CORROBORATED":
            self.counters["events_corroborated"] += 1
        elif outcome.status == "DUPLICATE_SKIPPED":
            self.counters["events_duplicate_skipped"] += 1
        elif outcome.status == "QUARANTINED":
            self.counters["events_quarantined"] += 1
        elif outcome.status == "RESOLVED":
            self.counters["conflicts_resolved"] += 1

        self._emit_log(
            "ingest_outcome",
            {
                "event_id": outcome.event_id,
                "event_type": envelope.event_type.value,
                "source_system": envelope.source_system,
                "status": outcome.status,
                "conflict_id": outcome.conflict_id,
                "version_count": len(outcome.version_ids),
            },
        )
        return outcome

    def ingest_raw_records(self, raw_records: Sequence[Dict[str, Any]]) -> List[IngestOutcome]:
        """Adapt and ingest a batch of raw event dicts in order."""
        outcomes: List[IngestOutcome] = []
        for raw in raw_records:
            env = raw_record_to_envelope(raw)
            outcomes.append(self.ingest_event(env))
        return outcomes

    def lookup_as_of(
        self,
        namespace: IdentifierNamespace,
        value: str,
        as_of_valid: datetime,
        as_of_observed: datetime,
        mic: Optional[str] = None,
    ) -> LookupResult:
        """Perform an audited bitemporal lookup."""
        try:
            res = self.engine.lookup_as_of(
                namespace=namespace,
                value=value,
                as_of_valid=as_of_valid,
                as_of_observed=as_of_observed,
                mic=mic,
            )
            self.counters["lookups_succeeded"] += 1
            self._emit_log(
                "lookup_succeeded",
                {
                    "namespace": namespace.value,
                    "value": value,
                    "mic": mic,
                    "instrument_id": res.instrument.instrument_id,
                    "listing_id": res.listing.listing_id if res.listing else None,
                    "as_of_valid": format_ts(as_of_valid),
                    "as_of_observed": format_ts(as_of_observed),
                },
            )
            return res
        except SecurityMasterError as exc:
            self.counters["lookups_failed"] += 1
            self._emit_log(
                "lookup_failed",
                {
                    "namespace": namespace.value,
                    "value": value,
                    "mic": mic,
                    "error_code": exc.code,
                    "as_of_valid": format_ts(as_of_valid),
                    "as_of_observed": format_ts(as_of_observed),
                },
            )
            raise

    def get_listings(
        self, instrument_id: str, as_of_valid: datetime, as_of_observed: datetime
    ) -> List[ListingVersion]:
        return self.engine.get_listings_for_instrument(instrument_id, as_of_valid, as_of_observed)

    def trace_corporate_actions(
        self, instrument_id: str, as_of_valid: datetime, as_of_observed: datetime
    ) -> Dict[str, Any]:
        return self.engine.trace_corporate_action_lineage(
            instrument_id, as_of_valid, as_of_observed
        )

    def list_conflicts(self, status: Optional[ConflictStatus] = None) -> List[ConflictTicket]:
        tickets = [
            self.engine.conflicts[cid] for cid in sorted(self.engine.conflicts.keys())
        ]
        if status is not None:
            tickets = [t for t in tickets if t.status == status]
        return tickets

    def resolve_conflict(
        self,
        conflict_id: str,
        reviewer_id: str,
        action: ReviewAction,
        rationale: str,
        observed_at: Optional[datetime] = None,
        override_payload: Optional[Dict[str, Any]] = None,
    ) -> IngestOutcome:
        """Submit an audited reviewer decision that resolves an open conflict ticket."""
        obs_ts = observed_at or datetime.now(timezone.utc)
        source_event_id = f"REV-{conflict_id}-{format_ts(obs_ts)}"
        envelope = EventEnvelope(
            event_id=make_deterministic_event_id(
                "REVIEWER_WORKFLOW", source_event_id, EventType.REVIEW_CORRECTION.value
            ),
            event_type=EventType.REVIEW_CORRECTION,
            schema_version=SCHEMA_VERSION,
            source_system="REVIEWER_WORKFLOW",
            source_event_id=source_event_id,
            valid_from=obs_ts,
            valid_to=None,
            observed_at=obs_ts,
            payload={
                "conflict_id": conflict_id,
                "reviewer_id": reviewer_id,
                "action": action.value,
                "rationale": rationale,
                "override_payload": override_payload,
            },
        )
        return self.ingest_event(envelope)

    def quality_metrics(self, as_of_observed: Optional[datetime] = None) -> QualityMetrics:
        obs_ts = as_of_observed or datetime.now(timezone.utc)
        return self.engine.compute_quality_metrics(obs_ts)

    def replay_from_journal(self) -> Dict[str, Any]:
        """Rebuild the bitemporal state from scratch by replaying the durable event journal."""
        events = self.store.load_events()
        fresh_engine = BitemporalStateEngine(self.config)
        for ev in events:
            fresh_engine.apply_event(ev)
        self.engine = fresh_engine
        self.store.sync_materialized_state(self.engine)
        self.counters["replays_executed"] += 1
        self._emit_log("journal_replayed", {"replayed_events": len(events)})
        return {
            "replayed_events": len(events),
            "snapshot_hash": self._snapshot_digest(),
        }

    def _snapshot_digest(self) -> str:
        import hashlib

        raw = json.dumps(self.engine.snapshot_summary(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def diagnostics(self) -> Dict[str, Any]:
        return {
            "config_version": self.config.config_version,
            "schema_version": self.config.schema_version,
            "storage_backend": self.config.storage.backend,
            "counters": dict(self.counters),
            "snapshot_hash": self._snapshot_digest(),
        }

    def close(self) -> None:
        self.store.close()
