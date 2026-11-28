"""Core OptionsMarketDataPlatform orchestrator connecting adapters, domain, and storage."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from omdp.adapters.json_adapter import FixtureBundle, SnapshotBatchInput
from omdp.config import PlatformConfig
from omdp.domain.contracts import (
    ChainSnapshot,
    ContractMetadata,
    DerivedOptionFeature,
    ExpiryCalendarEntry,
    IVStatus,
    OptionQuote,
    QualityReport,
    RateCurvePoint,
    SnapshotConsistencyError,
    UnderlyingQuote,
    format_utc_timestamp,
)
from omdp.domain.features import derive_chain_features
from omdp.domain.quality import build_quality_report
from omdp.domain.validation import validate_and_build_chain_snapshot
from omdp.storage.repository import StorageRepository
from omdp.storage.sqlite_store import SQLiteOptionStore


@dataclass
class DiagnosticCounters:
    contracts_ingested: int = 0
    calendars_ingested: int = 0
    rate_points_ingested: int = 0
    underlying_quotes_ingested: int = 0
    snapshots_ingested: int = 0
    quotes_processed: int = 0
    quotes_valid: int = 0
    quotes_flagged: int = 0
    quotes_rejected: int = 0
    features_derived: int = 0
    ivs_computed: int = 0
    replays_executed: int = 0
    replay_mismatches: int = 0
    consistency_errors: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "contracts_ingested": self.contracts_ingested,
            "calendars_ingested": self.calendars_ingested,
            "rate_points_ingested": self.rate_points_ingested,
            "underlying_quotes_ingested": self.underlying_quotes_ingested,
            "snapshots_ingested": self.snapshots_ingested,
            "quotes_processed": self.quotes_processed,
            "quotes_valid": self.quotes_valid,
            "quotes_flagged": self.quotes_flagged,
            "quotes_rejected": self.quotes_rejected,
            "features_derived": self.features_derived,
            "ivs_computed": self.ivs_computed,
            "replays_executed": self.replays_executed,
            "replay_mismatches": self.replay_mismatches,
            "consistency_errors": self.consistency_errors,
        }


@dataclass(frozen=True)
class IngestResult:
    snapshot: ChainSnapshot
    features: Tuple[DerivedOptionFeature, ...]
    quality_report: QualityReport

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot": self.snapshot.to_dict(),
            "features": [f.to_dict() for f in self.features],
            "quality_report": self.quality_report.to_dict(),
        }


@dataclass(frozen=True)
class ReplayResult:
    snapshot_id: str
    stored_snapshot_hash: str
    replayed_snapshot_hash: str
    snapshot_hash_match: bool
    stored_feature_ids: Tuple[str, ...]
    replayed_feature_ids: Tuple[str, ...]
    feature_ids_match: bool
    snapshot: ChainSnapshot
    features: Tuple[DerivedOptionFeature, ...]
    quality_report: QualityReport

    @property
    def is_identical(self) -> bool:
        return self.snapshot_hash_match and self.feature_ids_match

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "stored_snapshot_hash": self.stored_snapshot_hash,
            "replayed_snapshot_hash": self.replayed_snapshot_hash,
            "snapshot_hash_match": self.snapshot_hash_match,
            "feature_ids_match": self.feature_ids_match,
            "is_identical": self.is_identical,
            "stored_feature_count": len(self.stored_feature_ids),
            "replayed_feature_count": len(self.replayed_feature_ids),
            "snapshot": self.snapshot.to_dict(),
            "features": [f.to_dict() for f in self.features],
            "quality_report": self.quality_report.to_dict(),
        }


class OptionsMarketDataPlatform:
    """High-level service coordinating ingestion, feature derivation, querying, and replay."""

    def __init__(
        self,
        config: PlatformConfig,
        repository: Optional[StorageRepository] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        self.repo: StorageRepository = (
            repository if repository is not None else SQLiteOptionStore(config.storage.sqlite_path)
        )
        self.repo.initialize()
        self.logger = logger or logging.getLogger("omdp.platform")
        self.counters = DiagnosticCounters()
        self._structured_events: List[Dict[str, Any]] = []

    def close(self) -> None:
        self.repo.close()

    def _emit_log(self, event: str, **fields: Any) -> None:
        record = {
            "event": event,
            "timestamp": format_utc_timestamp(datetime.now(timezone.utc)),
            **fields,
        }
        self._structured_events.append(record)
        self.logger.info(json.dumps(record, sort_keys=True))

    @property
    def structured_events(self) -> List[Dict[str, Any]]:
        return list(self._structured_events)

    def register_contracts(self, contracts: Sequence[ContractMetadata]) -> int:
        count = self.repo.upsert_contracts(contracts)
        self.counters.contracts_ingested += count
        self._emit_log("contracts_registered", count=count)
        return count

    def register_calendars(self, entries: Sequence[ExpiryCalendarEntry]) -> int:
        count = self.repo.upsert_calendar_entries(entries)
        self.counters.calendars_ingested += count
        self._emit_log("calendars_registered", count=count)
        return count

    def register_rate_curves(self, points: Sequence[RateCurvePoint]) -> int:
        count = self.repo.upsert_rate_curve_points(points)
        self.counters.rate_points_ingested += count
        self._emit_log("rate_curves_registered", count=count)
        return count

    def ingest_underlying_quotes(self, quotes: Sequence[UnderlyingQuote]) -> int:
        count = self.repo.upsert_underlying_quotes(quotes)
        self.counters.underlying_quotes_ingested += count
        self._emit_log("underlying_quotes_ingested", count=count)
        return count

    def ingest_chain_snapshot(
        self,
        snapshot_id: str,
        underlying_symbol: str,
        snapshot_time: datetime,
        ingestion_time: datetime,
        source_id: str,
        schema_version: str,
        quotes: Sequence[OptionQuote],
    ) -> IngestResult:
        """Validate, persist, derive IV features, and record quality report for one chain snapshot."""
        contracts = self.repo.get_latest_contracts(underlying_symbol)
        calendar = self.repo.get_latest_calendar()

        try:
            snapshot = validate_and_build_chain_snapshot(
                snapshot_id=snapshot_id,
                underlying_symbol=underlying_symbol,
                snapshot_time=snapshot_time,
                ingestion_time=ingestion_time,
                source_id=source_id,
                schema_version=schema_version,
                quotes=quotes,
                contracts_by_id=contracts,
                calendar_by_date=calendar,
                config=self.config,
            )
        except SnapshotConsistencyError as exc:
            self.counters.consistency_errors += 1
            self._emit_log(
                "snapshot_consistency_error",
                snapshot_id=snapshot_id,
                underlying_symbol=underlying_symbol,
                error_code=exc.code,
                detail=str(exc),
            )
            raise

        self.repo.save_chain_snapshot(snapshot)

        underlying_quotes = self.repo.get_underlying_quotes(underlying_symbol, up_to_time=snapshot_time)
        rate_points = self.repo.get_latest_rate_curve(as_of_time=snapshot_time)

        features = derive_chain_features(
            snapshot=snapshot,
            contracts_by_id=contracts,
            underlying_quotes=underlying_quotes,
            rate_points=rate_points,
            calendar_by_date=calendar,
            config=self.config,
        )
        self.repo.save_derived_features(features)

        report = build_quality_report(
            snapshot=snapshot,
            features=features,
            generated_at=snapshot.ingestion_time,
        )
        self.repo.save_quality_report(report)

        self.counters.snapshots_ingested += 1
        self.counters.quotes_processed += snapshot.quote_count
        self.counters.quotes_valid += snapshot.valid_quote_count
        self.counters.quotes_flagged += snapshot.flagged_quote_count
        self.counters.quotes_rejected += snapshot.rejected_quote_count
        self.counters.features_derived += len(features)
        self.counters.ivs_computed += sum(1 for f in features if f.iv_status == IVStatus.COMPUTED)

        self._emit_log(
            "snapshot_ingested",
            snapshot_id=snapshot.snapshot_id,
            underlying_symbol=snapshot.underlying_symbol,
            snapshot_hash=snapshot.snapshot_hash,
            quote_count=snapshot.quote_count,
            valid_quotes=snapshot.valid_quote_count,
            flagged_quotes=snapshot.flagged_quote_count,
            rejected_quotes=snapshot.rejected_quote_count,
            computed_ivs=report.computed_iv_count,
            completeness_ratio=round(report.completeness_ratio, 6),
        )
        return IngestResult(snapshot=snapshot, features=features, quality_report=report)

    def ingest_bundle(self, bundle: FixtureBundle) -> List[IngestResult]:
        """Ingest all reference data, underlying quotes, and chain snapshots from a FixtureBundle."""
        if bundle.contracts:
            self.register_contracts(bundle.contracts)
        if bundle.calendars:
            self.register_calendars(bundle.calendars)
        if bundle.rate_curves:
            self.register_rate_curves(bundle.rate_curves)
        if bundle.underlying_quotes:
            self.ingest_underlying_quotes(bundle.underlying_quotes)

        results: List[IngestResult] = []
        for batch in bundle.snapshots:
            results.append(self._ingest_batch(batch))
        return results

    def _ingest_batch(self, batch: SnapshotBatchInput) -> IngestResult:
        return self.ingest_chain_snapshot(
            snapshot_id=batch.snapshot_id,
            underlying_symbol=batch.underlying_symbol,
            snapshot_time=batch.snapshot_time,
            ingestion_time=batch.ingestion_time,
            source_id=batch.source_id,
            schema_version=batch.schema_version,
            quotes=batch.quotes,
        )

    def get_chain_snapshot(self, snapshot_id: str) -> Optional[ChainSnapshot]:
        return self.repo.get_chain_snapshot(snapshot_id)

    def query_historical_chains(
        self,
        underlying_symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[ChainSnapshot]:
        return self.repo.list_chain_snapshots(
            underlying_symbol=underlying_symbol,
            start_time=start_time,
            end_time=end_time,
        )

    def get_derived_features(self, snapshot_id: str) -> List[DerivedOptionFeature]:
        return self.repo.get_derived_features(snapshot_id)

    def get_quality_report(self, snapshot_id: str) -> Optional[QualityReport]:
        return self.repo.get_quality_report(snapshot_id)

    def replay_snapshot(self, snapshot_id: str) -> ReplayResult:
        """Deterministically reconstruct a stored snapshot & its derived features from raw persisted inputs.

        Verifies exact snapshot_hash and derived feature_id reproducibility.
        """
        stored_snapshot = self.repo.get_chain_snapshot(snapshot_id)
        if stored_snapshot is None:
            raise KeyError(f"Snapshot '{snapshot_id}' not found in storage")
        stored_features = self.repo.get_derived_features(snapshot_id)

        raw_quotes = [vq.quote for vq in stored_snapshot.quotes]
        contracts = self.repo.get_latest_contracts(stored_snapshot.underlying_symbol)
        # If stored features referenced specific contract versions, load those exact versions
        for feat in stored_features:
            if feat.provenance.contract_version is not None:
                pinned = self.repo.get_contract_version(feat.option_id, feat.provenance.contract_version)
                if pinned is not None:
                    contracts[feat.option_id] = pinned

        calendar = self.repo.get_latest_calendar()
        replayed_snapshot = validate_and_build_chain_snapshot(
            snapshot_id=stored_snapshot.snapshot_id,
            underlying_symbol=stored_snapshot.underlying_symbol,
            snapshot_time=stored_snapshot.snapshot_time,
            ingestion_time=stored_snapshot.ingestion_time,
            source_id=stored_snapshot.source_id,
            schema_version=stored_snapshot.schema_version,
            quotes=raw_quotes,
            contracts_by_id=contracts,
            calendar_by_date=calendar,
            config=self.config,
        )

        underlying_quotes = self.repo.get_underlying_quotes(
            stored_snapshot.underlying_symbol,
            up_to_time=stored_snapshot.snapshot_time,
        )
        rate_points = self.repo.get_latest_rate_curve(as_of_time=stored_snapshot.snapshot_time)

        replayed_features = derive_chain_features(
            snapshot=replayed_snapshot,
            contracts_by_id=contracts,
            underlying_quotes=underlying_quotes,
            rate_points=rate_points,
            calendar_by_date=calendar,
            config=self.config,
        )
        replayed_report = build_quality_report(
            snapshot=replayed_snapshot,
            features=replayed_features,
            generated_at=replayed_snapshot.ingestion_time,
        )

        stored_fids = tuple(sorted(f.feature_id for f in stored_features))
        replayed_fids = tuple(sorted(f.feature_id for f in replayed_features))
        snap_match = stored_snapshot.snapshot_hash == replayed_snapshot.snapshot_hash
        feat_match = stored_fids == replayed_fids

        self.counters.replays_executed += 1
        if not (snap_match and feat_match):
            self.counters.replay_mismatches += 1

        self._emit_log(
            "snapshot_replayed",
            snapshot_id=snapshot_id,
            snapshot_hash_match=snap_match,
            feature_ids_match=feat_match,
            stored_snapshot_hash=stored_snapshot.snapshot_hash,
            replayed_snapshot_hash=replayed_snapshot.snapshot_hash,
        )

        return ReplayResult(
            snapshot_id=snapshot_id,
            stored_snapshot_hash=stored_snapshot.snapshot_hash,
            replayed_snapshot_hash=replayed_snapshot.snapshot_hash,
            snapshot_hash_match=snap_match,
            stored_feature_ids=stored_fids,
            replayed_feature_ids=replayed_fids,
            feature_ids_match=feat_match,
            snapshot=replayed_snapshot,
            features=replayed_features,
            quality_report=replayed_report,
        )

    def replay_range(
        self,
        underlying_symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[ReplayResult]:
        """Replay all historical snapshots in a time window in deterministic chronological order."""
        snapshots = self.query_historical_chains(
            underlying_symbol=underlying_symbol,
            start_time=start_time,
            end_time=end_time,
        )
        return [self.replay_snapshot(s.snapshot_id) for s in snapshots]
