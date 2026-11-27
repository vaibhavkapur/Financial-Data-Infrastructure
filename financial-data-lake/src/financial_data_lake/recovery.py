"""Backfill, deterministic replay from immutable raw landing, corruption recovery, and retention."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from financial_data_lake.catalog import CatalogRegistry
from financial_data_lake.config import LakeConfig
from financial_data_lake.errors import CorruptPartitionError, RecoveryError
from financial_data_lake.ingest import IngestionPipeline, IngestResult
from financial_data_lake.metrics import MetricsCollector
from financial_data_lake.models import AccessContext, PartitionManifest, ensure_utc
from financial_data_lake.storage import LocalObjectStore


@dataclass(frozen=True)
class IntegrityCheckReport:
    checked_at: datetime
    total_active_partitions: int
    healthy_partitions: int
    corrupt_partition_ids: list[str]
    details: dict[str, str]


@dataclass(frozen=True)
class RecoveryResult:
    corrupt_partition_id: str
    quarantine_path: str
    raw_source_partition_id: str
    recovered_partitions: list[PartitionManifest]
    recovered_rows: int


class RecoveryManager:
    """Handles integrity verification, quarantine/recovery, deterministic replay, backfill, and retention."""

    def __init__(
        self,
        *,
        config: LakeConfig,
        storage: LocalObjectStore,
        catalog: CatalogRegistry,
        ingest: IngestionPipeline,
        metrics: MetricsCollector,
    ) -> None:
        self.config = config
        self.storage = storage
        self.catalog = catalog
        self.ingest = ingest
        self.metrics = metrics

    def verify_integrity(self, dataset_id: str | None = None) -> IntegrityCheckReport:
        """Verify SHA-256 checksums and Parquet readability of all active partitions."""
        partitions = self.catalog.list_partitions(dataset_id=dataset_id, status="ACTIVE")
        corrupt_ids: list[str] = []
        details: dict[str, str] = {}

        for p in partitions:
            try:
                if p.layer == "raw":
                    self.storage.read_bytes(p.object_path, expected_sha256=p.sha256)
                else:
                    self.storage.read_parquet(p.object_path, expected_sha256=p.sha256)
            except CorruptPartitionError as exc:
                corrupt_ids.append(p.partition_id)
                details[p.partition_id] = exc.message
                self.metrics.increment(
                    "integrity.corrupt_detected",
                    tags={"dataset_id": p.dataset_id, "partition_id": p.partition_id},
                )

        return IntegrityCheckReport(
            checked_at=datetime.now(UTC),
            total_active_partitions=len(partitions),
            healthy_partitions=len(partitions) - len(corrupt_ids),
            corrupt_partition_ids=corrupt_ids,
            details=details,
        )

    def _find_raw_partition_for_run(self, dataset_id: str, ingestion_run_id: str) -> PartitionManifest:
        raw_parts = self.catalog.list_partitions(dataset_id=dataset_id, layer="raw", status="ACTIVE")
        for p in raw_parts:
            if p.ingestion_run_id == ingestion_run_id:
                return p
        raise RecoveryError(
            f"Immutable raw source partition not found for run '{ingestion_run_id}' in dataset '{dataset_id}'.",
            context={"dataset_id": dataset_id, "ingestion_run_id": ingestion_run_id},
        )

    def quarantine_and_recover_partition(
        self,
        partition_id: str,
        access: AccessContext,
    ) -> RecoveryResult:
        """Quarantine a corrupt normalized/curated partition and recover it deterministically from raw."""
        part = self.catalog.get_partition(partition_id)
        if part is None:
            raise RecoveryError(
                f"Partition '{partition_id}' not found in catalog.",
                context={"partition_id": partition_id},
            )
        if part.layer == "raw":
            raise RecoveryError(
                "Cannot recover a raw partition from itself; restore raw tier from immutable backup.",
                context={"partition_id": partition_id},
            )

        # Ensure caller is entitled for recovery on this dataset
        self.catalog.authorize(part.dataset_id, access, action="recover_partition")

        # 1. Copy corrupt artifact to quarantine/ for forensic evidence, then mark QUARANTINED
        quarantine_path = self.storage.copy_to_quarantine(
            part.object_path,
            quarantine_prefix=self.config.storage.quarantine_prefix,
        )
        self.catalog.update_partition_status(part.partition_id, "QUARANTINED")

        # Also quarantine sibling normalized/curated partitions from the same run & partition_value
        siblings = self.catalog.list_partitions(
            dataset_id=part.dataset_id,
            status="ACTIVE",
            partition_value=part.partition_value,
        )
        for sib in siblings:
            if sib.ingestion_run_id == part.ingestion_run_id and sib.layer in {"normalized", "curated"}:
                self.catalog.update_partition_status(sib.partition_id, "QUARANTINED")

        # 2. Verify and read the immutable raw landing object for this ingestion run
        raw_part = self._find_raw_partition_for_run(part.dataset_id, part.ingestion_run_id)
        raw_bytes = self.storage.read_bytes(raw_part.object_path, expected_sha256=raw_part.sha256)
        raw_records = [
            json.loads(line)
            for line in raw_bytes.decode("utf-8").splitlines()
            if line.strip()
        ]
        # Filter to the affected partition_value so recovery is surgical and does not duplicate sibling dates
        affected_records = [
            r for r in raw_records if str(r.get(part.partition_key)) == part.partition_value
        ]
        if not affected_records:
            raise RecoveryError(
                f"No records in raw partition '{raw_part.partition_id}' matched {part.partition_key}={part.partition_value}.",
                context={"partition_id": partition_id, "raw_partition_id": raw_part.partition_id},
            )

        # 3. Replay through medallion pipeline reusing the verified raw landing manifest
        ingest_res = self.ingest.ingest_bytes(
            dataset_id=part.dataset_id,
            raw_bytes=raw_bytes,
            records=affected_records,
            source_name=f"recovery:{raw_part.object_path}",
            provenance=f"recovered_from_raw:{raw_part.partition_id}|raw_sha256={raw_part.sha256}",
            access=access,
            force_replay=True,
            existing_raw_manifest=raw_part,
        )
        recovered = ingest_res.normalized_partitions + ingest_res.curated_partitions
        self.metrics.increment(
            "recovery.partition_recovered",
            tags={
                "dataset_id": part.dataset_id,
                "corrupt_partition_id": partition_id,
                "recovered_count": len(recovered),
            },
        )
        return RecoveryResult(
            corrupt_partition_id=partition_id,
            quarantine_path=quarantine_path,
            raw_source_partition_id=raw_part.partition_id,
            recovered_partitions=recovered,
            recovered_rows=ingest_res.curated_rows,
        )

    def replay_dataset_from_raw(
        self,
        *,
        dataset_id: str,
        target_ingest: IngestionPipeline,
        access: AccessContext,
    ) -> list[IngestResult]:
        """Replay every immutable raw landing artifact for a dataset into a clean target lake."""
        self.catalog.authorize(dataset_id, access, action="replay")
        raw_parts = self.catalog.list_partitions(dataset_id=dataset_id, layer="raw", status="ACTIVE")
        raw_parts.sort(key=lambda p: (p.created_at, p.version))

        results: list[IngestResult] = []
        for raw_part in raw_parts:
            raw_bytes = self.storage.read_bytes(raw_part.object_path, expected_sha256=raw_part.sha256)
            records = [
                json.loads(line)
                for line in raw_bytes.decode("utf-8").splitlines()
                if line.strip()
            ]
            res = target_ingest.ingest_bytes(
                dataset_id=dataset_id,
                raw_bytes=raw_bytes,
                records=records,
                source_name=f"replay:{raw_part.object_path}",
                provenance=f"replayed_from_raw:{raw_part.partition_id}|raw_sha256={raw_part.sha256}",
                access=access,
            )
            results.append(res)
        self.metrics.increment(
            "recovery.replay_completed",
            tags={"dataset_id": dataset_id, "batches": len(results)},
        )
        return results

    def reproduce_snapshot_in_lake(
        self,
        *,
        snapshot_name: str,
        target_lake: Any,
        access: AccessContext,
    ) -> dict[str, list[dict[str, Any]]]:
        """Replay raw artifacts referenced by a snapshot into a clean target lake and query at snapshot.as_of_ts."""
        snap = self.catalog.get_snapshot(snapshot_name)
        reproduced: dict[str, list[dict[str, Any]]] = {}
        for dataset_id in snap.datasets:
            self.replay_dataset_from_raw(
                dataset_id=dataset_id,
                target_ingest=target_lake.ingest,
                access=access,
            )
            reproduced[dataset_id] = target_lake.query_as_of(
                dataset_id,
                snap.as_of_ts,
                access,
            )
        return reproduced

    def backfill_batches(
        self,
        *,
        dataset_id: str,
        batches: list[tuple[str, list[dict[str, Any]]]],
        provenance: str,
        access: AccessContext,
    ) -> list[IngestResult]:
        """Idempotently backfill ordered historical batches (source_name, records)."""
        results: list[IngestResult] = []
        for source_name, records in batches:
            res = self.ingest.ingest_records(
                dataset_id=dataset_id,
                records=records,
                source_name=source_name,
                provenance=provenance,
                access=access,
            )
            results.append(res)
        self.metrics.increment(
            "recovery.backfill_completed",
            tags={"dataset_id": dataset_id, "batches": len(batches)},
        )
        return results

    def evaluate_retention_policy(self, as_of_ts: datetime) -> dict[str, Any]:
        """Identify partitions eligible for cold-tier immutable archival without destructive deletion."""
        as_of_utc = ensure_utc(as_of_ts)
        ret = self.config.retention
        layer_days = {
            "raw": ret.raw_retention_days,
            "normalized": ret.normalized_retention_days,
            "curated": ret.curated_retention_days,
        }

        eligible_for_cold_archive: list[dict[str, Any]] = []
        active_hot: list[str] = []

        for part in self.catalog.list_partitions(status="ACTIVE"):
            cutoff = as_of_utc - timedelta(days=layer_days[part.layer])
            ref_ts = part.max_event_ts or part.created_at
            if ref_ts < cutoff:
                eligible_for_cold_archive.append(
                    {
                        "partition_id": part.partition_id,
                        "dataset_id": part.dataset_id,
                        "layer": part.layer,
                        "object_path": part.object_path,
                        "ref_ts": ref_ts.isoformat(),
                        "action": ret.archival_mode,
                    }
                )
            else:
                active_hot.append(part.partition_id)

        return {
            "evaluated_at": as_of_utc.isoformat(),
            "destructive_rewrite_allowed": ret.allow_destructive_history_rewrite,
            "archival_mode": ret.archival_mode,
            "hot_partition_count": len(active_hot),
            "cold_archive_candidates": eligible_for_cold_archive,
        }
