"""Medallion ingestion pipeline: immutable Raw -> Normalized Parquet -> Curated Parquet."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

from financial_data_lake.adapters.clickhouse import ClickHouseTickAdapter
from financial_data_lake.adapters.fixture_loader import VerifiedFixture
from financial_data_lake.catalog import CatalogRegistry
from financial_data_lake.config import LakeConfig
from financial_data_lake.errors import QualityCheckError, SchemaValidationError
from financial_data_lake.metrics import MetricsCollector
from financial_data_lake.models import (
    DATASET_MODELS,
    AccessContext,
    LineageEdge,
    PartitionManifest,
    QualityReport,
)
from financial_data_lake.quality import QualityEngine
from financial_data_lake.schemas import SchemaRegistry
from financial_data_lake.storage import LocalObjectStore, sha256_bytes


@dataclass(frozen=True)
class IngestResult:
    run_id: str
    dataset_id: str
    source_name: str
    source_checksum: str
    skipped_idempotent: bool
    raw_rows: int
    normalized_rows: int
    deduplicated_rows: int
    curated_rows: int
    raw_partition: PartitionManifest | None
    normalized_partitions: list[PartitionManifest]
    curated_partitions: list[PartitionManifest]
    quality_report: QualityReport | None


def _parse_iso_if_str(value: Any) -> Any:
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    return value


class IngestionPipeline:
    """Deterministic, idempotent, entitlement-checked medallion ingestion pipeline."""

    def __init__(
        self,
        *,
        config: LakeConfig,
        storage: LocalObjectStore,
        catalog: CatalogRegistry,
        schema_registry: SchemaRegistry,
        quality: QualityEngine,
        metrics: MetricsCollector,
    ) -> None:
        self.config = config
        self.storage = storage
        self.catalog = catalog
        self.schema_registry = schema_registry
        self.quality = quality
        self.metrics = metrics

    def _load_existing_dedupe_keys(
        self,
        dataset_id: str,
        dedupe_key: list[str],
    ) -> set[tuple[Any, ...]]:
        """Read active normalized partitions to guarantee cross-batch idempotent deduplication."""
        existing: set[tuple[Any, ...]] = set()
        partitions = self.catalog.list_partitions(
            dataset_id=dataset_id,
            layer="normalized",
            status="ACTIVE",
        )
        for part in partitions:
            table = self.storage.read_parquet(part.object_path, expected_sha256=part.sha256)
            aligned = self.schema_registry.align_table_to_schema(table, dataset_id)
            cols = {k: aligned[k].to_pylist() for k in dedupe_key}
            for i in range(aligned.num_rows):
                existing.add(tuple(cols[k][i] for k in dedupe_key))
        return existing

    def _validate_records(
        self,
        dataset_id: str,
        raw_records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Validate canonical domain invariants and align extra evolved schema columns."""
        model_cls = DATASET_MODELS.get(dataset_id)
        if model_cls is None:
            raise SchemaValidationError(
                f"No domain model registered for dataset '{dataset_id}'.",
                context={"dataset_id": dataset_id},
            )

        schema_spec = self.schema_registry.get_schema(dataset_id)
        allowed_field_names = {f.name for f in schema_spec.fields}
        validated: list[dict[str, Any]] = []

        for idx, rec in enumerate(raw_records):
            unknown_fields = set(rec.keys()) - allowed_field_names
            if unknown_fields:
                raise SchemaValidationError(
                    f"Record {idx} for '{dataset_id}' contains unregistered columns {sorted(unknown_fields)}. "
                    "Register a backward-compatible schema evolution first.",
                    context={"dataset_id": dataset_id, "unknown_fields": sorted(unknown_fields)},
                )
            try:
                # Validate core domain fields via Pydantic model
                model_instance = model_cls.model_validate(rec)
            except Exception as exc:
                raise SchemaValidationError(
                    f"Record {idx} failed domain validation for '{dataset_id}': {exc}",
                    context={"dataset_id": dataset_id, "record_index": idx},
                ) from exc

            row_dict = model_instance.model_dump(mode="python")
            # Preserve any backward-compatible evolved fields registered in SchemaRegistry
            for field_spec in schema_spec.fields:
                if field_spec.name not in row_dict:
                    val = rec.get(field_spec.name, field_spec.default)
                    if field_spec.type == "timestamp_us_utc" and val is not None:
                        val = _parse_iso_if_str(val)
                    if not field_spec.nullable and val is None:
                        raise SchemaValidationError(
                            f"Missing non-nullable evolved field '{field_spec.name}' on record {idx}.",
                            context={"dataset_id": dataset_id, "field": field_spec.name},
                        )
                    row_dict[field_spec.name] = val
            validated.append(row_dict)
        return validated

    def _records_to_arrow(self, dataset_id: str, records: list[dict[str, Any]]) -> pa.Table:
        schema_spec = self.schema_registry.get_schema(dataset_id)
        arrow_schema = schema_spec.to_arrow_schema()
        columns: dict[str, list[Any]] = {f.name: [] for f in schema_spec.fields}
        for rec in records:
            for f in schema_spec.fields:
                columns[f.name].append(rec.get(f.name, f.default))
        arrays = [
            pa.array(columns[f.name], type=arrow_schema.field(f.name).type)
            for f in schema_spec.fields
        ]
        return pa.Table.from_arrays(arrays, schema=arrow_schema)

    def ingest_bytes(
        self,
        *,
        dataset_id: str,
        raw_bytes: bytes,
        records: list[dict[str, Any]],
        source_name: str,
        provenance: str,
        access: AccessContext,
        force_replay: bool = False,
        existing_raw_manifest: PartitionManifest | None = None,
    ) -> IngestResult:
        """Execute Raw -> Normalized -> Curated ingestion with entitlement, quality, and lineage."""
        started_at = datetime.now(UTC)

        # 1. Fail-closed entitlement authorization
        policy = self.catalog.authorize(dataset_id, access, action="ingest")
        source_checksum = sha256_bytes(raw_bytes)
        run_id = f"run-{uuid.uuid4().hex[:12]}"

        # 2. Batch-level idempotency check
        if not force_replay:
            prior = self.catalog.has_completed_ingest(dataset_id, source_checksum)
            if prior is not None:
                self.metrics.increment(
                    "ingest.idempotent_skip",
                    tags={"dataset_id": dataset_id, "source_name": source_name},
                )
                return IngestResult(
                    run_id=prior["run_id"],
                    dataset_id=dataset_id,
                    source_name=source_name,
                    source_checksum=source_checksum,
                    skipped_idempotent=True,
                    raw_rows=len(records),
                    normalized_rows=0,
                    deduplicated_rows=len(records),
                    curated_rows=0,
                    raw_partition=None,
                    normalized_partitions=[],
                    curated_partitions=[],
                    quality_report=None,
                )

        # 3. Validate records against domain contract & active schema BEFORE landing
        ds_schema_cfg = self.schema_registry.get_dataset_config(dataset_id)
        schema_spec = self.schema_registry.get_schema(dataset_id)
        self.catalog.record_schema_version(
            dataset_id,
            schema_spec.version,
            schema_spec.model_dump(mode="json"),
        )
        validated = self._validate_records(dataset_id, records)

        # 4. Raw landing layer (immutable write-once NDJSON)
        if existing_raw_manifest is not None:
            raw_manifest = existing_raw_manifest
        else:
            ingest_date = started_at.strftime("%Y-%m-%d")
            raw_ver = self.catalog.next_partition_version(dataset_id, "raw", "ingest_date", ingest_date)
            raw_rel = (
                f"{self.config.storage.raw_prefix}/{dataset_id}/"
                f"ingest_date={ingest_date}/v{raw_ver}_{run_id}_{source_checksum[:12]}.jsonl"
            )
            stored_raw = self.storage.put_bytes_immutable(raw_rel, raw_bytes)
            event_ts_vals = [r["event_ts"] for r in validated]
            avail_ts_vals = [r["available_at"] for r in validated]
            raw_manifest = PartitionManifest(
                partition_id=f"part-raw-{uuid.uuid4().hex[:12]}",
                dataset_id=dataset_id,
                layer="raw",
                partition_key="ingest_date",
                partition_value=ingest_date,
                version=raw_ver,
                schema_version=schema_spec.version,
                object_path=stored_raw.relative_path,
                sha256=stored_raw.sha256,
                byte_size=stored_raw.byte_size,
                row_count=len(records),
                min_event_ts=min(event_ts_vals) if event_ts_vals else None,
                max_event_ts=max(event_ts_vals) if event_ts_vals else None,
                min_available_at=min(avail_ts_vals) if avail_ts_vals else None,
                max_available_at=max(avail_ts_vals) if avail_ts_vals else None,
                ingestion_run_id=run_id,
                source_checksum=source_checksum,
                license_id=policy.license_id,
                provenance=provenance,
                created_at=datetime.now(UTC),
                status="ACTIVE",
            )
            self.catalog.register_partition(raw_manifest)

        # 5. Deduplicate within-batch AND across existing active normalized partitions
        existing_keys = (
            set()
            if force_replay
            else self._load_existing_dedupe_keys(dataset_id, ds_schema_cfg.dedupe_key)
        )
        seen_batch: set[tuple[Any, ...]] = set()
        deduped: list[dict[str, Any]] = []
        duplicates_removed = 0

        for row in validated:
            key = tuple(row[k] for k in ds_schema_cfg.dedupe_key)
            if key in seen_batch or key in existing_keys:
                duplicates_removed += 1
                continue
            seen_batch.add(key)
            deduped.append(row)

        if not deduped:
            completed_at = datetime.now(UTC)
            self.catalog.record_ingestion_run(
                run_id=run_id,
                dataset_id=dataset_id,
                source_name=source_name,
                source_checksum=source_checksum,
                license_id=policy.license_id,
                provenance=provenance,
                principal=access.principal,
                status="COMPLETED",
                raw_rows=len(records),
                normalized_rows=0,
                deduplicated_rows=duplicates_removed,
                curated_rows=0,
                started_at=started_at,
                completed_at=completed_at,
                details={"note": "All rows already present in active partitions"},
            )
            self.metrics.increment(
                "ingest.all_rows_deduplicated",
                tags={"dataset_id": dataset_id},
            )
            return IngestResult(
                run_id=run_id,
                dataset_id=dataset_id,
                source_name=source_name,
                source_checksum=source_checksum,
                skipped_idempotent=True,
                raw_rows=len(records),
                normalized_rows=0,
                deduplicated_rows=duplicates_removed,
                curated_rows=0,
                raw_partition=raw_manifest,
                normalized_partitions=[],
                curated_partitions=[],
                quality_report=None,
            )

        # 6. Quality gate before writing normalized/curated Parquet
        required_non_null = [f.name for f in schema_spec.fields if not f.nullable]
        q_report = self.quality.evaluate(
            dataset_id=dataset_id,
            layer="normalized",
            ingestion_run_id=run_id,
            records=deduped,
            required_non_null_fields=required_non_null,
            dedupe_key=ds_schema_cfg.dedupe_key,
            duplicates_removed=duplicates_removed,
        )
        self.catalog.record_quality_report(f"qr-{uuid.uuid4().hex[:12]}", q_report)
        if not q_report.passed and self.config.quality.reject_on_critical_failure:
            self.metrics.increment("quality.failure", tags={"dataset_id": dataset_id})
            raise QualityCheckError(
                f"Quality check failed for dataset '{dataset_id}'.",
                context={"dataset_id": dataset_id, "checks": [c.model_dump() for c in q_report.checks]},
            )

        # 7. Partition and persist Normalized + Curated Parquet
        part_col = ds_schema_cfg.partition_columns[0]
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in deduped:
            grouped.setdefault(str(row[part_col]), []).append(row)

        norm_manifests: list[PartitionManifest] = []
        curated_manifests: list[PartitionManifest] = []
        sort_keys = list(ds_schema_cfg.primary_key)

        for part_val in sorted(grouped.keys()):
            part_rows = sorted(grouped[part_val], key=lambda r: tuple(str(r[k]) for k in sort_keys))
            table = self._records_to_arrow(dataset_id, part_rows)

            ev_ts = [r["event_ts"] for r in part_rows]
            av_ts = [r["available_at"] for r in part_rows]

            # Normalized partition
            norm_ver = self.catalog.next_partition_version(dataset_id, "normalized", part_col, part_val)
            norm_rel = (
                f"{self.config.storage.normalized_prefix}/{dataset_id}/"
                f"{part_col}={part_val}/v{norm_ver}_{run_id}.parquet"
            )
            norm_obj = self.storage.put_parquet_immutable(
                norm_rel,
                table,
                compression=self.config.storage.parquet_compression,
            )
            norm_manifest = PartitionManifest(
                partition_id=f"part-norm-{uuid.uuid4().hex[:12]}",
                dataset_id=dataset_id,
                layer="normalized",
                partition_key=part_col,
                partition_value=part_val,
                version=norm_ver,
                schema_version=schema_spec.version,
                object_path=norm_obj.relative_path,
                sha256=norm_obj.sha256,
                byte_size=norm_obj.byte_size,
                row_count=len(part_rows),
                min_event_ts=min(ev_ts),
                max_event_ts=max(ev_ts),
                min_available_at=min(av_ts),
                max_available_at=max(av_ts),
                ingestion_run_id=run_id,
                source_checksum=source_checksum,
                license_id=policy.license_id,
                provenance=provenance,
                created_at=datetime.now(UTC),
                status="ACTIVE",
            )
            self.catalog.register_partition(norm_manifest)
            norm_manifests.append(norm_manifest)

            self.catalog.record_lineage(
                LineageEdge(
                    edge_id=f"lin-{uuid.uuid4().hex[:12]}",
                    ingestion_run_id=run_id,
                    source_dataset=dataset_id,
                    source_layer="raw",
                    source_path=raw_manifest.object_path,
                    source_sha256=raw_manifest.sha256,
                    target_dataset=dataset_id,
                    target_layer="normalized",
                    target_path=norm_manifest.object_path,
                    target_sha256=norm_manifest.sha256,
                    transformation="validate_dedupe_normalize_parquet",
                    created_at=datetime.now(UTC),
                )
            )

            # Curated partition (bitemporal-sorted, quality-certified Parquet)
            cur_ver = self.catalog.next_partition_version(dataset_id, "curated", part_col, part_val)
            cur_rel = (
                f"{self.config.storage.curated_prefix}/{dataset_id}/"
                f"{part_col}={part_val}/v{cur_ver}_{run_id}.parquet"
            )
            cur_obj = self.storage.put_parquet_immutable(
                cur_rel,
                table,
                compression=self.config.storage.parquet_compression,
            )
            cur_manifest = PartitionManifest(
                partition_id=f"part-cur-{uuid.uuid4().hex[:12]}",
                dataset_id=dataset_id,
                layer="curated",
                partition_key=part_col,
                partition_value=part_val,
                version=cur_ver,
                schema_version=schema_spec.version,
                object_path=cur_obj.relative_path,
                sha256=cur_obj.sha256,
                byte_size=cur_obj.byte_size,
                row_count=len(part_rows),
                min_event_ts=min(ev_ts),
                max_event_ts=max(ev_ts),
                min_available_at=min(av_ts),
                max_available_at=max(av_ts),
                ingestion_run_id=run_id,
                source_checksum=source_checksum,
                license_id=policy.license_id,
                provenance=provenance,
                created_at=datetime.now(UTC),
                status="ACTIVE",
            )
            self.catalog.register_partition(cur_manifest)
            curated_manifests.append(cur_manifest)

            self.catalog.record_lineage(
                LineageEdge(
                    edge_id=f"lin-{uuid.uuid4().hex[:12]}",
                    ingestion_run_id=run_id,
                    source_dataset=dataset_id,
                    source_layer="normalized",
                    source_path=norm_manifest.object_path,
                    source_sha256=norm_manifest.sha256,
                    target_dataset=dataset_id,
                    target_layer="curated",
                    target_path=cur_manifest.object_path,
                    target_sha256=cur_manifest.sha256,
                    transformation="curate_bitemporal_certified",
                    created_at=datetime.now(UTC),
                )
            )

        completed_at = datetime.now(UTC)
        self.catalog.record_ingestion_run(
            run_id=run_id,
            dataset_id=dataset_id,
            source_name=source_name,
            source_checksum=source_checksum,
            license_id=policy.license_id,
            provenance=provenance,
            principal=access.principal,
            status="COMPLETED",
            raw_rows=len(records),
            normalized_rows=len(deduped),
            deduplicated_rows=duplicates_removed,
            curated_rows=len(deduped),
            started_at=started_at,
            completed_at=completed_at,
            details={
                "schema_version": schema_spec.version,
                "partitions_written": len(curated_manifests),
            },
        )
        self.metrics.increment(
            "ingest.completed",
            tags={"dataset_id": dataset_id, "rows": len(deduped), "deduped": duplicates_removed},
        )
        return IngestResult(
            run_id=run_id,
            dataset_id=dataset_id,
            source_name=source_name,
            source_checksum=source_checksum,
            skipped_idempotent=False,
            raw_rows=len(records),
            normalized_rows=len(deduped),
            deduplicated_rows=duplicates_removed,
            curated_rows=len(deduped),
            raw_partition=raw_manifest,
            normalized_partitions=norm_manifests,
            curated_partitions=curated_manifests,
            quality_report=q_report,
        )

    def ingest_records(
        self,
        *,
        dataset_id: str,
        records: list[dict[str, Any]],
        source_name: str,
        provenance: str,
        access: AccessContext,
        force_replay: bool = False,
    ) -> IngestResult:
        """Serialize canonical records deterministically to NDJSON bytes and run medallion ingest."""
        lines = [
            json.dumps(r, sort_keys=True, default=str)
            for r in records
        ]
        raw_bytes = ("\n".join(lines) + "\n").encode("utf-8") if lines else b""
        return self.ingest_bytes(
            dataset_id=dataset_id,
            raw_bytes=raw_bytes,
            records=records,
            source_name=source_name,
            provenance=provenance,
            access=access,
            force_replay=force_replay,
        )

    def ingest_fixture(
        self,
        fixture: VerifiedFixture,
        access: AccessContext,
        *,
        clickhouse_adapter: ClickHouseTickAdapter | None = None,
    ) -> IngestResult:
        """Ingest a cryptographically verified fixture file (including ClickHouse tick exports)."""
        if fixture.format == "clickhouse_json_each_row":
            if clickhouse_adapter is None:
                raise SchemaValidationError(
                    "ClickHouseTickAdapter is required for clickhouse_json_each_row fixtures.",
                    context={"fixture": fixture.file_name},
                )
            records = clickhouse_adapter.adapt_jsonl_bytes(fixture.raw_bytes)
            # Store canonical NDJSON bytes alongside provenance referencing the ClickHouse fixture
            canonical_lines = [
                json.dumps(r, sort_keys=True, default=str)
                for r in records
            ]
            raw_bytes = ("\n".join(canonical_lines) + "\n").encode("utf-8")
        else:
            records = fixture.parse_jsonl()
            raw_bytes = fixture.raw_bytes

        return self.ingest_bytes(
            dataset_id=fixture.dataset_id,
            raw_bytes=raw_bytes,
            records=records,
            source_name=fixture.file_name,
            provenance=f"{fixture.provenance}|fixture_sha256={fixture.sha256}",
            access=access,
        )
