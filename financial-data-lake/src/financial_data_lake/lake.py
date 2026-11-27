"""High-level FinancialDataLake facade tying together catalog, storage, schemas, ingest, query, and recovery."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from financial_data_lake.adapters.clickhouse import ClickHouseTickAdapter
from financial_data_lake.adapters.fixture_loader import FixtureLoader
from financial_data_lake.catalog import CatalogRegistry
from financial_data_lake.config import LakeConfig
from financial_data_lake.ingest import IngestionPipeline, IngestResult
from financial_data_lake.metrics import MetricsCollector
from financial_data_lake.models import AccessContext, SnapshotManifest
from financial_data_lake.quality import QualityEngine
from financial_data_lake.query import AsOfQueryEngine
from financial_data_lake.recovery import IntegrityCheckReport, RecoveryManager, RecoveryResult
from financial_data_lake.schemas import FieldSpec, SchemaRegistry, SchemaVersionSpec
from financial_data_lake.storage import LocalObjectStore


class FinancialDataLake:
    """End-to-end local Financial Data Lake prototype."""

    def __init__(
        self,
        *,
        lake_root: Path | str,
        config_path: Path | str,
        entitlements_path: Path | str,
        schemas_path: Path | str,
    ) -> None:
        self.lake_root = Path(lake_root).resolve()
        self.lake_root.mkdir(parents=True, exist_ok=True)

        self.config = LakeConfig.load(config_path)
        self.storage = LocalObjectStore(self.lake_root / "object_store")
        self.catalog = CatalogRegistry(self.lake_root / "catalog.sqlite3")
        self.catalog.load_entitlements_file(entitlements_path)
        self.schema_registry = SchemaRegistry.load(schemas_path)
        self.quality = QualityEngine(self.config.quality)
        self.metrics = MetricsCollector()

        self.ingest = IngestionPipeline(
            config=self.config,
            storage=self.storage,
            catalog=self.catalog,
            schema_registry=self.schema_registry,
            quality=self.quality,
            metrics=self.metrics,
        )
        self.query = AsOfQueryEngine(
            config=self.config,
            storage=self.storage,
            catalog=self.catalog,
            schema_registry=self.schema_registry,
            metrics=self.metrics,
        )
        self.recovery = RecoveryManager(
            config=self.config,
            storage=self.storage,
            catalog=self.catalog,
            ingest=self.ingest,
            metrics=self.metrics,
        )

    def ingest_all_fixtures(
        self,
        fixtures_dir: Path | str,
        access: AccessContext,
        *,
        include_restricted: bool = True,
    ) -> list[IngestResult]:
        """Verify and ingest all fixtures in dependency order (reference instruments first)."""
        loader = FixtureLoader(fixtures_dir)
        verified = loader.verify_all()

        # Ingest reference_instruments first so symbol->instrument_id mapping is available for ClickHouse ticks
        ordered = sorted(
            verified,
            key=lambda f: (0 if f.dataset_id == "reference_instruments" else 1, f.file_name),
        )
        symbol_map: dict[str, str] = {}
        results: list[IngestResult] = []

        for fx in ordered:
            if not include_restricted and fx.dataset_id == "alt_sentiment":
                continue
            if fx.dataset_id == "reference_instruments":
                for row in fx.parse_jsonl():
                    symbol_map[row["symbol"]] = row["instrument_id"]
            adapter = (
                ClickHouseTickAdapter(symbol_map)
                if fx.format == "clickhouse_json_each_row"
                else None
            )
            results.append(self.ingest.ingest_fixture(fx, access, clickhouse_adapter=adapter))
        return results

    def evolve_schema(self, dataset_id: str, proposed_fields: list[FieldSpec]) -> SchemaVersionSpec:
        spec = self.schema_registry.register_evolution(dataset_id, proposed_fields)
        self.catalog.record_schema_version(dataset_id, spec.version, spec.model_dump(mode="json"))
        self.metrics.increment(
            "schema.evolved",
            tags={"dataset_id": dataset_id, "version": spec.version},
        )
        return spec

    def query_as_of(
        self,
        dataset_id: str,
        as_of_ts: datetime,
        access: AccessContext,
        *,
        include_all_revisions: bool = False,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        return self.query.query_as_of(
            dataset_id=dataset_id,
            as_of_ts=as_of_ts,
            access=access,
            include_all_revisions=include_all_revisions,
            filters=filters,
        )

    def query_adjusted_prices_as_of(
        self,
        as_of_ts: datetime,
        access: AccessContext,
        *,
        instrument_id: str | None = None,
        symbol: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.query.query_adjusted_prices_as_of(
            as_of_ts=as_of_ts,
            access=access,
            instrument_id=instrument_id,
            symbol=symbol,
        )

    def create_snapshot(
        self,
        name: str,
        datasets: list[str],
        as_of_ts: datetime,
        access: AccessContext,
    ) -> SnapshotManifest:
        return self.query.create_snapshot(
            name=name,
            datasets=datasets,
            as_of_ts=as_of_ts,
            access=access,
        )

    def query_snapshot(
        self,
        snapshot_name: str,
        dataset_id: str,
        access: AccessContext,
        *,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        return self.query.query_snapshot(
            snapshot_name=snapshot_name,
            dataset_id=dataset_id,
            access=access,
            filters=filters,
        )

    def verify_integrity(self, dataset_id: str | None = None) -> IntegrityCheckReport:
        return self.recovery.verify_integrity(dataset_id=dataset_id)

    def recover_partition(self, partition_id: str, access: AccessContext) -> RecoveryResult:
        return self.recovery.quarantine_and_recover_partition(partition_id, access)
