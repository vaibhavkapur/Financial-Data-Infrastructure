"""DuckDB-backed Point-in-Time (As-Of) Query Engine and Reproducible Snapshot Manager."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import duckdb
import pyarrow as pa

from financial_data_lake.catalog import CatalogRegistry
from financial_data_lake.config import LakeConfig
from financial_data_lake.errors import CorruptPartitionError, SnapshotError
from financial_data_lake.metrics import MetricsCollector
from financial_data_lake.models import (
    AccessContext,
    PartitionManifest,
    SnapshotManifest,
    ensure_utc,
)
from financial_data_lake.schemas import SchemaRegistry
from financial_data_lake.storage import LocalObjectStore, sha256_bytes


def compute_rows_digest(rows: list[dict[str, Any]]) -> str:
    """Deterministic SHA-256 content digest over query result rows."""
    canonical = json.dumps(rows, sort_keys=True, default=str).encode("utf-8")
    return sha256_bytes(canonical)


class AsOfQueryEngine:
    """Executes entitlement-checked, point-in-time SQL queries over verified immutable Parquet."""

    def __init__(
        self,
        *,
        config: LakeConfig,
        storage: LocalObjectStore,
        catalog: CatalogRegistry,
        schema_registry: SchemaRegistry,
        metrics: MetricsCollector,
    ) -> None:
        self.config = config
        self.storage = storage
        self.catalog = catalog
        self.schema_registry = schema_registry
        self.metrics = metrics

    def _load_verified_table(
        self,
        dataset_id: str,
        partitions: list[PartitionManifest],
    ) -> pa.Table:
        """Verify SHA-256 checksums and align all Parquet partitions to the current dataset schema."""
        target_schema = self.schema_registry.get_arrow_schema(dataset_id)
        if not partitions:
            return target_schema.empty_table()

        tables: list[pa.Table] = []
        for part in partitions:
            try:
                raw_table = self.storage.read_parquet(part.object_path, expected_sha256=part.sha256)
            except CorruptPartitionError:
                self.metrics.increment(
                    "storage.corrupt_partition_detected",
                    tags={"dataset_id": dataset_id, "partition_id": part.partition_id},
                )
                raise
            aligned = self.schema_registry.align_table_to_schema(raw_table, dataset_id)
            tables.append(aligned)

        return pa.concat_tables(tables)

    def _run_as_of_sql(
        self,
        *,
        dataset_id: str,
        table: pa.Table,
        as_of_ts: datetime,
        include_all_revisions: bool = False,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        as_of_utc = ensure_utc(as_of_ts)
        if table.num_rows == 0:
            return []

        ds_cfg = self.schema_registry.get_dataset_config(dataset_id)
        entity_cols = [c for c in ds_cfg.primary_key if c != "revision_number"]

        where_clauses = ["available_at <= ?"]
        params: list[Any] = [as_of_utc]

        for col, val in (filters or {}).items():
            if col not in table.column_names:
                raise ValueError(f"Unknown filter column '{col}' for dataset '{dataset_id}'.")
            where_clauses.append(f'"{col}" = ?')
            params.append(val)

        where_sql = " AND ".join(where_clauses)
        order_by_sql = ", ".join(f'"{c}"' for c in ds_cfg.primary_key)

        con = duckdb.connect(":memory:")
        try:
            con.register("dataset_tbl", table)
            if include_all_revisions or not entity_cols:
                sql = f"SELECT * FROM dataset_tbl WHERE {where_sql} ORDER BY {order_by_sql}"
            else:
                part_by = ", ".join(f'"{c}"' for c in entity_cols)
                sql = f"""
                    WITH ranked AS (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY {part_by}
                                   ORDER BY revision_number DESC, available_at DESC
                               ) AS _rn
                        FROM dataset_tbl
                        WHERE {where_sql}
                    )
                    SELECT * EXCLUDE (_rn)
                    FROM ranked
                    WHERE _rn = 1
                    ORDER BY {order_by_sql}
                """
            rel = con.execute(sql, params)
            result = rel.to_arrow_table().to_pylist()
        finally:
            con.close()

        for rec in result:
            for k, v in rec.items():
                if isinstance(v, datetime):
                    rec[k] = v.astimezone(UTC)
        return result

    def query_as_of(
        self,
        *,
        dataset_id: str,
        as_of_ts: datetime,
        access: AccessContext,
        layer: str = "curated",
        include_all_revisions: bool = False,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Query the latest point-in-time state of a dataset as known at `as_of_ts`."""
        self.catalog.authorize(dataset_id, access, action="query")
        partitions = self.catalog.list_partitions(
            dataset_id=dataset_id,
            layer=layer,
            status="ACTIVE",
        )
        table = self._load_verified_table(dataset_id, partitions)
        rows = self._run_as_of_sql(
            dataset_id=dataset_id,
            table=table,
            as_of_ts=as_of_ts,
            include_all_revisions=include_all_revisions,
            filters=filters,
        )
        self.metrics.increment(
            "query.as_of",
            tags={"dataset_id": dataset_id, "rows": len(rows)},
        )
        return rows

    def query_adjusted_prices_as_of(
        self,
        *,
        as_of_ts: datetime,
        access: AccessContext,
        instrument_id: str | None = None,
        symbol: str | None = None,
    ) -> list[dict[str, Any]]:
        """Compute point-in-time split- and dividend-adjusted EOD prices without lookahead bias.

        Only corporate actions with `available_at <= as_of_ts` are applied:
        - SPLIT (numerator/denominator): for bars with trade_date < ex_date, prices are multiplied by
          (denominator / numerator) and volumes are multiplied by (numerator / denominator).
        - CASH_DIVIDEND (cash_amount): for bars with trade_date < ex_date, prices are multiplied by
          (prev_close - cash_amount) / prev_close where prev_close is the latest unadjusted close
          strictly before ex_date.
        """
        filters: dict[str, Any] = {}
        if instrument_id is not None:
            filters["instrument_id"] = instrument_id
        if symbol is not None:
            filters["symbol"] = symbol

        bars = self.query_as_of(
            dataset_id="prices_eod",
            as_of_ts=as_of_ts,
            access=access,
            filters=filters,
        )
        actions = self.query_as_of(
            dataset_id="corporate_actions",
            as_of_ts=as_of_ts,
            access=access,
            filters=filters,
        )

        # Group bars & actions by instrument_id
        bars_by_inst: dict[str, list[dict[str, Any]]] = {}
        for bar in bars:
            bars_by_inst.setdefault(bar["instrument_id"], []).append(bar)
        actions_by_inst: dict[str, list[dict[str, Any]]] = {}
        for act in actions:
            actions_by_inst.setdefault(act["instrument_id"], []).append(act)

        adjusted_rows: list[dict[str, Any]] = []
        for inst_id in sorted(bars_by_inst.keys()):
            inst_bars = sorted(bars_by_inst[inst_id], key=lambda b: b["trade_date"])
            inst_actions = sorted(actions_by_inst.get(inst_id, []), key=lambda a: a["ex_date"])

            for bar in inst_bars:
                price_factor = 1.0
                volume_factor = 1.0
                applied_actions: list[str] = []

                for act in inst_actions:
                    if bar["trade_date"] >= act["ex_date"]:
                        continue
                    if act["action_type"] == "SPLIT":
                        ratio = float(act["split_numerator"]) / float(act["split_denominator"])
                        price_factor /= ratio
                        volume_factor *= ratio
                        applied_actions.append(act["action_id"])
                    elif act["action_type"] == "CASH_DIVIDEND":
                        # Locate last unadjusted close strictly prior to ex_date
                        prior_bars = [b for b in inst_bars if b["trade_date"] < act["ex_date"]]
                        if prior_bars:
                            prev_close = float(prior_bars[-1]["close"])
                            cash = float(act["cash_amount"])
                            if prev_close > cash > 0:
                                price_factor *= (prev_close - cash) / prev_close
                                applied_actions.append(act["action_id"])

                out = dict(bar)
                out["price_adjustment_factor"] = round(price_factor, 10)
                out["volume_adjustment_factor"] = round(volume_factor, 10)
                out["adj_open"] = round(float(bar["open"]) * price_factor, 6)
                out["adj_high"] = round(float(bar["high"]) * price_factor, 6)
                out["adj_low"] = round(float(bar["low"]) * price_factor, 6)
                out["adj_close"] = round(float(bar["close"]) * price_factor, 6)
                out["adj_volume"] = round(float(bar["volume"]) * volume_factor, 6)
                out["applied_corporate_actions"] = applied_actions
                adjusted_rows.append(out)

        return adjusted_rows

    # ---- Reproducible Snapshots ----

    def create_snapshot(
        self,
        *,
        name: str,
        datasets: list[str],
        as_of_ts: datetime,
        access: AccessContext,
        layer: str = "curated",
    ) -> SnapshotManifest:
        """Pin an immutable, checksum-verified point-in-time snapshot across one or more datasets."""
        as_of_utc = ensure_utc(as_of_ts)
        license_ids: dict[str, str] = {}
        partition_ids: list[str] = []
        partition_checksums: dict[str, str] = {}

        for ds in sorted(set(datasets)):
            policy = self.catalog.authorize(ds, access, action="create_snapshot")
            license_ids[ds] = policy.license_id
            parts = self.catalog.list_partitions(dataset_id=ds, layer=layer, status="ACTIVE")
            for p in parts:
                # Only include partitions that contain data available at or before as_of_ts
                if p.min_available_at is not None and p.min_available_at > as_of_utc:
                    continue
                if not self.storage.verify_object(p.object_path, p.sha256):
                    raise CorruptPartitionError(
                        f"Cannot create snapshot: partition '{p.partition_id}' failed checksum verification.",
                        context={"partition_id": p.partition_id, "object_path": p.object_path},
                    )
                partition_ids.append(p.partition_id)
                partition_checksums[p.partition_id] = p.sha256

        snapshot_id = f"snap-{uuid.uuid4().hex[:12]}"
        canonical_payload = {
            "snapshot_id": snapshot_id,
            "name": name,
            "as_of_ts": as_of_utc.isoformat(),
            "datasets": sorted(set(datasets)),
            "partition_ids": partition_ids,
            "partition_checksums": partition_checksums,
            "license_ids": license_ids,
        }
        payload_bytes = (json.dumps(canonical_payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
        manifest_sha = sha256_bytes(payload_bytes)

        snap = SnapshotManifest(
            snapshot_id=snapshot_id,
            name=name,
            as_of_ts=as_of_utc,
            datasets=sorted(set(datasets)),
            partition_ids=partition_ids,
            partition_checksums=partition_checksums,
            manifest_sha256=manifest_sha,
            created_by=access.principal,
            created_at=datetime.now(UTC),
            license_ids=license_ids,
        )

        # Persist immutable snapshot manifest artifact & catalog row
        snap_rel = f"{self.config.storage.snapshots_prefix}/{name}_{snapshot_id}.json"
        self.storage.put_bytes_immutable(snap_rel, payload_bytes)
        self.catalog.register_snapshot(snap)
        self.metrics.increment("snapshot.created", tags={"name": name, "datasets": len(datasets)})
        return snap

    def query_snapshot(
        self,
        *,
        snapshot_name: str,
        dataset_id: str,
        access: AccessContext,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Reproduce an exact point-in-time query pinned by a named snapshot."""
        self.catalog.authorize(dataset_id, access, action="query_snapshot")
        snap = self.catalog.get_snapshot(snapshot_name)
        if dataset_id not in snap.datasets:
            raise SnapshotError(
                f"Dataset '{dataset_id}' is not included in snapshot '{snapshot_name}'.",
                context={"snapshot": snapshot_name, "dataset_id": dataset_id},
            )

        pinned_parts: list[PartitionManifest] = []
        for pid in snap.partition_ids:
            part = self.catalog.get_partition(pid)
            if part is None:
                raise SnapshotError(
                    f"Pinned partition '{pid}' missing from catalog.",
                    context={"snapshot": snapshot_name, "partition_id": pid},
                )
            if part.dataset_id != dataset_id:
                continue
            expected_sha = snap.partition_checksums[pid]
            if part.status == "QUARANTINED" or not self.storage.verify_object(part.object_path, expected_sha):
                raise CorruptPartitionError(
                    f"Pinned snapshot partition '{pid}' is corrupt or quarantined; run recovery first.",
                    context={"snapshot": snapshot_name, "partition_id": pid},
                )
            pinned_parts.append(part)

        table = self._load_verified_table(dataset_id, pinned_parts)
        rows = self._run_as_of_sql(
            dataset_id=dataset_id,
            table=table,
            as_of_ts=snap.as_of_ts,
            filters=filters,
        )
        self.metrics.increment(
            "snapshot.reproduced",
            tags={"snapshot": snapshot_name, "dataset_id": dataset_id, "rows": len(rows)},
        )
        return rows
