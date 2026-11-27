"""End-to-end runnable demonstration of the Financial Data Lake (M1–M4)."""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from financial_data_lake import (
    AccessContext,
    CorruptPartitionError,
    EntitlementError,
    FieldSpec,
    FinancialDataLake,
    SchemaEvolutionError,
)
from financial_data_lake.query import compute_rows_digest


def run_demo(repo_root: Path | None = None) -> dict[str, Any]:
    root = (repo_root or Path(__file__).resolve().parents[1]).resolve()
    with tempfile.TemporaryDirectory(prefix="fdl_demo_") as tmp:
        tmp_path = Path(tmp)
        lake = FinancialDataLake(
            lake_root=tmp_path / "primary_lake",
            config_path=root / "config" / "lake_config.json",
            entitlements_path=root / "config" / "entitlements.json",
            schemas_path=root / "config" / "schemas" / "registry.json",
        )

        eng_ctx = AccessContext(principal="demo-ingest", role="data_engineer", purpose="ingest")
        res_ctx = AccessContext(principal="demo-quant", role="quant_research", purpose="research")
        rec_ctx = AccessContext(principal="demo-sre", role="system_recovery", purpose="recovery")
        unauth_ctx = AccessContext(principal="external-client", role="external_viewer", purpose="research")

        # M1/M2: Ingest all verified fixtures (including ClickHouse tick export & duplicate ticks)
        first_ingest = lake.ingest_all_fixtures(root / "data" / "fixtures", eng_ctx)
        # Re-ingest to prove idempotency (zero duplicate rows added)
        second_ingest = lake.ingest_all_fixtures(root / "data" / "fixtures", eng_ctx)

        # M3: Point-in-time queries: late tick correction, split/dividend adjustment, future fundamental revision
        tick_before_corr = lake.query_as_of(
            "market_ticks",
            datetime(2024, 6, 3, 14, 35, 0, tzinfo=UTC),
            res_ctx,
            filters={"tick_id": "TCK-20240603-0003"},
        )
        tick_after_corr = lake.query_as_of(
            "market_ticks",
            datetime(2024, 6, 3, 14, 50, 0, tzinfo=UTC),
            res_ctx,
            filters={"tick_id": "TCK-20240603-0003"},
        )

        adj_aapl_before_split = lake.query_adjusted_prices_as_of(
            datetime(2024, 6, 4, 21, 0, 0, tzinfo=UTC),
            res_ctx,
            symbol="AAPL",
        )
        adj_aapl_after_split = lake.query_adjusted_prices_as_of(
            datetime(2024, 6, 6, 23, 59, 59, tzinfo=UTC),
            res_ctx,
            symbol="AAPL",
        )
        adj_msft_after_div = lake.query_adjusted_prices_as_of(
            datetime(2024, 6, 6, 23, 59, 59, tzinfo=UTC),
            res_ctx,
            symbol="MSFT",
        )

        fund_before_restatement = lake.query_as_of(
            "fundamentals",
            datetime(2024, 6, 7, 23, 59, 59, tzinfo=UTC),
            res_ctx,
            filters={"instrument_id": "INST-AAPL", "metric_name": "revenue"},
        )
        fund_after_restatement = lake.query_as_of(
            "fundamentals",
            datetime(2024, 6, 11, 0, 0, 0, tzinfo=UTC),
            res_ctx,
            filters={"instrument_id": "INST-AAPL", "metric_name": "revenue"},
        )

        # Schema evolution check (compatible additive v2 accepted; breaking drop rejected)
        v1_fields = list(lake.schema_registry.get_schema("market_ticks").fields)
        v2_spec = lake.evolve_schema(
            "market_ticks",
            v1_fields + [FieldSpec(name="venue_mic", type="string", nullable=True, default=None)],
        )
        breaking_rejected = False
        try:
            lake.evolve_schema("market_ticks", v1_fields[:-1])
        except SchemaEvolutionError:
            breaking_rejected = True

        # Unauthorized access check
        unauthorized_blocked = False
        try:
            lake.query_as_of("alt_sentiment", datetime(2024, 6, 6, 0, 0, 0, tzinfo=UTC), unauth_ctx)
        except EntitlementError:
            unauthorized_blocked = True

        # M4: Create snapshot, corrupt a curated partition, verify detection, recover, and replay snapshot into clean lake
        snap = lake.create_snapshot(
            name="demo-snapshot-20240607",
            datasets=["market_ticks", "prices_eod", "corporate_actions", "fundamentals"],
            as_of_ts=datetime(2024, 6, 7, 23, 59, 59, tzinfo=UTC),
            access=res_ctx,
        )
        baseline_fund_rows = lake.query_snapshot(snap.name, "fundamentals", res_ctx)
        baseline_digest = compute_rows_digest(baseline_fund_rows)

        # Inject corruption into a curated prices_eod partition
        target_part = lake.catalog.list_partitions(dataset_id="prices_eod", layer="curated", status="ACTIVE")[0]
        corrupt_file = lake.storage.resolve_path(target_part.object_path)
        corrupt_file.write_bytes(b"CORRUPTED_PARQUET_BYTES")

        corrupt_detected = False
        try:
            lake.query_as_of("prices_eod", datetime(2024, 6, 7, 23, 59, 59, tzinfo=UTC), res_ctx)
        except CorruptPartitionError:
            corrupt_detected = True

        integrity_before_rec = lake.verify_integrity("prices_eod")
        recovery_res = lake.recover_partition(target_part.partition_id, rec_ctx)
        integrity_after_rec = lake.verify_integrity("prices_eod")

        # Full replay from raw into a fresh recovery lake and verify snapshot reproduction
        clean_lake = FinancialDataLake(
            lake_root=tmp_path / "recovery_lake",
            config_path=root / "config" / "lake_config.json",
            entitlements_path=root / "config" / "entitlements.json",
            schemas_path=root / "config" / "schemas" / "registry.json",
        )
        replay_ctx = AccessContext(principal="demo-sre", role="data_engineer", purpose="ingest")
        # Replay fundamentals (using recovery-entitled researcher for query & data_engineer for ingest)
        lake.recovery.replay_dataset_from_raw(
            dataset_id="fundamentals",
            target_ingest=clean_lake.ingest,
            access=replay_ctx,
        )
        replayed_fund_rows = clean_lake.query_as_of("fundamentals", snap.as_of_ts, res_ctx)
        replayed_digest = compute_rows_digest(replayed_fund_rows)

        retention_plan = lake.recovery.evaluate_retention_policy(datetime(2035, 1, 1, 0, 0, 0, tzinfo=UTC))

        return {
            "m1_m2_ingest": {
                "datasets_ingested": len(first_ingest),
                "total_curated_rows": sum(r.curated_rows for r in first_ingest),
                "duplicates_removed_first_pass": sum(r.deduplicated_rows for r in first_ingest),
                "second_pass_all_idempotent": all(r.skipped_idempotent for r in second_ingest),
            },
            "m3_bitemporal_and_controls": {
                "tick_price_before_correction": tick_before_corr[0]["price"],
                "tick_price_after_correction": tick_after_corr[0]["price"],
                "aapl_20240604_adj_close_before_split_avail": adj_aapl_before_split[-1]["adj_close"],
                "aapl_20240604_adj_close_after_split_avail": adj_aapl_after_split[1]["adj_close"],
                "msft_20240605_adj_close_after_div_avail": adj_msft_after_div[1]["adj_close"],
                "aapl_revenue_before_restatement": fund_before_restatement[0]["metric_value"],
                "aapl_revenue_after_restatement": fund_after_restatement[0]["metric_value"],
                "schema_evolved_to_version": v2_spec.version,
                "breaking_schema_rejected": breaking_rejected,
                "unauthorized_dataset_blocked": unauthorized_blocked,
            },
            "m4_recovery_and_snapshots": {
                "snapshot_id": snap.snapshot_id,
                "snapshot_manifest_sha256": snap.manifest_sha256,
                "corrupt_partition_detected": corrupt_detected,
                "corrupt_count_before_recovery": len(integrity_before_rec.corrupt_partition_ids),
                "corrupt_count_after_recovery": len(integrity_after_rec.corrupt_partition_ids),
                "quarantine_path": recovery_res.quarantine_path,
                "recovered_rows": recovery_res.recovered_rows,
                "snapshot_digest_baseline": baseline_digest,
                "snapshot_digest_replayed": replayed_digest,
                "snapshot_reproduction_exact_match": baseline_digest == replayed_digest,
                "retention_destructive_rewrite_allowed": retention_plan["destructive_rewrite_allowed"],
                "retention_cold_archive_candidates_2035": len(retention_plan["cold_archive_candidates"]),
            },
            "metrics_counters": lake.metrics.snapshot()["counters"],
        }


if __name__ == "__main__":
    output = run_demo()
    print(json.dumps(output, indent=2))
