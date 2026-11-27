"""End-to-end M1-M4 runnable demonstration for the Financial Feature Store."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from financial_feature_store import (
    FeatureDefinition,
    FeatureRegistry,
    GovernanceConfig,
    LifecycleStage,
    LineageTracker,
    NullPolicy,
    OfflineFeatureStore,
    OnlineFeatureStore,
    ParityVerifier,
    ParquetFixtureSourceAdapter,
    QualityEvaluator,
    QualityRuleSpec,
    SQLiteOnlineBackend,
    StalePolicy,
    SyntheticDataGenerator,
    TransformSpec,
    ValueType,
)

ROOT = Path(__file__).resolve().parent.parent


def run_demo(workspace_dir: Path | None = None) -> dict[str, object]:
    base_dir = workspace_dir or ROOT
    data_dir = base_dir / "data"
    fixtures_dir = data_dir / "fixtures"
    offline_dir = data_dir / "offline"
    exports_dir = data_dir / "exports"
    online_db = data_dir / "online" / "online_store.sqlite3"
    registry_file = data_dir / "registry" / "feature_registry.json"
    lineage_file = data_dir / "registry" / "lineage_graph.json"

    # Clean demo runtime directories (preserve fixtures regeneration deterministically)
    if online_db.exists():
        online_db.unlink()

    start_time = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC)
    gen = SyntheticDataGenerator(seed=42)

    # 1. Generate and verify deterministic synthetic fixtures with provenance manifests
    lob_raw = gen.generate_order_book_snapshots(
        entities=("EQ:AAPL", "EQ:MSFT", "EQ:NVDA"),
        start_time=start_time,
        steps=60,
        step_seconds=10,
        availability_delay_seconds=0.25,
    )
    lob_pq, lob_man, _ = gen.write_fixture_bundle(
        lob_raw,
        fixtures_dir / "lob_snapshots_synthetic.parquet",
        source_id="src_lob_synthetic",
        dataset_name="l1_order_book_snapshots",
        source_version="2026.01.15-r1",
        currency="USD",
    )

    tx_raw = gen.generate_account_transactions(
        accounts=("ACCT:0001", "ACCT:0002", "ACCT:0003"),
        start_time=start_time,
        steps=40,
        step_seconds=30,
        availability_delay_seconds=0.5,
    )
    tx_pq, tx_man, _ = gen.write_fixture_bundle(
        tx_raw,
        fixtures_dir / "account_transactions_synthetic.parquet",
        source_id="src_tx_synthetic",
        dataset_name="account_transactions",
        source_version="2026.01.15-r1",
        currency="USD",
    )

    # Load through verified ParquetFixtureSourceAdapter (verifies SHA-256 & UTC schema)
    lob_df, lob_contract = ParquetFixtureSourceAdapter(
        lob_pq,
        manifest_path=lob_man,
        required_columns=("best_bid", "best_ask", "bid_size", "ask_size"),
    ).load()
    tx_df, tx_contract = ParquetFixtureSourceAdapter(
        tx_pq,
        manifest_path=tx_man,
        required_columns=("amount",),
    ).load()

    # 2. M1: Define and register versioned features with governance controls
    lineage = LineageTracker()
    registry = FeatureRegistry(governance=GovernanceConfig())

    f_imbalance = FeatureDefinition(
        name="lob_imbalance_l1",
        feature_view="order_book_microstructure",
        version="1.0.0",
        entity_type="instrument",
        value_type=ValueType.FLOAT64,
        unit="ratio",
        description="Level-1 order book volume imbalance.",
        owner="quant-microstructure",
        source=lob_contract,
        transform=TransformSpec(
            transform_name="lob_order_book_imbalance",
            source_columns=("bid_size", "ask_size"),
        ),
        null_policy=NullPolicy.KEEP_NULL,
        ttl_seconds=30.0,
        stale_policy=StalePolicy.REJECT,
        quality_rules=QualityRuleSpec(
            min_row_count=10,
            max_null_ratio=0.05,
            min_value=-1.0,
            max_value=1.0,
            max_late_arrival_seconds=5.0,
        ),
        stage=LifecycleStage.STAGING,
    )

    f_spread = FeatureDefinition(
        name="lob_spread_bps",
        feature_view="order_book_microstructure",
        version="1.0.0",
        entity_type="instrument",
        value_type=ValueType.FLOAT64,
        unit="bps",
        description="Level-1 bid-ask spread in basis points.",
        owner="quant-microstructure",
        source=lob_contract,
        transform=TransformSpec(
            transform_name="lob_spread_bps",
            source_columns=("best_bid", "best_ask"),
        ),
        null_policy=NullPolicy.KEEP_NULL,
        ttl_seconds=30.0,
        stale_policy=StalePolicy.REJECT,
        quality_rules=QualityRuleSpec(
            min_row_count=10,
            max_null_ratio=0.05,
            min_value=0.0,
            max_value=500.0,
            max_late_arrival_seconds=5.0,
        ),
        stage=LifecycleStage.STAGING,
    )

    f_roll_imb = FeatureDefinition(
        name="lob_imbalance_roll_60s",
        feature_view="order_book_microstructure",
        version="1.0.0",
        entity_type="instrument",
        value_type=ValueType.FLOAT64,
        unit="ratio",
        description="Strictly causal 60s rolling mean of level-1 order book imbalance.",
        owner="quant-microstructure",
        source=lob_contract,
        transform=TransformSpec(
            transform_name="lob_rolling_imbalance_mean",
            source_columns=("bid_size", "ask_size"),
            params={"window_events": 6, "min_periods": 2},
            lookback_seconds=60.0,
        ),
        null_policy=NullPolicy.KEEP_NULL,
        ttl_seconds=30.0,
        stale_policy=StalePolicy.REJECT,
        quality_rules=QualityRuleSpec(
            min_row_count=10,
            max_null_ratio=0.10,
            min_value=-1.0,
            max_value=1.0,
            max_late_arrival_seconds=5.0,
        ),
        stage=LifecycleStage.STAGING,
    )

    f_tx_count = FeatureDefinition(
        name="acct_tx_count_5m",
        feature_view="account_risk_velocity",
        version="1.0.0",
        entity_type="account",
        value_type=ValueType.INT64,
        unit="count",
        description="Strictly causal 5-minute rolling account transaction count.",
        owner="risk-ml",
        source=tx_contract,
        transform=TransformSpec(
            transform_name="tx_rolling_count",
            source_columns=("amount",),
            lookback_seconds=300.0,
        ),
        null_policy=NullPolicy.KEEP_NULL,
        ttl_seconds=120.0,
        stale_policy=StalePolicy.WARN,
        quality_rules=QualityRuleSpec(
            min_row_count=10,
            max_null_ratio=0.0,
            min_value=1.0,
            max_late_arrival_seconds=5.0,
        ),
        stage=LifecycleStage.STAGING,
    )

    for fdef in (f_imbalance, f_spread, f_roll_imb, f_tx_count):
        registry.register(fdef)

    # 3. M2: Offline materialization + Quality gate + Promotion to ACTIVE
    offline = OfflineFeatureStore(offline_dir, registry, lineage=lineage)
    evaluator = QualityEvaluator()
    raw_map = {
        "lob_imbalance_l1": lob_df,
        "lob_spread_bps": lob_df,
        "lob_imbalance_roll_60s": lob_df,
        "acct_tx_count_5m": tx_df,
    }
    quality_summaries: dict[str, dict[str, object]] = {}
    for fname, src_df in raw_map.items():
        fdef = registry.get(fname, "1.0.0")
        mat_df, manifest = offline.materialize(fdef, src_df, run_id=f"demo-{fname}-v1")
        qrep = evaluator.evaluate(mat_df, fdef, run_id=manifest.run_id)
        assert qrep.passed, f"Quality gate failed for {fname}: {qrep}"
        registry.promote(fdef.name, fdef.version, LifecycleStage.ACTIVE, quality_report=qrep)
        quality_summaries[fname] = {
            "passed": qrep.passed,
            "row_count": qrep.row_count,
            "null_ratio": round(qrep.null_ratio, 6),
            "version_id": fdef.version_id,
        }

    # 4. M3: Online sync, freshness verification, and chronological replay parity
    lob_features = ["lob_imbalance_l1", "lob_spread_bps", "lob_imbalance_roll_60s"]
    spine_rows = [
        {"entity_id": ent, "as_of_time": start_time + timedelta(seconds=step * 10 + 2)}
        for step in range(2, 55)
        for ent in ("EQ:AAPL", "EQ:MSFT", "EQ:NVDA")
    ]
    spine_df = pl.DataFrame(
        spine_rows,
        schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
    )

    verifier = ParityVerifier(offline, registry, atol=1e-12, rtol=1e-12)
    parity_report = verifier.verify_replay_parity(
        spine_df,
        lob_features,
        raw_sources_by_feature={k: lob_df for k in lob_features},
        backend_factory=lambda: SQLiteOnlineBackend(online_db),
    )
    assert parity_report.passed, f"Parity check failed: {parity_report}"

    # Sync final state into durable SQLite online store and inspect live vector
    online = OnlineFeatureStore(SQLiteOnlineBackend(online_db), registry, lineage=lineage)
    latest_cutoff = start_time + timedelta(seconds=595)
    for fname in lob_features:
        online.materialize_and_sync_from_raw(fname, lob_df, as_of_time=latest_cutoff)
    sample_vec = online.get_online_features("EQ:AAPL", lob_features, now=latest_cutoff + timedelta(seconds=3))

    # 5. M4: Schema evolution (1.0.0 -> 1.1.0), idempotent backfill, and training dataset export
    f_roll_v11 = f_roll_imb.model_copy(
        update={
            "version": "1.1.0",
            "transform": TransformSpec(
                transform_name="lob_rolling_imbalance_mean",
                source_columns=("bid_size", "ask_size"),
                params={"window_events": 10, "min_periods": 3},
                lookback_seconds=120.0,
            ),
            "stage": LifecycleStage.STAGING,
        }
    )
    registry.register(f_roll_v11)
    mat_v11, man_v11 = offline.materialize(f_roll_v11, lob_df, run_id="demo-roll-v11")
    qrep_v11 = evaluator.evaluate(mat_v11, f_roll_v11, run_id=man_v11.run_id)
    assert qrep_v11.passed
    registry.promote(f_roll_v11.name, f_roll_v11.version, LifecycleStage.ACTIVE, quality_report=qrep_v11)

    bf_report = offline.backfill(
        "lob_imbalance_l1",
        lob_df,
        window_start=start_time + timedelta(seconds=100),
        window_end=start_time + timedelta(seconds=250),
        run_id="demo-backfill-1",
    )

    exported_df, export_manifest = offline.export_training_dataset(
        spine_df,
        lob_features,
        export_dir=exports_dir,
        export_name="lob_microstructure_train_v1",
        seed=42,
    )

    registry.save(registry_file)
    lineage.save(lineage_file)

    return {
        "registry_snapshot_hash": registry.snapshot_hash(),
        "registered_features": len(registry.list_all()),
        "quality_gates": quality_summaries,
        "parity": {
            "passed": parity_report.passed,
            "total_comparisons": parity_report.total_comparisons,
            "max_abs_diff": parity_report.max_abs_diff,
        },
        "online_sample_aapl": {
            "all_fresh": sample_vec.all_fresh,
            "features": {
                k: {
                    "value": round(float(v.value), 6) if v.value is not None else None,
                    "age_seconds": v.age_seconds,
                    "freshness_status": v.freshness_status.value,
                    "version_id": v.version_id,
                }
                for k, v in sample_vec.features.items()
            },
        },
        "backfill": {
            "run_id": bf_report.run_id,
            "rows_written": bf_report.rows_written,
            "rows_deduplicated": bf_report.rows_deduplicated,
        },
        "training_export": {
            "export_name": export_manifest.export_name,
            "row_count": exported_df.height,
            "sha256_checksum": export_manifest.sha256_checksum,
            "split_boundaries": export_manifest.split_boundaries,
            "zero_leakage_verified": export_manifest.zero_leakage_verified,
        },
        "lineage_nodes": len(lineage.snapshot().nodes),
        "lineage_edges": len(lineage.snapshot().edges),
    }


def main() -> None:
    summary = run_demo()
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
