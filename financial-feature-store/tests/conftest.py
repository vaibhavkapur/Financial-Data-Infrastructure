"""Shared pytest fixtures for Financial Feature Store test suite."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from financial_feature_store import (
    FeatureDefinition,
    FeatureRegistry,
    GovernanceConfig,
    LifecycleStage,
    LineageTracker,
    NullPolicy,
    OfflineFeatureStore,
    QualityEvaluator,
    QualityRuleSpec,
    SourceDatasetContract,
    StalePolicy,
    SyntheticDataGenerator,
    TransformSpec,
    ValueType,
)
from financial_feature_store.adapters.sources import compute_schema_hash


@pytest.fixture()
def base_time() -> datetime:
    return datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC)


@pytest.fixture()
def synthetic_lob_df(base_time: datetime) -> pl.DataFrame:
    return SyntheticDataGenerator(seed=42).generate_order_book_snapshots(
        entities=("EQ:AAPL", "EQ:MSFT"),
        start_time=base_time,
        steps=20,
        step_seconds=10,
        availability_delay_seconds=0.5,
    )


@pytest.fixture()
def synthetic_bars_df(base_time: datetime) -> pl.DataFrame:
    return SyntheticDataGenerator(seed=42).generate_market_bars(
        entities=("EQ:AAPL", "EQ:MSFT"),
        start_time=base_time,
        steps=20,
        step_seconds=60,
        availability_delay_seconds=1.0,
    )


@pytest.fixture()
def synthetic_tx_df(base_time: datetime) -> pl.DataFrame:
    return SyntheticDataGenerator(seed=42).generate_account_transactions(
        accounts=("ACCT:0001", "ACCT:0002"),
        start_time=base_time,
        steps=15,
        step_seconds=30,
        availability_delay_seconds=0.5,
    )


@pytest.fixture()
def lob_source_contract(synthetic_lob_df: pl.DataFrame) -> SourceDatasetContract:
    return SourceDatasetContract(
        source_id="src_lob_synthetic",
        dataset_name="l1_order_book_snapshots",
        source_version="2026.01.15-r1",
        schema_hash=compute_schema_hash(synthetic_lob_df),
        entitlement="synthetic-internal",
        license="internal-synthetic",
        provenance_uri="data/fixtures/lob_snapshots.parquet",
        currency="USD",
    )


@pytest.fixture()
def lob_imbalance_def(lob_source_contract: SourceDatasetContract) -> FeatureDefinition:
    return FeatureDefinition(
        name="lob_imbalance_l1",
        feature_view="order_book_microstructure",
        version="1.0.0",
        entity_type="instrument",
        value_type=ValueType.FLOAT64,
        unit="ratio",
        description="Level-1 order book size imbalance (bid_size - ask_size) / (bid_size + ask_size).",
        owner="quant-microstructure-team",
        source=lob_source_contract,
        transform=TransformSpec(
            transform_name="lob_order_book_imbalance",
            source_columns=("bid_size", "ask_size"),
            params={},
            lookback_seconds=0.0,
        ),
        null_policy=NullPolicy.KEEP_NULL,
        ttl_seconds=30.0,
        stale_policy=StalePolicy.REJECT,
        quality_rules=QualityRuleSpec(
            min_row_count=5,
            max_null_ratio=0.05,
            min_value=-1.0,
            max_value=1.0,
            max_late_arrival_seconds=10.0,
        ),
        stage=LifecycleStage.STAGING,
    )


@pytest.fixture()
def lob_rolling_imbalance_def(lob_source_contract: SourceDatasetContract) -> FeatureDefinition:
    return FeatureDefinition(
        name="lob_imbalance_roll_30s",
        feature_view="order_book_microstructure",
        version="1.0.0",
        entity_type="instrument",
        value_type=ValueType.FLOAT64,
        unit="ratio",
        description="Strictly causal 30-second rolling mean of level-1 order book imbalance.",
        owner="quant-microstructure-team",
        source=lob_source_contract,
        transform=TransformSpec(
            transform_name="lob_rolling_imbalance_mean",
            source_columns=("bid_size", "ask_size"),
            params={"window_events": 3, "min_periods": 2},
            lookback_seconds=30.0,
        ),
        null_policy=NullPolicy.KEEP_NULL,
        ttl_seconds=30.0,
        stale_policy=StalePolicy.REJECT,
        quality_rules=QualityRuleSpec(
            min_row_count=5,
            max_null_ratio=0.15,
            min_value=-1.0,
            max_value=1.0,
            max_late_arrival_seconds=10.0,
        ),
        stage=LifecycleStage.STAGING,
    )


@pytest.fixture()
def active_store_env(
    tmp_path: Path,
    synthetic_lob_df: pl.DataFrame,
    lob_imbalance_def: FeatureDefinition,
    lob_rolling_imbalance_def: FeatureDefinition,
) -> tuple[FeatureRegistry, OfflineFeatureStore, LineageTracker]:
    lineage = LineageTracker()
    registry = FeatureRegistry(governance=GovernanceConfig())
    registry.register(lob_imbalance_def)
    registry.register(lob_rolling_imbalance_def)

    offline = OfflineFeatureStore(tmp_path / "offline", registry, lineage=lineage)
    evaluator = QualityEvaluator()

    for fdef in (lob_imbalance_def, lob_rolling_imbalance_def):
        mat_df, manifest = offline.materialize(fdef, synthetic_lob_df, run_id=f"init-{fdef.name}")
        qrep = evaluator.evaluate(mat_df, fdef, run_id=manifest.run_id)
        assert qrep.passed, f"Fixture quality check unexpectedly failed: {qrep}"
        registry.promote(fdef.name, fdef.version, LifecycleStage.ACTIVE, quality_report=qrep)

    return registry, offline, lineage
