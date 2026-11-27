"""Unit tests for shared causal transforms, quality evaluator, and lineage tracker."""

from __future__ import annotations

import math
from datetime import datetime, timedelta

import polars as pl
import pytest

from financial_feature_store import (
    FeatureDefinition,
    LeakageViolationError,
    LineageTracker,
    QualityEvaluator,
    QualityRuleSpec,
    SourceDatasetContract,
    TransformSpec,
    ValueType,
    compute_causal_feature_frame,
)
from financial_feature_store.adapters.sources import compute_schema_hash


def test_order_book_transforms_are_exact(base_time: datetime) -> None:
    df = pl.DataFrame(
        {
            "entity_id": ["EQ:AAPL", "EQ:AAPL"],
            "event_time": [base_time, base_time + timedelta(seconds=10)],
            "availability_time": [
                base_time + timedelta(milliseconds=200),
                base_time + timedelta(seconds=10, milliseconds=200),
            ],
            "best_bid": [100.0, 100.5],
            "best_ask": [100.2, 100.7],
            "bid_size": [300.0, 100.0],
            "ask_size": [100.0, 300.0],
        }
    )
    src = SourceDatasetContract(
        source_id="src_lob",
        dataset_name="lob",
        source_version="v1",
        schema_hash=compute_schema_hash(df),
        provenance_uri="mem://lob",
        currency="USD",
    )

    mid_def = FeatureDefinition(
        name="lob_mid",
        feature_view="lob",
        version="1.0.0",
        entity_type="instrument",
        value_type=ValueType.FLOAT64,
        unit="USD",
        currency="USD",
        description="Mid price of top of book.",
        owner="quant",
        source=src,
        transform=TransformSpec(transform_name="lob_mid_price", source_columns=("best_bid", "best_ask")),
        ttl_seconds=60.0,
    )
    mid_out = compute_causal_feature_frame(df, mid_def, run_id="r1")
    assert mid_out["value"].to_list() == pytest.approx([100.1, 100.6])

    imb_def = mid_def.model_copy(
        update={
            "name": "lob_imb",
            "unit": "ratio",
            "currency": None,
            "transform": TransformSpec(
                transform_name="lob_order_book_imbalance",
                source_columns=("bid_size", "ask_size"),
            ),
        }
    )
    imb_out = compute_causal_feature_frame(df, imb_def, run_id="r1")
    assert imb_out["value"].to_list() == pytest.approx([0.5, -0.5])

    micro_def = mid_def.model_copy(
        update={
            "name": "lob_micro",
            "transform": TransformSpec(
                transform_name="lob_microprice",
                source_columns=("best_bid", "best_ask", "bid_size", "ask_size"),
            ),
        }
    )
    micro_out = compute_causal_feature_frame(df, micro_def, run_id="r1")
    # (100.0*100 + 100.2*300)/400 = 100.15
    assert micro_out["value"][0] == pytest.approx(100.15)


def test_bar_log_return_rejects_non_causal_lag(synthetic_bars_df: pl.DataFrame) -> None:
    src = SourceDatasetContract(
        source_id="src_bars",
        dataset_name="bars",
        source_version="v1",
        schema_hash=compute_schema_hash(synthetic_bars_df),
        provenance_uri="mem://bars",
        currency="USD",
    )
    bad_def = FeatureDefinition(
        name="bad_return",
        feature_view="bars",
        version="1.0.0",
        entity_type="instrument",
        value_type=ValueType.FLOAT64,
        unit="log_return",
        description="Invalid negative lag should raise LeakageViolationError.",
        owner="quant",
        source=src,
        transform=TransformSpec(
            transform_name="bar_log_return",
            source_columns=("close",),
            params={"lag": -1},
        ),
        ttl_seconds=120.0,
    )
    with pytest.raises(LeakageViolationError):
        compute_causal_feature_frame(synthetic_bars_df, bad_def, run_id="r1")


def test_transaction_velocity_and_zscore_are_strictly_causal(
    synthetic_tx_df: pl.DataFrame,
) -> None:
    src = SourceDatasetContract(
        source_id="src_tx",
        dataset_name="account_tx",
        source_version="v1",
        schema_hash=compute_schema_hash(synthetic_tx_df),
        provenance_uri="mem://tx",
        currency="USD",
    )
    cnt_def = FeatureDefinition(
        name="tx_count_5m",
        feature_view="account_risk",
        version="1.0.0",
        entity_type="account",
        value_type=ValueType.INT64,
        unit="count",
        description="Causal 5-minute rolling transaction count per account.",
        owner="risk-ml",
        source=src,
        transform=TransformSpec(
            transform_name="tx_rolling_count",
            source_columns=("amount",),
            lookback_seconds=300.0,
        ),
        ttl_seconds=300.0,
    )
    cnt_out = compute_causal_feature_frame(synthetic_tx_df, cnt_def, run_id="r1")
    a1_counts = cnt_out.filter(pl.col("entity_id") == "ACCT:0001")["value"].to_list()
    assert a1_counts[0] == 1
    assert a1_counts[1] == 2
    assert max(a1_counts) <= 11  # 300s window with 30s steps -> at most 11 events inclusive

    z_def = cnt_def.model_copy(
        update={
            "name": "tx_amt_zscore_5m",
            "value_type": ValueType.FLOAT64,
            "unit": "zscore",
            "transform": TransformSpec(
                transform_name="tx_amount_zscore",
                source_columns=("amount",),
                params={"min_periods": 3},
                lookback_seconds=300.0,
            ),
        }
    )
    z_out = compute_causal_feature_frame(synthetic_tx_df, z_def, run_id="r1")
    a1_z = z_out.filter(pl.col("entity_id") == "ACCT:0001")["value"].to_list()
    # First 3 observations have fewer than 3 strictly prior observations -> None (no lookahead!)
    assert a1_z[:3] == [None, None, None]
    assert all(v is not None and math.isfinite(v) for v in a1_z[3:])


def test_quality_evaluator_catches_bounds_and_late_arrival(
    synthetic_lob_df: pl.DataFrame,
    lob_imbalance_def: FeatureDefinition,
) -> None:
    strict_def = lob_imbalance_def.model_copy(
        update={
            "quality_rules": QualityRuleSpec(
                min_row_count=5,
                max_null_ratio=0.05,
                min_value=-0.01,
                max_value=0.01,
                max_late_arrival_seconds=0.1,
            )
        }
    )
    mat_df = compute_causal_feature_frame(synthetic_lob_df, strict_def, run_id="r-qual")
    report = QualityEvaluator().evaluate(mat_df, strict_def, run_id="r-qual")
    assert not report.passed
    failed = {c.check_name for c in report.checks if not c.passed}
    assert "max_late_arrival_seconds" in failed
    assert ("min_value_bound" in failed) or ("max_value_bound" in failed)


def test_lineage_tracker_upstream_and_downstream(lob_imbalance_def: FeatureDefinition) -> None:
    tracker = LineageTracker()
    tracker.record_feature_definition(lob_imbalance_def)
    feat_node = f"feature:{lob_imbalance_def.version_id}"
    tracker.record_edge(feat_node, "offline_run:r1", "materialized_to")
    tracker.record_edge(feat_node, "training_export:ds1", "exported_to_training")

    upstream = tracker.upstream_of("training_export:ds1")
    assert feat_node in upstream
    assert any(n.startswith("source:") for n in upstream)

    src_node = f"source:{lob_imbalance_def.source.source_id}@{lob_imbalance_def.source.source_version}"
    downstream = tracker.downstream_of(src_node)
    assert "training_export:ds1" in downstream
    assert "offline_run:r1" in downstream
