"""Unit tests for domain contracts, UTC timestamps, registry governance, and schema evolution."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from financial_feature_store import (
    FeatureDefinition,
    FeatureProliferationError,
    FeatureRecord,
    FeatureRef,
    FeatureRegistry,
    GovernanceConfig,
    ImmutableVersionError,
    LifecycleStage,
    NullPolicy,
    QualityCheckResult,
    QualityGateError,
    QualityReport,
    SchemaCompatibilityError,
    SourceDatasetContract,
    TimestampContractError,
    TransformSpec,
    ValueType,
    VersionMismatchError,
    ensure_utc,
)


def test_ensure_utc_rejects_naive_datetime() -> None:
    naive = datetime(2026, 1, 15, 14, 30, 0)
    with pytest.raises(TimestampContractError):
        ensure_utc(naive)


def test_ensure_utc_normalizes_non_utc_offset() -> None:
    ist = timezone(timedelta(hours=5, minutes=30))
    dt = datetime(2026, 1, 15, 20, 0, 0, tzinfo=ist)
    utc = ensure_utc(dt)
    assert utc.tzinfo == UTC
    assert utc.hour == 14 and utc.minute == 30


def test_feature_record_rejects_availability_before_event() -> None:
    t0 = datetime(2026, 1, 15, 14, 30, 10, tzinfo=UTC)
    t_earlier = datetime(2026, 1, 15, 14, 30, 5, tzinfo=UTC)
    with pytest.raises(TimestampContractError):
        FeatureRecord(
            entity_id="EQ:AAPL",
            feature_name="lob_imbalance_l1",
            feature_version="1.0.0",
            version_id="lob_imbalance_l1@1.0.0+deadbeef",
            event_time=t0,
            availability_time=t_earlier,
            materialized_at=t0,
            value=0.1,
            unit="ratio",
            source_version="r1",
            run_id="r1",
        )


def test_version_id_identifies_exact_transform_and_source(
    lob_imbalance_def: FeatureDefinition,
    lob_source_contract: SourceDatasetContract,
) -> None:
    # Different transform parameter changes version_id
    changed_transform = lob_imbalance_def.model_copy(
        update={
            "transform": TransformSpec(
                transform_name="lob_order_book_imbalance",
                source_columns=("bid_size", "ask_size"),
                params={"bid_size_col": "bid_size_l2"},
                lookback_seconds=0.0,
            )
        }
    )
    assert changed_transform.version_id != lob_imbalance_def.version_id

    # Different source version changes version_id
    changed_source = lob_imbalance_def.model_copy(
        update={
            "source": lob_source_contract.model_copy(update={"source_version": "2026.01.16-r2"})
        }
    )
    assert changed_source.version_id != lob_imbalance_def.version_id


def test_registry_enforces_immutability_and_idempotency(
    lob_imbalance_def: FeatureDefinition,
) -> None:
    registry = FeatureRegistry()
    registry.register(lob_imbalance_def)
    # Re-registering identical contract is idempotent
    assert registry.register(lob_imbalance_def).version_id == lob_imbalance_def.version_id

    # Mutating transform under the same semver is rejected
    mutated = lob_imbalance_def.model_copy(
        update={
            "transform": TransformSpec(
                transform_name="lob_spread_bps",
                source_columns=("best_bid", "best_ask"),
            )
        }
    )
    with pytest.raises(ImmutableVersionError):
        registry.register(mutated)


def test_registry_blocks_unbounded_feature_proliferation(
    lob_imbalance_def: FeatureDefinition,
) -> None:
    registry = FeatureRegistry(
        governance=GovernanceConfig(max_active_features_per_view=2, max_total_active_features=3)
    )
    f1 = lob_imbalance_def.model_copy(update={"name": "feat_one"})
    f2 = lob_imbalance_def.model_copy(update={"name": "feat_two"})
    f3 = lob_imbalance_def.model_copy(update={"name": "feat_three"})

    registry.register(f1)
    registry.register(f2)
    with pytest.raises(FeatureProliferationError):
        registry.register(f3)

    # Another view still respects total limit
    f3_other_view = f3.model_copy(update={"feature_view": "other_view"})
    registry.register(f3_other_view)
    f4_other_view = f3.model_copy(update={"name": "feat_four", "feature_view": "other_view"})
    with pytest.raises(FeatureProliferationError):
        registry.register(f4_other_view)


def test_registry_schema_evolution_semver_rules(
    lob_imbalance_def: FeatureDefinition,
) -> None:
    registry = FeatureRegistry()
    registry.register(lob_imbalance_def)

    # Breaking unit change with only minor bump is rejected
    breaking_minor = lob_imbalance_def.model_copy(update={"version": "1.1.0", "unit": "bps"})
    with pytest.raises(SchemaCompatibilityError):
        registry.register(breaking_minor)

    # Breaking unit change with major bump is accepted
    breaking_major = lob_imbalance_def.model_copy(update={"version": "2.0.0", "unit": "bps"})
    registry.register(breaking_major)

    # Transform change with only patch bump is rejected
    transform_patch = breaking_major.model_copy(
        update={
            "version": "2.0.1",
            "transform": TransformSpec(
                transform_name="lob_spread_bps",
                source_columns=("best_bid", "best_ask"),
            ),
        }
    )
    with pytest.raises(SchemaCompatibilityError):
        registry.register(transform_patch)

    # Transform change with minor bump is accepted
    transform_minor = transform_patch.model_copy(update={"version": "2.1.0"})
    registry.register(transform_minor)
    assert [v.version for v in registry.list_versions(lob_imbalance_def.name)] == ["1.0.0", "2.0.0", "2.1.0"]


def test_registry_promotion_gate_and_persistence(
    tmp_path: Path,
    lob_imbalance_def: FeatureDefinition,
) -> None:
    registry = FeatureRegistry()
    registry.register(lob_imbalance_def)

    # Cannot promote without a QualityReport
    with pytest.raises(QualityGateError):
        registry.promote(lob_imbalance_def.name, "1.0.0", LifecycleStage.ACTIVE)

    # Failed QualityReport blocks promotion
    failed_report = QualityReport(
        feature_name=lob_imbalance_def.name,
        feature_version="1.0.0",
        version_id=lob_imbalance_def.version_id,
        run_id="run-bad",
        passed=False,
        row_count=10,
        null_count=5,
        null_ratio=0.5,
        late_arrival_count=0,
        max_late_arrival_seconds=0.0,
        checks=(
            QualityCheckResult(
                check_name="max_null_ratio",
                passed=False,
                observed=0.5,
                threshold=0.05,
                message="Null ratio exceeded",
            ),
        ),
    )
    with pytest.raises(QualityGateError):
        registry.promote(lob_imbalance_def.name, "1.0.0", LifecycleStage.ACTIVE, quality_report=failed_report)

    # Passing QualityReport unlocks promotion
    passing_report = failed_report.model_copy(
        update={
            "passed": True,
            "null_count": 0,
            "null_ratio": 0.0,
            "checks": (
                QualityCheckResult(
                    check_name="max_null_ratio",
                    passed=True,
                    observed=0.0,
                    threshold=0.05,
                    message="OK",
                ),
            ),
        }
    )
    promoted = registry.promote(
        lob_imbalance_def.name, "1.0.0", LifecycleStage.ACTIVE, quality_report=passing_report
    )
    assert promoted.stage == LifecycleStage.ACTIVE

    # Round-trip save/load preserves snapshot hash and evidence
    reg_file = registry.save(tmp_path / "registry.json")
    loaded = FeatureRegistry.load(reg_file)
    assert loaded.snapshot_hash() == registry.snapshot_hash()
    assert loaded.resolve(FeatureRef(name=lob_imbalance_def.name)).version_id == lob_imbalance_def.version_id

    # Pinned version_id mismatch fails resolution
    with pytest.raises(VersionMismatchError):
        loaded.resolve(FeatureRef(name=lob_imbalance_def.name, expected_version_id="wrong@1.0.0+0000"))

    # Default value null policy requires default_value
    with pytest.raises(ValueError):
        FeatureDefinition(
            name="bad_null_def",
            feature_view="test_view",
            version="1.0.0",
            entity_type="instrument",
            value_type=ValueType.FLOAT64,
            unit="ratio",
            description="Missing default value should fail validation.",
            owner="quant-team",
            source=lob_imbalance_def.source,
            transform=lob_imbalance_def.transform,
            null_policy=NullPolicy.DEFAULT_VALUE,
            default_value=None,
            ttl_seconds=10.0,
        )
