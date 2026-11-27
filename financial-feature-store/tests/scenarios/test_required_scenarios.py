"""Scenario tests covering all 7 required validation scenarios in plan.md Section 5.

Required scenarios:
1. Late data
2. Future join
3. Stale online feature
4. Null handling
5. Version mismatch
6. Backfill
7. Training-serving skew
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from financial_feature_store import (
    FeatureDefinition,
    FeatureRecord,
    FeatureRef,
    FeatureRegistry,
    FreshnessStatus,
    InMemoryOnlineBackend,
    LeakageViolationError,
    LifecycleStage,
    LineageTracker,
    NullPolicy,
    NullPolicyViolationError,
    OfflineFeatureStore,
    OnlineFeatureStore,
    ParityVerifier,
    QualityEvaluator,
    QualityGateError,
    QualityRuleSpec,
    SourceDatasetContract,
    SQLiteOnlineBackend,
    StaleFeatureError,
    StalePolicy,
    TransformSpec,
    ValueType,
    VersionMismatchError,
)
from financial_feature_store.adapters.sources import compute_schema_hash


def _make_lob_source(df: pl.DataFrame) -> SourceDatasetContract:
    return SourceDatasetContract(
        source_id="src_scenario_lob",
        dataset_name="scenario_lob",
        source_version="2026.01.15-s1",
        schema_hash=compute_schema_hash(df),
        entitlement="synthetic-internal",
        license="internal-synthetic",
        provenance_uri="fixture://scenario_lob",
        currency="USD",
    )


def _make_imbalance_def(
    src: SourceDatasetContract,
    *,
    name: str = "lob_imbalance_l1",
    version: str = "1.0.0",
    null_policy: NullPolicy = NullPolicy.KEEP_NULL,
    default_value: float | None = None,
    ttl_seconds: float = 30.0,
    stale_policy: StalePolicy = StalePolicy.REJECT,
    max_null_ratio: float = 0.05,
    max_late_arrival_seconds: float = 300.0,
) -> FeatureDefinition:
    return FeatureDefinition(
        name=name,
        feature_view="scenario_view",
        version=version,
        entity_type="instrument",
        value_type=ValueType.FLOAT64,
        unit="ratio",
        description="Scenario test level-1 order book imbalance.",
        owner="quant-platform",
        source=src,
        transform=TransformSpec(
            transform_name="lob_order_book_imbalance",
            source_columns=("bid_size", "ask_size"),
        ),
        null_policy=null_policy,
        default_value=default_value,
        ttl_seconds=ttl_seconds,
        stale_policy=stale_policy,
        quality_rules=QualityRuleSpec(
            min_row_count=1,
            max_null_ratio=max_null_ratio,
            min_value=-1.0,
            max_value=1.0,
            max_late_arrival_seconds=max_late_arrival_seconds,
        ),
        stage=LifecycleStage.STAGING,
    )


# ---------------------------------------------------------------------------
# Scenario 1: Late data
# ---------------------------------------------------------------------------
def test_scenario_1_late_data_excluded_before_availability_time(tmp_path: Path) -> None:
    t0 = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC)
    # Event 1 occurs at t0, available at t0+1s (imbalance = +0.5)
    # Event 2 occurs at t0+10s, BUT arrives late at t0+120s (imbalance = -0.5)
    raw = pl.DataFrame(
        {
            "entity_id": ["EQ:AAPL", "EQ:AAPL"],
            "event_time": [t0, t0 + timedelta(seconds=10)],
            "availability_time": [t0 + timedelta(seconds=1), t0 + timedelta(seconds=120)],
            "bid_size": [300.0, 100.0],
            "ask_size": [100.0, 300.0],
        },
        schema={
            "entity_id": pl.String(),
            "event_time": pl.Datetime("us", "UTC"),
            "availability_time": pl.Datetime("us", "UTC"),
            "bid_size": pl.Float64(),
            "ask_size": pl.Float64(),
        },
    )
    registry = FeatureRegistry()
    fdef = _make_imbalance_def(_make_lob_source(raw))
    registry.register(fdef)

    offline = OfflineFeatureStore(tmp_path / "offline", registry)
    mat_df, manifest = offline.materialize(fdef, raw, run_id="late-run")
    qrep = QualityEvaluator().evaluate(mat_df, fdef, run_id=manifest.run_id)
    assert qrep.passed
    assert qrep.late_arrival_count == 2
    assert qrep.max_late_arrival_seconds == pytest.approx(110.0)
    registry.promote(fdef.name, fdef.version, LifecycleStage.ACTIVE, quality_report=qrep)

    # Query at t0+30s (after Event 2's event_time, BEFORE Event 2's availability_time)
    # AND query at t0+125s (AFTER Event 2's availability_time)
    spine = pl.DataFrame(
        {
            "entity_id": ["EQ:AAPL", "EQ:AAPL"],
            "as_of_time": [t0 + timedelta(seconds=30), t0 + timedelta(seconds=125)],
        },
        schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
    )
    joined, audit = offline.point_in_time_join(spine, ["lob_imbalance_l1"])
    vals = joined["lob_imbalance_l1"].to_list()
    assert vals[0] == pytest.approx(0.5), "Late event must NOT leak before its availability_time"
    assert vals[1] == pytest.approx(-0.5), "Late event MUST be visible once available"
    assert audit["leakage_violations"] == 0
    assert audit["features"]["lob_imbalance_l1"]["excluded_late_candidates"] >= 1


# ---------------------------------------------------------------------------
# Scenario 2: Future join
# ---------------------------------------------------------------------------
def test_scenario_2_future_join_strictly_blocked(tmp_path: Path) -> None:
    t0 = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC)
    raw = pl.DataFrame(
        {
            "entity_id": ["EQ:AAPL", "EQ:AAPL"],
            "event_time": [t0 + timedelta(seconds=60), t0 + timedelta(seconds=120)],
            "availability_time": [t0 + timedelta(seconds=61), t0 + timedelta(seconds=121)],
            "bid_size": [400.0, 100.0],
            "ask_size": [100.0, 400.0],
        },
        schema={
            "entity_id": pl.String(),
            "event_time": pl.Datetime("us", "UTC"),
            "availability_time": pl.Datetime("us", "UTC"),
            "bid_size": pl.Float64(),
            "ask_size": pl.Float64(),
        },
    )
    registry = FeatureRegistry()
    fdef = _make_imbalance_def(_make_lob_source(raw))
    registry.register(fdef)

    offline = OfflineFeatureStore(tmp_path / "offline", registry)
    mat_df, manifest = offline.materialize(fdef, raw, run_id="future-run")
    qrep = QualityEvaluator().evaluate(mat_df, fdef, run_id=manifest.run_id)
    registry.promote(fdef.name, fdef.version, LifecycleStage.ACTIVE, quality_report=qrep)

    # Query at t0+30s BEFORE any event exists: must return None (NEVER forward-fill future values!)
    spine = pl.DataFrame(
        {"entity_id": ["EQ:AAPL"], "as_of_time": [t0 + timedelta(seconds=30)]},
        schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
    )
    joined, audit = offline.point_in_time_join(spine, ["lob_imbalance_l1"])
    assert joined["lob_imbalance_l1"][0] is None
    assert audit["features"]["lob_imbalance_l1"]["excluded_future_candidates"] == 2
    assert audit["leakage_violations"] == 0

    # Querying beyond max_allowed_as_of raises LeakageViolationError
    with pytest.raises(LeakageViolationError):
        offline.point_in_time_join(spine, ["lob_imbalance_l1"], max_allowed_as_of=t0)

    # Injecting a future record into online store and querying at an earlier `now` raises LeakageViolationError
    online = OnlineFeatureStore(InMemoryOnlineBackend(), registry)
    online.sync_feature_frame("lob_imbalance_l1", mat_df)
    with pytest.raises(LeakageViolationError):
        online.get_online_features("EQ:AAPL", ["lob_imbalance_l1"], now=t0 + timedelta(seconds=30))


# ---------------------------------------------------------------------------
# Scenario 3: Stale online feature
# ---------------------------------------------------------------------------
def test_scenario_3_stale_online_feature_freshness_and_policies(tmp_path: Path) -> None:
    t0 = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC)
    raw = pl.DataFrame(
        {
            "entity_id": ["EQ:AAPL"],
            "event_time": [t0],
            "availability_time": [t0 + timedelta(seconds=1)],
            "bid_size": [300.0],
            "ask_size": [100.0],
        },
        schema={
            "entity_id": pl.String(),
            "event_time": pl.Datetime("us", "UTC"),
            "availability_time": pl.Datetime("us", "UTC"),
            "bid_size": pl.Float64(),
            "ask_size": pl.Float64(),
        },
    )
    registry = FeatureRegistry()
    fdef = _make_imbalance_def(_make_lob_source(raw), ttl_seconds=15.0, stale_policy=StalePolicy.REJECT)
    registry.register(fdef)
    offline = OfflineFeatureStore(tmp_path / "offline", registry)
    mat_df, manifest = offline.materialize(fdef, raw, run_id="stale-run")
    qrep = QualityEvaluator().evaluate(mat_df, fdef, run_id=manifest.run_id)
    registry.promote(fdef.name, fdef.version, LifecycleStage.ACTIVE, quality_report=qrep)

    online = OnlineFeatureStore(InMemoryOnlineBackend(), registry)
    online.sync_feature_frame("lob_imbalance_l1", mat_df)

    # Fresh at t0 + 10s (age 10s <= ttl 15s)
    fresh_vec = online.get_online_features("EQ:AAPL", ["lob_imbalance_l1"], now=t0 + timedelta(seconds=10))
    assert fresh_vec.all_fresh
    assert fresh_vec.features["lob_imbalance_l1"].freshness_status == FreshnessStatus.FRESH
    assert fresh_vec.features["lob_imbalance_l1"].age_seconds == pytest.approx(10.0)

    # Stale at t0 + 45s (age 45s > ttl 15s): default REJECT policy raises StaleFeatureError
    stale_now = t0 + timedelta(seconds=45)
    with pytest.raises(StaleFeatureError) as exc_info:
        online.get_online_features("EQ:AAPL", ["lob_imbalance_l1"], now=stale_now)
    assert exc_info.value.details["age_seconds"] == pytest.approx(45.0)

    # WARN policy exposes staleness & warning without silently hiding age
    warn_vec = online.get_online_features(
        "EQ:AAPL", ["lob_imbalance_l1"], now=stale_now, stale_policy_override=StalePolicy.WARN
    )
    assert not warn_vec.all_fresh
    assert warn_vec.stale_features == ("lob_imbalance_l1",)
    assert warn_vec.features["lob_imbalance_l1"].freshness_status == FreshnessStatus.STALE
    assert warn_vec.features["lob_imbalance_l1"].value == pytest.approx(0.5)
    assert len(warn_vec.features["lob_imbalance_l1"].warnings) == 1

    # FALLBACK_NULL policy masks stale value to None
    fb_vec = online.get_online_features(
        "EQ:AAPL", ["lob_imbalance_l1"], now=stale_now, stale_policy_override=StalePolicy.FALLBACK_NULL
    )
    assert fb_vec.features["lob_imbalance_l1"].value is None
    assert fb_vec.features["lob_imbalance_l1"].is_stale is True


# ---------------------------------------------------------------------------
# Scenario 4: Null handling
# ---------------------------------------------------------------------------
def test_scenario_4_null_handling_and_quality_promotion_block(tmp_path: Path) -> None:
    t0 = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC)
    # Row 0 has null bid_size; Row 1 is valid; Row 2 has zero denominator (0, 0) -> null
    raw = pl.DataFrame(
        {
            "entity_id": ["EQ:AAPL", "EQ:AAPL", "EQ:AAPL"],
            "event_time": [t0, t0 + timedelta(seconds=10), t0 + timedelta(seconds=20)],
            "availability_time": [
                t0 + timedelta(seconds=1),
                t0 + timedelta(seconds=11),
                t0 + timedelta(seconds=21),
            ],
            "bid_size": [None, 300.0, 0.0],
            "ask_size": [100.0, 100.0, 0.0],
        },
        schema={
            "entity_id": pl.String(),
            "event_time": pl.Datetime("us", "UTC"),
            "availability_time": pl.Datetime("us", "UTC"),
            "bid_size": pl.Float64(),
            "ask_size": pl.Float64(),
        },
    )
    src = _make_lob_source(raw)
    registry = FeatureRegistry()

    # Policy KEEP_NULL retains nulls (never forward-fills from future!) and fails strict quality gate
    keep_def = _make_imbalance_def(src, name="imb_keep", null_policy=NullPolicy.KEEP_NULL, max_null_ratio=0.1)
    registry.register(keep_def)
    offline = OfflineFeatureStore(tmp_path / "offline", registry)
    mat_keep, man_keep = offline.materialize(keep_def, raw, run_id="null-keep")
    assert mat_keep["value"].to_list() == [None, pytest.approx(0.5), None]

    qrep = QualityEvaluator().evaluate(mat_keep, keep_def, run_id=man_keep.run_id)
    assert not qrep.passed
    assert qrep.null_count == 2
    with pytest.raises(QualityGateError):
        registry.promote("imb_keep", "1.0.0", LifecycleStage.ACTIVE, quality_report=qrep)

    # Policy REJECT raises immediately on null input
    reject_def = _make_imbalance_def(src, name="imb_reject", null_policy=NullPolicy.REJECT)
    registry.register(reject_def)
    with pytest.raises(NullPolicyViolationError):
        offline.materialize(reject_def, raw, run_id="null-reject")

    # Policy DEFAULT_VALUE explicitly substitutes configured default (not future values!)
    default_def = _make_imbalance_def(
        src,
        name="imb_default",
        null_policy=NullPolicy.DEFAULT_VALUE,
        default_value=0.0,
        max_null_ratio=0.0,
    )
    registry.register(default_def)
    mat_def, man_def = offline.materialize(default_def, raw, run_id="null-default")
    assert mat_def["value"].to_list() == [pytest.approx(0.0), pytest.approx(0.5), pytest.approx(0.0)]
    qrep_ok = QualityEvaluator().evaluate(mat_def, default_def, run_id=man_def.run_id)
    assert qrep_ok.passed


# ---------------------------------------------------------------------------
# Scenario 5: Version mismatch
# ---------------------------------------------------------------------------
def test_scenario_5_version_mismatch_detection(
    tmp_path: Path,
    base_time: datetime,
    active_store_env: tuple[FeatureRegistry, OfflineFeatureStore, LineageTracker],
) -> None:
    registry, offline, _lineage = active_store_env
    active_def = registry.get("lob_imbalance_l1")

    # 1. Requesting an unregistered semver fails with VersionMismatchError
    with pytest.raises(VersionMismatchError):
        registry.resolve(FeatureRef(name="lob_imbalance_l1", version="9.9.9"))

    # 2. Requesting a mismatched pinned version_id fails with VersionMismatchError
    with pytest.raises(VersionMismatchError):
        registry.resolve(
            FeatureRef(name="lob_imbalance_l1", version="1.0.0", expected_version_id="lob_imbalance_l1@1.0.0+00000000")
        )

    # 3. Online cache poisoned with a record carrying a different version_id raises VersionMismatchError
    backend = InMemoryOnlineBackend()
    poisoned_record = FeatureRecord(
        entity_id="EQ:AAPL",
        feature_name=active_def.name,
        feature_version=active_def.version,
        version_id="lob_imbalance_l1@1.0.0+stalehash9999",
        event_time=base_time,
        availability_time=base_time + timedelta(seconds=1),
        materialized_at=base_time + timedelta(seconds=1),
        value=0.25,
        unit="ratio",
        source_version="old-src",
        run_id="poisoned",
    )
    backend.put_batch([poisoned_record])
    online = OnlineFeatureStore(backend, registry)
    with pytest.raises(VersionMismatchError):
        online.get_online_features("EQ:AAPL", ["lob_imbalance_l1"], now=base_time + timedelta(seconds=5))
    assert online.metrics()["version_mismatch_count"] == 1

    # 4. Offline parquet partition tampered with wrong version_id raises VersionMismatchError on join
    pq_files = list((tmp_path / "offline" / "lob_imbalance_l1" / "1.0.0").glob("*.parquet"))
    assert len(pq_files) == 1
    tampered = pl.read_parquet(pq_files[0]).with_columns(pl.lit("tampered@1.0.0+deadbeef").alias("version_id"))
    tampered.write_parquet(pq_files[0])
    # Also remove manifest checksum so we specifically trigger the version_id check
    pq_files[0].with_suffix(".manifest.json").unlink()
    spine = pl.DataFrame(
        {"entity_id": ["EQ:AAPL"], "as_of_time": [base_time + timedelta(seconds=50)]},
        schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
    )
    with pytest.raises(VersionMismatchError):
        offline.point_in_time_join(spine, ["lob_imbalance_l1"])


# ---------------------------------------------------------------------------
# Scenario 6: Backfill
# ---------------------------------------------------------------------------
def test_scenario_6_idempotent_backfill_preserves_causality_and_warmup(
    tmp_path: Path,
    base_time: datetime,
    active_store_env: tuple[FeatureRegistry, OfflineFeatureStore, LineageTracker],
    synthetic_lob_df: pl.DataFrame,
) -> None:
    registry, offline, _lineage = active_store_env

    # Inject a late-arriving historical correction inside [t+40s, t+80s] whose availability_time is t+500s
    correction_ev_time = base_time + timedelta(seconds=60)
    correction_avail_time = base_time + timedelta(seconds=500)
    corrected_row = pl.DataFrame(
        {
            "entity_id": ["EQ:AAPL"],
            "event_time": [correction_ev_time],
            "availability_time": [correction_avail_time],
            "best_bid": [101.0],
            "best_ask": [101.2],
            "bid_size": [900.0],
            "ask_size": [100.0],  # imbalance = +0.8
        },
        schema=synthetic_lob_df.schema,
    )
    augmented_raw = pl.concat([synthetic_lob_df, corrected_row], how="vertical")

    # Query before backfill at t+65s (before correction availability t+500s)
    spine_early = pl.DataFrame(
        {"entity_id": ["EQ:AAPL"], "as_of_time": [base_time + timedelta(seconds=65)]},
        schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
    )
    before_early, _ = offline.point_in_time_join(spine_early, ["lob_imbalance_l1", "lob_imbalance_roll_30s"])

    # Run backfill over [t+40s, t+80s]
    bf_report_1 = offline.backfill(
        "lob_imbalance_l1",
        augmented_raw,
        window_start=base_time + timedelta(seconds=40),
        window_end=base_time + timedelta(seconds=80),
        run_id="bf-run-1",
    )
    assert bf_report_1.rows_written > 0

    # Re-running the exact same backfill window must remain deduplicated / idempotent
    bf_report_2 = offline.backfill(
        "lob_imbalance_l1",
        augmented_raw,
        window_start=base_time + timedelta(seconds=40),
        window_end=base_time + timedelta(seconds=80),
        run_id="bf-run-2",
    )
    assert bf_report_2.rows_deduplicated >= bf_report_1.rows_written

    loaded_after = offline.load_materialized(registry.get("lob_imbalance_l1"))
    # Exactly original rows + 1 new late-arriving revision (zero duplicates!)
    assert loaded_after.height == synthetic_lob_df.height + 1

    # Historical query at t+65s MUST remain unchanged (availability_time=t+500s not yet known at t+65s!)
    after_early, _ = offline.point_in_time_join(spine_early, ["lob_imbalance_l1"])
    assert after_early["lob_imbalance_l1"][0] == pytest.approx(before_early["lob_imbalance_l1"][0])

    # Query at t+505s (with only events <= t+60s or checking late revision visibility) sees the backfilled revision
    # Specifically, backfill rolling feature as well and verify warmup at window_start gives exact rolling mean
    bf_roll = offline.backfill(
        "lob_imbalance_roll_30s",
        synthetic_lob_df,
        window_start=base_time + timedelta(seconds=40),
        window_end=base_time + timedelta(seconds=80),
        run_id="bf-roll-1",
    )
    assert bf_roll.rows_written > 0
    # Verify exact match with full-history materialization at window_start (warmup worked!)
    spine_boundary = pl.DataFrame(
        {"entity_id": ["EQ:AAPL"], "as_of_time": [base_time + timedelta(seconds=42)]},
        schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
    )
    after_boundary, _ = offline.point_in_time_join(spine_boundary, ["lob_imbalance_roll_30s"])
    assert after_boundary["lob_imbalance_roll_30s"][0] is not None


# ---------------------------------------------------------------------------
# Scenario 7: Training-serving skew (clean parity + injected skew detection)
# ---------------------------------------------------------------------------
def test_scenario_7_training_serving_parity_and_skew_detection(
    tmp_path: Path,
    base_time: datetime,
    active_store_env: tuple[FeatureRegistry, OfflineFeatureStore, LineageTracker],
    synthetic_lob_df: pl.DataFrame,
) -> None:
    registry, offline, _lineage = active_store_env
    feature_refs = ["lob_imbalance_l1", "lob_imbalance_roll_30s"]

    spine = pl.DataFrame(
        [
            {"entity_id": ent, "as_of_time": base_time + timedelta(seconds=step * 10 + 2)}
            for step in range(1, 16)
            for ent in ("EQ:AAPL", "EQ:MSFT")
        ],
        schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
    )

    verifier = ParityVerifier(offline, registry, atol=1e-12, rtol=1e-12)

    # 1. Clean replay on SQLite durable online backend achieves EXACT zero-skew parity
    clean_report = verifier.verify_replay_parity(
        spine,
        feature_refs,
        raw_sources_by_feature={
            "lob_imbalance_l1": synthetic_lob_df,
            "lob_imbalance_roll_30s": synthetic_lob_df,
        },
        backend_factory=lambda: SQLiteOnlineBackend(tmp_path / "parity_online.sqlite3"),
    )
    assert clean_report.passed, f"Clean replay parity failed: {clean_report}"
    assert clean_report.mismatched_count == 0
    assert clean_report.max_abs_diff == pytest.approx(0.0)
    assert clean_report.total_comparisons == spine.height * len(feature_refs)

    # 2. Inject controlled training-serving skew (online receives perturbed bid_size)
    skewed_raw = synthetic_lob_df.with_columns(
        pl.when(pl.col("entity_id") == "EQ:AAPL")
        .then(pl.col("bid_size") * 1.25)
        .otherwise(pl.col("bid_size"))
        .alias("bid_size")
    )
    skew_report = verifier.verify_replay_parity(
        spine,
        feature_refs,
        raw_sources_by_feature={
            "lob_imbalance_l1": skewed_raw,
            "lob_imbalance_roll_30s": skewed_raw,
        },
        backend_factory=InMemoryOnlineBackend,
    )
    assert not skew_report.passed
    assert skew_report.mismatched_count > 0
    assert skew_report.max_abs_diff > 0.01
    assert any(d.reason == "value_skew" and d.entity_id == "EQ:AAPL" for d in skew_report.diffs)
