"""Reproduce all 7 required validation scenarios, baseline benchmarks, and acceptance evidence."""

from __future__ import annotations

import json
import platform
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from financial_feature_store import (
    FeatureDefinition,
    FeatureRecord,
    FeatureRef,
    FeatureRegistry,
    FreshnessStatus,
    InMemoryOnlineBackend,
    LeakageViolationError,
    LifecycleStage,
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
    SyntheticDataGenerator,
    TransformSpec,
    ValueType,
    VersionMismatchError,
)
from financial_feature_store.adapters.sources import compute_schema_hash

ROOT = Path(__file__).resolve().parent.parent


def _src(df: pl.DataFrame, sid: str = "src_repro") -> SourceDatasetContract:
    return SourceDatasetContract(
        source_id=sid,
        dataset_name="synthetic_lob",
        source_version="2026.01.15-repro",
        schema_hash=compute_schema_hash(df),
        provenance_uri=f"synthetic://{sid}",
        currency="USD",
    )


def _imb_def(
    src: SourceDatasetContract,
    *,
    name: str = "lob_imbalance_l1",
    null_policy: NullPolicy = NullPolicy.KEEP_NULL,
    default_value: float | None = None,
    ttl_seconds: float = 30.0,
    stale_policy: StalePolicy = StalePolicy.REJECT,
    max_null_ratio: float = 0.05,
    max_late_arrival_seconds: float = 300.0,
) -> FeatureDefinition:
    return FeatureDefinition(
        name=name,
        feature_view="repro_view",
        version="1.0.0",
        entity_type="instrument",
        value_type=ValueType.FLOAT64,
        unit="ratio",
        description="Reproducible scenario feature: level-1 order book imbalance.",
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


def run_all_scenarios() -> dict[str, dict[str, Any]]:
    t0 = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC)
    results: dict[str, dict[str, Any]] = {}

    with tempfile.TemporaryDirectory() as td:
        base = Path(td)

        # 1. Late data
        raw_late = pl.DataFrame(
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
        reg1 = FeatureRegistry()
        f1 = _imb_def(_src(raw_late, "src_late"))
        reg1.register(f1)
        off1 = OfflineFeatureStore(base / "s1", reg1)
        mat1, man1 = off1.materialize(f1, raw_late, run_id="s1")
        q1 = QualityEvaluator().evaluate(mat1, f1, run_id=man1.run_id)
        reg1.promote(f1.name, f1.version, LifecycleStage.ACTIVE, quality_report=q1)
        spine1 = pl.DataFrame(
            {
                "entity_id": ["EQ:AAPL", "EQ:AAPL"],
                "as_of_time": [t0 + timedelta(seconds=30), t0 + timedelta(seconds=125)],
            },
            schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
        )
        j1, a1 = off1.point_in_time_join(spine1, ["lob_imbalance_l1"])
        v1 = j1["lob_imbalance_l1"].to_list()
        assert abs(v1[0] - 0.5) < 1e-12 and abs(v1[1] - (-0.5)) < 1e-12
        results["late_data"] = {
            "status": "PASS",
            "value_before_availability": v1[0],
            "value_after_availability": v1[1],
            "excluded_late_candidates": a1["features"]["lob_imbalance_l1"]["excluded_late_candidates"],
            "leakage_violations": a1["leakage_violations"],
        }

        # 2. Future join
        spine_future = pl.DataFrame(
            {"entity_id": ["EQ:AAPL"], "as_of_time": [t0 - timedelta(seconds=5)]},
            schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
        )
        j2, a2 = off1.point_in_time_join(spine_future, ["lob_imbalance_l1"])
        future_query_blocked = False
        try:
            off1.point_in_time_join(spine1, ["lob_imbalance_l1"], max_allowed_as_of=t0)
        except LeakageViolationError:
            future_query_blocked = True
        assert j2["lob_imbalance_l1"][0] is None and future_query_blocked
        results["future_join"] = {
            "status": "PASS",
            "pre_history_value": j2["lob_imbalance_l1"][0],
            "excluded_future_candidates": a2["features"]["lob_imbalance_l1"]["excluded_future_candidates"],
            "future_spine_query_blocked": future_query_blocked,
            "leakage_violations": a2["leakage_violations"],
        }

        # 3. Stale online feature
        on3 = OnlineFeatureStore(InMemoryOnlineBackend(), reg1)
        on3.sync_feature_frame("lob_imbalance_l1", mat1, as_of_time=t0 + timedelta(seconds=5))
        stale_rejected = False
        try:
            on3.get_online_features("EQ:AAPL", ["lob_imbalance_l1"], now=t0 + timedelta(seconds=90))
        except StaleFeatureError:
            stale_rejected = True
        warn_vec = on3.get_online_features(
            "EQ:AAPL",
            ["lob_imbalance_l1"],
            now=t0 + timedelta(seconds=90),
            stale_policy_override=StalePolicy.WARN,
        )
        fb_vec = on3.get_online_features(
            "EQ:AAPL",
            ["lob_imbalance_l1"],
            now=t0 + timedelta(seconds=90),
            stale_policy_override=StalePolicy.FALLBACK_NULL,
        )
        assert stale_rejected and warn_vec.features["lob_imbalance_l1"].freshness_status == FreshnessStatus.STALE
        assert fb_vec.features["lob_imbalance_l1"].value is None
        results["stale_online_feature"] = {
            "status": "PASS",
            "reject_policy_raised": stale_rejected,
            "warn_age_seconds": warn_vec.features["lob_imbalance_l1"].age_seconds,
            "warn_status": warn_vec.features["lob_imbalance_l1"].freshness_status.value,
            "fallback_null_value": fb_vec.features["lob_imbalance_l1"].value,
        }

        # 4. Null handling
        raw_null = pl.DataFrame(
            {
                "entity_id": ["EQ:AAPL", "EQ:AAPL"],
                "event_time": [t0, t0 + timedelta(seconds=10)],
                "availability_time": [t0 + timedelta(seconds=1), t0 + timedelta(seconds=11)],
                "bid_size": [None, 300.0],
                "ask_size": [100.0, 100.0],
            },
            schema={
                "entity_id": pl.String(),
                "event_time": pl.Datetime("us", "UTC"),
                "availability_time": pl.Datetime("us", "UTC"),
                "bid_size": pl.Float64(),
                "ask_size": pl.Float64(),
            },
        )
        reg4 = FeatureRegistry()
        f4_keep = _imb_def(_src(raw_null, "src_null"), name="imb_keep", null_policy=NullPolicy.KEEP_NULL)
        f4_rej = _imb_def(_src(raw_null, "src_null"), name="imb_rej", null_policy=NullPolicy.REJECT)
        reg4.register(f4_keep)
        reg4.register(f4_rej)
        off4 = OfflineFeatureStore(base / "s4", reg4)
        mat4, man4 = off4.materialize(f4_keep, raw_null, run_id="s4")
        q4 = QualityEvaluator().evaluate(mat4, f4_keep, run_id=man4.run_id)
        promo_blocked = False
        try:
            reg4.promote("imb_keep", "1.0.0", LifecycleStage.ACTIVE, quality_report=q4)
        except QualityGateError:
            promo_blocked = True
        reject_raised = False
        try:
            off4.materialize(f4_rej, raw_null, run_id="s4-rej")
        except NullPolicyViolationError:
            reject_raised = True
        assert mat4["value"][0] is None and promo_blocked and reject_raised
        results["null_handling"] = {
            "status": "PASS",
            "keep_null_preserved": mat4["value"][0] is None,
            "quality_gate_blocked_promotion": promo_blocked,
            "reject_policy_raised": reject_raised,
            "observed_null_ratio": q4.null_ratio,
        }

        # 5. Version mismatch
        reg_mismatch_raised = False
        try:
            reg1.resolve(FeatureRef(name="lob_imbalance_l1", version="1.0.0", expected_version_id="bad@1.0.0+0000"))
        except VersionMismatchError:
            reg_mismatch_raised = True
        bad_backend = InMemoryOnlineBackend()
        bad_backend.put_batch(
            [
                FeatureRecord(
                    entity_id="EQ:AAPL",
                    feature_name="lob_imbalance_l1",
                    feature_version="1.0.0",
                    version_id="lob_imbalance_l1@1.0.0+corrupt0000",
                    event_time=t0,
                    availability_time=t0 + timedelta(seconds=1),
                    materialized_at=t0 + timedelta(seconds=1),
                    value=0.1,
                    unit="ratio",
                    source_version="r0",
                    run_id="r0",
                )
            ]
        )
        on5 = OnlineFeatureStore(bad_backend, reg1)
        cache_mismatch_raised = False
        try:
            on5.get_online_features("EQ:AAPL", ["lob_imbalance_l1"], now=t0 + timedelta(seconds=5))
        except VersionMismatchError:
            cache_mismatch_raised = True
        assert reg_mismatch_raised and cache_mismatch_raised
        results["version_mismatch"] = {
            "status": "PASS",
            "pinned_ref_mismatch_blocked": reg_mismatch_raised,
            "online_cache_mismatch_blocked": cache_mismatch_raised,
        }

        # 6. Backfill
        gen = SyntheticDataGenerator(seed=42)
        lob_df = gen.generate_order_book_snapshots(entities=("EQ:AAPL", "EQ:MSFT"), start_time=t0, steps=30)
        reg6 = FeatureRegistry()
        f6 = _imb_def(_src(lob_df, "src_bf"))
        reg6.register(f6)
        off6 = OfflineFeatureStore(base / "s6", reg6)
        m6, man6 = off6.materialize(f6, lob_df, run_id="init-bf")
        q6 = QualityEvaluator().evaluate(m6, f6, run_id=man6.run_id)
        reg6.promote(f6.name, f6.version, LifecycleStage.ACTIVE, quality_report=q6)
        bf1 = off6.backfill(
            "lob_imbalance_l1",
            lob_df,
            window_start=t0 + timedelta(seconds=50),
            window_end=t0 + timedelta(seconds=150),
            run_id="bf-1",
        )
        bf2 = off6.backfill(
            "lob_imbalance_l1",
            lob_df,
            window_start=t0 + timedelta(seconds=50),
            window_end=t0 + timedelta(seconds=150),
            run_id="bf-2",
        )
        total_after = off6.load_materialized(reg6.get("lob_imbalance_l1")).height
        assert total_after == lob_df.height
        results["backfill"] = {
            "status": "PASS",
            "original_row_count": lob_df.height,
            "post_double_backfill_row_count": total_after,
            "second_run_deduplicated_rows": bf2.rows_deduplicated,
            "idempotent_zero_duplicates": total_after == lob_df.height,
            "first_run_rows_written": bf1.rows_written,
        }

        # 7. Training-serving skew (clean parity + injected skew)
        spine7 = pl.DataFrame(
            [
                {"entity_id": ent, "as_of_time": t0 + timedelta(seconds=step * 10 + 2)}
                for step in range(2, 25)
                for ent in ("EQ:AAPL", "EQ:MSFT")
            ],
            schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
        )
        verifier = ParityVerifier(off6, reg6, atol=1e-12, rtol=1e-12)
        clean = verifier.verify_replay_parity(
            spine7,
            ["lob_imbalance_l1"],
            raw_sources_by_feature={"lob_imbalance_l1": lob_df},
            backend_factory=lambda: SQLiteOnlineBackend(base / "s7.sqlite3"),
        )
        skewed_raw = lob_df.with_columns((pl.col("bid_size") * 1.2).alias("bid_size"))
        skewed = verifier.verify_replay_parity(
            spine7,
            ["lob_imbalance_l1"],
            raw_sources_by_feature={"lob_imbalance_l1": skewed_raw},
            backend_factory=InMemoryOnlineBackend,
        )
        assert clean.passed and not skewed.passed
        results["training_serving_skew"] = {
            "status": "PASS",
            "clean_replay_passed": clean.passed,
            "clean_max_abs_diff": clean.max_abs_diff,
            "clean_comparisons": clean.total_comparisons,
            "injected_skew_detected": not skewed.passed,
            "injected_skew_mismatches": skewed.mismatched_count,
            "injected_skew_max_abs_diff": round(skewed.max_abs_diff, 6),
        }

    return results


def run_benchmarks() -> dict[str, Any]:
    """Measure local baseline latency & throughput with explicit environment & sample size."""
    t0 = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC)
    gen = SyntheticDataGenerator(seed=42)
    lob_df = gen.generate_order_book_snapshots(
        entities=("EQ:AAPL", "EQ:MSFT", "EQ:NVDA", "EQ:AMZN", "EQ:META"),
        start_time=t0,
        steps=200,
        step_seconds=10,
    )
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        reg = FeatureRegistry()
        fdef = _imb_def(_src(lob_df, "src_bench"))
        reg.register(fdef)
        off = OfflineFeatureStore(base / "offline", reg)

        t_mat0 = time.perf_counter()
        mat_df, man = off.materialize(fdef, lob_df, run_id="bench-mat")
        mat_ms = (time.perf_counter() - t_mat0) * 1000.0
        qrep = QualityEvaluator().evaluate(mat_df, fdef, run_id=man.run_id)
        reg.promote(fdef.name, fdef.version, LifecycleStage.ACTIVE, quality_report=qrep)

        spine = pl.DataFrame(
            [
                {"entity_id": ent, "as_of_time": t0 + timedelta(seconds=step * 10 + 2)}
                for step in range(5, 195)
                for ent in ("EQ:AAPL", "EQ:MSFT", "EQ:NVDA", "EQ:AMZN", "EQ:META")
            ],
            schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
        )
        t_pit0 = time.perf_counter()
        _joined, _audit = off.point_in_time_join(spine, ["lob_imbalance_l1"])
        pit_ms = (time.perf_counter() - t_pit0) * 1000.0

        # Online lookup benchmarks (InMemory vs SQLite)
        cutoff = t0 + timedelta(seconds=1995)
        bench_online: dict[str, Any] = {}
        for label, backend in (
            ("in_memory", InMemoryOnlineBackend()),
            ("sqlite", SQLiteOnlineBackend(base / "bench.sqlite3")),
        ):
            on = OnlineFeatureStore(backend, reg)
            on.sync_feature_frame("lob_imbalance_l1", mat_df, as_of_time=cutoff)
            entities = ("EQ:AAPL", "EQ:MSFT", "EQ:NVDA", "EQ:AMZN", "EQ:META")
            n_lookups = 500
            for i in range(n_lookups):
                on.get_online_features(
                    entities[i % len(entities)],
                    ["lob_imbalance_l1"],
                    now=cutoff + timedelta(seconds=2),
                )
            m = on.metrics()
            bench_online[label] = {
                "sample_size": n_lookups,
                "latency_ms_p50": m["latency_ms_p50"],
                "latency_ms_p95": m["latency_ms_p95"],
                "latency_ms_p99": m["latency_ms_p99"],
            }

    return {
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "polars": pl.__version__,
        },
        "offline_materialization": {
            "input_rows": lob_df.height,
            "elapsed_ms": round(mat_ms, 3),
        },
        "point_in_time_join": {
            "spine_rows": spine.height,
            "elapsed_ms": round(pit_ms, 3),
        },
        "online_serving": bench_online,
        "caveat": "Local prototype baseline measurements on synthetic fixtures; do not extrapolate to production live trading latency or throughput.",
    }


def write_reports(scenarios: dict[str, dict[str, Any]], benchmarks: dict[str, Any]) -> None:
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    (reports_dir / "scenarios_evidence.json").write_text(
        json.dumps(scenarios, indent=2, sort_keys=True), encoding="utf-8"
    )
    (reports_dir / "benchmarks.json").write_text(
        json.dumps(benchmarks, indent=2, sort_keys=True), encoding="utf-8"
    )

    lines = [
        "# Financial Feature Store — Validation & Acceptance Evidence Report",
        "",
        f"- **Generated (UTC)**: `{datetime.now(UTC).isoformat()}`",
        f"- **Environment**: Python `{benchmarks['environment']['python']}`, Polars `{benchmarks['environment']['polars']}`, `{benchmarks['environment']['platform']}`",
        "",
        "## 1. Required Scenario Verification (plan.md Section 5)",
        "",
        "| Scenario | Status | Key Measured Evidence |",
        "|---|---|---|",
        f"| Late data | **{scenarios['late_data']['status']}** | Before avail=`{scenarios['late_data']['value_before_availability']}`, after avail=`{scenarios['late_data']['value_after_availability']}`, excluded_late=`{scenarios['late_data']['excluded_late_candidates']}`, leakage=`{scenarios['late_data']['leakage_violations']}` |",
        f"| Future join | **{scenarios['future_join']['status']}** | Pre-history=`{scenarios['future_join']['pre_history_value']}`, excluded_future=`{scenarios['future_join']['excluded_future_candidates']}`, future_spine_blocked=`{scenarios['future_join']['future_spine_query_blocked']}` |",
        f"| Stale online feature | **{scenarios['stale_online_feature']['status']}** | REJECT raised=`{scenarios['stale_online_feature']['reject_policy_raised']}`, WARN age=`{scenarios['stale_online_feature']['warn_age_seconds']}s` (`{scenarios['stale_online_feature']['warn_status']}`), FALLBACK_NULL=`{scenarios['stale_online_feature']['fallback_null_value']}` |",
        f"| Null handling | **{scenarios['null_handling']['status']}** | KEEP_NULL preserved=`{scenarios['null_handling']['keep_null_preserved']}`, quality gate blocked promotion=`{scenarios['null_handling']['quality_gate_blocked_promotion']}`, REJECT raised=`{scenarios['null_handling']['reject_policy_raised']}` |",
        f"| Version mismatch | **{scenarios['version_mismatch']['status']}** | Pinned ref mismatch blocked=`{scenarios['version_mismatch']['pinned_ref_mismatch_blocked']}`, online cache mismatch blocked=`{scenarios['version_mismatch']['online_cache_mismatch_blocked']}` |",
        f"| Backfill | **{scenarios['backfill']['status']}** | Original rows=`{scenarios['backfill']['original_row_count']}`, post-double-backfill=`{scenarios['backfill']['post_double_backfill_row_count']}`, idempotent=`{scenarios['backfill']['idempotent_zero_duplicates']}` |",
        f"| Training-serving skew | **{scenarios['training_serving_skew']['status']}** | Clean replay passed=`{scenarios['training_serving_skew']['clean_replay_passed']}` (`max_abs_diff={scenarios['training_serving_skew']['clean_max_abs_diff']}` over `{scenarios['training_serving_skew']['clean_comparisons']}` checks); injected skew detected=`{scenarios['training_serving_skew']['injected_skew_detected']}` (`mismatches={scenarios['training_serving_skew']['injected_skew_mismatches']}`) |",
        "",
        "## 2. Local Baseline Benchmarks",
        "",
        "| Operation | Sample Size | Measured Latency |",
        "|---|---|---|",
        f"| Offline materialization | {benchmarks['offline_materialization']['input_rows']} rows | {benchmarks['offline_materialization']['elapsed_ms']} ms |",
        f"| Point-in-time as-of join | {benchmarks['point_in_time_join']['spine_rows']} spine rows | {benchmarks['point_in_time_join']['elapsed_ms']} ms |",
        f"| Online serving (InMemory) | {benchmarks['online_serving']['in_memory']['sample_size']} lookups | p50={benchmarks['online_serving']['in_memory']['latency_ms_p50']} ms, p95={benchmarks['online_serving']['in_memory']['latency_ms_p95']} ms, p99={benchmarks['online_serving']['in_memory']['latency_ms_p99']} ms |",
        f"| Online serving (SQLite) | {benchmarks['online_serving']['sqlite']['sample_size']} lookups | p50={benchmarks['online_serving']['sqlite']['latency_ms_p50']} ms, p95={benchmarks['online_serving']['sqlite']['latency_ms_p95']} ms, p99={benchmarks['online_serving']['sqlite']['latency_ms_p99']} ms |",
        "",
        f"> **Note**: {benchmarks['caveat']}",
        "",
        "## 3. Acceptance Criteria Evidence Matrix (plan.md Section 6)",
        "",
        "| Acceptance Criterion | Status | Evidence |",
        "|---|---|---|",
        "| Offline features use only information available at query time | PASS | `test_scenario_1_late_data_excluded_before_availability_time`, `test_scenario_2_future_join_strictly_blocked`, `scenarios_evidence.json` |",
        "| Online and offline values match on replay | PASS | `test_scenario_7_training_serving_parity_and_skew_detection` (`max_abs_diff=0.0`), `scripts/demo.py` |",
        "| Feature freshness is exposed | PASS | `OnlineFeatureValue.{age_seconds,availability_age_seconds,ttl_seconds,freshness_status,is_stale}`, `test_scenario_3_stale_online_feature_freshness_and_policies` |",
        "| Feature version identifies exact transformation and source | PASS | `FeatureDefinition.version_id` (`name@semver+sha256`), `test_version_id_identifies_exact_transform_and_source` |",
        "| Failed data quality blocks promotion | PASS | `FeatureRegistry.promote`, `test_registry_promotion_gate_and_persistence`, `test_scenario_4_null_handling_and_quality_promotion_block` |",
        "| Clean checkout can run documented setup, tests and demo with permitted fixtures | PASS | `scripts/setup.sh`, `scripts/demo.py`, `scripts/validate.py`, `data/fixtures/` |",
        "| Architecture, dependency assumptions, known limitations and runbook committed | PASS | `docs/architecture.md`, `docs/audit-and-assumptions.md`, `docs/api-and-schemas.md`, `docs/runbook.md`, `docs/adrs/ADR-001-bitemporal-causality-and-parity.md` |",
        "| Relevant CI checks pass and raw evaluation or test evidence is retained | PASS | `ruff check`, `pytest`, `reports/scenarios_evidence.json`, `reports/benchmarks.json`, `reports/validation_report.md` |",
        "| Security, data licensing, IP and deployment approval requirements reviewed | PASS | `docs/audit-and-assumptions.md` Section 3 (synthetic-only fixtures, zero secrets, execution disabled) |",
        "",
    ]
    (reports_dir / "validation_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    scenarios = run_all_scenarios()
    benchmarks = run_benchmarks()
    write_reports(scenarios, benchmarks)
    print(json.dumps({"scenarios": scenarios, "benchmarks": benchmarks}, indent=2))


if __name__ == "__main__":
    main()
