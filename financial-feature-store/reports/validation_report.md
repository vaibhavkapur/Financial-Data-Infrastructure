# Financial Feature Store — Validation & Acceptance Evidence Report

- **Generated (UTC)**: `2026-11-27T09:49:18.043258+00:00`
- **Environment**: Python `3.12.15`, Polars `2.2.0`, `macOS-26.5.1-arm64-arm-64bit`

## 1. Required Scenario Verification (plan.md Section 5)

| Scenario | Status | Key Measured Evidence |
|---|---|---|
| Late data | **PASS** | Before avail=`0.5`, after avail=`-0.5`, excluded_late=`1`, leakage=`0` |
| Future join | **PASS** | Pre-history=`None`, excluded_future=`2`, future_spine_blocked=`True` |
| Stale online feature | **PASS** | REJECT raised=`True`, WARN age=`90.0s` (`stale`), FALLBACK_NULL=`None` |
| Null handling | **PASS** | KEEP_NULL preserved=`True`, quality gate blocked promotion=`True`, REJECT raised=`True` |
| Version mismatch | **PASS** | Pinned ref mismatch blocked=`True`, online cache mismatch blocked=`True` |
| Backfill | **PASS** | Original rows=`60`, post-double-backfill=`60`, idempotent=`True` |
| Training-serving skew | **PASS** | Clean replay passed=`True` (`max_abs_diff=0.0` over `46` checks); injected skew detected=`True` (`mismatches=46`) |

## 2. Local Baseline Benchmarks

| Operation | Sample Size | Measured Latency |
|---|---|---|
| Offline materialization | 1000 rows | 10.071 ms |
| Point-in-time as-of join | 950 spine rows | 130.036 ms |
| Online serving (InMemory) | 500 lookups | p50=0.007 ms, p95=0.008 ms, p99=0.0464 ms |
| Online serving (SQLite) | 500 lookups | p50=0.078 ms, p95=0.1057 ms, p99=0.8019 ms |

> **Note**: Local prototype baseline measurements on synthetic fixtures; do not extrapolate to production live trading latency or throughput.

## 3. Acceptance Criteria Evidence Matrix (plan.md Section 6)

| Acceptance Criterion | Status | Evidence |
|---|---|---|
| Offline features use only information available at query time | PASS | `test_scenario_1_late_data_excluded_before_availability_time`, `test_scenario_2_future_join_strictly_blocked`, `scenarios_evidence.json` |
| Online and offline values match on replay | PASS | `test_scenario_7_training_serving_parity_and_skew_detection` (`max_abs_diff=0.0`), `scripts/demo.py` |
| Feature freshness is exposed | PASS | `OnlineFeatureValue.{age_seconds,availability_age_seconds,ttl_seconds,freshness_status,is_stale}`, `test_scenario_3_stale_online_feature_freshness_and_policies` |
| Feature version identifies exact transformation and source | PASS | `FeatureDefinition.version_id` (`name@semver+sha256`), `test_version_id_identifies_exact_transform_and_source` |
| Failed data quality blocks promotion | PASS | `FeatureRegistry.promote`, `test_registry_promotion_gate_and_persistence`, `test_scenario_4_null_handling_and_quality_promotion_block` |
| Clean checkout can run documented setup, tests and demo with permitted fixtures | PASS | `scripts/setup.sh`, `scripts/demo.py`, `scripts/validate.py`, `data/fixtures/` |
| Architecture, dependency assumptions, known limitations and runbook committed | PASS | `docs/architecture.md`, `docs/audit-and-assumptions.md`, `docs/api-and-schemas.md`, `docs/runbook.md`, `docs/adrs/ADR-001-bitemporal-causality-and-parity.md` |
| Relevant CI checks pass and raw evaluation or test evidence is retained | PASS | `ruff check`, `pytest`, `reports/scenarios_evidence.json`, `reports/benchmarks.json`, `reports/validation_report.md` |
| Security, data licensing, IP and deployment approval requirements reviewed | PASS | `docs/audit-and-assumptions.md` Section 3 (synthetic-only fixtures, zero secrets, execution disabled) |
