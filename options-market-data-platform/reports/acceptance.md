# Acceptance Report — Options Market Data Platform

**Date:** 2026-11-28  
**Environment:** Python 3.9.6, macOS (`Darwin`, `arm64`), single-node local SQLite (`3.51.0`)  
**Config Version / Hash:** `1.0.0` / `7dfe44232711a30721a02586bbf43488f48c5b696cc05d0e8b87b3eda0d0e721`

> All measured numbers below are from **synthetic fixtures** executed locally in this environment—not live market data, production benchmarks, or multi-node clusters.

## 1. Acceptance Criteria Traceability

| Criterion (from `plan.md` §5) | Status | Automated Evidence |
|---|---|---|
| Every derived feature links to input versions | **PASS** | `tests/test_pricing_and_iv.py::test_reference_fixture_oracle_and_full_provenance` verifies `snapshot_hash`, `contract_version`/`contract_hash`, `option_quote_version`/`option_quote_hash`, `underlying_quote_version`/`underlying_quote_hash`, `rate_curve_version`/`rate_curve_hashes`, `calendar_version`/`calendar_hash`, and `config_hash` on all 6 derived features. Raw output in `reports/demo_output.json`. |
| Invalid quotes excluded or flagged | **PASS** | `tests/test_domain_validation.py::test_invalid_and_edge_cases_fixture_end_to_end` and `test_excess_decimal_precision_mismatch` verify expired options (`EXPIRED_OPTION`), strike/decimal mismatches (`STRIKE_DECIMAL_MISMATCH`), crossed quotes (`CROSSED_QUOTE`), locked quotes (`LOCKED_QUOTE`), unsupported exercise style (`UNSUPPORTED_EXERCISE_STYLE`), and arbitrage violations (`ARBITRAGE_BOUND_VIOLATION`). |
| Replay returns identical chain snapshots | **PASS** | `tests/test_integration_scenarios.py`, `tests/test_snapshot_consistency.py::test_restart_and_rerun_idempotency_with_persistent_sqlite`, `scripts/run_demo.py` (4/4 snapshots), `scripts/run_evaluation.py`, and `scripts/run_benchmark.py` (20/20 snapshots) all confirm `snapshot_hash_match=True` and `feature_ids_match=True` (`replay_mismatches=0`). |
| All required test scenarios pass in supported environment | **PASS** | 18/18 tests pass in `0.037s` via `python3 -m unittest discover -s tests -v`. Unavailable optional backends explicitly marked not run below. |
| README documents setup, demo, validation, assumptions, and limitations | **PASS** | See `README.md`, `docs/architecture.md`, `docs/contracts.md`, `docs/integration.md`, `docs/runbook.md`, and `docs/adr/0001-stack-storage-and-iv-oracle.md`. |
| No real transactions, customer data, credentials, or proprietary/SAAT artifacts | **PASS** | 100% synthetic fixtures (`SYNTH`, `EDGE`, `NOUND`, `DELAY`, `BENCH`); zero secrets, credentials, or external proprietary code. |

## 2. Test and Validation Matrix Mapping (`plan.md` §4)

| Test Area | Required Scenario | Test / Report Evidence | Measured Result |
|---|---|---|---|
| **Domain correctness** | Expired options, strike/decimal mismatch, crossed quotes | `tests/test_domain_validation.py`, `tests/test_pricing_and_iv.py`, `reports/evaluation_report.json` | 6/6 reference contracts match analytical Black-Scholes oracle (`max_abs_iv_error = 1.197e-08 <= 1e-06`, `max_price_residual = 1.747e-11`); all invalid quotes flagged/rejected. |
| **Integration & resilience** | Missing underlying and delayed/stale quotes | `tests/test_integration_scenarios.py`, `reports/demo_output.json` | `NOUND` produces `MISSING_UNDERLYING` + `EXCLUDED_MISSING_UNDERLYING`; `DELAY` flags `DELAYED_QUOTE`, selects `quote_version=2` over crossed `v1`, and excludes `STALE_QUOTE` from IV. |
| **Boundary & failure behavior** | Chain snapshot consistency | `tests/test_snapshot_consistency.py`, `data/fixtures/consistency_scenarios.json` | `MIXED_UNDERLYING_SNAPSHOT`, `DUPLICATE_QUOTE_VERSION`, `SNAPSHOT_ID_MISMATCH`, and `EMPTY_SNAPSHOT` raise `SnapshotConsistencyError` atomically with zero partial persistence. |
| **Reproducibility** | Versioned inputs/config + fixed seed (`20260619`) | `tests/test_snapshot_consistency.py::test_restart_and_rerun_idempotency_with_persistent_sqlite`, `reports/benchmark_results.json` | Exact SHA-256 `snapshot_hash` and `feature_id` equality across restarts, reruns, and 20-snapshot replay. |
| **Operational review** | Missing config, unsupported inputs, restart/rerun | `tests/test_snapshot_consistency.py`, `tests/test_domain_validation.py`, `tests/test_http_and_cli.py`, `docs/runbook.md` | Explicit `ConfigurationError` (CLI exit `2`), `UnsupportedInputError`, `OptionalDependencyError`, and verified persistent SQLite restart recovery. |

## 3. Measured Local Benchmark Summary (`reports/benchmark_results.json`)

- **Workload:** Synthetic chain (`seed=20260619`), 20 snapshots × 100 contracts = **2,000 option quotes**.
- **Topology:** Single-node local SQLite (`WAL` mode).
- **Ingest + Validate + IV Derive + Persist:** `0.2094s` (**9,550.25 quotes/s**).
- **Historical Range Query (20 snapshots):** `0.0412s`.
- **Full Deterministic Replay (20 snapshots):** `0.2910s` (**6,872.37 quotes/s**), `all_replays_identical=True`.
- **SQLite DB Size:** `5,185,536` bytes (~4.95 MiB).

## 4. Explicitly Untested / Not Run Capabilities

| Capability | Reason Not Run | Mitigation / Path Forward |
|---|---|---|
| PostgreSQL live storage backend | No PostgreSQL server or `psycopg` driver installed in offline sandbox | Schema maintained in `config/schema_postgres.sql`; `StorageRepository` protocol isolates backend implementation. |
| ClickHouse live analytical backend | No ClickHouse server installed in offline sandbox | Schema maintained in `config/schema_clickhouse.sql`; DDL aligns with canonical contracts. |
| Polars DataFrame export (`polars==1.9.0`) | Optional `polars` package not installed in offline sandbox | `features_to_records()` tested and verified; missing-package path tested via `OptionalDependencyError`. |
| Live TCP loopback socket binding test | Offline sandbox denies `socket.connect` (`EPERM`) on loopback | HTTP routing, query params, status codes (`200`/`400`/`404`), and JSON serialization are fully tested in-process via `dispatch_request()`; live `ThreadingHTTPServer` works outside restricted sandboxes via `omdp serve`. |
| American / exotic option IV solvers | Out of scope for European BSM baseline (ADR-0001) | Non-European contracts are explicitly rejected with `UNSUPPORTED_EXERCISE_STYLE` / `EXCLUDED_UNSUPPORTED_STYLE`. |
