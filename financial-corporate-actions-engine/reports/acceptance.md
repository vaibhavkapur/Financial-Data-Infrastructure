# Acceptance Evidence Report — Financial Corporate Actions Engine

**Environment Observed:** Python `3.9.6` on `macOS-26.5.1-arm64-arm-64bit` (stdlib-only sandbox; `polars` and `psycopg`/PostgreSQL server unavailable in this environment)  
**Configuration:** [`config/engine_config.json`](../config/engine_config.json) (`config_version=1.0.0`, `schema_version=1.0.0`, `random_seed=42`, `reconciliation_tolerance=0.000001`)  
**Data Provenance:** 100% synthetic deterministic fixtures (`data/fixtures/`) and synthetic seed-42 benchmark series.

---

## 1. Project-Specific Acceptance Criteria (plan.md §5)

| # | Acceptance Criterion | Status | Verified Evidence |
|---|---|---|---|
| 1 | Position and cash changes match worked fixtures | **PASS** | `tests/test_domain_actions.py` (3/3 tests pass against hand-calculated [`data/fixtures/expected_oracle.json`](../data/fixtures/expected_oracle.json)); [`reports/demo_output.json`](demo_output.json) confirms `SEC-ACME` end qty `400.000000`, cost basis `$40.000000`, settled cash `$3720.00`; `SEC-EURO` end qty `100.000000`, settled cash `$687.00` (after EUR/USD 1.10 FX and 15% tax); `SEC-REV` end qty `100.000000`, cash `$0.00`. |
| 2 | No future-known action in PIT snapshot | **PASS** | `tests/test_pit_and_replay.py::test_no_future_known_action_in_pit_snapshot` & `test_late_correction_pit_before_and_replay_diff` confirm: at `2025-01-05T12:00:00Z`, 0 future-ex/future-announced actions apply (`CA-ACME-SPLIT-01` deferred to `pending_future_ex_actions`, later events absent); at `2025-01-13T22:00:00Z`, only v1 (`$0.90`) is visible and v2 (`$1.80`, available `2025-01-14T18:00:00Z`) is excluded. |
| 3 | Adjusted series and raw series clearly separated | **PASS** | `SnapshotResult` exposes separate `raw_series` (`RawPriceBar`) and `adjusted_series` (`AdjustedPriceBar` with explicit `raw_*`, `split_adj_*`, and `tr_adj_*` fields). Verified in `test_acme_split_special_div_symbol_change_and_regular_div_matches_oracle` and [`reports/demo_output.json`](demo_output.json). |
| 4 | All required test scenarios pass in supported environment; unavailable dependencies explicitly marked not run | **PASS** | **21/21 automated tests pass in 0.027s** (`bash scripts/validate.sh`). Optional Polars DataFrame export and live PostgreSQL server tests are explicitly marked **Not Run** in Section 3 below (graceful `DependencyUnavailableError` degradation verified by `tests/test_adapters.py`). |
| 5 | README documents setup, demo, validation, assumptions, known limitations, and distinguishes observations from synthetic fixtures/estimates/targets | **PASS** | Documented in [`README.md`](../README.md), [`docs/architecture.md`](../docs/architecture.md), [`docs/contracts.md`](../docs/contracts.md), [`docs/integration.md`](../docs/integration.md), [`docs/runbook.md`](../docs/runbook.md), and [`docs/adr/0001-architecture-and-stack.md`](../docs/adr/0001-architecture-and-stack.md). |
| 6 | No real transactions, customer data, credentials or proprietary employer/SAAT artifacts | **PASS** | Verified across all files in `src/`, `tests/`, `config/`, `data/fixtures/`, `scripts/`, `docs/`, and `reports/`. Only synthetic IDs (`SEC-ACME`, `SEC-EURO`, `SEC-REV`, `ACCT-SIM-001`) are used. |

---

## 2. Test & Validation Matrix Mapping (plan.md §4)

| Test Area | Required Scenario | Automated Test / Artifact | Measured Result |
|---|---|---|---|
| Domain correctness | Split (4:1 & 1:5 reverse), special dividend (`$5.00`), regular dividend, FX dividend, symbol change (`ACME` -> `ACMX`) | `tests/test_domain_actions.py`, `data/fixtures/expected_oracle.json` | 3/3 passed; exact `Decimal` match to hand-calculated oracle |
| Integration & resilience | Late correction (`CA-ACME-DIV-01` v1 -> v2 replay) & unsupported merger (`CA-EURO-MRG-01`) | `tests/test_pit_and_replay.py`, `reports/demo_output.json` | 4/4 passed; replay detects `CA-ACME-DIV-01@v1->v2`, 7 changed dates, `+$360.00` cash delta; merger quarantined with `UNSUPPORTED_ACTION_TYPE` (and raises `UnsupportedActionError` in strict mode) |
| Boundary & failure behavior | Multiple actions on same day (compatible split+reg+special div, pre-split vs post-split basis, duplicate split conflict, dividend >= prior close) | `tests/test_reconciliation_and_boundaries.py` | 6/6 passed; compatible actions compose deterministically and reconcile; duplicate same-day splits raise `SAME_DAY_CONFLICT` |
| Reproducibility | Same versioned inputs/config & fixed seed (`42`) | `scripts/run_demo.py`, `test_durable_sqlite_persistence_across_restart_rerun` | Identical SHA-256 `output_digest` (`1461c64c3c944711...`) across fresh runs and SQLite restart/rerun |
| Operational review | Missing config, malformed JSON, invalid schema version, missing FX/prior bar, freshness SLA breach, CLI exit codes | `tests/test_contracts_and_validation.py`, `tests/test_reconciliation_and_boundaries.py` | 5/5 contract/config tests + CLI exit code `2` verified; zero silent successes |

---

## 3. Measured Synthetic Benchmark & Untested Capabilities

### Actual Measured Observations (Synthetic Workload, Seed 42)
From [`reports/benchmark_results.json`](benchmark_results.json) (10 synthetic securities × 1,000 daily bars = 10,000 bars + 150 corporate actions on Python 3.9.6 / macOS arm64 / in-memory SQLite):
- **Ingestion time:** `0.0448 s` (`222,989.7 bars/sec`)
- **PIT query + position ledger + TR reconciliation latency (1,000-day series):** mean `66.47 ms`, median `66.19 ms`, max `68.84 ms`
- **Reconciliation accuracy:** `10/10` series reconciled (`max_return_discrepancy = 0.0000000492 <= 0.000001`)

> *Note:* These metrics are measured on synthetic deterministic fixtures in a single-process SQLite configuration and are not production SLA guarantees.

### Explicitly Marked Not Run / Out of Scope
| Capability | Status | Reason |
|---|---|---|
| Live PostgreSQL persistence (`PostgresStore` against Postgres server) | **Not Run** | `psycopg` driver and PostgreSQL daemon are not installed in this offline sandbox; DDL (`sql/postgres_schema.sql`) and graceful `DependencyUnavailableError` are unit-tested. |
| Polars DataFrame serialization (`adjusted_series_to_polars`, `position_ledger_to_polars`) | **Not Run** | `polars` optional package is not installed in this offline sandbox; graceful `DependencyUnavailableError` is unit-tested. |
| Complex mergers, spinoffs, tender offers, stock dividends with fractional cash-in-lieu | **Out of Scope (Quarantined)** | Explicitly excluded by `plan.md §1`; engine emits `QuarantineRecord` / `UnsupportedActionError` rather than silently adjusting. |
| Short positions / intraday tick-level corporate actions | **Out of Scope** | Prototype supports long daily positions (`quantity >= 0`) and daily OHLCV bars. |
