# Security Master Platform — Acceptance & Verification Report

## 1. Execution Environment & Provenance

- **Environment:** macOS (`darwin/arm64`), Python `3.9.6`, SQLite (Python stdlib `sqlite3`, WAL mode), zero external runtime dependencies.
- **Configuration:** `config/default.json` (`config_version: 1.0.0`, `schema_version: 1.0.0`, `conflict_resolution.policy: quarantine_all`, `deterministic_seed: 20260101`).
- **Data Provenance:** All test inputs, demo scenarios, and benchmarks use 100% synthetic deterministic fixtures (`data/fixtures/*.json`) with fictional issuers and algorithmically verified ISO 6166 synthetic ISINs. No real transactions, customer records, credentials, or proprietary/SAAT artifacts exist anywhere in source, fixtures, logs, or reports.

## 2. Project-Specific Acceptance Criteria Mapping (`plan.md` Section 5)

| Acceptance Criterion | Status | Automated Test & Artifact Evidence |
|---|---|---|
| **No ambiguous mapping silently resolved** | **PASS** | `tests/test_domain_bitemporal.py::test_dual_listing_requires_mic_and_shares_canonical_instrument` (`AMBIGUOUS_MAPPING` `409` on bare dual-listed `ORION`); `tests/test_domain_bitemporal.py::test_overlapping_ticker_reuse_is_quarantined_never_silently_resolved`; `tests/test_integration_scenarios.py::test_cross_instrument_isin_collision_quarantined`; `tests/test_api_dashboard_review.py::test_full_api_dashboard_and_reviewer_accept_incoming_workflow` (`CONFLICT_BLOCKED` `409`); `reports/demo_baseline.json` |
| **As-of fixtures match expected identities** | **PASS** | `tests/test_reference_minimal.py::test_minimal_reference_matches_independent_oracle` (exact match vs hand-calculated `data/fixtures/reference_minimal_expected.json`); `tests/test_domain_bitemporal.py::test_ticker_reuse_resolves_distinct_instruments_by_valid_time`; `tests/test_integration_scenarios.py::test_late_correction_conflicting_ids_and_corporate_action_linkage` |
| **All changes retain source lineage** | **PASS** | `lineage_coverage_ratio = 1.0` verified across all instrument, listing, identifier, and corporate-action versions in `reports/demo_baseline.json`, `tests/test_integration_scenarios.py` (`supersedes_version_id` & `source_event_id` assertions), and `tests/test_api_dashboard_review.py` (`reviewer_id`, `conflict_id`, `review_rationale` retained on review-resolved versions) |
| **All required test scenarios pass (or unavailable deps marked not run)** | **PASS** | 19/19 tests passing in `reports/test_report.json` (`0` failures, `0` errors); unavailable/untested external capabilities explicitly listed in Section 5 below |
| **README documents setup, demo, validation, assumptions, limitations, and measured vs synthetic distinction** | **PASS** | Verified in `README.md`, `docs/architecture.md`, `docs/contracts.md`, `docs/integration.md`, `docs/runbook.md`, and `docs/adr/ADR-001` & `ADR-002` |
| **No real transactions, customer data, credentials, or proprietary employer/SAAT artifacts** | **PASS** | 100% original code and synthetic fixtures; restricted namespaces (`CUSIP`, `SEDOL`) blocked by default (`tests/test_contracts_and_validation.py::test_unlicensed_sedol_rejected_with_licensing_error`) |

## 3. Test & Validation Matrix Coverage (`plan.md` Section 4)

| Test Area | Required Scenario | Automated Verification | Result |
|---|---|---|---|
| **Domain correctness** | Ticker reuse and dual listing | `tests/test_domain_bitemporal.py` (`test_ticker_reuse_resolves_distinct_instruments_by_valid_time`, `test_overlapping_ticker_reuse_is_quarantined_never_silently_resolved`, `test_dual_listing_requires_mic_and_shares_canonical_instrument`) + `tests/test_reference_minimal.py` | **PASS** |
| **Integration and resilience** | Late reference correction and conflicting IDs | `tests/test_integration_scenarios.py` (`test_late_correction_conflicting_ids_and_corporate_action_linkage`, `test_cross_instrument_isin_collision_quarantined`) + `tests/test_api_dashboard_review.py` | **PASS** |
| **Boundary and failure behavior** | Historical lookup before observation time | `tests/test_boundary_and_failure.py::test_historical_lookup_before_observation_time_raises_explicit_error` + `tests/test_reference_minimal.py` + `tests/test_api_dashboard_review.py` (HTTP `422 PRE_OBSERVATION_LOOKUP`) | **PASS** |
| **Reproducibility** | Same versioned inputs/config and deterministic seed | `tests/test_boundary_and_failure.py::test_reproducibility_identical_inputs_yield_identical_snapshot_hash` + `reports/demo_baseline.json` (`pre_replay == post_replay == restarted == 964f4fca7362504e2ceffd3e3c80da608da12a6a5ea30fc0890de10647674dd1`) | **PASS** |
| **Operational review** | Missing config, unsupported inputs, restart/rerun | `tests/test_contracts_and_validation.py::test_config_errors_on_missing_malformed_or_unsupported`, `test_boundary_and_invalid_fixture_cases`, `tests/test_boundary_and_failure.py::test_cli_returns_nonzero_on_missing_config`, `tests/test_integration_scenarios.py` restart & journal replay | **PASS** |

## 4. Measured Observations (Synthetic Prototype Workload)

Actual measurements captured on single-node macOS `darwin/arm64` (Python `3.9.6`) from `reports/benchmark_results.json` (`100` synthetic instruments / `400` canonical events / `200` as-of lookups). These are **local prototype observations on synthetic fixtures**, not production throughput guarantees:

- **In-memory pure domain ingestion:** ~33,000 events/sec (`0.012s` for 400 events)
- **Durable SQLite per-event ACID ingestion (journal + full materialized table sync):** ~494 events/sec (`0.81s` for 400 events)
- **Durable SQLite full event-journal replay:** ~20,800 events/sec (`0.019s` for 400 events)
- **As-of bitemporal lookup latency:** median `~0.034 ms`, p95 `~0.035 ms`, max `~0.046 ms`

See exact raw numbers and timestamps in [reports/benchmark_results.json](benchmark_results.json), [reports/demo_baseline.json](demo_baseline.json), and [reports/test_report.json](test_report.json).

## 5. Explicitly Untested / Not-Run Capabilities

| Capability | Status | Reason / Mitigation |
|---|---|---|
| **Live PostgreSQL 16+ execution** (`config/postgres_schema.sql`) | **NOT RUN** | No PostgreSQL daemon is provisioned in the offline prototype environment; requesting `backend: "postgres"` fails explicitly with `CONFIG_ERROR`. Runtime executes against SQLite with equivalent logical bitemporal semantics (see `docs/adr/ADR-001-stack-and-storage.md`). |
| **Optional FastAPI ASGI runtime** (`src/sec_master/fastapi_app.py`) | **NOT RUN** | Optional `[fastapi]` extra is not installed in the offline stdlib environment; all REST API and Quality Dashboard endpoints are verified end-to-end against the stdlib WSGI application (`src/sec_master/http_api.py`). |
| **Multi-node distributed concurrency / HA failover** | **NOT RUN** | Single-node research prototype scope only. |
| **Commercial identifier vendor feeds (`CUSIP`, `SEDOL`)** | **OUT OF SCOPE / BLOCKED** | Restricted by default licensing guardrail (`LICENSING_VIOLATION` `403`). |
