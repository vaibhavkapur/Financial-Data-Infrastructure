# Acceptance Checklist & Evidence — Financial Data Lake (MVP)

| # | Acceptance Criterion (`plan.md` Section 6) | Status | Verified Evidence |
|---|---|---|---|
| 1 | Each dataset has provenance, license and checksum manifest. | Passed | `data/fixtures/manifest.json`, `config/entitlements.json`, `PartitionManifest` in `catalog.sqlite3`; verified by `test_medallion_lineage_and_manifests` & `test_fixture_loader_detects_tampering`. |
| 2 | As-of queries exclude unavailable records. | Passed | `AsOfQueryEngine` (`available_at <= as_of_ts`); verified by `test_scenario_6_future_revision`, `test_scenario_2_late_correction`, and `test_scenario_3_split_and_dividend`. |
| 3 | Re-ingest does not duplicate rows. | Passed | Batch checksum idempotency + cross-batch `dedupe_key` filtering; verified by `test_scenario_1_duplicate_ticks` and `demo.py` (`second_pass_all_idempotent: true`). |
| 4 | Corrections are versioned. | Passed | Immutable versioned rows keyed by `revision_number` + `correction_of`; verified by `test_scenario_2_late_correction` (both rev 1 and rev 2 retained and queryable). |
| 5 | Recovery reproduces a selected snapshot. | Passed | `RecoveryManager.quarantine_and_recover_partition` & `replay_dataset_from_raw`; verified by `test_scenario_5_corrupt_partition`, `test_snapshot_pinning_and_replay_reproduction`, and `demo.py` (`snapshot_reproduction_exact_match: true`). |
| 6 | A clean checkout can run the documented setup, tests and demo with permitted fixtures. | Passed | `scripts/setup.sh`, `uv.lock`, `scripts/demo.py`, `scripts/validate.py`, `notebooks/research_as_of_demo.ipynb` all execute cleanly using synthetic fixtures. |
| 7 | Architecture, dependency assumptions, known limitations and runbook are committed. | Passed | `docs/architecture.md`, `docs/clickhouse_audit.md`, `docs/assumptions_and_limitations.md`, `docs/runbook.md`, and `docs/adrs/ADR-001..003`. |
| 8 | Relevant CI checks pass and raw evaluation or test evidence is retained. | Passed | `ruff check` (0 errors), `pytest -v` (30/30 passed, 0 warnings), notebook execution passed, and baseline micro-benchmark saved to `reports/validation_report.json`. |
| 9 | Security, data licensing, IP and any deployment approval requirements are reviewed. | Documented (Prototype Scope) | Synthetic-only non-redistributable fixtures, zero secrets in repo, order execution & redistribution disabled in config validation, external repo IP respected (clean contract adapters, zero copied code). Production deployment still requires SAAT asset class, vendor licensing, and legal/security sign-off per `plan.md`. |
