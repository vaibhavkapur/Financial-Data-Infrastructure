# Security Master Platform — Operator & Developer Runbook

## 1. Environment & Clean Setup

```bash
cd security-master-platform
make setup
```

- Verified runtime: Python 3.9+ standard library (`sqlite3`, `wsgiref`, `unittest`, `decimal`).
- Configuration file: `config/default.json`. Missing or invalid configuration fails fast with exit code `2` and structured `CONFIG_ERROR` JSON on `stderr`.

## 2. Running Validation, Demo & Benchmarks

```bash
# Run all 19 unit, oracle, integration, boundary, and HTTP/reviewer tests
make test

# Run machine-readable test report generator -> reports/test_report.json
PYTHONPATH=src python3 scripts/run_validation_report.py

# Run reproducible end-to-end demo -> reports/demo_baseline.json
make demo

# Run deterministic synthetic benchmark -> reports/benchmark_results.json
make benchmark
```

## 3. Demo Walkthrough (Interactive Quality Dashboard & Reviewer Workflow)

1. Start the local server pre-seeded with the synthetic fixtures (which includes 1 quarantined cross-vendor ISIN conflict on `SEC-4524211962DF`):
   ```bash
   PYTHONPATH=src python3 scripts/serve.py --seed-demo --host 127.0.0.1 --port 8080
   ```
2. Open `http://127.0.0.1:8080/dashboard` in a browser (or inspect JSON at `http://127.0.0.1:8080/api/v1/quality`).
3. Observe `Open Conflicts: 1` and the side-by-side **Existing Claim** (`VENDOR_ALPHA: US0000004046`) vs. **Quarantined Incoming Claim** (`VENDOR_BETA: US0000009995`).
4. Verify lookup protection while the conflict is open:
   ```bash
   curl "http://127.0.0.1:8080/api/v1/lookup?namespace=MIC_TICKER&value=XNAS:NOVA&as_of_valid=2024-01-05T00:00:00Z&as_of_observed=2024-01-15T00:00:00Z"
   # Returns HTTP 409 CONFLICT_BLOCKED
   ```
5. Resolve the ticket via the dashboard form (or `POST /api/v1/conflicts/<conflict_id>/resolve` / CLI `resolve`) supplying `reviewer_id`, `action` (`KEEP_EXISTING`, `ACCEPT_INCOMING`, or `CUSTOM_OVERRIDE`), and `rationale`.
6. Re-run the lookup: it now returns HTTP `200` with full source and reviewer lineage, while `as_of_observed=2024-01-05T00:00:00Z` continues to return the historical pre-correction record.

## 4. CLI Operations

```bash
# Ingest a fixture into the configured SQLite database
PYTHONPATH=src python3 -m sec_master.cli ingest data/fixtures/ticker_reuse_dual_listing.json

# Point-in-time lookup
PYTHONPATH=src python3 -m sec_master.cli lookup \
  --namespace MIC_TICKER --value XNYS:ACME \
  --as-of-valid 2020-06-01T00:00:00Z --as-of-observed 2024-01-01T00:00:00Z

# Inspect open conflicts
PYTHONPATH=src python3 -m sec_master.cli conflicts --status OPEN

# Resolve a conflict via CLI
PYTHONPATH=src python3 -m sec_master.cli resolve \
  --conflict-id CNF-... --reviewer-id ops_reviewer \
  --action KEEP_EXISTING --rationale "Verified against exchange notice"

# Inspect quality & freshness metrics
PYTHONPATH=src python3 -m sec_master.cli quality --as-of-observed 2024-11-01T12:00:00Z
```

## 5. Diagnostics, Restart & Disaster Recovery (Replay)

- **Structured Logs:** `SecurityMasterService` emits JSON log lines (`ingest_outcome`, `ingest_rejected`, `lookup_succeeded`, `lookup_failed`, `journal_replayed`) via the `sec_master` logger.
- **Diagnostic Counters:** Available at `GET /health` and `GET /api/v1/quality` (`events_ingested`, `events_applied`, `events_quarantined`, `events_rejected`, `lookups_succeeded`, `lookups_failed`, `conflicts_resolved`, `replays_executed`, plus `snapshot_hash`).
- **Restart Recovery:** On process restart, `SecurityMasterService` automatically reloads all ordered events from SQLite `event_journal` and rebuilds the exact bitemporal state.
- **Manual Deterministic Replay:** If materialized projection tables are suspected stale or after an upgrade, trigger replay via:
  ```bash
  PYTHONPATH=src python3 -m sec_master.cli replay
  # or POST /api/v1/replay
  ```
  Verify the returned `snapshot_hash` matches the expected baseline hash.

## 6. Troubleshooting Common Errors

| Symptom | Cause | Action |
|---|---|---|
| `CONFIG_ERROR` on startup | Missing config file, bad JSON, or `backend: "postgres"` in offline runtime | Point `--config` to `config/default.json` and use `sqlite` or `memory` backend |
| `LICENSING_VIOLATION` (`403`) | Event attempted to ingest `CUSIP` or `SEDOL` without license allowlist | Confirm commercial license rights before adding namespace to `licensing.licensed_namespaces`, or strip proprietary IDs at the source adapter |
| `AMBIGUOUS_MAPPING` (`409`) | Query used bare `TICKER` on a dual-listed security | Pass `mic` query parameter or use `MIC_TICKER` / `ISIN` / `OPENFIGI` |
| `PRE_OBSERVATION_LOOKUP` (`422`) | `as_of_observed` is earlier than `earliest_observed_at` | Expected point-in-time safeguard; inspect `details.earliest_observed_at` to confirm observation lag |
