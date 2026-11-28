# Operator & Developer Runbook — Options Market Data Platform

## 1. Prerequisites & Clean-Checkout Setup

- **Runtime:** Python 3.9+ (tested on Python 3.9.6, macOS arm64; zero required third-party packages).
- **Working directory:** `options-market-data-platform/`

Verify environment:
```bash
cd options-market-data-platform
python3 --version
```

## 2. Validation & Test Commands

Run the full deterministic unit, integration, consistency, HTTP routing, and CLI test suite:
```bash
python3 -m unittest discover -s tests -v
```

Run the analytical Black-Scholes oracle evaluation (writes `reports/evaluation_report.json`):
```bash
python3 scripts/run_evaluation.py
```

Run the single-node synthetic throughput & replay benchmark (writes `reports/benchmark_results.json`):
```bash
python3 scripts/run_benchmark.py
```

## 3. Demo Walkthrough

1. **End-to-end scripted demo** (ingests reference, edge-case, and integration fixtures, verifies replay, writes `reports/demo_output.json`):
   ```bash
   python3 scripts/run_demo.py
   ```
2. **CLI workflow** against a local SQLite database:
   ```bash
   PYTHONPATH=src python3 -m omdp.cli --db /tmp/omdp_demo.sqlite3 ingest --input data/fixtures/reference_chain.json
   PYTHONPATH=src python3 -m omdp.cli --db /tmp/omdp_demo.sqlite3 quality --snapshot-id SYNTH-20260619T200000Z
   PYTHONPATH=src python3 -m omdp.cli --db /tmp/omdp_demo.sqlite3 replay --snapshot-id SYNTH-20260619T200000Z
   PYTHONPATH=src python3 -m omdp.cli --db /tmp/omdp_demo.sqlite3 query --underlying SYNTH
   ```
3. **HTTP query & replay server** (local loopback):
   ```bash
   PYTHONPATH=src python3 -m omdp.cli --db /tmp/omdp_demo.sqlite3 serve --host 127.0.0.1 --port 8080
   ```
   Available endpoints:
   - `GET /health`
   - `GET /v1/chains?underlying=SYNTH&start=...&end=...`
   - `GET /v1/chains/{snapshot_id}`
   - `GET /v1/chains/{snapshot_id}/features`
   - `GET /v1/chains/{snapshot_id}/quality`
   - `POST /v1/chains/{snapshot_id}/replay`
   - `GET /v1/metrics`

## 4. Diagnostics & Failure Recovery

| Failure Symptom | Cause / Error Code | Recovery Action |
|---|---|---|
| Exit code `2` with `CONFIGURATION_ERROR` | Missing config file, missing required key, negative thresholds, or unsupported storage backend (`clickhouse`/`postgres` without runtime driver) | Restore `config/default_config.json` or fix the named key reported in JSON stderr. |
| `SnapshotConsistencyError` (`MIXED_UNDERLYING_SNAPSHOT`, `DUPLICATE_QUOTE_VERSION`, `SNAPSHOT_ID_MISMATCH`, `EMPTY_SNAPSHOT`) | Corrupted or mis-partitioned source batch | Snapshot is rejected atomically (nothing persisted). Fix upstream partitioning/version numbering and re-ingest the corrected batch. |
| High `EXCLUDED_MISSING_UNDERLYING` in `QualityReport` | Underlying quote feed lagging or missing for `underlying_symbol` | Ingest the missing `UnderlyingQuote` version(s) and rerun `ingest` for the affected `snapshot_id` (idempotent overwrite) or replay to verify updated derived features. |
| High `EXCLUDED_STALE_QUOTE` or `DELAYED_QUOTE` | Feed delivery lag exceeds `max_quote_staleness_seconds` (300s) or `max_ingestion_delay_seconds` (60s) | Inspect `max_quote_staleness_seconds` and `max_ingestion_delay_seconds` in `QualityReport`; verify upstream clock sync before adjusting thresholds in versioned config. |
| `replay` exits with code `3` (`is_identical: false`) | Active `config_hash` or reference data changed since original ingestion | Compare `stored_snapshot_hash` vs `replayed_snapshot_hash` and `provenance.config_hash`; run replay with the original pinned config version. |
| Process crash mid-ingest | Unexpected termination | SQLite transactions (`with self._conn:`) ensure atomic snapshot persistence; restart process and re-run `ingest` idempotently. |
