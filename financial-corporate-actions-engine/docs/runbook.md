# Operator & Developer Runbook

## 1. Environment Setup

Requires **Python 3.9+** (tested on Python 3.9.6, zero mandatory third-party packages).

```bash
cd financial-corporate-actions-engine
export PYTHONPATH="$PWD/src"
```

Optional extras (when network/pip environment permits):
```bash
pip install -e ".[dev,polars,postgres]"
```

## 2. Running Validation (Tests + Demo + Benchmark)

Run the full automated verification pipeline in one command:

```bash
bash scripts/validate.sh
```

Or run each stage independently:

```bash
# Unit, integration, boundary, and adapter tests
PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py" -v

# Reproducible end-to-end demo (writes reports/demo_output.json & reports/reconciliation_report.json)
python3 scripts/run_demo.py

# Deterministic synthetic benchmark (writes reports/benchmark_results.json)
python3 scripts/run_benchmark.py
```

## 3. CLI Usage (Ad-Hoc PIT Queries & Correction Replays)

```bash
# Point-in-time query for SEC-ACME after late correction
PYTHONPATH=src python3 -m corp_actions.cli query \
  --security-id SEC-ACME \
  --as-of 2025-01-15T22:00:00Z \
  --account-id ACCT-SIM-001

# Replay diff across late dividend revision v1 ($0.90) -> v2 ($1.80)
PYTHONPATH=src python3 -m corp_actions.cli replay \
  --security-id SEC-ACME \
  --from-as-of 2025-01-13T22:00:00Z \
  --to-as-of 2025-01-15T22:00:00Z \
  --end-date 2025-01-13 \
  --account-id ACCT-SIM-001
```

Exit codes: `0` on success, `2` on structured domain/configuration/validation error (JSON error printed to `stderr`).

## 4. Diagnostics & Monitoring Signals

Every `SnapshotResult` includes:
- `output_digest` (SHA-256 over logical series, position ledger, effective actions, and quarantine records) and `run_id` for deterministic auditability.
- `reconciliation`: `reconciled`, `return_discrepancy`, `split_jump_checks_passed`, `completeness_ratio`, `max_freshness_lag_seconds`, `freshness_sla_breaches`, `quarantined_action_count`.
- `diagnostics`: `pending_future_ex_actions`, `cancelled_actions`, and `counters`.
- Structured JSON warnings are emitted to `stderr` whenever an unsupported action (`MERGER`, `SPINOFF`) is quarantined.

## 5. Failure Modes & Recovery Procedures

| Symptom / Error Code | Cause | Recovery Action |
|---|---|---|
| `CONFIGURATION_ERROR` | Missing/corrupt `engine_config.json` or invalid policy value | Restore from versioned `config/engine_config.json`; verify `decimal_precision` in `[12, 64]` and valid policy strings |
| `SAME_DAY_CONFLICT` | Two unlinked splits or symbol changes on the same `(security_id, ex_date)` | Inspect vendor feeds; cancel duplicate `action_id` with a `version=2, revision_type=CANCELLATION` event and re-run PIT query |
| `MISSING_FX_RATE` | Foreign-currency dividend lacks spot/inverse FX rate on or before conversion date | Append missing `FxRate` via `engine.ingest_fx_rates()` and re-run query (store is append-only and idempotent) |
| `MISSING_MARKET_DATA` | Dividend ex-date has no prior trading-day close bar | Backfill missing prior `RawPriceBar` via `engine.ingest_prices()` and re-run |
| `UNSUPPORTED_ACTION` / Quarantined `MERGER` or `SPINOFF` | Complex corporate restructuring requiring tax-lot/deal valuation assumptions | Review `QuarantineRecord.remediation`; handle outside automated split/dividend pipeline |
| Late vendor correction | Vendor revises dividend/split after initial EOD snapshot | Ingest new version (`version = prev + 1, revision_type = CORRECTION`) with its true `availability_time`; run `replay_correction(from_as_of, to_as_of)` to audit exact downstream price and cash deltas without mutating historical PIT snapshots |
| Process crash / restart | Engine stops mid-session | Re-instantiate `CorporateActionsEngine` pointing to the same durable SQLite file (`sqlite_path`) or PostgreSQL DSN; all committed writes are ACID-durable and re-queries produce identical `output_digest`s |
