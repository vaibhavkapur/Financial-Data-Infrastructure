# Options Market Data Platform (OMDP)

An independently developed educational and research prototype for ingesting and normalizing versioned option chains, validating quotes, deriving timestamped Black-Scholes implied-volatility features with full input-version provenance, and serving/replaying historical chain snapshots.

> **Scope & Limitations:** Synthetic fixtures only; implied volatilities are model outputs and **not** executable market prices. Live capital, real customer records, production credentials, and proprietary feeds are excluded.

## Quickstart

Requires **Python 3.9+** (zero required third-party dependencies for core execution).

```bash
cd options-market-data-platform

# 1. Run full test suite (unit, integration, consistency, HTTP routing, CLI)
python3 -m unittest discover -s tests -v

# 2. Run reproducible end-to-end demo (writes reports/demo_output.json)
python3 scripts/run_demo.py

# 3. Verify derived IVs against analytical Black-Scholes oracle (writes reports/evaluation_report.json)
python3 scripts/run_evaluation.py

# 4. Run deterministic local synthetic benchmark (writes reports/benchmark_results.json)
python3 scripts/run_benchmark.py
```

### CLI Usage

```bash
PYTHONPATH=src python3 -m omdp.cli --db /tmp/omdp.sqlite3 ingest --input data/fixtures/reference_chain.json
PYTHONPATH=src python3 -m omdp.cli --db /tmp/omdp.sqlite3 quality --snapshot-id SYNTH-20260619T200000Z
PYTHONPATH=src python3 -m omdp.cli --db /tmp/omdp.sqlite3 replay --snapshot-id SYNTH-20260619T200000Z
PYTHONPATH=src python3 -m omdp.cli --db /tmp/omdp.sqlite3 serve --host 127.0.0.1 --port 8080
```

## Core Capabilities

- **Canonical Option IDs & Contracts (`src/omdp/domain/contracts.py`):** Deterministic `OPT:<UNDERLYING>:<YYYYMMDD>:<C|P>:<STRIKE_4DP>` identifiers with 21-char OCC round-trip conversion, strict UTC bitemporal timestamps (`event_time`, `ingestion_time`), exact `Decimal` strikes/prices, and SHA-256 content hashes.
- **Pure Domain Validation (`src/omdp/domain/validation.py`):** Detects expired options, strike/decimal precision mismatches, crossed/locked/negative quotes, staleness (`snapshot_time - event_time`), ingestion delay (`ingestion_time - event_time`), unsupported exercise styles, and snapshot consistency violations (`MIXED_UNDERLYING_SNAPSHOT`, `DUPLICATE_QUOTE_VERSION`, `SNAPSHOT_ID_MISMATCH`, `EMPTY_SNAPSHOT`).
- **Implied Volatility & Provenance (`src/omdp/domain/pricing.py`, `features.py`):** Safeguarded Newton-Raphson + Brent European Black-Scholes-Merton IV solver with no-arbitrage bound checks, ACT/365F time-to-expiry, interpolated rate/dividend curves, spot & log-forward moneyness, delta, vega, and full `FeatureProvenance` linking every derived feature to input versions and SHA-256 hashes.
- **Durable Storage, Quality Reports & Replay (`src/omdp/storage/sqlite_store.py`, `service/`):** ACID SQLite store with pluggable `StorageRepository` protocol (plus PostgreSQL/ClickHouse DDL templates in `config/`), completeness/freshness `QualityReport`s, HTTP query/replay API, and deterministic hash-verified replay.

## Example Output (Measured on Synthetic Fixtures)

From `python3 scripts/run_evaluation.py` (`reports/evaluation_report.json`, Python 3.9.6, macOS arm64):

```text
Evaluation complete: 6 contracts, all_passed=True, max_iv_err=1.197e-08, replay_identical=True
```

From `python3 scripts/run_benchmark.py` (`reports/benchmark_results.json`, 20 synthetic snapshots × 100 contracts = 2,000 quotes, single-node SQLite, seed `20260619`):

| Metric (Measured Locally on Synthetic Workload) | Observed Value |
|---|---|
| Ingest + Validate + IV Derive + Persist | `0.2094s` (`9,550.25 quotes/s`) |
| Historical Range Query (20 snapshots) | `0.0412s` |
| Deterministic Replay (20 snapshots) | `0.2910s` (`6,872.37 quotes/s`, 20/20 identical) |
| Unit / Integration / Consistency Tests | 18/18 passed in `0.037s` |

## Assumptions & Known Limitations

- **Pricing model:** European Black-Scholes-Merton with continuous dividend yield and ACT/365F day count. Non-European contracts (e.g., American equity options) are explicitly rejected (`UNSUPPORTED_EXERCISE_STYLE`) rather than silently approximated.
- **Storage & Analytics Backends:** Core runtime uses single-node SQLite (`sqlite3` stdlib). PostgreSQL (`config/schema_postgres.sql`), ClickHouse (`config/schema_clickhouse.sql`), and Polars (`src/omdp/adapters/polars_adapter.py`) are supported via adapter boundaries/templates and explicitly marked **not run** in [reports/acceptance.md](reports/acceptance.md) in this offline environment.
- **Single-Node Scope:** Local SQLite throughput is not distributed scalability evidence.

## Documentation & Evidence

- [plan.md](plan.md) — Original implementation plan & milestones (M1–M4)
- [docs/architecture.md](docs/architecture.md) — Component diagram, data flow, invariants, and trade-offs
- [docs/contracts.md](docs/contracts.md) — Schemas, canonical IDs, units, timestamp semantics, and error codes
- [docs/integration.md](docs/integration.md) — Adapter boundaries, sibling repo interfaces, and license attestation
- [docs/runbook.md](docs/runbook.md) — Setup, demo walkthrough, validation, diagnostics, and recovery
- [docs/adr/0001-stack-storage-and-iv-oracle.md](docs/adr/0001-stack-storage-and-iv-oracle.md) — Stack, storage, and IV oracle rationale
- [reports/acceptance.md](reports/acceptance.md) — Acceptance criteria mapped to automated test & benchmark evidence
- Raw reports: [reports/demo_output.json](reports/demo_output.json), [reports/evaluation_report.json](reports/evaluation_report.json), [reports/benchmark_results.json](reports/benchmark_results.json)
