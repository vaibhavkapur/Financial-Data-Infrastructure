# Financial Data Lake

Versioned, point-in-time (bitemporal) financial data lake prototype for market ticks, EOD prices, reference instruments, corporate actions, fundamentals, and entitled alternative datasets.

## Getting started

Use uv with Python 3.12, or Python 3.11 or later with pip. Run these commands from the project directory.

```bash
# 1. Set up the local environment
./scripts/setup.sh

# 2. Run end-to-end demo (M1–M4)
.venv/bin/python scripts/demo.py

# 3. Run full test suite, linter, micro-benchmarks, and generate reports/
.venv/bin/python scripts/validate.py
```

### CLI Examples

```bash
.venv/bin/fdl --lake-root .lake_data ingest-fixtures
.venv/bin/fdl --lake-root .lake_data query-as-of --dataset fundamentals --as-of 2024-06-07T23:59:59Z
.venv/bin/fdl --lake-root .lake_data verify
.venv/bin/fdl --lake-root .lake_data export-catalog
```

## Capabilities (MVP Scope)

- **Data Catalog & Entitlement Registry** (`SQLite WAL`): fail-closed role/purpose/license-window/zero-redistribution authorization with durable audit logging, schema version history, partition manifests, checksums, and end-to-end lineage.
- **Medallion Layers (`raw -> normalized -> curated`)**: write-once immutable local object store (`LocalObjectStore`, behind an `ObjectStore` protocol) producing partitioned Parquet with SHA-256 checksums, within-batch and cross-batch deduplication, and deterministic quality gates.
- **ClickHouse Tick Adapter & Verified Fixture Loader**: clean contract adapter (`ClickHouseTickAdapter`) mapping ClickHouse `JSONEachRow` tick streams to canonical UTC `TickRecord` contracts, plus SHA-256-verified fixture loading (`FixtureLoader`).
- **Schema Evolution Guard**: versioned Arrow schemas permitting safe additive/widening changes and rejecting breaking drops, narrowings, or non-nullable additions without defaults.
- **DuckDB As-Of Query Engine & Reproducible Snapshots**: point-in-time queries excluding unavailable/future records (`available_at <= as_of_ts`), lookahead-free split/dividend price adjustments, and checksum-pinned immutable snapshots.
- **Backfill, Replay, Corruption Recovery & Retention**: automated partition integrity verification, forensic quarantine + deterministic recovery from immutable raw landing, full raw replay reproducing selected snapshots, and non-destructive cold-tier archival planning.

## Repository Structure

```text
financial-data-lake/
  plan.md
  README.md
  pyproject.toml / uv.lock
  config/                  # Versioned lake settings, entitlements, and schema registry (no secrets)
  data/fixtures/           # Synthetic fixtures + SHA-256 manifest.json
  docs/                    # Architecture, ClickHouse audit, ADRs, limitations, operational runbook
  notebooks/               # Sample point-in-time research notebook
  src/financial_data_lake/ # Core lake implementation, adapters, CLI
  tests/                   # Unit, integration, and all 7 required failure/edge-case scenario tests
  scripts/                 # Setup, demo, and validation/report generator
  reports/                 # Milestone reports, validation JSON evidence, and acceptance checklist
```

## Documentation

- [Architecture](docs/architecture.md)
- [ClickHouse Schema & Entitlement Audit (M1)](docs/clickhouse_audit.md)
- [Assumptions, Limitations & Unsupported Features](docs/assumptions_and_limitations.md)
- [Operational Runbook](docs/runbook.md)
- [ADRs](docs/adrs/ADR-001-bitemporal-as-of-model.md)
- [Sample Research Notebook](notebooks/research_as_of_demo.ipynb)
- [Acceptance Checklist & Evidence](reports/acceptance_checklist.md)
- [Implementation plan](plan.md)
