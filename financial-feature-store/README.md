# Financial Feature Store

Versioned causal financial feature store providing point-in-time offline retrieval, online serving with freshness/TTL enforcement, data quality promotion gates, lineage tracking, idempotent backfill, and training/serving parity verification.

## Getting started

Use Python 3.11 or later with pip. Run these commands from the project directory.

```bash
./scripts/setup.sh

# Run full test suite and linter
.venv/bin/pytest -v
.venv/bin/ruff check src tests scripts

# Run end-to-end M1-M4 demonstration
.venv/bin/python scripts/demo.py

# Reproduce all 7 validation scenarios + baseline benchmarks and write evidence reports
.venv/bin/python scripts/reproduce.py

# Full validation gate (ruff + pytest + demo + reproduce)
.venv/bin/python scripts/validate.py
```

## Key Capabilities

- **Feature Definition Registry (`registry.py`)**: Immutable SemVer contracts with content-addressed `version_id` (`name@semver+hash`) tying each feature version to its exact causal transformation, source version, schema hash, unit, and null policy. Enforces SemVer schema evolution rules and anti-proliferation budgets.
- **Shared Strictly-Causal Transform Engine (`transforms.py`)**: Single deterministic kernel shared by offline materialization and online serving across order-book microstructure (`lob_mid_price`, `lob_spread_bps`, `lob_order_book_imbalance`, `lob_microprice`, `lob_rolling_imbalance_mean`), market bars (`bar_log_return`, `bar_rolling_volatility`, `bar_vwap_deviation_bps`), and entity/account risk velocity (`tx_rolling_count`, `tx_rolling_amount_sum`, `tx_amount_zscore`).
- **Offline Materialization & Point-in-Time As-Of Joins (`offline.py`)**: Versioned Parquet partitions with SHA-256 manifests, bi-temporal leakage barriers (`event_time <= as_of_time` AND `availability_time <= as_of_time`), idempotent historical backfills with causal warmup windows, and reproducible chronological training dataset exports.
- **Online Serving & Freshness (`online.py`, `adapters/online_backends.py`)**: Pluggable `InMemoryOnlineBackend`, durable `SQLiteOnlineBackend`, and optional `RedisOnlineBackend` (isolated behind `RedisClientProtocol`). Exposes `age_seconds`, `availability_age_seconds`, `freshness_status`, `is_stale`, and configurable `StalePolicy` (`reject`, `warn`, `fallback_null`), plus latency/hit/stale metrics.
- **Quality Promotion Gates & Lineage (`quality.py`)**: Materialized data must pass quality checks (`min_row_count`, `version_id_integrity`, `bi_temporal_causality`, `max_null_ratio`, `max_late_arrival_seconds`, `no_nan_or_inf`, value bounds) before promotion to `ACTIVE` / online serving. Tracks directed lineage graphs from sources to exports.
- **Training/Serving Parity Harness (`parity.py`)**: Chronological replay verifier proving zero skew between offline as-of joins and online serving, and detecting injected skew faults with per-entity diagnostics.

## Repository Layout

```text
financial-feature-store/
  plan.md                   # Original implementation plan & acceptance checklist
  README.md                 # Project overview and quickstart
  pyproject.toml            # Build configuration, dependencies, pytest & ruff config
  requirements.lock         # Pinned dependency lockfile from verified environment
  config/
    store_config.json       # Versioned non-secret store & governance settings
  data/
    fixtures/               # Deterministic seeded synthetic Parquet fixtures + manifests
  docs/
    architecture.md         # Architecture diagram, boundaries, and data flows
    audit-and-assumptions.md # Foundation audit, IP/licensing notes, assumptions, unsupported features
    api-and-schemas.md      # Domain contracts, schemas, and error codes
    runbook.md              # Operational controls, monitoring, and incident response
    adrs/
      ADR-001-bitemporal-causality-and-parity.md
  src/financial_feature_store/
  tests/
    unit/
    integration/
    scenarios/
  scripts/
    setup.sh
    demo.py
    reproduce.py
    validate.py
  reports/                  # Generated test, scenario, parity, benchmark, and acceptance evidence
```

## Documentation

- [Architecture](docs/architecture.md)
- [Foundation Audit, Assumptions & Unsupported Features](docs/audit-and-assumptions.md)
- [API & Schema Reference](docs/api-and-schemas.md)
- [Operational Runbook](docs/runbook.md)
- [ADR-001: Bi-Temporal Causality & Shared Transform Parity](docs/adrs/ADR-001-bitemporal-causality-and-parity.md)
- [Acceptance & Validation Report](reports/validation_report.md)
- [Implementation plan](plan.md)
