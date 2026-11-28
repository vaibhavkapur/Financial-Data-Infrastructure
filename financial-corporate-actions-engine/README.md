# Financial Corporate Actions Engine

A point-in-time (PIT), bi-temporal corporate actions engine implemented in Python using exact `decimal.Decimal` arithmetic. It applies splits, reverse splits, regular cash dividends, special dividends, FX-denominated dividends with withholding tax, and symbol changes to historical price series and portfolio positions **without hindsight**, supports late-correction replay diffs, reconciles total-return series, and explicitly quarantines unsupported corporate actions (mergers, spinoffs).

> **Scope:** Educational / research prototype with offline synthetic fixtures only. No live capital, customer data, or production credentials.

## Quickstart & Setup

Requires **Python 3.9+** with **zero mandatory third-party dependencies** (embedded `sqlite3` + `decimal.Decimal` default; optional `[polars]` and `[postgres]` adapters in [`pyproject.toml`](pyproject.toml)).

```bash
cd financial-corporate-actions-engine

# Run full validation (21 automated tests + end-to-end demo + synthetic benchmark)
bash scripts/validate.sh
```

### Run Individual Commands

```bash
# 1. Automated test suite (unittest / pytest compatible)
PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py" -v

# 2. Reproducible end-to-end demo (writes reports/demo_output.json & reports/reconciliation_report.json)
python3 scripts/run_demo.py

# 3. Deterministic synthetic benchmark (writes reports/benchmark_results.json)
python3 scripts/run_benchmark.py

# 4. Ad-hoc CLI PIT query & late-correction replay
PYTHONPATH=src python3 -m corp_actions.cli query \
  --security-id SEC-ACME --as-of 2025-01-15T22:00:00Z --account-id ACCT-SIM-001

PYTHONPATH=src python3 -m corp_actions.cli replay \
  --security-id SEC-ACME \
  --from-as-of 2025-01-13T22:00:00Z --to-as-of 2025-01-15T22:00:00Z \
  --end-date 2025-01-13 --account-id ACCT-SIM-001
```

## Example Output (Observed from `scripts/run_demo.py`)

```text
=== Financial Corporate Actions Engine Demo ===
Python: 3.9.6 | Platform: macOS-26.5.1-arm64-arm-64bit
Reproducible digest match: True
[SEC-ACME early PIT 2025-01-05] applied=0, pending=['CA-ACME-SPLIT-01@v1(ex=2025-01-06)']
[SEC-ACME post-correction 2025-01-15] digest=1461c64c3c944711, end_qty=400.000000, end_cash=3720.00, reconciled=True
[SEC-ACME replay v1->v2] corrected=('CA-ACME-DIV-01@v1->v2',), cash_delta=360.00, changed_dates=7
[SEC-EURO same-day split+FX div] end_qty=100.000000, end_cash=687.00, quarantined=['CA-EURO-MRG-01']
[SEC-REV reverse split+cancel] end_qty=100.000000, cancelled=['CA-REV-DIV-01@v2']
```

## Measured Results (Synthetic Workload, Python 3.9.6 / macOS arm64)

*Actual observations on synthetic fixtures (distinguished from production targets):*

- **Automated tests:** `21/21` passed in `0.027 s`
- **Hand-calculated oracle (`data/fixtures/expected_oracle.json`):** Exact `Decimal` match across all 4 reference scenarios (`0.0000000000` return discrepancy)
- **Synthetic 10-security × 1,000-day benchmark (`reports/benchmark_results.json`, seed `42`):**
  - Ingestion: `0.0448 s` for `10,000` bars + `150` actions (`222,989.7 bars/sec`)
  - PIT query + position ledger + TR reconciliation: `66.47 ms` mean (`66.19 ms` median, `68.84 ms` max) per 1,000-day security
  - Reconciliation: `10/10` reconciled, max return discrepancy `0.0000000492` (tolerance target: `0.000001`)

## Project Layout & Documentation

| Path | Description |
|---|---|
| [`src/corp_actions/`](src/corp_actions/) | Contracts, validation, pure domain engine (`factors`, `positions`, `series`, `reconcile`), storage (`sqlite_store`, `postgres_store`), adapters, `engine`, `cli` |
| [`tests/`](tests/) | Unit, integration, boundary, operational, and adapter tests (21 test cases) |
| [`config/engine_config.json`](config/engine_config.json) | Versioned precision, rounding, tolerance, FX, and same-day policies |
| [`data/fixtures/`](data/fixtures/) | Synthetic valid, invalid, and hand-calculated oracle fixtures |
| [`sql/postgres_schema.sql`](sql/postgres_schema.sql) | Optional bi-temporal PostgreSQL DDL (`NUMERIC` + `TIMESTAMPTZ`) |
| [`docs/architecture.md`](docs/architecture.md) | Mermaid architecture/sequence diagrams, invariants, and trade-offs |
| [`docs/contracts.md`](docs/contracts.md) | Canonical input/output schemas, units, versions, and error codes |
| [`docs/integration.md`](docs/integration.md) | Sibling TDI adapter boundaries, optional dependencies, and license inventory |
| [`docs/runbook.md`](docs/runbook.md) | Setup, validation, diagnostics, replay, and recovery procedures |
| [`docs/adr/0001-architecture-and-stack.md`](docs/adr/0001-architecture-and-stack.md) | ADR-001 documenting stack decisions and backward-adjustment conventions |
| [`reports/acceptance.md`](reports/acceptance.md) | Acceptance criteria mapped to measured test, demo, and benchmark evidence |
| [`reports/demo_output.json`](reports/demo_output.json) | Versioned machine-readable demo snapshots and replay diff |
| [`reports/reconciliation_report.json`](reports/reconciliation_report.json) | Total-return reconciliation and data-quality diagnostics |
| [`reports/benchmark_results.json`](reports/benchmark_results.json) | Synthetic benchmark metrics and runtime environment metadata |
| [`../.github/workflows/financial-corporate-actions-engine.yml`](../.github/workflows/financial-corporate-actions-engine.yml) | GitHub Actions CI validation matrix (Python 3.9, 3.11, 3.12) |

## Key Assumptions & Known Limitations

- **Adjustment convention:** Uses CRSP/S&P backward proportional adjustment with gross dividends for price factors and configurable `tax_withholding_rate` for net investor cash ledgers.
- **Unsupported corporate actions:** `MERGER` and `SPINOFF` require external tax-lot allocation and deal-valuation assumptions; they are explicitly quarantined (`QuarantineRecord`) or rejected (`UnsupportedActionError` in strict mode) rather than silently adjusted.
- **Short positions & intraday actions:** Prototype scopes to long daily positions (`quantity >= 0`) and daily EOD OHLCV bars.
- **Optional integrations not run locally:** Live PostgreSQL (`psycopg`) and Polars DataFrame serialization were not executed in the offline stdlib-only environment; both are explicitly marked **Not Run** in [`reports/acceptance.md`](reports/acceptance.md).
