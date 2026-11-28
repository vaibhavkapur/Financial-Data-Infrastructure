# Security Master Platform

Bitemporal financial instrument identity, exchange listing, identifier lineage, and conflict-quarantine reference prototype (`security-master-platform/`).

> **Scope & Disclaimer:** Independently developed educational/research prototype using 100% synthetic deterministic fixtures. Contains no live capital, real transactions, real customer records, production credentials, or proprietary/licensed vendor data. Any production deployment requires separate security, operational, legal, identifier-licensing, and risk reviews.

## Core Capabilities

- **Canonical Ticker-Independent Identity:** Stable opaque instrument IDs (`SEC-<12HEX>`) and exchange-listing IDs (`LST-<12HEX>`) across explicit namespaces (`OPENFIGI`, `COMPOSITE_FIGI`, `ISIN`, `LEI`, `MIC_TICKER`, `TICKER`, `VENDOR_INTERNAL`), with strict licensing guards rejecting unlicensed proprietary namespaces (`CUSIP`, `SEDOL`) by default.
- **Bitemporal Versioning (`[valid_from, valid_to)` × `[observed_at, superseded_at)`):** Preserves full history across both business-effective time and platform observation time without mutating or deleting superseded versions.
- **Zero Silent Ambiguity Resolution:** Cross-source identifier/attribute conflicts and overlapping symbol collisions are quarantined into `OPEN` `ConflictTicket`s; bare dual-listed tickers raise `AMBIGUOUS_MAPPING` (`409`); lookups on unresolved conflicts raise `CONFLICT_BLOCKED` (`409`).
- **As-of Lookup & Corporate-Action Lineage:** Point-in-time lookups (`as_of_valid`, `as_of_observed`), explicit `PRE_OBSERVATION_LOOKUP` (`422`) when querying before earliest observation time, and cycle-safe lineage traversal across `TICKER_CHANGE`, `MERGER`, `SPLIT`, `REVERSE_SPLIT`, and `SPINOFF` links.
- **Quality Dashboard & Reviewer Correction Workflow:** Built-in zero-dependency HTTP REST API and HTML dashboard (`GET /dashboard`) reporting completeness, lineage coverage, freshness lag, stale sources, and audited conflict resolution (`ACCEPT_INCOMING`, `KEEP_EXISTING`, `CUSTOM_OVERRIDE`).
- **Durable Journal & Deterministic Replay:** Append-only SQLite event journal + materialized bitemporal tables supporting restart rehydration and bit-for-bit identical snapshot hash replay. Reference PostgreSQL 16+ `tstzrange`/`btree_gist` DDL is included in [config/postgres_schema.sql](config/postgres_schema.sql).

## Quickstart & Setup

Requires **Python 3.9+** (standard library only; zero mandatory third-party packages).

```bash
cd security-master-platform

# 1. Verify runtime and create directories
make setup

# 2. Run the full unit, integration, boundary, and API test suite
make test

# 3. Run the reproducible end-to-end demo (writes reports/demo_baseline.json)
make demo

# 4. Run the deterministic synthetic benchmark (writes reports/benchmark_results.json)
make benchmark

# 5. Launch the local HTTP API & Quality Dashboard UI (with demo fixtures seeded)
PYTHONPATH=src python3 scripts/serve.py --seed-demo --host 127.0.0.1 --port 8080
# Then open http://127.0.0.1:8080/dashboard
```

Full validation (tests + machine-readable test report + demo + benchmark):

```bash
make validate
```

## Example Demo Output

Actual output observed from `make demo` on Python 3.9.6 (`darwin/arm64`) against synthetic deterministic fixtures:

```json
{
  "status": "DEMO_PASSED",
  "snapshot_hash": "964f4fca7362504e2ceffd3e3c80da608da12a6a5ea30fc0890de10647674dd1",
  "active_instruments": 6,
  "active_listings": 7,
  "open_conflicts": 0,
  "resolved_conflicts": 1,
  "report_path": "reports/demo_baseline.json"
}
```

## Project Layout

| Path | Description |
|---|---|
| `src/sec_master/` | Pure domain state machine (`domain.py`), contracts (`models.py`), validation (`validation.py`), source adapters (`adapters.py`), durable SQLite store (`storage.py`), service (`service.py`), HTTP API/UI (`http_api.py`), optional FastAPI adapter (`fastapi_app.py`), CLI (`cli.py`) |
| `config/` | Default versioned config (`default.json`) and reference PostgreSQL DDL (`postgres_schema.sql`) |
| `data/fixtures/` | Synthetic deterministic valid, oracle, domain, integration, corporate-action, and invalid/boundary fixtures |
| `tests/` | 19 deterministic unit, oracle, integration, boundary, and HTTP/reviewer workflow tests |
| `scripts/` | Reproducible demo (`run_demo.py`), benchmark (`run_benchmark.py`), test report runner (`run_validation_report.py`), local server (`serve.py`) |
| `docs/` | [Architecture](docs/architecture.md), [Contracts](docs/contracts.md), [Integration](docs/integration.md), [Runbook](docs/runbook.md), and ADRs (`docs/adr/`) |
| `reports/` | [Acceptance Report](reports/acceptance.md) and raw JSON outputs (`demo_baseline.json`, `benchmark_results.json`, `test_report.json`) |

## Acceptance Evidence & Verification

See [reports/acceptance.md](reports/acceptance.md) for the complete mapping of every project acceptance criterion and test-matrix scenario to automated tests, measured results, and explicitly marked untested capabilities.

## Assumptions, Provenance, Licensing & Known Limitations

- **Fixture Provenance:** All fixtures in `data/fixtures/` are 100% synthetic, algorithmically generated for this prototype using fictional issuers (`Synthetic Acme Legacy Corp`, `Synthetic Orion Dual Corp`, etc.), valid ISO 6166 Luhn synthetic ISINs (`US0000001018`–`US0000009995`), and fictional `BBG000SYN...` OpenFIGI values.
- **Dependency Licenses:** Core runtime uses Python Software Foundation (PSF) License standard library only (`dependencies = []` in `pyproject.toml`). Optional extras (`fastapi` MIT, `uvicorn` BSD-3-Clause, `pydantic` MIT, `psycopg` LGPL-3.0, `pytest` MIT) are not required to run or validate the platform.
- **Measured vs. Synthetic/Target Distinction:** Benchmark metrics in `reports/benchmark_results.json` are actual single-node measurements on synthetic generated events, not production SLA guarantees or distributed throughput evidence.
- **Known Limitations / Untested Capabilities:**
  - Live PostgreSQL 16 execution (`config/postgres_schema.sql`) is **not run** in offline CI (no PostgreSQL server provisioned); runtime executes against SQLite with equivalent logical bitemporal semantics.
  - Optional FastAPI ASGI wrapper (`src/sec_master/fastapi_app.py`) is **not run** in offline CI where `fastapi` is not installed; HTTP endpoints are exercised via the stdlib WSGI server (`src/sec_master/http_api.py`).
  - Complex tax-lot reorganizations, multi-tier option chains, and real vendor license feeds (`CUSIP`/`SEDOL` redistribution) are explicitly out of scope and rejected at validation boundaries.
