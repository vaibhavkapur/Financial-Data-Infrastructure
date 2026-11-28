# Integration Boundaries & Dependencies

## 1. Sibling `Trading-Data-Infrastructure` Projects

`financial-corporate-actions-engine/` lives at the top level of `Trading-Data-Infrastructure` alongside independent sibling projects (`security-master-platform/`, `distributed-tick-data-database/`, `options-market-data-platform/`, `distributed-quant-research-platform/`, `financial-data-lake/`, `financial-feature-store/`, and `alternative-data-platform/`).

To keep each project independently runnable without cross-project package coupling, this project defines **standalone runtime-checkable `Protocol` adapter boundaries** in [`src/corp_actions/adapters/tdi_interfaces.py`](../src/corp_actions/adapters/tdi_interfaces.py):

| Sibling Project | Boundary Protocol | Purpose |
|---|---|---|
| `security-master-platform` | `SecurityMasterAdapter` | Maps point-in-time tickers to immutable `security_id`s across symbol changes |
| `distributed-tick-data-database` | `TickDataPriceAdapter` | Supplies unadjusted bi-temporal `RawPriceBar` series |
| `distributed-quant-research-platform` | `QuantResearchSeriesConsumer` | Receives PIT `SnapshotResult` adjusted series and factor schedules for backtesting without look-ahead bias |

## 2. Optional Third-Party Adapters

| Adapter | File | Optional Extra | Behavior When Absent |
|---|---|---|---|
| **Polars DataFrame Export** | `src/corp_actions/adapters/polars_adapter.py` | `pip install -e '.[polars]'` (`polars>=1.0,<2.0`) | Raises `DependencyUnavailableError` (`DEPENDENCY_UNAVAILABLE`); core engine runs without it |
| **PostgreSQL Bi-Temporal Store** | `src/corp_actions/storage/postgres_store.py` + `sql/postgres_schema.sql` | `pip install -e '.[postgres]'` (`psycopg[binary]>=3.1,<4.0`) | Raises `DependencyUnavailableError` (`DEPENDENCY_UNAVAILABLE`); `SQLiteStore` provides full ACID bi-temporal persistence by default |

## 3. Dependency & License Inventory

| Component | Version / Constraint | License | Scope |
|---|---|---|---|
| Python Standard Library (`decimal`, `sqlite3`, `unittest`, `json`, `dataclasses`, `hashlib`) | Python `>=3.9` (verified on `3.9.6`) | PSF License | Core runtime (zero external dependencies) |
| `polars` *(optional)* | `>=1.0.0,<2.0.0` | MIT | Optional DataFrame export adapter |
| `psycopg[binary]` *(optional)* | `>=3.1.0,<4.0.0` | LGPL-3.0 | Optional PostgreSQL storage adapter |
| `pytest`, `ruff`, `mypy` *(optional dev)* | Pinned in `pyproject.toml` | MIT | Optional CI/dev tooling |

## 4. Provenance & Compliance Statement

- All source code, schemas, scripts, and tests are original implementations created for this educational/research prototype.
- All fixture data in `data/fixtures/` and benchmark inputs are 100% synthetic (synthetic IDs `SEC-ACME`, `SEC-EURO`, `SEC-REV`, `ACCT-SIM-001`).
- No live broker/custodian connections, real customer records, production credentials, or proprietary third-party artifacts exist anywhere in this project.
