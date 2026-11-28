# Integration Guide — Options Market Data Platform

## 1. Repository Context (`Trading-Data-Infrastructure`)

Sibling project paths at the root of `Trading-Data-Infrastructure`:

| Sibling Project Path | Potential Integration Boundary (Adapter Only) |
|---|---|
| `security-master-platform/` | Upstream bitemporal `ContractMetadata` & canonical identifier feed (map security-master instrument IDs to `CanonicalOptionID` via an independent adapter). |
| `distributed-tick-data-database/` | Upstream high-volume `UnderlyingQuote` and `OptionQuote` replay stream fed into `omdp.adapters`. |
| `financial-corporate-actions-engine/` | Upstream split/special-dividend adjustment events producing new `contract_version` entries (adjusted strikes/multipliers) without mutating historical versions. |
| `distributed-quant-research-platform/` | Downstream consumer of versioned `DerivedOptionFeature` exports (JSON or Polars DataFrame) keyed by immutable `snapshot_hash` and `feature_id`. |
| `financial-data-lake/` & `financial-feature-store/` | Downstream storage and causal feature-serving consumers of immutable option chain snapshots and derived IV features. |

Because sibling projects are independently runnable prototypes, OMDP maintains **zero direct code imports** from sibling projects. All integration occurs across explicit file/HTTP/JSON data contracts defined in [contracts.md](contracts.md).

## 2. Adapter Boundaries

- **Source Ingestion Adapters (`src/omdp/adapters/`):**
  - `json_adapter.py`: Loads canonical JSON fixture/feed bundles (`FixtureBundle`).
  - `csv_adapter.py`: Loads tabular CSV option and underlying quote feeds.
  - New vendor feeds (e.g., OPRA, exchange multicast, or security-master exports) should implement a thin parser returning `ContractMetadata`, `UnderlyingQuote`, and `OptionQuote` without importing domain pricing or storage internals.
- **Storage Boundary (`src/omdp/storage/repository.py`):**
  - `StorageRepository` defines the exact protocol consumed by `OptionsMarketDataPlatform`.
  - Default runtime implementation: `SQLiteOptionStore` (`src/omdp/storage/sqlite_store.py`).
  - Target analytical/production schemas: `config/schema_postgres.sql` and `config/schema_clickhouse.sql`. These DDL templates are verified for schema alignment with `contracts.md`, while runtime drivers are excluded from offline default execution.
- **Analytics Export (`src/omdp/adapters/polars_adapter.py`):**
  - `features_to_records()` provides zero-dependency columnar record export.
  - `features_to_polars_dataframe()` converts derived features into a `polars.DataFrame` when the optional `analytics` extra (`polars==1.9.0`) is installed, raising `OptionalDependencyError` otherwise.

## 3. Clean-Room & Compliance Attestation

- All source code, schemas, scripts, and fixtures in `options-market-data-platform/` are original educational/research implementations.
- Zero dependency on proprietary feeds, live trading accounts, customer data, or production credentials.
- Runtime dependencies: Python 3.9+ standard library only (PSF License). Optional extras (`polars` MIT, `psycopg` LGPL-3.0, `pytest` MIT) are pinned in `pyproject.toml`.
