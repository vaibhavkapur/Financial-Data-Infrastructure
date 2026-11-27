# Assumptions, Known Limitations & Unsupported Features

## 1. Dependency & Environment Assumptions

- **Local prototype storage**: Uses `LocalObjectStore` on the local filesystem behind an `ObjectStore` protocol. Production deployment will swap in an S3/GCS-backed implementation with bucket Object Lock (WORM) enabled.
- **Catalog database**: Uses SQLite in WAL mode (`catalog.sqlite3`) for single-node ACID metadata, entitlements, lineage, and audit logging. Multi-writer concurrent production deployments should migrate the same DDL to PostgreSQL.
- **Query engine**: Uses embedded DuckDB over PyArrow tables read from checksum-verified Parquet partitions.
- **Synthetic fixtures**: All committed data under `data/fixtures/` is 100% synthetic (deterministic seed, non-redistributable internal test license). No licensed vendor market data is stored in this repository.
- **Upstream repository IP**: External personal repositories referenced in `plan.md` were not copied; integration is defined through clean adapter contracts (`ClickHouseTickAdapter`).

## 2. Operational & Security Controls

- **No secrets in repo**: `config/lake_config.json` and `config/entitlements.json` contain zero credentials or tokens.
- **Order execution disabled**: `LakeConfig.execution_controls.order_execution_enabled` is validated to remain `False`.
- **Redistribution disabled**: `LakeConfig.execution_controls.allow_redistribution` is validated to remain `False`.
- **No destructive history rewrite**: `LakeConfig.retention.allow_destructive_history_rewrite` is validated to remain `False`. Retention evaluation emits cold-tier immutable archival plans rather than deleting historical records.

## 3. Explicit List of Unsupported Features (Out of MVP Scope)

The following capabilities are **intentionally unsupported** in this MVP prototype and require separate design/approval before production rollout:

1. **Live vendor connectors & streaming ingest** (e.g., live ClickHouse/Kafka consumers, Bloomberg/Refinitiv/Polygon API clients, FIX feeds).
2. **Distributed cloud object storage & multi-writer catalog locking** (S3/GCS Object Lock adapter, PostgreSQL catalog migration, distributed orchestrator such as Dagster/Airflow).
3. **External IAM / SSO / KMS integration** (RBAC is currently enforced against caller-supplied `AccessContext` checked against SQLite entitlements; production requires cryptographically signed service/user identities and KMS-managed encryption at rest).
4. **Complex multi-leg corporate actions** (spin-offs, mergers/acquisitions, stock dividends, rights issues, special return-of-capital distributions, FX-converted cross-currency dividends). MVP supports stock splits (`SPLIT`) and same-currency cash dividends (`CASH_DIVIDEND`).
5. **Sub-millisecond tick query serving / real-time low-latency market data cache** (analytical batch/research point-in-time queries are supported; ultra-low-latency live trading feeds belong in a separate tick plant).
6. **Automatic cross-partition compaction** (MVP retains immutable per-run versioned Parquet partitions and relies on DuckDB + manifest pruning; background compaction creating superseded manifest generations is deferred).
7. **Live order execution or capital allocation** (explicitly disabled).
