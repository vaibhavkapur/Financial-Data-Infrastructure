# ADR-002: Immutable Parquet Partitions & SQLite Catalog Registry

- **Status**: Accepted
- **Date**: 2026-11-27

## Context
The MVP requires a zero-infrastructure, reproducible local prototype using Parquet, Arrow, DuckDB, and a transactional catalog database without locking the codebase to a single cloud vendor.

## Decision
- Persist data in three immutable medallion layers (`raw` NDJSON, `normalized` Parquet, `curated` Parquet) behind an `ObjectStore` protocol (`LocalObjectStore` implementation).
- Enforce write-once immutability (`ImmutabilityViolationError` on overwrite) with atomic temporary-file writes and mandatory SHA-256 checksums stored in the catalog.
- Use SQLite in WAL mode as the local catalog storing partition manifests, checksums, ingestion runs, lineage edges, quality reports, schema versions, snapshots, entitlements, and audit logs.
- Use DuckDB in-memory over verified, schema-aligned PyArrow tables for analytical as-of SQL execution.

## Consequences
- Clean checkout runs deterministically with zero external daemons or containers.
- Clear migration path to S3/GCS Object Lock + PostgreSQL catalog without changing core domain/query logic.
