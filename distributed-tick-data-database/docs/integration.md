# Integration & Dependencies — Distributed Tick Data Database

## 1. Repository Context (`Trading-Data-Infrastructure`)

`distributed-tick-data-database/` lives at the top level of `Trading-Data-Infrastructure` alongside independent sibling projects (`security-master-platform/`, `options-market-data-platform/`, `financial-corporate-actions-engine/`, `distributed-quant-research-platform/`, `financial-data-lake/`, `financial-feature-store/`, and `alternative-data-platform/`).

To preserve strict build isolation and zero cross-language runtime coupling, `distributed-tick-data-database` defines clean, adapter-based boundaries for interoperability across the repository:

| Neighboring Project | Integration Boundary | Contract Compatibility |
|---|---|---|
| `security-master-platform` | `instrument_id` & `venue` | Opaque string keys; compatible with canonical security master IDs or MIC+symbol identifiers without schema changes |
| `options-market-data-platform` | `RawTick` JSONL / HTTP `/v1/ingest` | Can stream options trades, quotes, and book deltas using `schema_version: "1.0.0"` and `price_nanos` / `size_nanos` |
| `financial-corporate-actions-engine` | Read-only historical replay boundary | Tick store preserves **unadjusted** raw exchange prices/sizes; corporate-action price/split adjustments belong downstream of replay export |
| `distributed-quant-research-platform` | Replay export (`parquet`, `jsonl`, `csv`) & `/v1/query` | Research jobs consume deterministic, SHA-256-fingerprinted Parquet/JSONL replay files with explicit completeness & gap metadata |

## 2. Adapter Boundaries in This Codebase

- **Source Adapters (`src/ingest/adapters.go`):**
  - `GenerateSyntheticStream`: Fixed-seed deterministic market tick generator for testing, demos, and benchmarks.
  - `ReadJSONLFile` / `ReadJSONL`: File/stream adapter parsing `RawTick` JSONL with line-level parse diagnostics.
- **HTTP Service Adapter (`src/server/http.go`):**
  - Exposes `/health`, `/metrics`, `/v1/ingest`, `/v1/query`, and `/v1/gaps` over standard HTTP+JSON.
- **Optional External ClickHouse Adapter (`src/ingest/adapters.go` + `config/clickhouse/init.sql` + `docker-compose.yml`):**
  - `ClickHouseSinkAdapter` batches canonical ticks to ClickHouse HTTP `INSERT INTO tickdb.market_ticks FORMAT JSONEachRow`.
  - Schema DDL uses `ReplacingMergeTree(ingest_time_ns)` partitioned by `(toYYYYMMDD(event_date), shard_id)` with `Delta(8), ZSTD(3)` codecs mirroring the embedded engine's layout.
  - Unit-tested via in-process HTTP `RoundTripper` in `tests/integration_test.go`. Live Docker/ClickHouse execution is **optional and not required** for local validation.
- **Apache Parquet Export (`src/codec/parquet.go`):**
  - Self-contained Parquet 1.0 writer and reader (`PAR1` magic, Thrift Compact Protocol `FileMetaData`, `PLAIN` + `GZIP` data pages, page CRC32) for analytical interoperability without external CGo or third-party dependencies.

## 3. Dependency & License Manifest

This project uses the **Go standard library exclusively** (`go.mod` specifies `go 1.24` with **zero** third-party runtime or test dependencies):

| Dependency | Version | License | Scope |
|---|---|---|---|
| Go Standard Library (`go1.24` / verified on `go1.27.1`) | Stdlib | BSD-3-Clause | Core engine, codecs, storage, HTTP server, CLI, tests |
| ClickHouse Server (optional Docker service only) | `24.8-alpine` (in `docker-compose.yml`) | Apache-2.0 | Optional external deployment; not executed in offline tests |

## 4. Fixture Provenance & Safety

- All fixtures in `data/fixtures/` and synthetic benchmarks are 100% original, deterministically generated mock market data.
- No live capital, real customer records, production credentials, or proprietary artifacts appear anywhere in source code, fixtures, configuration, logs, or reports.
