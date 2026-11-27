# M1 Audit: ClickHouse Tick Schemas, Fixture Loaders & Entitlement Specification

## 1. Upstream Repository Inspection & IP Posture

Per `plan.md` Section 2:
- Referenced foundations: `Trading-Software/projects/08-market-data-platform` (TS-08) and `Quantum-Trading/projects/02-hybrid-quantum-portfolio-optimizer`.
- Neither repository is publicly accessible or licensed under confirmed SAAT ownership in this workspace environment.
- Therefore, in strict accordance with `plan.md` Section 2 ("If access or licensing is unresolved, implement against clean, documented contracts instead"), **zero code was copied** from external personal repositories. Instead, we defined explicit adapter contracts (`ClickHouseTickRow`, `ClickHouseTickAdapter`, `FixtureLoader`) that isolate upstream wire schemas from core lake logic.

## 2. ClickHouse Tick Schema Contract Audit

Typical ClickHouse `MergeTree` tick tables (`market_ticks_raw`) store high-throughput event streams optimized for append latency rather than bitemporal point-in-time reproducibility. Common gaps identified during design audit and addressed by the Financial Data Lake contract:

| Dimension | ClickHouse `market_ticks_raw` Typical Shape | Gap / Risk | Financial Data Lake Canonical Mitigation |
|---|---|---|---|
| Timestamps | `ts_event_ns` (`Int64` epoch ns), `ts_ingest_ns` (`Int64` epoch ns) | Risk of naive/local timezone confusion or `ts_ingest_ns < ts_event_ns` clock skew | Normalized to timezone-aware UTC `timestamp[us, tz=UTC]` (`event_ts`, `available_at`) with hard invariant `available_at >= event_ts`. |
| Identifiers | `symbol` (`LowCardinality(String)`), `seq_id` (`String`) | Ticker symbols are reused over time; not stable across renames/splits | Adapter resolves `symbol` against `reference_instruments` into a stable `instrument_id` (`INST-*`) before landing. |
| Side encoding | `aggressor_side` (`'B'`, `'S'`, `'U'`) | Vendor-specific shorthand | Mapped explicitly to canonical enum `BUY | SELL | UNKNOWN`; unrecognized codes fail validation. |
| Duplicates | ReplacingMergeTree / at-least-once Kafka ingestion | Duplicate rows persist until asynchronous background merges | Synchronous within-batch AND cross-batch deduplication on `(instrument_id, tick_id, revision_number)` before Parquet commit. |
| Late corrections | Often overwritten in-place or appended without revision link | Destroys point-in-time history; creates lookahead bias | Explicit `revision_number >= 1` and `correction_of`; older revisions remain immutable and as-of queries select the latest revision with `available_at <= as_of_ts`. |
| Currency & units | Implicit or `ccy` code | Ambiguous pricing units across asset classes | Mandatory ISO-4217 3-letter `currency` and explicit `unit` on bars/fundamentals. |

## 3. Fixture Loader Audit & Provenance Contract

Every fixture ingested in prototype/CI mode must be registered in `data/fixtures/manifest.json` with:
- `dataset_id` and `format` (`canonical_jsonl` or `clickhouse_json_each_row`)
- `source_vendor`, `license_id`, and `provenance`
- Exact `record_count`, `byte_size`, and `sha256` checksum
- `redistribution_allowed: false`

`FixtureLoader.load_verified()` verifies the cryptographic SHA-256 digest and line count before yielding bytes to `IngestionPipeline`. Any tampering fails fast with `FixtureVerificationError`.

## 4. Source Entitlement Specification

Configured in `config/entitlements.json` and loaded into SQLite `entitlements` table:
- **Fail-closed default**: Unregistered datasets are denied.
- **Role & purpose matrix**: Separate roles (`data_engineer`, `quant_research`, `system_recovery`, `auditor`) and purposes (`ingest`, `research`, `backtest`, `validation`, `recovery`, `audit`).
- **Temporal validity**: Access outside `[effective_from, expires_at]` (e.g., `legacy_expired_feed`) is denied.
- **Zero redistribution**: Requests with redistribution/export purpose are denied unless `redistribution_allowed=true` (disabled globally in config and all policies).
- **Restricted alternative data (#19 contract)**: `alt_sentiment` is restricted to `quant_research` and `data_engineer` roles only; unauthorized roles (e.g., `external_partner`, `intern_viewer`) are blocked and logged to `audit_log`.

## 5. Schema Evolution Specification

Enforced by `SchemaRegistry.validate_evolution()`:
- **Permitted (backward-compatible)**:
  - Adding a new nullable column.
  - Adding a new non-nullable column with an explicit non-null default value.
  - Widening numeric types (`int32 -> int64`, `int32 -> float64`, `int64 -> float64`).
- **Rejected (`SchemaEvolutionError`)**:
  - Dropping any existing column.
  - Tightening a nullable column to non-nullable.
  - Incompatible type changes (e.g., `float64 -> int64`, `string -> int64`).
  - Adding a non-nullable column without a default.
