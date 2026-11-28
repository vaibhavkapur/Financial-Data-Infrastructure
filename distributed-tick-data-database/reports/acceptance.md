# Acceptance & Verification Report — Distributed Tick Data Database

**Date:** 2026-11-28  
**Environment:** `macOS 26.5.1 (darwin/arm64)`, Apple M5 (10 cores, 16 GiB RAM), `go1.27.1`  
**Measured Topology:** `single-node-multi-shard` (4 shards on local NVMe filesystem, `fsync_per_batch` WAL)  
**Important Scope Disclosure:** All measured results below were observed on a **single-node multi-shard** local deployment using deterministic synthetic fixtures. Single-node tests are **not** distributed multi-host scalability evidence.

---

## 1. Project-Specific Acceptance Criteria Matrix

| # | Acceptance Criterion (`plan.md` §5) | Status | Verification Evidence |
|---|---|---|---|
| 1 | Query outputs match reference replay | **PASS** | `TestReferenceFixtureMatchesHandCalculatedOracle` (`tests/domain_test.go`), `TestNodeRestartPreservesFlushedAndWALOnlyDataAndIdempotency` (`tests/integration_test.go`), and `reports/demo_replay_comparison.json`: 10-event hand-calculated fixture (`sha256: d6f207852f84...`) and 5,000-row synthetic stream across restart + JSONL/CSV/Parquet round-trip (`sha256: 27643523b420...`) match `domain.ReferenceOracle` with **0 deviation**. |
| 2 | No silently lost accepted ticks | **PASS** | `TestNodeRestartPreservesFlushedAndWALOnlyDataAndIdempotency`, `TestPartialBatchWriteRecoveryAndOrphanTempCleanup`, `TestIngestionBackpressureRejectsExplicitlyWithZeroSilentLoss` (`tests/boundary_test.go`), and `reports/benchmark_results.json` (`recovery_under_load.zero_accepted_ticks_lost: true`, `10,000 / 10,000` uncommitted WAL ticks recovered after crash + 42-byte corrupt tail truncation). |
| 3 | Publish measured ingestion throughput, disk usage and p99 query latency | **PASS** | Published below (§3) and archived in raw machine-readable form in `reports/benchmark_results.json`. |
| 4 | All required test scenarios pass in the supported environment, or unavailable dependencies marked not run | **PASS** | All 12 Go test suites pass under `go test -v -race ./...` (raw log: `reports/test_results.jsonl`). Unavailable dependencies (Docker, live ClickHouse server, multi-host cluster) are explicitly marked **Not Run** in §4. |
| 5 | README documents setup, demo, validation, assumptions, and known limitations; measured results distinguish observations from fixtures/targets | **PASS** | Documented in `README.md`, `docs/architecture.md`, `docs/runbook.md`, and `docs/adr/0001-partitioning-compression-and-storage-engine.md`. |
| 6 | No real transactions, customer data, credentials or proprietary employer/SAAT artifacts | **PASS** | All fixtures (`data/fixtures/`), code (`src/`, `tests/`), configs (`config/`), and reports (`reports/`) contain original mock data only. |

---

## 2. Test & Validation Matrix Coverage (`plan.md` §4)

| Test Area | Required Scenario | Test / Script | Result | Raw Evidence |
|---|---|---|---|---|
| Domain correctness | Duplicate sequence and late event | `TestReferenceFixtureMatchesHandCalculatedOracle`, `TestDuplicateSequenceAndConflictAndLateEventPolicies`, `TestInvalidAndBoundaryFixtures` | **PASS** | `reports/test_results.jsonl`, `reports/demo_replay_comparison.json` |
| Integration and resilience | Node restart and partial batch write | `TestNodeRestartPreservesFlushedAndWALOnlyDataAndIdempotency`, `TestPartialBatchWriteRecoveryAndOrphanTempCleanup`, `TestHTTPAPIAndClickHouseAdapterIntegration` | **PASS** | `reports/test_results.jsonl`, `reports/benchmark_results.json` |
| Boundary and failure behavior | Large range query and backpressure | `TestLargeRangeQueryGuardsAndBoundedPagination`, `TestIngestionBackpressureRejectsExplicitlyWithZeroSilentLoss` | **PASS** | `reports/test_results.jsonl` |
| Codecs & Replay Export | Columnar `.tseg` & Apache Parquet v1 (`PAR1`) | `TestColumnarSegmentCodecRoundTripAndCorruptionDetection`, `TestParquetCodecRoundTripAndMagicValidation` | **PASS** | `reports/test_results.jsonl`, `reports/demo_replay_comparison.json` |
| Reproducibility | Fixed seed, versioned config & inputs | `TestReproducibilityDeterministicAcrossCleanRunsAndFormats` | **PASS** | `reports/test_results.jsonl` |
| Operational review | Missing config, invalid inputs, rerun idempotency | `TestOperationalMissingConfigInvalidInputsAndRerunIdempotency` | **PASS** | `reports/test_results.jsonl` |

---

## 3. Measured Benchmark Summary (Observed Local Single-Node Multi-Shard)

Source: `reports/benchmark_results.json` (`config/benchmark.json`, `seed: 20261128`, `100,970` raw synthetic ticks → `100,000` accepted unique ticks, `970` deduplicated, `1,324` late accepted, `batch_size: 2000`, `wal.sync_mode: fsync_per_batch`, `compression: delta_dict_gzip`).

| Metric | Observed Measurement | Notes |
|---|---|---|
| Ingestion throughput (WAL `fsync_per_batch`) | **142,671 accepted ticks/s** (144,055 raw ticks/s) | 700.91 ms for 51 durable batches |
| Ingestion throughput (incl. final segment flush) | **116,294 accepted ticks/s** | 859.89 ms total end-to-end |
| Raw JSONL baseline size (100,970 raw ticks) | **40,216,286 bytes** (38.35 MiB) | Reference uncompressed text baseline |
| Uncompressed columnar payload size | **4,539,019 bytes** (4.33 MiB) | Delta-of-Delta + Delta + Varint + Dict-RLE |
| On-disk `.tseg` compressed size (19 segments) | **2,452,842 bytes** (2.34 MiB) | **24.53 bytes/accepted tick** (**16.40×** vs raw JSONL) |
| Apache Parquet export (20,000-row sample) | **561,541 bytes** (28.08 bytes/row) | Exported in 42.71 ms (`PAR1` + Thrift + GZIP) |
| Range query latency — p50 (300 × 5-min queries) | **2.139 ms** | Avg 201.7 rows/query |
| Range query latency — p95 | **4.197 ms** | `83.78%` segment pruning (`1,643` pruned / `318` scanned) |
| Range query latency — p99 | **6.594 ms** | Max observed: `7.399 ms` |
| WAL crash recovery under load (10,000 WAL ticks + 42B corrupt tail) | **27.95 ms** | `10,000 / 10,000` ticks recovered (`0` lost); 42B tail truncated |

---

## 4. Explicitly Untested / Not-Run Capabilities

| Capability | Status | Reason |
|---|---|---|
| Live ClickHouse server ingestion (`docker-compose.yml`, `config/clickhouse/init.sql`) | **NOT RUN** | `docker` and `clickhouse-server` binaries are not installed in the local validation environment. HTTP request encoding and `JSONEachRow` formatting are verified via in-process mock `RoundTripper` in `TestHTTPAPIAndClickHouseAdapterIntegration`. |
| Multi-host distributed deployment & network-partition / consensus failover | **NOT RUN** | Local environment is a single host (`darwin/arm64`). Reported metrics reflect single-node 4-shard storage only. |
| External reader validation of `.parquet` with PyArrow / DuckDB | **NOT RUN** | `duckdb` and `pyarrow` are not installed locally; Parquet structural compliance (`PAR1` magic, Thrift Compact Protocol `FileMetaData`, page CRC32, GZIP pages) is verified via the project's independent round-trip reader `codec.ReadParquet`. |

---

## 5. Raw Machine-Readable Evidence Artifacts

- `reports/benchmark_results.json` — Versioned raw benchmark measurements and runtime/config metadata.
- `reports/demo_replay_comparison.json` — Raw oracle vs engine comparison for the hand-calculated fixture and 5,000-tick restart demo across JSONL, CSV, and Parquet.
- `reports/test_results.jsonl` — Machine-readable `go test -json ./...` test execution log.
