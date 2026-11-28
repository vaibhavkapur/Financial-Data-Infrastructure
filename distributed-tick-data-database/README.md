# Distributed Tick Data Database

An independently developed educational/research prototype for storing, validating, deduplicating, gap-tracking, querying, and replaying high-volume market tick data with measurable throughput, disk efficiency, crash recovery, and deterministic reproducibility.

> **Scope & Topology Notice:** This prototype runs offline with synthetic market fixtures only (no live capital, customer records, or production credentials). All reported benchmark numbers were measured on a **single-node multi-shard** topology (`4` local shards) and are **not** evidence of multi-host distributed scalability.

---

## Key Capabilities

- **Canonical Data Contract & Lineage (`src/domain/`):** Fixed-point nanounit (`int64`, $10^{-9}$) prices and sizes, UTC nanosecond `event_time_ns` and `ingest_time_ns`, `source_id` / `feed_session_id` / `batch_id` lineage, `schema_version` enforcement (`1.0.0`), and structured `ERR_*` validation diagnostics.
- **Deterministic Partitioning & Columnar Compression (`src/domain/partition.go`, `src/codec/`):**
  - Shard routing via `FNV-1a-32(instrument_id) % num_shards` and UTC `hour`/`day` time bucketing derived from `event_time_ns`.
  - `.tseg` immutable columnar segments using Delta-of-Delta timestamps/sequences, Delta prices, Varint sizes, Dictionary+RLE strings, Byte-RLE flags, GZIP block compression, CRC32 checksums, and uncompressed **ZoneMap headers** for O(1) segment pruning.
  - Native **Apache Parquet 1.0** writer & round-trip reader (`PAR1` magic, Thrift Compact Protocol `FileMetaData`, `PLAIN` + `GZIP` data pages, page CRC32) with zero third-party dependencies.
- **Durable Batched Ingestion with Idempotency, Gap Tracking & Backpressure (`src/storage/`, `src/ingest/`):**
  - Per-shard CRC32-checksummed binary WAL (`fsync_per_batch`), atomic `.tmp` → `fsync` → `rename` segment flushes, automatic partial-batch WAL tail recovery, and orphan `.tmp` cleanup.
  - Per-`(source_id, instrument_id, sequence_number)` deduplication with conflicting-payload detection (`ERR_DUPLICATE_SEQUENCE_CONFLICT`), exact open gap interval tracking `[start_seq, end_seq]`, late-event gap reconciliation, and completeness/freshness metrics.
  - Bounded ingestion queue with explicit `ERR_BACKPRESSURE_QUEUE_FULL` signaling and zero silent drops of accepted ticks.
- **Bounded Historical Range Queries & Deterministic Replay Export (`src/query/`):**
  - Enforced range-duration, row-limit, partition-budget, and timeout guardrails with cursor-based pagination.
  - Canonical replay export to `jsonl`, `csv`, and `parquet`, fingerprinted with deterministic `logical_sha256` and `file_sha256` digests.
- **Optional External ClickHouse & Docker Compose Profile (`docker-compose.yml`, `config/clickhouse/init.sql`, `src/ingest/adapters.go`):**
  - Clean HTTP `ClickHouseSinkAdapter` (`JSONEachRow`) and `ReplacingMergeTree` DDL for optional external ClickHouse deployment.

---

## Quickstart

### Prerequisites
- Go `1.24+` (verified on `go1.27.1`, `darwin/arm64`) — **zero external Go module dependencies**.

### Build, Test, Demo & Benchmark
```bash
cd distributed-tick-data-database

# 1. Run static checks and full test suite with race detector
go vet ./...
go test -v -race ./...

# 2. Run reproducible end-to-end demo (oracle comparison + restart + Parquet round-trip)
./scripts/run_demo.sh

# 3. Run throughput, compression, query p99, and crash-recovery benchmarks
./scripts/run_benchmarks.sh

# 4. Run full CI validation pipeline
./scripts/validate.sh
```

### Example CLI Walkthrough
```bash
go build -o bin/tickdb ./src/cmd/tickdb

# Generate deterministic synthetic ticks (seed=42)
./bin/tickdb generate --config config/default.json --count 2000 --out data/sample.jsonl

# Ingest into sharded storage
./bin/tickdb ingest --config config/default.json --input data/sample.jsonl --batch-size 500

# Inspect open sequence gaps and completeness metrics
./bin/tickdb gaps --config config/default.json

# Bounded historical range query
./bin/tickdb query --config config/default.json \
  --instrument AAPL \
  --start 1764320400000000000 \
  --end 1764324000000000000 \
  --limit 25

# Deterministic Apache Parquet replay export
./bin/tickdb replay --config config/default.json \
  --instrument AAPL \
  --start 1764320400000000000 \
  --end 1764324000000000000 \
  --format parquet \
  --out data/aapl_replay.parquet
```

---

## Measured Results (Observed Local Single-Node Multi-Shard)

> All metrics below are **actual observations** recorded on 2026-11-28 (`macOS 26.5.1`, Apple M5 10-core, `go1.27.1`, 4 local shards, `fsync_per_batch` WAL) on deterministic synthetic fixtures (`seed=20261128`). Raw JSON outputs are stored in [`reports/benchmark_results.json`](reports/benchmark_results.json) and [`reports/demo_replay_comparison.json`](reports/demo_replay_comparison.json).

| Metric | Observed Result | Configuration / Workload |
|---|---|---|
| **Ingestion throughput (WAL `fsync_per_batch`)** | **142,671 accepted ticks/s** (116,294 ticks/s incl. final segment flush) | 100,970 raw synthetic ticks (100,000 unique, 970 dups, 1,324 late), batch size 2,000 |
| **Disk usage & compression** | **2.34 MiB** (`24.53 B/tick`, **16.40×** vs 38.35 MiB raw JSONL) | 19 `.tseg` segments (`delta_dict_gzip`) |
| **Parquet export (20,000 rows)** | **561.5 KB** (`28.08 B/row`) in **42.71 ms** | Native Apache Parquet 1.0 (`PAR1`, GZIP pages) |
| **Bounded range query latency** | **p50: 2.14 ms · p95: 4.20 ms · p99: 6.59 ms** (max: 7.40 ms) | 300 × 5-min single-instrument queries (`83.78%` segment pruning) |
| **Crash & partial WAL recovery** | **27.95 ms** · **10,000 / 10,000** WAL ticks recovered (**0 lost**) | Unflushed WAL + 42-byte simulated corrupt tail truncated cleanly |
| **Reference oracle parity** | **Exact match** (`0` deviation across JSONL, CSV, Parquet) | Hand-calculated 10-tick fixture + 5,000-tick restart demo |

---

## Project Layout

```text
distributed-tick-data-database/
├── plan.md                                 # Original project specification
├── README.md                               # This overview & quickstart
├── go.mod                                  # Zero third-party dependencies (Go 1.24+)
├── Makefile                                # build / test / demo / bench / validate
├── Dockerfile & docker-compose.yml         # Optional external ClickHouse deployment profile
├── config/
│   ├── default.json                        # Versioned runtime configuration
│   ├── benchmark.json                      # Versioned benchmark configuration
│   └── clickhouse/init.sql                 # Optional ClickHouse ReplacingMergeTree DDL
├── data/fixtures/
│   ├── valid_reference_ticks.jsonl         # Hand-calculated valid + dup + gap + late fixture
│   ├── expected_reference_result.json      # Independent hand-calculated expected outputs
│   ├── invalid_ticks.jsonl                 # 10 schema/domain validation error fixtures
│   └── boundary_ticks.jsonl                # UTC hour boundary, locked quote, 1-nano size
├── src/
│   ├── config/                             # Strict versioned config loader & validation
│   ├── domain/                             # Pure I/O-free schema, partitioner, sequencer, ReferenceOracle
│   ├── codec/                              # Columnar .tseg codec + Apache Parquet 1.0 writer/reader
│   ├── storage/                            # CRC32 binary WAL, atomic shard store, multi-shard engine
│   ├── ingest/                             # Bounded backpressure pipeline, synthetic/JSONL/ClickHouse adapters
│   ├── query/                              # Guarded historical range query engine & replay exporter
│   ├── server/                             # HTTP API (/health, /metrics, /v1/ingest, /v1/query, /v1/gaps)
│   └── cmd/tickdb/                         # CLI entrypoint
├── tests/                                  # Unit, codec, integration, boundary, reproducibility & ops suites
├── scripts/                                # run_demo.sh, run_benchmarks.sh, validate.sh
├── docs/
│   ├── architecture.md                     # Component diagram, data flow, invariants, trade-offs
│   ├── contracts.md                        # Input/output contracts, units, error codes, fixtures
│   ├── integration.md                      # Adapter boundaries, sibling repo notes, license manifest
│   ├── runbook.md                          # Setup, walkthrough, diagnostics, incident recovery
│   └── adr/0001-partitioning-compression-and-storage-engine.md
└── reports/
    ├── acceptance.md                       # Acceptance criteria mapped to observed evidence & not-run items
    ├── benchmark_results.json              # Raw machine-readable benchmark output
    ├── demo_replay_comparison.json         # Raw oracle vs engine comparison output
    └── test_results.jsonl                  # Raw go test -json output
```

---

## Documentation & Acceptance Evidence

- **Acceptance Report:** [`reports/acceptance.md`](reports/acceptance.md)
- **Architecture & Invariants:** [`docs/architecture.md`](docs/architecture.md)
- **ADR 0001 (Partitioning, Compression & Storage):** [`docs/adr/0001-partitioning-compression-and-storage-engine.md`](docs/adr/0001-partitioning-compression-and-storage-engine.md)
- **Data Contracts & Error Codes:** [`docs/contracts.md`](docs/contracts.md)
- **Integration & Dependencies:** [`docs/integration.md`](docs/integration.md)
- **Operator & Developer Runbook:** [`docs/runbook.md`](docs/runbook.md)

---

## Assumptions & Known Limitations

1. **Single-node topology only:** Multi-host horizontal scaling, cross-node replication (Raft / ClickHouse Keeper), and network-partition fault injection are **not** tested in this prototype.
2. **Optional external dependencies not run:** `docker-compose.yml` and `config/clickhouse/init.sql` were not executed live because Docker and ClickHouse are unavailable in the validation environment (see [`reports/acceptance.md`](reports/acceptance.md) §4).
3. **Unbounded dedup map per stream:** `StreamState.SeenDigests` tracks exact per-sequence digests in memory for deterministic conflict detection; multi-month production retention would require a sliding retention window or disk-backed index.
4. **No background segment compaction:** Each memtable flush produces immutable segments per time bucket; long-running ingestion with very small memtables would benefit from periodic compaction.
5. **Unadjusted market data:** Prices and sizes represent raw unadjusted market events; corporate action adjustments belong downstream.
