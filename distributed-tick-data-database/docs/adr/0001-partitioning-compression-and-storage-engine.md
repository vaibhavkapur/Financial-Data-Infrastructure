# ADR 0001: Partitioning, Compression, and Storage Engine Architecture

- **Status:** Accepted
- **Date:** 2026-11-28
- **Project:** Distributed Tick Data Database (`distributed-tick-data-database/`)

## Context

High-volume market tick data exhibits strong temporal locality, monotonic timestamps and sequence numbers per `(source_id, instrument_id)`, low-cardinality string dimensions (`instrument_id`, `venue`, `source_id`, `event_type`, `side`), and small step-to-step price deltas. At the same time, trading data infrastructure requires:

1. **Zero silent data loss** for acknowledged ticks, even across crashes or partial batch writes.
2. **Idempotent ingestion** with explicit sequence-gap tracking and late-arrival reconciliation.
3. **Bounded historical range queries** and deterministic **replay export** (JSONL, CSV, and Apache Parquet).
4. **Offline reproducibility** with zero fragile external runtime dependencies during local development and CI, while preserving a clean adapter path to ClickHouse and multi-node deployments.

## Decision

### 1. Stack Selection: Pure Go Standard Library Engine + Optional ClickHouse Adapter

We implement the core engine in Go (standard library only, zero third-party module dependencies) to maximize build reproducibility, compile speed, concurrency ergonomics, and license clarity. An HTTP ClickHouse sink adapter (`src/ingest/clickhouse_adapter.go`) and `docker-compose.yml` + `config/clickhouse/init.sql` provide optional external ClickHouse integration without blocking local/CI verification when Docker or ClickHouse is absent.

### 2. Partitioning Scheme

Every canonical tick is routed deterministically to a shard and time partition:

- **Shard routing:** `shard_id = FNV-1a-32(instrument_id) % num_shards`
  - Co-locates all ticks for a given instrument on a single shard, preserving local per-instrument ordering, local deduplication, and single-shard fast-path queries for single-instrument range scans.
- **Time partitioning:** UTC `hour` (`YYYY-MM-DDTHH`) or `day` (`YYYY-MM-DD`) buckets derived from `event_time_ns`, never `ingest_time_ns`.
  - Enables O(1) partition pruning for historical range queries regardless of when late events arrived.
- **Within-segment clustering:** Sorted by `(instrument_id, event_time_ns, sequence_number, source_id)`.

### 3. Storage Engine & Durability (WAL + Immutable Columnar Segments)

Each shard operates an LSM-inspired storage layout:

1. **Write-Ahead Log (WAL):** Append-only binary log with per-batch length prefix, CRC32-IEEE checksum, batch ID, and configurable `fsync_per_batch` durability. A batch is acknowledged to the caller only after its WAL frame is durably written and synced.
2. **Partial-Write Recovery:** On startup, the WAL reader verifies magic bytes, frame length, and CRC32 for each frame. Any truncated or CRC-mismatched trailing frame (simulating power loss or process kill mid-write) is explicitly logged and safely truncated without affecting previously committed batches.
3. **MemTable & Atomic Segment Flush:** Committed ticks accumulate in an in-memory sorted buffer and flush atomically (write `.tseg.tmp` → `fsync` → atomic `rename` to `.tseg` → update `manifest.json.tmp` → atomic `rename` → WAL checkpoint/rotate).
4. **Sequence & Gap State Persistence:** Dedup high-water marks, recent sequence sets, and open gap intervals are persisted alongside WAL frames and checkpoints so idempotency and gap tracking survive restarts.

### 4. Columnar Encoding & Compression (`.tseg` and `.parquet`)

- **Fixed-point pricing & sizing:** Prices and sizes are stored as `int64` nanounits ($10^{-9}$), eliminating floating-point drift.
- **Columnar `.tseg` segments:**
  - `event_time_ns`, `ingest_time_ns`, `sequence_number`: Delta-of-Delta + ZigZag + Varint
  - `price_nanos`, `bid_price_nanos`, `ask_price_nanos`: Delta + ZigZag + Varint
  - `size_nanos`, `bid_size_nanos`, `ask_size_nanos`: ZigZag + Varint
  - `instrument_id`, `venue`, `source_id`, `feed_session_id`, `batch_id`: Dictionary + Run-Length Encoding (RLE)
  - `event_type`, `side`, `late_arrival`: Bit/byte-packed RLE
  - Compressed payload wrapped with GZIP and verified by CRC32 checksum, preceded by an uncompressed **Zone Map header** (min/max `event_time_ns`, min/max `sequence_number`, distinct instruments, row count) enabling segment pruning without decompressing non-matching segments.
- **Apache Parquet Export (`.parquet`):** Native Parquet v1 writer/reader implementing `PAR1` magic bytes, Thrift Compact Protocol `FileMetaData`, `PLAIN` encoded pages with `GZIP` compression, and column statistics for interoperability with analytical tools.

## Consequences

- **Pros:** Zero external build/test dependencies; deterministic replay; high compression ratios (typically 6x–15x vs JSONL); fast partition & zone-map pruned range queries; provable crash recovery for partial WAL frames.
- **Cons / Trade-offs:** Single-node multi-shard execution validates sharding, routing, durability, and query merge logic locally, but does **not** constitute distributed multi-host scalability or network-partition (Jepsen) proof. True multi-host replication (e.g., Raft or ClickHouse Keeper) remains outside this single-node prototype scope and is explicitly documented as such.
