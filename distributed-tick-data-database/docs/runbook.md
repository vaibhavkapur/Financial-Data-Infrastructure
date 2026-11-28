# Operator & Developer Runbook — Distributed Tick Data Database

## 1. Local Setup

### Prerequisites
- Go `1.24+` (verified on `go1.27.1`, `darwin/arm64`)
- Bash (`scripts/*.sh`) and `make` (optional)
- Zero external Go packages or network access required to build, test, demo, or benchmark.
- Optional: Docker + Docker Compose (only if exercising the external ClickHouse profile in `docker-compose.yml`).

### Build
```bash
cd distributed-tick-data-database
go build -o bin/tickdb ./src/cmd/tickdb
```

## 2. Validation & Demo Commands

### Full Validation Suite (Vet + Race Tests + Demo + Benchmarks)
```bash
./scripts/validate.sh
```

### Run Unit, Integration, Resilience, Boundary, and Reproducibility Tests
```bash
go test -v -race ./...
```

### Run Reproducible End-to-End Demo
```bash
./scripts/run_demo.sh
```
This runs both the hand-calculated reference fixture and a 5,000-tick synthetic stream with duplicates, gaps, late arrivals, abrupt restart WAL recovery, and JSONL/CSV/Parquet export verification, writing raw evidence to `reports/demo_replay_comparison.json`.

### Run Benchmarks
```bash
./scripts/run_benchmarks.sh
```
Measures ingestion throughput, disk usage/compression ratio, p50/p95/p99 range query latency, and WAL crash recovery latency, writing raw results to `reports/benchmark_results.json`.

### CLI Workflow Walkthrough
```bash
# 1. Generate a deterministic synthetic feed (seed=42 from config/default.json)
./bin/tickdb generate --config config/default.json --count 2000 --out data/sample.jsonl

# 2. Ingest into local 4-shard storage
./bin/tickdb ingest --config config/default.json --input data/sample.jsonl --batch-size 500

# 3. Inspect sequence gaps and completeness metrics
./bin/tickdb gaps --config config/default.json

# 4. Run a bounded historical range query for AAPL
./bin/tickdb query --config config/default.json \
  --instrument AAPL \
  --start 1764320400000000000 \
  --end 1764324000000000000 \
  --limit 50

# 5. Export deterministic replay to Apache Parquet
./bin/tickdb replay --config config/default.json \
  --instrument AAPL \
  --start 1764320400000000000 \
  --end 1764324000000000000 \
  --format parquet \
  --out data/aapl_replay.parquet
```

## 3. Diagnostics & Observability

- **Structured CLI error output:** All CLI failures emit a single JSON line to `stderr` with `level`, `command`, and typed error code (`ERR_*`) and exit non-zero.
- **HTTP server diagnostics (`./bin/tickdb serve --config config/default.json --listen :8081`):**
  - `GET /health` — node ID, topology, shard count.
  - `GET /metrics` — storage stats (segments, bytes, WAL bytes, memtable depth), pipeline counters (`backpressure_rejects`, `accepted_events`, `duplicate_events`), freshness lag (`lag_p50_ms`, `lag_p95_ms`, `lag_p99_ms`), and per-stream completeness ratios.
  - `GET /v1/gaps` — exact list of open `[start_seq, end_seq]` sequence gaps and stream completeness metrics.

## 4. Incident & Recovery Procedures

### Scenario A: Node Crash or Power Loss Mid-Write (Partial WAL Frame)
1. **Detection:** On restart, `storage.OpenShard` validates every WAL frame's `"TWAL"` magic, payload length, and CRC32 checksum.
2. **Automatic Recovery (Default: `wal.truncate_corrupt_tail_on_recovery: true`):**
   - The incomplete/corrupt trailing frame is truncated back to the last valid frame offset and `fsync`ed.
   - All previously acknowledged batches are replayed into the sequencer and memtable.
   - Any orphaned `*.tmp` files from interrupted flushes are removed automatically.
   - `ShardRecoveryReport` logs exact `truncated_corrupt_bytes`, `applied_wal_frames`, `applied_wal_events`, and `orphaned_temp_files_cleaned`.
3. **Strict Forensic Mode (`wal.truncate_corrupt_tail_on_recovery: false`):**
   - Startup aborts with `ERR_WAL_CORRUPT_TAIL` and byte offset so operators can copy `shards/shard-<NN>/wal/wal.log` for forensic inspection before enabling truncation.

### Scenario B: Ingestion Backpressure (`ERR_BACKPRESSURE_QUEUE_FULL` / HTTP 429)
1. **Symptoms:** Producers receive `ERR_BACKPRESSURE_QUEUE_FULL` with `retry_after_ms`; `/metrics` shows rising `pipeline.backpressure_rejects` and `current_queue_depth == queue_capacity_batches`.
2. **Mitigation:**
   - Honor `retry_after_ms` with exponential backoff; do **not** drop batches—retrying committed batches is safe because ingestion is idempotent per `(source_id, instrument_id, sequence_number)`.
   - Increase `ingest.queue_capacity_batches` or batch size (`ingest.max_batch_size`) to amortize `fsync` overhead, or verify underlying disk I/O latency.

### Scenario C: Large Range Query Rejected (`ERR_QUERY_RANGE_TOO_LARGE` / `ERR_QUERY_LIMIT_EXCEEDED` / `ERR_QUERY_PARTITION_BUDGET_EXCEEDED`)
1. **Cause:** Query violates configured bounded scan guardrails.
2. **Mitigation:**
   - Filter by a single `instrument_id` (cuts scanned shards from `N` to `1`).
   - Split the time range into windows `<= query.max_range_duration_hours`.
   - Use `after_cursor` from `RangeResponse.NextCursor` when `has_more == true` to page through results in bounded chunks.

### Scenario D: Missing or Invalid Configuration
- Startup fails fast with `ERR_CONFIG_NOT_FOUND`, `ERR_CONFIG_PARSE_FAILED`, `ERR_CONFIG_UNSUPPORTED_VERSION`, or `ERR_CONFIG_INVALID` (naming the exact offending field). Validate your config against `config/default.json` before restarting.
