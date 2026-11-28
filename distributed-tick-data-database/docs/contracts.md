# Data Contracts — Distributed Tick Data Database

**Contract Version:** `1.0.0`

## 1. Input Contract (`RawTick`)

External source adapters emit newline-delimited JSON (JSONL) or HTTP JSON payloads conforming to `domain.RawTick`:

| Field | Type | Units / Semantics | Constraints |
|---|---|---|---|
| `schema_version` | `string` | Semantic version | Must equal `"1.0.0"` |
| `source_id` | `string` | Upstream feed identifier | Required, non-empty |
| `feed_session_id` | `string` | Feed session lineage ID | Required, non-empty |
| `instrument_id` | `string` | Canonical security/symbol ID | Required, non-empty; shard routing key |
| `venue` | `string` | MIC or exchange identifier (e.g. `XNAS`, `XCME`, `COINBASE`) | Required, non-empty |
| `event_type` | `string` | Enum | `"trade"`, `"quote"`, or `"order_book_delta"` |
| `side` | `string` | Enum | `"buy"`, `"sell"`, or `"none"` (`trade` requires `"buy"` or `"sell"`) |
| `event_time_ns` | `int64` | Exchange event timestamp in UTC Unix epoch nanoseconds | Must lie in `[946684800000000000, 4102444800000000000]` (2000-01-01 to 2100-01-01 UTC) |
| `sequence_number` | `uint64` | Per-`(source_id, instrument_id)` monotonic sequence | Must be `>= 1` |
| `price_nanos` | `int64` | Fixed-point price scaled by $10^9$ (nanounits; e.g. \$185.25 = `185250000000`) | `> 0` for `trade` and `order_book_delta`; `0` allowed for `quote` |
| `size_nanos` | `int64` | Fixed-point quantity scaled by $10^9$ (e.g. 100 shares = `100000000000`) | `> 0` for `trade`; `>= 0` for `order_book_delta` and `quote` |
| `bid_price_nanos` | `int64` | Best bid price scaled by $10^9$ | `>= 0`; if both bid and ask `> 0`, `bid_price_nanos <= ask_price_nanos` |
| `bid_size_nanos` | `int64` | Best bid size scaled by $10^9$ | `>= 0` |
| `ask_price_nanos` | `int64` | Best ask price scaled by $10^9$ | `>= 0`; at least one of bid/ask `> 0` for `quote` |
| `ask_size_nanos` | `int64` | Best ask size scaled by $10^9$ | `>= 0` |

### Timestamp Semantics
- **`event_time_ns`**: Authoritative market/exchange event time in UTC epoch nanoseconds. Used for UTC time partitioning (`hour` or `day`), ZoneMap min/max bounds, range query filtering, and canonical replay ordering.
- **`ingest_time_ns`**: Availability/ingestion timestamp assigned upon batch acceptance in UTC epoch nanoseconds. Used for freshness lag calculation (`ingest_time_ns - event_time_ns`) and audit lineage.

## 2. Stored & Output Contract (`CanonicalTick`)

All query responses (`RangeResponse.Ticks`) and replay exports (`jsonl`, `csv`, `parquet`) expose every `RawTick` field plus enriched storage & lineage metadata:

| Added Field | Type | Semantics |
|---|---|---|
| `batch_id` | `string` | Caller or pipeline batch identifier |
| `ingest_time_ns` | `int64` | Ingestion/availability time (UTC epoch ns) |
| `late_arrival` | `bool` | `true` if arrived out-of-sequence (`seq < high_water_seq`) or behind stream event-time watermark |
| `shard_id` | `uint16` | Deterministic owning shard (`FNV-1a-32(instrument_id) % num_shards`) |
| `partition_key` | `string` | Canonical partition key `<YYYY-MM-DDTHH|YYYY-MM-DD>/shard-<NN>` |

### Canonical Replay Ordering
All query and replay outputs are deterministically sorted by the total key:
`(event_time_ns ASC, instrument_id ASC, source_id ASC, sequence_number ASC)`.

### Deterministic Digest (`logical_sha256`)
Every `RangeResponse` and `ExportReceipt` publishes `logical_sha256`: the SHA-256 digest over the canonical ordered logical fields (excluding volatile `batch_id` and `ingest_time_ns`), enabling bit-for-bit reproducibility checks across runs and output formats (`jsonl`, `csv`, `parquet`).

## 3. Machine-Readable Error Codes

### Validation & Domain Errors (`domain.DomainError`)
| Code | Trigger |
|---|---|
| `ERR_UNSUPPORTED_SCHEMA_VERSION` | `schema_version != "1.0.0"` |
| `ERR_MISSING_SOURCE_ID` | Empty `source_id` |
| `ERR_MISSING_FEED_SESSION_ID` | Empty `feed_session_id` |
| `ERR_MISSING_INSTRUMENT_ID` | Empty `instrument_id` |
| `ERR_MISSING_VENUE` | Empty `venue` |
| `ERR_INVALID_EVENT_TYPE` | Unknown `event_type` |
| `ERR_INVALID_SIDE` | Unknown `side` |
| `ERR_INVALID_EVENT_TIME` | `event_time_ns` outside `[2000-01-01, 2100-01-01]` UTC |
| `ERR_INVALID_SEQUENCE_NUMBER` | `sequence_number == 0` |
| `ERR_NEGATIVE_BOOK_VALUE` | Any bid/ask price or size `< 0` |
| `ERR_CROSSED_QUOTE` | `bid_price_nanos > ask_price_nanos` when both `> 0` |
| `ERR_INVALID_TRADE_PRICE` / `ERR_INVALID_TRADE_SIZE` / `ERR_INVALID_TRADE_SIDE` | Invalid trade fields |
| `ERR_EMPTY_QUOTE` | Quote with neither bid nor ask `> 0` |
| `ERR_INVALID_DELTA_PRICE` / `ERR_INVALID_DELTA_SIZE` | Invalid `order_book_delta` fields |
| `ERR_DUPLICATE_SEQUENCE_CONFLICT` | Same `(source_id, instrument_id, sequence_number)` with different payload digest |
| `ERR_EVENT_TOO_LATE` | Late event exceeds watermark lag under `late_event_policy: "reject_too_late"` |
| `ERR_BATCH_TOO_LARGE` | Batch length exceeds `ingest.max_batch_size` |

### Ingestion, Storage, Query & Config Errors
| Code | Component | Trigger |
|---|---|---|
| `ERR_BACKPRESSURE_QUEUE_FULL` | `ingest.Pipeline` | Bounded queue stayed saturated for `backpressure_timeout_ms` |
| `ERR_WAL_CORRUPT_TAIL` | `storage.WAL` | Corrupt/truncated WAL tail when `truncate_corrupt_tail_on_recovery: false` |
| `ERR_SEGMENT_CORRUPT` | `codec.Segment` | Invalid magic, truncated bytes, or CRC32 mismatch |
| `ERR_PARQUET_CORRUPT` | `codec.Parquet` | Invalid `PAR1` magic, Thrift footer error, or page CRC32 mismatch |
| `ERR_QUERY_INVALID_RANGE` | `query.Service` | `end_time_ns < start_time_ns` |
| `ERR_QUERY_RANGE_TOO_LARGE` | `query.Service` | Requested span exceeds `query.max_range_duration_hours` |
| `ERR_QUERY_LIMIT_EXCEEDED` | `query.Service` | Requested `limit` exceeds `query.max_limit_rows` |
| `ERR_QUERY_PARTITION_BUDGET_EXCEEDED` | `query.Service` | `buckets * shards` exceeds `query.max_scan_partitions` |
| `ERR_QUERY_TIMEOUT` | `query.Service` | Context exceeded `query.timeout_ms` |
| `ERR_UNSUPPORTED_EXPORT_FORMAT` | `query.Service` | Format not in `{jsonl, csv, parquet}` |
| `ERR_CONFIG_NOT_FOUND` / `ERR_CONFIG_PARSE_FAILED` / `ERR_CONFIG_INVALID` / `ERR_CONFIG_UNSUPPORTED_VERSION` | `config` | Missing, malformed, or invalid configuration |

## 4. Fixtures

All fixtures live in `data/fixtures/` and are verified automatically in `tests/domain_test.go`:
- `data/fixtures/valid_reference_ticks.jsonl`: 10-event deterministic stream across `AAPL` and `MSFT` exercising in-order ticks, 1 exact duplicate, 2 late events (one closing a gap, one narrowing a gap), 1 remaining open gap (`AAPL` seq 4), and 1 UTC hour partition boundary.
- `data/fixtures/expected_reference_result.json`: Hand-calculated expected counts, ordered sequences, open gaps, and completeness ratios.
- `data/fixtures/invalid_ticks.jsonl`: 10 invalid ticks covering unsupported schema versions, missing IDs, sequence 0, bad enum values, negative prices, zero trade size, crossed quotes, and out-of-bounds timestamps.
- `data/fixtures/boundary_ticks.jsonl`: Exact UTC hour boundary transition (`...999999999` → `...000000000`), locked zero-spread quote (`bid == ask`), and 1-nanounit minimum trade size.
