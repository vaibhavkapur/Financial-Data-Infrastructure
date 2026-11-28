-- ClickHouse DDL for optional external ClickHouse deployment via docker-compose.yml
-- Not executed automatically unless Docker / ClickHouse is available.

CREATE DATABASE IF NOT EXISTS tickdb;

CREATE TABLE IF NOT EXISTS tickdb.market_ticks
(
    schema_version   LowCardinality(String),
    source_id        LowCardinality(String),
    feed_session_id  LowCardinality(String),
    batch_id         String,
    instrument_id    LowCardinality(String),
    venue            LowCardinality(String),
    event_type       Enum8('trade' = 1, 'quote' = 2, 'order_book_delta' = 3),
    side             Enum8('none' = 0, 'buy' = 1, 'sell' = 2),
    event_time_ns    Int64 CODEC(Delta(8), ZSTD(3)),
    ingest_time_ns   Int64 CODEC(Delta(8), ZSTD(3)),
    sequence_number  UInt64 CODEC(Delta(8), ZSTD(3)),
    price_nanos      Int64 CODEC(Delta(8), ZSTD(3)),
    size_nanos       Int64 CODEC(ZSTD(3)),
    bid_price_nanos  Int64 CODEC(Delta(8), ZSTD(3)),
    bid_size_nanos   Int64 CODEC(ZSTD(3)),
    ask_price_nanos  Int64 CODEC(Delta(8), ZSTD(3)),
    ask_size_nanos   Int64 CODEC(ZSTD(3)),
    late_arrival     UInt8 CODEC(ZSTD(1)),
    shard_id         UInt16,
    event_date       Date MATERIALIZED toDate(intDiv(event_time_ns, 1000000000))
)
ENGINE = ReplacingMergeTree(ingest_time_ns)
PARTITION BY (toYYYYMMDD(event_date), shard_id)
ORDER BY (instrument_id, event_time_ns, sequence_number, source_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS tickdb.sequence_gaps
(
    source_id       LowCardinality(String),
    instrument_id   LowCardinality(String),
    gap_start_seq   UInt64,
    gap_end_seq     UInt64,
    detected_at_ns  Int64,
    resolved_at_ns  Int64,
    status          Enum8('open' = 1, 'resolved' = 2)
)
ENGINE = ReplacingMergeTree(detected_at_ns)
ORDER BY (source_id, instrument_id, gap_start_seq, gap_end_seq);
