-- ClickHouse DDL template for Options Market Data Platform (Schema v1.0.0)
-- Note: Local prototype runs against SQLite by default; ClickHouse execution is
-- documented as an optional analytical target and marked not run in offline local CI.

CREATE TABLE IF NOT EXISTS option_quotes_ch (
    snapshot_id String,
    option_id LowCardinality(String),
    quote_version UInt32,
    underlying_symbol LowCardinality(String),
    reported_strike Decimal(18, 8),
    event_time DateTime64(6, 'UTC'),
    ingestion_time DateTime64(6, 'UTC'),
    bid Decimal(18, 6),
    ask Decimal(18, 6),
    bid_size UInt32,
    ask_size UInt32,
    source_id LowCardinality(String),
    source_sequence UInt64,
    schema_version LowCardinality(String),
    validation_status LowCardinality(String),
    quality_flags Array(LowCardinality(String)),
    record_hash FixedString(64)
)
ENGINE = ReplacingMergeTree(quote_version)
PARTITION BY toYYYYMM(event_time)
ORDER BY (underlying_symbol, snapshot_id, option_id, quote_version);

CREATE TABLE IF NOT EXISTS derived_option_features_ch (
    feature_id FixedString(64),
    snapshot_id String,
    option_id LowCardinality(String),
    underlying_symbol LowCardinality(String),
    valuation_time DateTime64(6, 'UTC'),
    mid_price Nullable(Float64),
    spread Nullable(Float64),
    time_to_expiry_years Nullable(Float64),
    underlying_mid Nullable(Float64),
    forward_price Nullable(Float64),
    spot_moneyness Nullable(Float64),
    log_forward_moneyness Nullable(Float64),
    implied_vol Nullable(Float64),
    delta Nullable(Float64),
    vega Nullable(Float64),
    iv_status LowCardinality(String),
    provenance_json String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(valuation_time)
ORDER BY (underlying_symbol, valuation_time, snapshot_id, option_id);
