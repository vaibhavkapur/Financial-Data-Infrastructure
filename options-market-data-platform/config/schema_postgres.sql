-- PostgreSQL DDL template for Options Market Data Platform (Schema v1.0.0)
-- Note: Local prototype runs against SQLite by default; PostgreSQL execution is
-- documented as an optional target backend and marked not run in offline local CI.

CREATE TABLE IF NOT EXISTS contract_metadata (
    option_id TEXT NOT NULL,
    contract_version INTEGER NOT NULL,
    occ_symbol TEXT,
    underlying_symbol TEXT NOT NULL,
    expiry_date DATE NOT NULL,
    option_right CHAR(1) NOT NULL CHECK (option_right IN ('C', 'P')),
    strike NUMERIC(18, 6) NOT NULL CHECK (strike > 0),
    strike_scale_decimals INTEGER NOT NULL CHECK (strike_scale_decimals >= 0),
    contract_multiplier INTEGER NOT NULL CHECK (contract_multiplier > 0),
    exercise_style TEXT NOT NULL,
    currency CHAR(3) NOT NULL,
    source_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL,
    record_hash CHAR(64) NOT NULL,
    PRIMARY KEY (option_id, contract_version)
);

CREATE TABLE IF NOT EXISTS expiry_calendars (
    calendar_id TEXT NOT NULL,
    calendar_version INTEGER NOT NULL,
    expiry_date DATE NOT NULL,
    expiry_timestamp_utc TIMESTAMPTZ NOT NULL,
    is_trading_day BOOLEAN NOT NULL,
    settlement_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    record_hash CHAR(64) NOT NULL,
    PRIMARY KEY (calendar_id, calendar_version, expiry_date)
);

CREATE TABLE IF NOT EXISTS rate_curves (
    curve_id TEXT NOT NULL,
    curve_version INTEGER NOT NULL,
    as_of_time TIMESTAMPTZ NOT NULL,
    tenor_days INTEGER NOT NULL CHECK (tenor_days > 0),
    risk_free_rate DOUBLE PRECISION NOT NULL,
    dividend_yield DOUBLE PRECISION NOT NULL,
    source_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    record_hash CHAR(64) NOT NULL,
    PRIMARY KEY (curve_id, curve_version, tenor_days)
);

CREATE TABLE IF NOT EXISTS underlying_quotes (
    underlying_symbol TEXT NOT NULL,
    quote_version INTEGER NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL,
    bid NUMERIC(18, 6) NOT NULL,
    ask NUMERIC(18, 6) NOT NULL,
    last_price NUMERIC(18, 6),
    source_id TEXT NOT NULL,
    source_sequence BIGINT NOT NULL,
    schema_version TEXT NOT NULL,
    record_hash CHAR(64) NOT NULL,
    PRIMARY KEY (underlying_symbol, event_time, quote_version)
);

CREATE TABLE IF NOT EXISTS chain_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    underlying_symbol TEXT NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL,
    source_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    config_hash CHAR(64) NOT NULL,
    snapshot_hash CHAR(64) NOT NULL,
    quote_count INTEGER NOT NULL,
    valid_quote_count INTEGER NOT NULL,
    rejected_quote_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS option_quotes (
    snapshot_id TEXT NOT NULL REFERENCES chain_snapshots(snapshot_id),
    option_id TEXT NOT NULL,
    quote_version INTEGER NOT NULL,
    underlying_symbol TEXT NOT NULL,
    reported_strike NUMERIC(18, 8) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL,
    bid NUMERIC(18, 6) NOT NULL,
    ask NUMERIC(18, 6) NOT NULL,
    bid_size INTEGER NOT NULL,
    ask_size INTEGER NOT NULL,
    source_id TEXT NOT NULL,
    source_sequence BIGINT NOT NULL,
    schema_version TEXT NOT NULL,
    validation_status TEXT NOT NULL,
    quality_flags JSONB NOT NULL,
    record_hash CHAR(64) NOT NULL,
    PRIMARY KEY (snapshot_id, option_id, quote_version)
);

CREATE TABLE IF NOT EXISTS derived_option_features (
    feature_id CHAR(64) PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES chain_snapshots(snapshot_id),
    option_id TEXT NOT NULL,
    valuation_time TIMESTAMPTZ NOT NULL,
    mid_price DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    time_to_expiry_years DOUBLE PRECISION,
    underlying_mid DOUBLE PRECISION,
    forward_price DOUBLE PRECISION,
    spot_moneyness DOUBLE PRECISION,
    log_forward_moneyness DOUBLE PRECISION,
    implied_vol DOUBLE PRECISION,
    delta DOUBLE PRECISION,
    vega DOUBLE PRECISION,
    iv_status TEXT NOT NULL,
    provenance JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chain_snapshots_symbol_time
    ON chain_snapshots (underlying_symbol, snapshot_time);

CREATE INDEX IF NOT EXISTS idx_derived_features_snapshot
    ON derived_option_features (snapshot_id, option_id);
