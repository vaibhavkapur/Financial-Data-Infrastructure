-- Bi-temporal PostgreSQL DDL for Financial Corporate Actions Engine (schema v1.0.0)
-- Uses exact NUMERIC types for prices, ratios, cash, and quantities; TIMESTAMPTZ for UTC bi-temporality.

CREATE TABLE IF NOT EXISTS raw_prices (
    security_id    TEXT           NOT NULL,
    symbol         TEXT           NOT NULL,
    trade_date     DATE           NOT NULL,
    open           NUMERIC(28,10) NOT NULL CHECK (open > 0),
    high           NUMERIC(28,10) NOT NULL CHECK (high > 0),
    low            NUMERIC(28,10) NOT NULL CHECK (low > 0),
    close          NUMERIC(28,10) NOT NULL CHECK (close > 0),
    volume         NUMERIC(28,6)  NOT NULL CHECK (volume >= 0),
    currency       CHAR(3)        NOT NULL,
    event_time     TIMESTAMPTZ    NOT NULL,
    ingestion_time TIMESTAMPTZ    NOT NULL,
    source         TEXT           NOT NULL,
    schema_version TEXT           NOT NULL,
    PRIMARY KEY (security_id, trade_date, ingestion_time, source)
);

CREATE INDEX IF NOT EXISTS idx_raw_prices_pit
    ON raw_prices (security_id, trade_date, ingestion_time DESC);

CREATE TABLE IF NOT EXISTS position_snapshots (
    account_id           TEXT           NOT NULL,
    security_id          TEXT           NOT NULL,
    symbol               TEXT           NOT NULL,
    as_of_date           DATE           NOT NULL,
    quantity             NUMERIC(28,6)  NOT NULL CHECK (quantity >= 0),
    cost_basis_per_share NUMERIC(28,10) NOT NULL CHECK (cost_basis_per_share >= 0),
    currency             CHAR(3)        NOT NULL,
    cash_balance         NUMERIC(28,6)  NOT NULL,
    event_time           TIMESTAMPTZ    NOT NULL,
    ingestion_time       TIMESTAMPTZ    NOT NULL,
    source               TEXT           NOT NULL,
    schema_version       TEXT           NOT NULL,
    PRIMARY KEY (account_id, security_id, as_of_date, ingestion_time, source)
);

CREATE TABLE IF NOT EXISTS fx_rates (
    base_currency  CHAR(3)        NOT NULL,
    quote_currency CHAR(3)        NOT NULL,
    rate_date      DATE           NOT NULL,
    rate           NUMERIC(28,12) NOT NULL CHECK (rate > 0),
    event_time     TIMESTAMPTZ    NOT NULL,
    ingestion_time TIMESTAMPTZ    NOT NULL,
    source         TEXT           NOT NULL,
    schema_version TEXT           NOT NULL,
    PRIMARY KEY (base_currency, quote_currency, rate_date, ingestion_time, source)
);

CREATE TABLE IF NOT EXISTS corporate_actions (
    action_id            TEXT           NOT NULL,
    version              INTEGER        NOT NULL CHECK (version >= 1),
    revision_type        TEXT           NOT NULL CHECK (revision_type IN ('NEW', 'CORRECTION', 'CANCELLATION')),
    action_type          TEXT           NOT NULL CHECK (action_type IN ('SPLIT', 'REVERSE_SPLIT', 'CASH_DIVIDEND', 'SPECIAL_DIVIDEND', 'SYMBOL_CHANGE', 'MERGER', 'SPINOFF')),
    security_id          TEXT           NOT NULL,
    symbol               TEXT           NOT NULL,
    announcement_date    DATE           NOT NULL,
    ex_date              DATE           NOT NULL,
    record_date          DATE,
    pay_date             DATE,
    ratio_numerator      NUMERIC(28,10),
    ratio_denominator    NUMERIC(28,10),
    cash_amount          NUMERIC(28,10),
    currency             CHAR(3),
    tax_withholding_rate NUMERIC(10,6)  NOT NULL DEFAULT 0,
    new_symbol           TEXT,
    dividend_basis       TEXT           CHECK (dividend_basis IS NULL OR dividend_basis IN ('post_split', 'pre_split')),
    event_time           TIMESTAMPTZ    NOT NULL,
    ingestion_time       TIMESTAMPTZ    NOT NULL,
    availability_time    TIMESTAMPTZ    NOT NULL,
    source               TEXT           NOT NULL,
    lineage_id           TEXT           NOT NULL,
    schema_version       TEXT           NOT NULL,
    metadata_json        JSONB          NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (action_id, version)
);

CREATE INDEX IF NOT EXISTS idx_corp_actions_pit
    ON corporate_actions (security_id, availability_time, ex_date);
