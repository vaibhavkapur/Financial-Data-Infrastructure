-- Reference PostgreSQL 16+ Bitemporal Schema for Security Master Platform
-- Status: Reference DDL (prototype runtime executes against SQLite with identical logical semantics;
-- PostgreSQL live execution is explicitly documented as not run in offline CI).

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS event_journal (
    seq_id            BIGSERIAL PRIMARY KEY,
    event_id          TEXT NOT NULL UNIQUE,
    event_type        TEXT NOT NULL,
    schema_version    TEXT NOT NULL,
    source_system     TEXT NOT NULL,
    source_event_id   TEXT NOT NULL,
    valid_from        TIMESTAMPTZ NOT NULL,
    valid_to          TIMESTAMPTZ,
    observed_at       TIMESTAMPTZ NOT NULL,
    payload_json      JSONB NOT NULL,
    payload_sha256    TEXT NOT NULL,
    supersedes_event_id TEXT,
    UNIQUE (source_system, source_event_id)
);

CREATE TABLE IF NOT EXISTS instrument_versions (
    version_id        TEXT PRIMARY KEY,
    instrument_id     TEXT NOT NULL,
    name              TEXT NOT NULL,
    asset_class       TEXT NOT NULL,
    issuer_name       TEXT NOT NULL,
    country_of_risk   TEXT NOT NULL,
    status            TEXT NOT NULL,
    valid_range       TSTZRANGE NOT NULL,
    system_range      TSTZRANGE NOT NULL,
    source_system     TEXT NOT NULL,
    source_event_id   TEXT NOT NULL,
    schema_version    TEXT NOT NULL,
    supersedes_version_id TEXT,
    EXCLUDE USING GIST (
        instrument_id WITH =,
        valid_range WITH &&,
        system_range WITH &&
    )
);

CREATE TABLE IF NOT EXISTS listing_versions (
    version_id        TEXT PRIMARY KEY,
    listing_id        TEXT NOT NULL,
    instrument_id     TEXT NOT NULL,
    mic               TEXT NOT NULL,
    ticker            TEXT NOT NULL,
    currency          TEXT NOT NULL,
    is_primary        BOOLEAN NOT NULL DEFAULT FALSE,
    status            TEXT NOT NULL,
    valid_range       TSTZRANGE NOT NULL,
    system_range      TSTZRANGE NOT NULL,
    source_system     TEXT NOT NULL,
    source_event_id   TEXT NOT NULL,
    schema_version    TEXT NOT NULL,
    supersedes_version_id TEXT,
    EXCLUDE USING GIST (
        listing_id WITH =,
        valid_range WITH &&,
        system_range WITH &&
    )
);

CREATE TABLE IF NOT EXISTS identifier_versions (
    version_id        TEXT PRIMARY KEY,
    instrument_id     TEXT NOT NULL,
    listing_id        TEXT,
    namespace         TEXT NOT NULL,
    identifier_value  TEXT NOT NULL,
    mic               TEXT,
    valid_range       TSTZRANGE NOT NULL,
    system_range      TSTZRANGE NOT NULL,
    source_system     TEXT NOT NULL,
    source_event_id   TEXT NOT NULL,
    schema_version    TEXT NOT NULL,
    supersedes_version_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_identifier_lookup
    ON identifier_versions USING GIST (namespace, identifier_value, valid_range, system_range);

CREATE TABLE IF NOT EXISTS corporate_action_links (
    link_id           TEXT PRIMARY KEY,
    action_type       TEXT NOT NULL,
    from_instrument_id TEXT NOT NULL,
    to_instrument_id  TEXT NOT NULL,
    from_listing_id   TEXT,
    to_listing_id     TEXT,
    ratio             NUMERIC(20, 8) NOT NULL,
    effective_at      TIMESTAMPTZ NOT NULL,
    observed_at       TIMESTAMPTZ NOT NULL,
    superseded_at     TIMESTAMPTZ,
    source_system     TEXT NOT NULL,
    source_event_id   TEXT NOT NULL,
    schema_version    TEXT NOT NULL,
    metadata_json     JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS conflict_tickets (
    conflict_id       TEXT PRIMARY KEY,
    conflict_type     TEXT NOT NULL,
    entity_key        TEXT NOT NULL,
    status            TEXT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL,
    resolved_at       TIMESTAMPTZ,
    existing_claim    JSONB NOT NULL,
    incoming_claim    JSONB NOT NULL,
    resolution_json   JSONB
);
