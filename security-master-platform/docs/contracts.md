# Security Master Platform — Data Contracts (`schema_version: 1.0.0`)

## 1. Canonical Identifiers & Namespaces

Never use a bare exchange ticker as a primary key.

| Identifier | Format | Scope | Mutability / Licensing |
|---|---|---|---|
| `instrument_id` | `SEC-<12 UPPERCASE HEX>` | Canonical underlying financial instrument | Immutable; deterministic hash of `(seed_tag, asset_class, issuer_key)` |
| `listing_id` | `LST-<12 UPPERCASE HEX>` | Canonical `(instrument_id, mic, currency)` venue listing | Immutable; deterministic hash of `(instrument_id, mic, currency)` |
| `OPENFIGI` / `COMPOSITE_FIGI` | `^BBG[A-Z0-9]{9}$` | Instrument / composite | Open license (enabled by default) |
| `ISIN` | ISO 6166 (`^[A-Z]{2}[A-Z0-9]{9}[0-9]$`) + **Luhn check digit** | Instrument | Validated strictly; enabled in synthetic prototype config |
| `LEI` | ISO 17442 (`^[A-Z0-9]{20}$`) | Legal entity / issuer | Open license |
| `MIC_TICKER` | `<ISO 10383 4-char MIC>:<TICKER>` | Exchange listing | Globally unique per valid-time interval |
| `TICKER` | `^[A-Z0-9.\-]{1,16}$` | Non-unique local symbol | Requires `mic` for disambiguation on dual-listed instruments |
| `CUSIP` / `SEDOL` | Vendor proprietary | Instrument / listing | **Restricted by default**; rejected with `LICENSING_VIOLATION` (`403`) unless explicitly added to `licensing.licensed_namespaces` |
| `VENDOR_INTERNAL` | Non-empty string | Source-scoped | Non-unique across sources |

## 2. Timestamp & Unit Semantics

- **Timezone:** All timestamps (`valid_from`, `valid_to`, `observed_at`, `superseded_at`, `as_of_valid`, `as_of_observed`) MUST be ISO-8601 strings with explicit timezone offset (`Z` or `+00:00`), normalized internally to UTC (`YYYY-MM-DDTHH:MM:SSZ`). Naive timestamps are rejected with `VALIDATION_ERROR`.
- **Half-open windows:** `[valid_from, valid_to)` and `[observed_at, superseded_at)` where `null` end means open-ended (`+infinity`). `valid_to <= valid_from` is rejected with `VALIDATION_ERROR`.
- **Currencies & Countries:** ISO 4217 3-letter uppercase (`USD`, `GBP`, ...) and ISO 3166-1 alpha-2 (`US`, `GB`, ...).
- **Corporate Action Ratios:** Exact decimal strings parsed via `decimal.Decimal` (must be strictly `> 0`; e.g., `"2.0"` for 2-for-1 split, `"0.5"` for 1-for-2 merger exchange).

## 3. Canonical Input Event Envelope (`schema_version = "1.0.0"`)

```json
{
  "event_type": "INSTRUMENT_UPSERT | LISTING_UPSERT | IDENTIFIER_MAP | CORPORATE_ACTION_LINK | REVIEW_CORRECTION",
  "schema_version": "1.0.0",
  "source_system": "EXCHANGE_OFFICIAL",
  "source_event_id": "EXCH-2024-0001",
  "valid_from": "2024-01-01T00:00:00Z",
  "valid_to": null,
  "observed_at": "2024-01-01T08:00:00Z",
  "supersedes_event_id": null,
  "payload": {}
}
```

### Supported Payload Contracts

- **`INSTRUMENT_UPSERT`**: `instrument_id`, `name`, `asset_class` (`EQUITY | ETF | FIXED_INCOME | ADR`), `issuer_name`, `country_of_risk`, `status` (`ACTIVE | INACTIVE | DELISTED | SUSPENDED`).
- **`LISTING_UPSERT`**: `listing_id`, `instrument_id`, `mic`, `ticker`, `currency`, `is_primary` (bool), `status`. Automatically synchronizes derived `MIC_TICKER` and `TICKER` identifier versions.
- **`IDENTIFIER_MAP`**: `instrument_id`, `listing_id` (nullable), `namespace`, `identifier_value`, `mic` (nullable).
- **`CORPORATE_ACTION_LINK`**: `action_type` (`TICKER_CHANGE | MERGER | SPINOFF | SPLIT | REVERSE_SPLIT`), `from_instrument_id`, `to_instrument_id`, `from_listing_id` (nullable), `to_listing_id` (nullable), `ratio` (positive decimal string), `metadata` (object).
- **`REVIEW_CORRECTION`**: `conflict_id`, `reviewer_id`, `action` (`ACCEPT_INCOMING | KEEP_EXISTING | CUSTOM_OVERRIDE`), `rationale`, optional `override_payload`.

## 4. Output Contracts

- **Ingest outcome (`POST /api/v1/events`):** `event_id`, `status` (`APPLIED | CORROBORATED | DUPLICATE_SKIPPED | QUARANTINED | RESOLVED`), `version_ids`, `conflict_id`, `message`.
- **As-of lookup (`GET /api/v1/lookup`):** `instrument`, `listing` (nullable for instrument-level multi-listing identifiers without unique primary), `matched_identifier`, `identifiers` (all active as-of), `corporate_actions` (all active as-of), `as_of_valid`, `as_of_observed`.
- **Quality metrics (`GET /api/v1/quality`):** `as_of_observed`, `total_events`, `active_instruments`, `active_listings`, `active_identifier_mappings`, `corporate_action_links`, `open_conflicts`, `resolved_conflicts`, `completeness_ratio`, `incomplete_instruments`, `lineage_coverage_ratio`, `max_observation_lag_seconds`, `stale_sources`.

## 5. Structured Error Contract

All domain and operational errors return a machine-readable JSON envelope (never silent success):

```json
{
  "error": {
    "code": "PRE_OBSERVATION_LOOKUP",
    "message": "Historical lookup ... precedes earliest observation time ...",
    "details": { "earliest_observed_at": "2020-01-01T08:00:00Z" }
  }
}
```

| Error Code | HTTP Status | Trigger |
|---|---|---|
| `VALIDATION_ERROR` | `400` | Missing field, malformed ID, failed ISIN Luhn check, naive timestamp, `valid_to <= valid_from`, mutated duplicate event |
| `LICENSING_VIOLATION` | `403` | Ingestion of restricted/unlicensed namespace (`CUSIP`, `SEDOL`) |
| `NOT_FOUND` | `404` | No matching entity/mapping or route |
| `AMBIGUOUS_MAPPING` | `409` | Query matches multiple distinct canonical instruments/listings (e.g., bare dual-listed ticker) |
| `CONFLICT_BLOCKED` | `409` | Requested entity has an unresolved `OPEN` conflict ticket |
| `UNSUPPORTED_ENTITY` | `422` | Unsupported `asset_class`, `event_type`, `namespace`, or `action_type` |
| `PRE_OBSERVATION_LOOKUP` | `422` | Records exist for `as_of_valid`, but all have `observed_at > as_of_observed` |
| `CONFIG_ERROR` | `500` | Missing/malformed config file or unsupported backend/policy |

## 6. Reference Fixtures

| Fixture File | Purpose |
|---|---|
| `data/fixtures/reference_minimal.json` + `reference_minimal_expected.json` | M1 valid baseline + hand-calculated independent oracle |
| `data/fixtures/ticker_reuse_dual_listing.json` | M2 ticker reuse across eras + dual listing (`XNYS` USD & `XLON` GBP) |
| `data/fixtures/late_correction_conflicts.json` | M3 late same-source correction + cross-vendor conflicting ISIN quarantine |
| `data/fixtures/corporate_actions.json` | M3 ticker rename, merger, 2-for-1 split, and spinoff linkage |
| `data/fixtures/boundary_and_invalid.json` | M1/M4 invalid ISIN, unlicensed CUSIP, unsupported asset class/action, inverted window, naive timestamp |
