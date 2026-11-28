# Data Contracts — Financial Corporate Actions Engine

**Canonical Schema Version:** `1.0.0` (`corp_actions.contracts.SCHEMA_VERSION`)

All monetary values, quantities, FX rates, and adjustment factors are serialized as base-10 decimal strings in JSON and parsed into `decimal.Decimal` (never IEEE-754 `float`). All timestamps must be timezone-aware ISO-8601 strings normalized to UTC (`Z`).

## 1. Input Contracts

### 1.1 `RawPriceBar` (`raw_prices.json`)

| Field | Type / Unit | Required | Constraints |
|---|---|---|---|
| `security_id` | `str` (immutable security identifier) | Yes | Non-empty |
| `symbol` | `str` (point-in-time ticker) | Yes | Non-empty |
| `trade_date` | ISO `YYYY-MM-DD` | Yes | Exchange trade date |
| `open`, `high`, `low`, `close` | `Decimal` (quote currency / share) | Yes | Strictly `> 0`; `high >= max(open, close) >= min(open, close) >= low` |
| `volume` | `Decimal` (shares) | Yes | `>= 0` |
| `currency` | `str` (ISO-4217) | Yes | 3 uppercase alpha characters (e.g. `USD`) |
| `event_time` | ISO-8601 UTC timestamp | Yes | Timezone-aware (market close) |
| `ingestion_time` | ISO-8601 UTC timestamp | Yes | Timezone-aware (storage write time) |
| `source` | `str` | Yes | Feed lineage identifier |
| `schema_version` | `str` | Yes | Must equal `"1.0.0"` |

### 1.2 `PositionSnapshot` (`positions.json`)

| Field | Type / Unit | Required | Constraints |
|---|---|---|---|
| `account_id` | `str` | Yes | Non-empty |
| `security_id` | `str` | Yes | Non-empty |
| `symbol` | `str` | Yes | Non-empty |
| `as_of_date` | ISO `YYYY-MM-DD` | Yes | Starting ledger date |
| `quantity` | `Decimal` (shares) | Yes | `>= 0` (short positions out of scope) |
| `cost_basis_per_share` | `Decimal` (currency / share) | Yes | `>= 0` |
| `currency` | `str` (ISO-4217) | Yes | 3 uppercase alpha characters |
| `cash_balance` | `Decimal` (currency) | Yes | Starting settled cash balance |
| `event_time`, `ingestion_time` | ISO-8601 UTC | Yes | Timezone-aware |
| `source`, `schema_version` | `str` | Yes | `schema_version == "1.0.0"` |

### 1.3 `FxRate` (`fx_rates.json`)

| Field | Type / Unit | Required | Constraints |
|---|---|---|---|
| `base_currency`, `quote_currency` | `str` (ISO-4217) | Yes | 1 unit `base_currency` = `rate` units `quote_currency` |
| `rate_date` | ISO `YYYY-MM-DD` | Yes | Spot fixing date |
| `rate` | `Decimal` | Yes | Strictly `> 0` |
| `event_time`, `ingestion_time` | ISO-8601 UTC | Yes | Timezone-aware |
| `source`, `schema_version` | `str` | Yes | `schema_version == "1.0.0"` |

### 1.4 `CorporateActionEvent` (`corporate_actions.json`)

| Field | Type / Unit | Required | Constraints |
|---|---|---|---|
| `action_id` | `str` | Yes | Stable ID shared across revisions of the same corporate action |
| `version` | `int` | Yes | `>= 1`; `NEW` requires `1`; `CORRECTION`/`CANCELLATION` require `>= 2` |
| `revision_type` | Enum | Yes | `NEW`, `CORRECTION`, `CANCELLATION` |
| `action_type` | Enum | Yes | Supported: `SPLIT`, `REVERSE_SPLIT`, `CASH_DIVIDEND`, `SPECIAL_DIVIDEND`, `SYMBOL_CHANGE`. Quarantined/Unsupported: `MERGER`, `SPINOFF`. |
| `security_id`, `symbol` | `str` | Yes | Non-empty |
| `announcement_date`, `ex_date` | ISO `YYYY-MM-DD` | Yes | Required for all events |
| `record_date`, `pay_date` | ISO `YYYY-MM-DD` | Conditional | `pay_date` required for dividends; `pay_date >= ex_date` and `pay_date >= record_date` |
| `ratio_numerator`, `ratio_denominator` | `Decimal` | Conditional | Required for splits (`> 0`). `SPLIT` requires `num/den > 1`; `REVERSE_SPLIT` requires `num/den < 1`. |
| `cash_amount` | `Decimal` (currency / share) | Conditional | Required (`> 0`) for `CASH_DIVIDEND`, `SPECIAL_DIVIDEND` |
| `currency` | `str` (ISO-4217) | Conditional | Required for dividends |
| `tax_withholding_rate` | `Decimal` (fraction) | No (default `0`) | In `[0, 1)` |
| `new_symbol` | `str` | Conditional | Required for `SYMBOL_CHANGE`; must differ from `symbol` |
| `dividend_basis` | `str` | No | `"post_split"` (default) or `"pre_split"` for same-day split+dividend |
| `event_time`, `ingestion_time`, `availability_time` | ISO-8601 UTC | Yes | Timezone-aware; PIT visibility filters strictly on `availability_time <= as_of_time` |
| `source`, `lineage_id`, `schema_version` | `str` | Yes | Full vendor/batch lineage; `schema_version == "1.0.0"` |

## 2. Output Contracts

- **`SnapshotResult`**: Immutable PIT output containing:
  - `run_id` (`pit-<security_id>-<digest[:12]>`) and deterministic SHA-256 `output_digest`
  - `raw_series`: Tuple of untouched `RawPriceBar`s
  - `adjusted_series`: Tuple of `AdjustedPriceBar`s clearly separating `raw_*`, `split_adj_*`, and `tr_adj_*` columns plus active symbol and cumulative factors
  - `factor_schedule`: Tuple of `AdjustmentFactorPoint`s per `trade_date`
  - `position_ledger`: Tuple of `PositionLedgerEntry` records tracking shares, cost basis per share, total cost basis, daily gross/tax/net dividend accrual, `pending_receivable_cash`, and `settled_cash_balance`
  - `effective_actions` & `quarantined_actions` (`QuarantineRecord`)
  - `reconciliation`: `ReconciliationResult` (TR series return, portfolio gross return, discrepancy, tolerance, completeness ratio, freshness lag, SLA breach count)
- **`ReplayDiff`**: Deterministic comparison between `from_as_of` and `to_as_of`, reporting `added_actions`, `corrected_actions`, `cancelled_actions`, `quarantined_actions`, `changed_dates`, per-date `price_deltas`, `cash_delta_end`, `quantity_delta_end`, and both digests.

## 3. Structured Error Codes

| Error Class | Code | Trigger |
|---|---|---|
| `ConfigurationError` | `CONFIGURATION_ERROR` | Missing/malformed config file, unknown keys, out-of-range precision/tolerance |
| `SchemaVersionError` | `SCHEMA_VERSION_ERROR` | Record `schema_version != "1.0.0"` |
| `ValidationError` | `VALIDATION_ERROR` | OHLC violation, naive timestamp, negative ratio, `pay_date < ex_date`, dividend `>=` prior close |
| `UnsupportedActionError` | `UNSUPPORTED_ACTION` | `MERGER` / `SPINOFF` when `strict_unsupported_actions=True` |
| `SameDayConflictError` | `SAME_DAY_CONFLICT` | Multiple unlinked splits or symbol changes on same `(security_id, ex_date)` |
| `MissingMarketDataError` | `MISSING_MARKET_DATA` | Dividend ex-date lacks a prior close bar |
| `MissingFxRateError` | `MISSING_FX_RATE` | Foreign-currency dividend lacks spot/inverse FX rate on or before conversion date |
| `StorageError` | `STORAGE_ERROR` | SQLite/PostgreSQL connection or DDL failure |
| `DependencyUnavailableError` | `DEPENDENCY_UNAVAILABLE` | Optional `polars` or `psycopg` adapter invoked without installing extra |

## 4. Fixture Coverage

- **Valid fixtures**: `data/fixtures/raw_prices.json`, `positions.json`, `fx_rates.json`, `corporate_actions.json` (3 synthetic securities covering split, reverse split, regular dividend, special dividend, FX dividend with withholding, symbol change, late correction, cancellation, same-day split+dividend, and unsupported merger).
- **Expected oracle**: `data/fixtures/expected_oracle.json` (hand-calculated exact `Decimal` reference outputs).
- **Invalid fixtures**: `data/fixtures/invalid_actions.json` (5 structured negative test cases).
