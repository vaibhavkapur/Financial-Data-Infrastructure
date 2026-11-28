# Data Contracts — Options Market Data Platform (Schema v1.0.0)

## 1. Canonical Option Identifier

Every option contract is identified by a deterministic canonical string:

```text
OPT:<UNDERLYING>:<YYYYMMDD>:<C|P>:<STRIKE_4DP>
```

- Example: `OPT:SYNTH:20270619:C:100.0000`
- `<UNDERLYING>`: 1–12 uppercase alphanumeric characters (`[A-Z0-9._-]`).
- `<YYYYMMDD>`: ISO expiry date without separators.
- `<C|P>`: `C` for Call, `P` for Put.
- `<STRIKE_4DP>`: Strictly positive decimal strike formatted to 4 decimal places (`Decimal`, `ROUND_HALF_UP`).
- **OCC Alias:** When `len(underlying) <= 6` and strike has at most 3 significant decimal places, the standard 21-character OCC symbol (e.g., `SYNTH 270619C00100000`) is automatically derived and parsed via `CanonicalOptionID.from_occ()`.

## 2. Timestamp Semantics & Units

- All timestamps MUST be ISO-8601 strings with explicit timezone (`Z` or `+00:00`) and are normalized to UTC (`datetime` with `timezone.utc`). Naive timestamps raise `ContractValidationError`.
- **`event_time`**: Venue/source observation timestamp of the quote.
- **`ingestion_time`**: Platform availability timestamp when the quote entered OMDP.
- **`snapshot_time`**: Logical point-in-time valuation timestamp of a `ChainSnapshot`.
- **Units:**
  - Prices & strikes: exact `Decimal` in contract currency (default `USD`).
  - Rates (`risk_free_rate`, `dividend_yield`): continuously compounded annualized decimals (e.g., `0.05` = 5% p.a.).
  - Time-to-expiry (`time_to_expiry_years`): ACT/365F (`seconds / (365 * 86400)`).
  - Implied volatility (`implied_vol`): annualized Black-Scholes volatility decimal (e.g., `0.20` = 20% p.a.).
  - Vega: $\partial P / \partial \sigma$ per 1.00 unit of volatility (divide by 100 for per-1% vol).

## 3. Input Contracts

### 3.1 `ContractMetadata`
| Field | Type | Constraints |
|---|---|---|
| `option_id` | `str` | Matches canonical ID derived from fields |
| `contract_version` | `int` | `>= 1` |
| `underlying_symbol` | `str` | Uppercase canonical symbol |
| `expiry_date` | `YYYY-MM-DD` | Valid calendar date |
| `option_right` | `"C" \| "P"` | Call or Put |
| `strike` | `Decimal` | `> 0`, decimal places `<= strike_scale_decimals` |
| `strike_scale_decimals` | `int` | `0..4` |
| `contract_multiplier` | `int` | `> 0` (typically `100`) |
| `exercise_style` | `str` | `"EUROPEAN"` supported; others flagged `UNSUPPORTED_EXERCISE_STYLE` |
| `currency` | `str` | 3-char ISO currency code (e.g. `USD`) |
| `source_id`, `schema_version`, `ingestion_time` | lineage | Required; `schema_version` must be in `supported_schema_versions` |

### 3.2 `ExpiryCalendarEntry`
- `calendar_id`, `calendar_version (>= 1)`, `expiry_date`, `expiry_timestamp_utc` (UTC close timestamp), `is_trading_day`, `settlement_type` (`AM`/`PM`), `source_id`, `schema_version`.

### 3.3 `RateCurvePoint`
- `curve_id`, `curve_version (>= 1)`, `as_of_time` (UTC), `tenor_days (> 0)`, `risk_free_rate`, `dividend_yield`, `source_id`, `schema_version`.

### 3.4 `UnderlyingQuote`
- `underlying_symbol`, `quote_version (>= 1)`, `event_time`, `ingestion_time`, `bid`, `ask`, `last_price` (optional), `source_id`, `source_sequence`, `schema_version`.

### 3.5 `OptionQuote`
- `snapshot_id`, `option_id`, `quote_version (>= 1)`, `underlying_symbol`, `reported_strike` (`Decimal`), `event_time`, `ingestion_time`, `bid`, `ask`, `bid_size`, `ask_size`, `source_id`, `source_sequence`, `schema_version`.

## 4. Validation Errors & Data Quality Flags

### Hard Structural / Configuration Errors (Exceptions)
- `ConfigurationError`: Missing config file, missing required keys, invalid bounds, or unsupported backend.
- `ContractValidationError`: Malformed canonical ID, naive timestamp, non-finite/negative strike, or mismatched contract fields.
- `UnsupportedInputError`: Unrecognized `schema_version`.
- `SnapshotConsistencyError`:
  - `EMPTY_SNAPSHOT`: Zero quotes in snapshot batch.
  - `SNAPSHOT_ID_MISMATCH`: Quote `snapshot_id` disagrees with batch `snapshot_id`.
  - `MIXED_UNDERLYING_SNAPSHOT`: Quote `underlying_symbol` disagrees with snapshot `underlying_symbol`.
  - `OPTION_ID_UNDERLYING_MISMATCH`: Embedded symbol in `option_id` disagrees with snapshot underlying.
  - `DUPLICATE_QUOTE_VERSION`: Duplicate `(option_id, quote_version)` inside the same snapshot.

### Per-Quote Quality Flags (`DataQualityFlag`)
| Flag | Trigger Condition | Default Handling |
|---|---|---|
| `EXPIRED_OPTION` | `event_time >= expiry_ts` or `snapshot_time >= expiry_ts` | `REJECTED`, `EXCLUDED_EXPIRED` |
| `STRIKE_DECIMAL_MISMATCH` | `reported_strike <= 0`, decimal places `> strike_scale_decimals`, or `reported_strike != contract.strike` | `REJECTED`, `EXCLUDED_INVALID_QUOTE` |
| `CROSSED_QUOTE` | `bid > ask` | `REJECTED`, `EXCLUDED_INVALID_QUOTE` |
| `LOCKED_QUOTE` | `bid == ask > 0` | `FLAGGED` (usable if `allow_locked_quotes=true`) |
| `INVALID_PRICE` | `bid < 0` or `ask <= 0` | `REJECTED`, `EXCLUDED_INVALID_QUOTE` |
| `INVALID_SIZE` | `bid_size < 0` or `ask_size < 0` | `REJECTED`, `EXCLUDED_INVALID_QUOTE` |
| `FUTURE_DATED_QUOTE` | `event_time > snapshot_time + max_future_clock_skew_seconds` | `REJECTED`, `EXCLUDED_INVALID_QUOTE` |
| `STALE_QUOTE` | `snapshot_time - event_time > max_quote_staleness_seconds` | `REJECTED` when `exclude_stale_quotes_from_iv=true` (`EXCLUDED_STALE_QUOTE`) |
| `DELAYED_QUOTE` | `ingestion_time - event_time > max_ingestion_delay_seconds` | `FLAGGED` (retained with freshness metrics if not stale) |
| `MISSING_CONTRACT` | `option_id` absent from contract registry | `REJECTED`, `EXCLUDED_INVALID_QUOTE` |
| `UNSUPPORTED_EXERCISE_STYLE` | `exercise_style` not in `supported_exercise_styles` | `REJECTED`, `EXCLUDED_UNSUPPORTED_STYLE` |
| `MISSING_UNDERLYING` / `STALE_UNDERLYING` / `CROSSED_UNDERLYING` | No valid timely underlying quote | `EXCLUDED_MISSING_UNDERLYING` |
| `MISSING_RATE_CURVE` | No rate curve available as of `snapshot_time` | `EXCLUDED_MISSING_RATE` |

## 5. Output Contracts

- **`ChainSnapshot`**: Canonically ordered `ValidatedOptionQuote` list with `snapshot_hash` (SHA-256), `config_hash`, and summary counts.
- **`DerivedOptionFeature`**: Deterministic `feature_id` (SHA-256), `mid_price`, `spread`, `time_to_expiry_years`, `underlying_mid`, `forward_price`, `spot_moneyness` ($S/K$), `log_forward_moneyness` ($\ln(K/F)$), `implied_vol`, `delta`, `vega`, `iv_status`, `quality_flags`, and `FeatureProvenance` linking every input version and SHA-256 hash.
- **`QualityReport`**: `completeness_ratio`, `iv_coverage_ratio`, `max_quote_staleness_seconds`, `max_ingestion_delay_seconds`, `flag_counts`, `iv_status_counts`, and `snapshot_hash`.

## 6. Fixture Inventory (`data/fixtures/`)

- `reference_chain.json` — Valid synthetic chain with analytically calculated BSM prices and independent IV oracle.
- `invalid_and_edge_cases.json` — Expired option, strike scaling mismatch, crossed quote, locked quote, American exercise style, and arbitrage bound violation.
- `integration_scenarios.json` — Missing underlying (`NOUND`), delayed quote + multi-version revision (`DELAY` Call v1→v2), and stale quote (`DELAY` Put).
- `consistency_scenarios.json` — Negative snapshots testing `MIXED_UNDERLYING_SNAPSHOT` and `DUPLICATE_QUOTE_VERSION`.
