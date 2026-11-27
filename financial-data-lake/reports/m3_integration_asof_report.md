# Milestone 3 Report — Integration and Validation

## Scope Delivered
- Added `corporate_actions` (splits and cash dividends), `fundamentals` (initial filings + restatements), and restricted `alt_sentiment` (#19 contract) fixtures and schemas.
- Implemented `QualityEngine` (`quality.py`) validating non-empty batches, required non-nulls, UTC bitemporal ordering, primary key uniqueness, OHLC/split/dividend domain invariants, and revision availability monotonicity.
- Implemented DuckDB-backed `AsOfQueryEngine` (`query.py`) enforcing `available_at <= as_of_ts`, selecting the latest revision available at `as_of_ts`, computing point-in-time split and cash-dividend price/volume adjustments without lookahead bias, and pinning/reproducing immutable checksum-verified `SnapshotManifest` artifacts.

## Verification Evidence
- Required scenarios verified in `tests/scenarios/test_required_scenarios.py`:
  - **Late correction**: `TCK-20240603-0003` returns `200.50` before `14:45Z`, `200.10` at/after `14:45Z`; both revisions retained. `MSFT` 2024-06-04 close returns `401.0` before `2024-06-05T12:00Z` and `400.0` at/after.
  - **Split/dividend**: `AAPL` 2-for-1 split adjusts pre-ex-date `2024-06-04` close `200.0 -> 100.0` and volume `1.2M -> 2.4M` only when queried `as_of >= 2024-06-04T22:00Z`; `MSFT` `$2.00` cash dividend adjusts `2024-06-05` close `400.0 -> 398.0`.
  - **Schema change**: Additive nullable `v2` (`venue_mic`) accepted and queried seamlessly across `v1` + `v2` partitions; breaking drop/narrow/tighten rejected with `SchemaEvolutionError`.
  - **Future revision**: `AAPL` 2024Q1 restatement (`available_at=2024-06-10T21:00Z`) excluded at `2024-06-07` and `2024-06-10T20:59:59Z`, included at `2024-06-10T21:00:00Z`.
  - **Unauthorized dataset**: Unauthorized role on `alt_sentiment`, redistribution attempt on `prices_eod`, and expired `legacy_expired_feed` denied with `EntitlementError` and recorded in `audit_log`.
