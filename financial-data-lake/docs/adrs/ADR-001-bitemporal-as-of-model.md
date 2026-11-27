# ADR-001: Bitemporal As-Of Data Model

- **Status**: Accepted
- **Date**: 2026-11-27

## Context
Financial research and backtesting suffer from silent lookahead bias when late corrections, fundamental restatements, or corporate actions overwrite historical records in-place.

## Decision
Every canonical record across all datasets carries:
1. `event_ts` (timezone-aware UTC): when the market event or fiscal period occurred.
2. `available_at` (timezone-aware UTC): when the record became known and legally usable, enforcing `available_at >= event_ts`.
3. `revision_number` (monotonically increasing integer >= 1): distinguishes initial observations from late corrections or restatements.

All point-in-time queries filter `available_at <= as_of_ts` and select the highest `revision_number` per entity key via window ranking in DuckDB. Corporate action adjustments only apply actions whose `available_at <= as_of_ts`.

## Consequences
- Zero destructive overwrites; full revision history is preserved for audit and replay.
- Slightly larger storage footprint due to retained revisions, mitigated by Parquet columnar compression.
