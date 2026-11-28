# ADR-0001: Bi-Temporal Exact-Decimal Core with Pluggable Storage & Analytics Adapters

- **Status:** Accepted
- **Date:** 2026-11-28
- **Context:** Project 54 (`financial-corporate-actions-engine`) in `Trading-Data-Infrastructure`

## Context

`plan.md` proposes Python, Polars, `Decimal`, and PostgreSQL as suggested engineering targets while emphasizing that numerical thresholds and technology choices may be adjusted with documented rationale. Additionally, the local verification environment runs Python 3.9.6 without pre-installed third-party packages (`polars`, `psycopg`) or a running PostgreSQL daemon, and requires clean reproduction from a fresh checkout.

## Decision

1. **Pure-Python Exact-Decimal Domain Core (`src/corp_actions/domain/`):**
   - Implement all corporate-action resolution, factor computation, position/cash transitions, adjusted series generation, and total-return reconciliation strictly using Python's standard-library `decimal.Decimal` (`prec=28`, `ROUND_HALF_EVEN`) with zero I/O side effects.
   - Adopt the CRSP / S&P backward-adjustment convention:
     - Split price factor = `1 / (numerator / denominator)`; volume factor = `numerator / denominator`.
     - Dividend factor = `(P_ref - D_gross) / P_ref`, where `P_ref = prior_close / same_day_split_ratio`.
     - Total-return factor = `split_price_factor * dividend_factor`.
   - Enforce strict bi-temporal PIT filtering (`availability_time <= as_of_time` AND `ex_date <= min(end_date, as_of_time.date())`) so historical snapshots never leak future-known or un-exed events.

2. **Durable Storage Protocol with Embedded SQLite Default + Optional PostgreSQL Adapter:**
   - Define `CorporateActionStore` (`src/corp_actions/storage/base.py`).
   - Provide `SQLiteStore` (stdlib `sqlite3`, exact `TEXT` Decimal storage, UTC ISO-8601 indexed bi-temporal tables) as the zero-dependency default for reproducible local execution, CI, and restart-safe persistence.
   - Provide `PostgresStore` (`src/corp_actions/storage/postgres_store.py`) and complete PostgreSQL DDL (`sql/postgres_schema.sql` using `NUMERIC` and `TIMESTAMPTZ`) behind the optional `.[postgres]` extra, degrading cleanly with `DependencyUnavailableError` when `psycopg` is unavailable.

3. **Optional Polars Adapter:**
   - Provide `src/corp_actions/adapters/polars_adapter.py` behind the optional `.[polars]` extra to export separated raw/adjusted series and position ledgers to Polars DataFrames when installed, without blocking stdlib-only environments.

## Consequences

- **Positive:** Clean checkout runs all 21 automated tests, the end-to-end demo, and 10,000-bar synthetic benchmarks with zero external installation friction and exact `Decimal` precision (max reconciliation discrepancy `4.92e-8` across 10 four-year synthetic series).
- **Trade-off:** Live PostgreSQL integration and Polars DataFrame serialization are not executed in offline stdlib-only environments and are explicitly marked **Not Run (optional dependency unavailable)** in `reports/acceptance.md` until tested in a CI/container environment with Postgres and Polars installed.
