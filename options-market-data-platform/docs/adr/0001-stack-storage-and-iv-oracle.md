# ADR-0001: Runtime Stack, Storage Boundary, and Implied-Volatility Oracle

- **Status:** Accepted
- **Date:** 2026-11-28
- **Project:** Options Market Data Platform (`options-market-data-platform/`)

## Context

`plan.md` suggests Go or Python, ClickHouse or PostgreSQL, and Polars, while noting:

> Numerical thresholds and technology choices in this plan are proposed engineering targets, not measured results or guarantees. Adjust them only with documented rationale and retain baseline evidence.

The platform must run reproducibly from a clean local checkout without requiring live database servers, production credentials, or external network downloads, while preserving strict boundaries between:
1. Source adapters
2. Pure canonical domain contracts, validation, and pricing/IV derivation
3. Durable versioned storage
4. Query, quality-reporting, and replay interfaces

## Decision

1. **Core implementation in Python 3.9+ standard library (`src/omdp/`):**
   - Uses `decimal.Decimal` for exact strike, bid/ask, and tick-scale validation (eliminating binary floating-point rounding bugs in strike/decimal mismatch detection).
   - Uses `math.erf`, `math.exp`, `math.log`, and `math.sqrt` for closed-form Black-Scholes European option pricing and analytical vega/delta.
   - Zero required external runtime dependencies so unit, integration, replay, and benchmark suites execute deterministically offline.

2. **Durable Storage Boundary (`StorageRepository` protocol + SQLite default):**
   - `src/omdp/storage/sqlite_store.py` provides durable ACID local storage using Python's built-in `sqlite3` module.
   - PostgreSQL and ClickHouse DDL templates are maintained in `config/schema_postgres.sql` and `config/schema_clickhouse.sql`, respectively, with identical logical schemas and provenance fields. Because local Postgres/ClickHouse daemons are not provisioned in this offline environment, those backends are explicitly marked as **not run** in `reports/acceptance.md`.

3. **Optional Polars Adapter:**
   - `src/omdp/adapters/polars_adapter.py` exports chains and derived features to `polars.DataFrame` when `polars` is installed, and raises a clear `OptionalDependencyError` (or returns canonical record lists via helper) when unavailable.

4. **Implied Volatility Oracle and Tolerance:**
   - **Pricing model:** European Black-Scholes-Merton with continuous dividend yield $q$ and ACT/365F time-to-expiry $T$.
   - **Solver:** Safeguarded Newton-Raphson with automatic fallback to Brent's root-finding method over bracket $[10^{-6}, 5.0]$.
   - **Reference oracle:** Exact analytical round-trip fixtures (known $\sigma \to \text{BS price} \to \text{recovered } \hat{\sigma}$) plus classic textbook closed-form benchmarks ($S=100, K=100, T=1, r=0.05, q=0, \sigma=0.20$).
   - **Documented tolerance:** $|\hat{\sigma} - \sigma| \le 10^{-8}$ and $|P(\hat{\sigma}) - P_{\text{mid}}| \le 10^{-9}$ for valid non-boundary options.
   - **Unsupported styles:** Non-European exercise styles (e.g., `AMERICAN`, `BERMUDAN`) are explicitly flagged `UNSUPPORTED_EXERCISE_STYLE` rather than silently priced with a European formula.

## Consequences

- **Positive:** Clean checkout reproducibility in under 2 seconds; exact decimal precision for contract checks; full bitemporal provenance and deterministic replay verified against durable disk state.
- **Trade-offs:** Single-node SQLite is suitable for local research and educational datasets (thousands to low millions of quotes), not multi-node horizontal write throughput. PostgreSQL/ClickHouse adapters remain schema-documented and unbenchmarked until provisioned.
