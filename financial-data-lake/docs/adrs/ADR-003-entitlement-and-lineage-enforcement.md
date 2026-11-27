# ADR-003: Fail-Closed Entitlement Enforcement & End-to-End Lineage

- **Status**: Accepted
- **Date**: 2026-11-27

## Context
Market and alternative data licenses prohibit unauthorized role access, expired dataset usage, and external redistribution. Regulators and internal governance also require full provenance from raw landing to research snapshots.

## Decision
- Every ingestion, query, snapshot creation, snapshot query, recovery, and replay operation requires an explicit `AccessContext(principal, role, purpose, requested_at)`.
- `CatalogRegistry.authorize()` fails closed if a dataset lacks an entitlement, the license window is inactive, the role or purpose is not permitted, or redistribution is attempted.
- Every `ALLOW` and `DENY` decision is durably logged to `audit_log`.
- Every transition (`raw -> normalized -> curated`) writes a `LineageEdge` with source/target paths and SHA-256 checksums, enabling automated forensic tracing and recovery.

## Consequences
- Prevents accidental lookahead or unlicensed dataset leakage across research workflows.
- Provides a reviewable audit trail for security and licensing sign-off.
