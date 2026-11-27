# ADR-001: Bi-Temporal Causality and Shared Causal Transform Parity

- **Status**: Accepted
- **Date**: 2026-11-27
- **Context**: Financial ML pipelines frequently suffer from two critical failure modes: (1) lookahead leakage caused by joining on event time without modeling knowledge availability time, or forward-filling future values; and (2) training/serving skew caused by maintaining separate batch (SQL/Pandas) and online (streaming/Python) feature implementations.

## Decision

1. **Mandatory Bi-Temporal Columns**: Every raw source row and materialized feature record MUST carry timezone-aware UTC `event_time` and `availability_time` with `availability_time >= event_time`.
2. **Strict Point-in-Time Barrier**: Offline as-of joins enforce BOTH `event_time <= as_of_time` AND `availability_time <= as_of_time`, selecting max `(event_time, availability_time, materialized_at)` and executing a post-join assertion that `leakage_violations == 0`.
3. **Single Shared Transform Kernel**: Both `OfflineFeatureStore.materialize(...)` and `OnlineFeatureStore.materialize_and_sync_from_raw(...)` call `compute_causal_feature_frame(...)` in `transforms.py`. Rolling and lag kernels additionally verify `rows[j]["availability_time"] <= current_availability_time` so late-arriving records cannot retroactively contaminate earlier rolling aggregates.
4. **Content-Addressed Version Pinning**: `FeatureDefinition.version_id` hashes transform parameters, source version, schema hash, unit, and null policy. Both offline Parquet reads and online cache reads verify `version_id` equality on every query.

## Consequences

- **Positive**: Exact floating-point parity (`max_abs_diff == 0.0`) on chronological replay; late-arriving data and future joins are structurally prevented from leaking; silent cache/schema drift is caught immediately.
- **Trade-offs**: Python-level per-entity causal kernel execution prioritizes exactness and auditability over vectorized multi-million-row throughput in the local prototype; future scaling can swap hot kernels to Rust/Polars native expressions while preserving the exact contract and parity test suite.
