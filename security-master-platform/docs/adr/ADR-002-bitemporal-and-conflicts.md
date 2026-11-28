# ADR-002: Snodgrass Bitemporal Slicing and Strict Conflict Quarantine

- **Status:** Accepted
- **Date:** 2026-11-28
- **Context:** Financial reference data routinely experiences ticker reuse across eras, dual listings across exchanges/currencies, late vendor corrections, and cross-vendor identifier disagreements. Silently picking a "winning" record or overwriting history introduces survivorship bias, look-ahead bias, and silent mis-pricing in downstream trading and research pipelines.

## Decision

1. **Orthogonal Valid-Time and System-Time Axes:** Model every version with half-open UTC intervals `[valid_from, valid_to)` and `[observed_at, superseded_at)`. Never mutate or delete historical rows; perform valid-time interval subtraction (Snodgrass slicing) on partial-window superseding updates so every `(as_of_valid, as_of_observed)` query remains exact and reproducible.
2. **Quarantine-All Conflict Policy:**
   - Only explicit same-source revisions (`source_system` match or `supersedes_event_id`) and audited `REVIEWER_WORKFLOW` events may supersede existing active versions.
   - Any cross-source attribute/identifier disagreement or overlapping symbol/ID collision across distinct canonical entities is **quarantined** into an `OPEN` `ConflictTicket`.
   - Lookups touching an `OPEN` conflict raise `CONFLICT_BLOCKED` (`409`). Bare-ticker queries matching multiple listings raise `AMBIGUOUS_MAPPING` (`409`).
3. **Explicit Pre-Observation Boundary:** When valid records exist for `as_of_valid` but were only observed after `as_of_observed`, raise `PRE_OBSERVATION_LOOKUP` (`422`) with `earliest_observed_at` instead of silently returning empty/not-found.

## Consequences

- **Positive:** Satisfies strict point-in-time auditability, guarantees no ambiguous mapping is silently resolved, and retains 100% source + reviewer lineage.
- **Negative:** Requires human operator review via the Quality Dashboard or API whenever vendors disagree, increasing operational touchpoints in exchange for data integrity.
