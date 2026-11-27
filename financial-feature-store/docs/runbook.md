# Operational Runbook — Financial Feature Store

## 1. Health & Operational Metrics

`OnlineFeatureStore.metrics()` exposes:
- `requests_total`, `feature_lookups_total`, `hits`, `misses`
- `stale_count` (stale TTL breaches)
- `version_mismatch_count` (cache/registry drift alerts)
- `leakage_blocked_count` (future-timestamp serving blocks)
- `latency_ms_p50`, `latency_ms_p95`, `latency_ms_p99`
- `backend_health` (`backend`, `status`, `entries`)

## 2. Failure Modes & Operator Response

| Incident | Signal / Error Code | Safe Fallback | Operator Response |
|---|---|---|---|
| **Data Quality Gate Failure** | `QUALITY_GATE_FAILED` during `registry.promote(...)` | Previous `ACTIVE` version remains active; bad version never reaches online serving | Inspect `QualityReport.checks`, verify upstream source data, fix source/rules, re-materialize, re-evaluate before promotion |
| **Stale Online Feature** | `STALE_FEATURE` or rising `stale_count` | Downstream model receives explicit rejection (`REJECT`) or `None` (`FALLBACK_NULL`) — never silent stale values | Check upstream feed lag and online sync job health; trigger catch-up sync; verify `availability_time` delay |
| **Version Mismatch** | `VERSION_MISMATCH` / `version_mismatch_count > 0` | Serving request fails closed | Compare registry `version_id` against online backend and Parquet manifests; flush stale online keys via `backend.delete_feature(...)` and re-sync from verified offline version |
| **Training/Serving Skew** | `ParityReport.passed == False` | Block model promotion to paper/sandbox | Inspect `ParityReport.diffs` (`value_skew`, `null_mismatch`, `version_id_mismatch`); verify both paths share identical `version_id` and raw source inputs |
| **Corrupt Offline Partition** | `VERSION_MISMATCH` (checksum mismatch) or `ADAPTER_ERROR` | Read aborts; corrupted partition is never joined | Quarantine corrupted `.parquet`, re-run idempotent `offline.backfill(...)` from verified source fixture/lake snapshot |
| **Late-Arriving Corrections** | Elevated `late_arrival_seconds` | Historical point-in-time queries remain unchanged; new queries after `availability_time` incorporate correction | Run `offline.backfill(...)` over affected window, verify idempotency and manifests, sync latest to online store |

## 3. Rollback Procedure

1. Identify the last known-good SemVer in `FeatureRegistry.list_versions(feature_name)`.
2. Re-promote the known-good version with its stored `QualityReport` (`registry.get_promotion_evidence(name, prev_version)`).
3. Clear the faulty version from the online backend via `backend.delete_feature(name, bad_version)` and re-sync the known-good offline partition.
4. Re-run `ParityVerifier.verify_replay_parity(...)` to confirm zero skew before resuming downstream consumers.
