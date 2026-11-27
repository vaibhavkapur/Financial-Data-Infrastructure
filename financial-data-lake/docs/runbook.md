# Operational Runbook — Financial Data Lake

## 1. Local Setup & Verification

```bash
./scripts/setup.sh
.venv/bin/python scripts/validate.py
```

Or via CLI:

```bash
.venv/bin/fdl --lake-root .lake_data ingest-fixtures
.venv/bin/fdl --lake-root .lake_data verify
.venv/bin/fdl --lake-root .lake_data export-catalog
```

## 2. Routine Operations

### Ingesting a New Batch
1. Ensure dataset entitlement is registered in `config/entitlements.json` and not expired.
2. Verify source manifest checksum (`FixtureLoader.load_verified`) before ingesting.
3. Run `FinancialDataLake.ingest.ingest_fixture(...)` or `ingest_records(...)` with a `data_engineer` `AccessContext(purpose="ingest")`.
4. Confirm `QualityReport.passed == True` and zero unexpected duplicates.

### Schema Evolution Procedure
1. Construct the proposed `list[FieldSpec]` preserving all existing fields.
2. Call `lake.evolve_schema(dataset_id, proposed_fields)`.
3. If `SchemaEvolutionError` is raised, **do not bypass** — redesign the change as additive/nullable or create a new versioned dataset ID.

### Creating a Reproducible Research Snapshot
1. Select the exact UTC `as_of_ts` cutoff (e.g., `2024-06-05T23:59:59Z`).
2. Call `lake.create_snapshot(name, datasets, as_of_ts, access)`.
3. Record the returned `snapshot_id` and `manifest_sha256` alongside experiment artifacts.

## 3. Incident Response & Safe Fallbacks

### Incident A: Corrupt Partition Detected (`CorruptPartitionError`)
**Symptoms**: `query_as_of`, `query_snapshot`, or `verify_integrity` raises `CorruptPartitionError` / reports non-empty `corrupt_partition_ids`.

**Operator Response**:
1. **Fail-closed**: Do NOT disable checksum verification or ignore the error; downstream consumers must halt reads on the affected dataset.
2. Run `lake.verify_integrity()` to enumerate all affected `partition_id` values.
3. Execute `lake.recover_partition(partition_id, AccessContext(principal="oncall-sre", role="system_recovery", purpose="recovery"))`.
   - This preserves the corrupt file under `object_store/quarantine/` for forensics.
   - Marks the corrupt partition `QUARANTINED` in `catalog.sqlite3`.
   - Verifies the immutable raw landing object's SHA-256 checksum and deterministically replays affected records into a new immutable partition version.
4. Re-run `lake.verify_integrity()` and verify `corrupt_partition_ids == []`.
5. If the **raw** partition itself is corrupt (`RecoveryError`), restore the immutable raw object from off-host WORM backup before replaying.

### Incident B: Full Disaster Recovery / Snapshot Reproduction
1. Provision a clean `FinancialDataLake` directory with the committed `config/` files.
2. Use `source_lake.recovery.reproduce_snapshot_in_lake(snapshot_name=..., target_lake=clean_lake, access=...)`.
3. Compare `compute_rows_digest(rows)` against the baseline snapshot digest to confirm exact bit-level row reproduction.

### Incident C: Unauthorized Access Attempt (`EntitlementError`)
**Symptoms**: `EntitlementError` raised and `DENY` entry written to `audit_log`.
**Operator Response**:
1. Inspect `lake.catalog.get_audit_log(dataset_id=...)`.
2. Confirm whether the license expired, role lacks permission, or redistribution was attempted.
3. Renew/update `config/entitlements.json` only after legal/vendor licensing sign-off. Never bypass entitlement checks.

### Incident D: Quality Gate Rejection (`QualityCheckError`)
**Symptoms**: Ingestion halted before normalized/curated Parquet write.
**Operator Response**:
1. Inspect failed checks in `lake.catalog.list_quality_reports(dataset_id=...)`.
2. Raw payload remains preserved in `raw/` for inspection; no bad data entered `normalized/` or `curated/`.
3. Fix upstream feed or adapter mapping, then re-ingest cleanly.

## 4. Retention Policy Enforcement
- Run `lake.recovery.evaluate_retention_policy(as_of_ts)` periodically.
- Eligible old partitions are marked for `cold_tier_immutable` archival. Destructive deletion or history rewrite is blocked by configuration invariants.
