# Milestone 1 Report — Foundation and Design

## Scope Delivered
- Audited ClickHouse `MergeTree` tick schema conventions and fixture loader requirements against clean adapter contracts ([clickhouse_audit.md](../docs/clickhouse_audit.md)).
- Defined canonical Pydantic domain contracts (`models.py`) with mandatory timezone-aware UTC `event_ts` / `available_at`, `available_at >= event_ts`, stable `instrument_id`, explicit ISO-4217 currency and units, and structured errors (`errors.py`).
- Specified versioned source entitlements (`config/entitlements.json`), schema registry & backward-compatible evolution rules (`config/schemas/registry.json`, `schemas.py`), and versioned lake configuration (`config/lake_config.json`, `config.py`) with hard guardrails forbidding destructive rewrites, redistribution, and order execution.
- Committed ADRs ([ADR-001](../docs/adrs/ADR-001-bitemporal-as-of-model.md), [ADR-002](../docs/adrs/ADR-002-immutable-parquet-and-sqlite-catalog.md), [ADR-003](../docs/adrs/ADR-003-entitlement-and-lineage-enforcement.md)) and system architecture ([architecture.md](../docs/architecture.md)).

## Verification Evidence
- Unit tests: `tests/unit/test_models_and_schemas.py` (7 passed) and `tests/unit/test_entitlements_and_catalog.py` (5 passed).
- Adapter & fixture verification: `tests/integration/test_adapters_and_ingest.py` verifies `ClickHouseTickAdapter` mapping and SHA-256 tamper detection in `FixtureLoader`.
- Full automated evidence retained in [validation_report.json](validation_report.json).
