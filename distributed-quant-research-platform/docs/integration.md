# Integration & Adapter Boundaries

## Repository Context (`Trading-Data-Infrastructure`)

This project lives at `distributed-quant-research-platform/` alongside independent sibling projects in `Trading-Data-Infrastructure`:

- `distributed-tick-data-database/`
- `security-master-platform/`
- `financial-corporate-actions-engine/`
- `options-market-data-platform/`
- `financial-data-lake/`
- `financial-feature-store/`
- `alternative-data-platform/`

To keep each project self-contained and independently runnable, `dqrp` exposes standalone adapter interfaces rather than importing sibling packages directly.

## Reusable Integration Boundaries

1. **Dataset Manifest & Source Adapter (`dqrp.storage.dataset`):**
   - Any upstream data producer (e.g., future `distributed-tick-data-database` exports or corporate-action-adjusted bar snapshots from `financial-corporate-actions-engine`) can feed `dqrp` simply by emitting `dqrp.v1` `MarketBar` partitions (JSONL, CSV, or Parquet) and a SHA-256-sealed `DatasetManifest`.
   - Security identifiers are carried opaquely in `MarketBar.symbol`, allowing future `security-master-platform` canonical IDs without modifying the scheduler or backtest engine.

2. **Worker Execution Adapter (`dqrp.workers.base.WorkerAdapter`):**
   - Exposes `submit(contract)`, `poll_completed(timeout)`, `cancel(trial_ids)`, `shutdown()`, and `capabilities`.
   - Built-in adapters:
     - `LocalWorkerAdapter`: in-process reference oracle.
     - `DistributedProcessWorkerAdapter`: multi-process `spawn` pool with OS process crash isolation.
   - Optional adapters (installed via `pyproject.toml` extras):
     - `RayWorkerAdapter` (`pip install -e ".[ray]"`)
     - `DaskWorkerAdapter` (`pip install -e ".[dask]"`)

3. **Experiment Registry Adapter (`dqrp.storage.registry.ExperimentRegistry`):**
   - Default durable implementation: `SqliteExperimentRegistry` (WAL mode, zero external server required).
   - Optional implementation boundary: `PostgresExperimentRegistry` (`pip install -e ".[postgres]"`), disabled by default in this offline prototype (production credentials are out of scope).

4. **Artifact Store (`dqrp.storage.artifacts.ArtifactStore`):**
   - Content-addressed local filesystem store with atomic `tmp -> fsync -> os.replace` commit and SHA-256 sidecars. Can be replaced by an object-store adapter (e.g., S3/GCS with conditional PUT) implementing the same `commit_trial_artifact` / `verify_and_load` interface.

## Dependency & Provenance Policy

- Zero proprietary code or external vendor feed dependencies.
- Zero live broker, exchange, customer, or production database connections.
- Optional third-party dependencies are strictly pinned with permissive/standard open-source licenses in `requirements-lock.txt` and `pyproject.toml`.
