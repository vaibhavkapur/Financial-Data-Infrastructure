# ADR-0001: Deterministic Execution Architecture, Zero-Dependency Core, and Optional Distributed Adapters

- **Status:** Accepted
- **Date:** 2026-11-28
- **Project:** `distributed-quant-research-platform/`

## Context

Quantitative research sweeps require strict reproducibility across local debugging and multi-worker parallel runs, hard budget guardrails, and resilience against worker crashes, duplicate deliveries, and partial writes. The project plan proposes Python with Ray or Dask, Parquet, and optional PostgreSQL, while allowing engineering adjustments when documented with rationale.

## Decision

1. **Zero-Dependency Pure Python Core (`>= 3.9`):**
   - Implement the canonical contracts (`dqrp.contracts`), canonical SHA-256 hashing (`dqrp.hashing`), pure backtest domain (`dqrp.domain.backtest`), scheduling/budget/retry state machine (`dqrp.domain.scheduler`), JSONL/CSV dataset loaders, atomic artifact store (`dqrp.storage.artifacts`), SQLite experiment registry (`dqrp.storage.registry`), and both `LocalWorkerAdapter` and multi-process `DistributedProcessWorkerAdapter` using only the Python standard library.
   - This guarantees clean offline reproducibility from any Python 3.9+ checkout without network access or native binary toolchain breakage.

2. **Shared Trial Execution Core for Local/Distributed Parity:**
   - All worker backends (`local`, `distributed`, `ray`, `dask`) invoke the exact same pure function (`execute_trial_core` -> `run_strategy_backtest`) over immutable `JobContract` records with deterministically derived per-trial seeds (`derive_trial_seed(base_seed, experiment_sha256, trial_index)`).
   - Results are aggregated in canonical `(trial_index, trial_id)` order, producing a backend-independent `aggregate_sha256`.

3. **Optional Adapters Fail Fast (No Silent Fallback):**
   - `ParquetBarSource` (`pyarrow`), `RayWorkerAdapter` (`ray`), `DaskWorkerAdapter` (`dask.distributed`), and `PostgresExperimentRegistry` (`psycopg`) are implemented behind clean boundaries and pinned as optional extras in `pyproject.toml`.
   - If invoked without the corresponding optional dependency installed, they raise `BackendUnavailableError` explicitly instead of silently falling back to another backend.

4. **Atomic Artifact Commit & Database Uniqueness:**
   - Artifacts are written to `<trial_id>.attempt-<n>.tmp`, `fsync`'d, and atomically replaced onto `<trial_id>.json` + `<trial_id>.sha256`. Partial or corrupted files are quarantined on detection.
   - Both `ExperimentStateMachine` and `SqliteExperimentRegistry` (`PRIMARY KEY (run_id, trial_id)`) enforce zero duplicate completed trials.

## Consequences

- **Positive:** 100% deterministic local/distributed parity, real multi-core scaling with OS process isolation (`multiprocessing` spawn), real hard-crash (`os._exit`) testing, and zero setup friction.
- **Trade-offs:** Multi-node network cluster execution requires installing optional `ray` or `dask` extras; columnar Parquet I/O requires `pyarrow`. Both are explicitly marked as not run in environments where those optional packages are absent.
