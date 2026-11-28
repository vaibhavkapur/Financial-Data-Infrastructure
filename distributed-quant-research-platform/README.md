# Distributed Quant Research Platform (`dqrp`)

Reproducible, budgeted parallel quantitative research platform operating on versioned datasets with immutable SHA-256 input/artifact hashes, bounded scheduling, retries, cooperative cancellation, deterministic result aggregation, and a durable SQLite experiment registry.

> **Scope:** Educational / research prototype operating strictly offline on deterministic synthetic datasets. No live capital, real market feeds, customer records, production credentials, or proprietary code are used.

## Key Capabilities

- **Immutable Contracts & Hashing (`dqrp.v1`):** SHA-256-sealed `DatasetManifest` partitions, point-in-time invariants (`availability_time >= event_time`), content-addressed `JobContract` IDs, and deterministic per-trial seed derivation.
- **Pure Domain Isolation:** Quantitative strategies (`sma_crossover`, `momentum_zscore`, `mean_reversion`) and the scheduling/retry/budget state machine (`ExperimentStateMachine`) are strictly isolated from disk/network I/O.
- **Local & Distributed Worker Adapters:** In-process reference oracle (`local`), real OS-isolated multi-process worker pool (`distributed` using `multiprocessing` `spawn` with hard-crash detection and respawning), plus optional fail-fast adapters for `ray` and `dask`.
- **Fault Resilience & Exactly-Once Completion:** Atomic `tmp -> fsync -> os.replace` artifact commits with SHA-256 sidecars, automatic quarantine of partial/corrupted artifacts, duplicate task/completion suppression, cooperative cancellation, `--resume` artifact replay, and hard compute/wall/trial/memory/cost budgets.
- **Experiment Registry & Observability:** Durable SQLite WAL registry (`PRIMARY KEY (run_id, trial_id)`), JSON export, structured JSONL event logs, diagnostic counters, and transparent synthetic cost estimation.

## Quickstart & Validation

Zero third-party dependencies are required for the core platform (Python `>= 3.9` stdlib only):

```bash
cd distributed-quant-research-platform

# 1. Run full unit, integration, failure, and parity test suite (24 tests)
PYTHONPATH=src python3 -m unittest discover -s tests -v

# 2. Run reproducible end-to-end demo (oracle check + 16-trial local/distributed parity)
python3 scripts/run_demo.py

# 3. Run 24-trial scaling benchmarks and cost projections
python3 scripts/run_benchmarks.py

# 4. Run via CLI directly
PYTHONPATH=src python3 -m dqrp validate-manifest data/fixtures/equities_synthetic_v1/manifest.json
PYTHONPATH=src python3 -m dqrp run config/demo_experiment.json --backend distributed --workers 4
PYTHONPATH=src python3 -m dqrp registry list
```

## Example Output (Observed Demo Run)

```json
{
  "bit_exact_match": true,
  "distributed_aggregate_sha256": "48a9ab3e3527ca79e2d3d20eeb3b0058393b267755c6362cc8931c04d09cd600",
  "distributed_best_trial_id": "trial-0012-2bd40d243303",
  "distributed_duplicate_completed_trials": 0,
  "distributed_run_id": "demo-sweep-distributed-4w",
  "distributed_wall_seconds": 0.215445,
  "local_aggregate_sha256": "48a9ab3e3527ca79e2d3d20eeb3b0058393b267755c6362cc8931c04d09cd600",
  "local_best_trial_id": "trial-0012-2bd40d243303",
  "local_duplicate_completed_trials": 0,
  "local_run_id": "demo-sweep-local",
  "local_wall_seconds": 0.305564
}
```

## Measured Scaling Summary (Synthetic 24-Trial Benchmark)

Observed on `macOS-26.5.1-arm64` (CPython 3.9.6, 10 CPU cores) running `config/benchmark_experiment.json` (24 CPU-bound trials × 2,500 seeded bootstrap draws); raw data in [`reports/scaling_benchmark.json`](reports/scaling_benchmark.json):

| Backend | Workers | Wall Time (s) | Total CPU (s) | Peak RSS (MiB) | Speedup vs Local | Parity Match | Duplicates |
|---|---:|---:|---:|---:|---:|---|---:|
| `local` | 1 | 3.0946 | 2.9901 | 17.92 | 1.0000x | Exact (`7a9e8a0a...`) | 0 |
| `distributed` | 1 | 3.1671 | 3.0721 | 18.28 | 0.9771x | Exact (`7a9e8a0a...`) | 0 |
| `distributed` | 2 | 1.6634 | 3.0737 | 18.25 | **1.8605x** | Exact (`7a9e8a0a...`) | 0 |
| `distributed` | 4 | 1.0072 | 3.4596 | 18.28 | **3.0726x** | Exact (`7a9e8a0a...`) | 0 |

*Note: Speedup numbers are observed on this machine for synthetic CPU-bound bootstrap trials; cost figures in `reports/scaling_benchmark.json` are synthetic estimates from `CostModel`, not actual cloud billing.*

## Documentation & Evidence

- [`plan.md`](plan.md) — Implementation plan and acceptance checklist
- [`docs/architecture.md`](docs/architecture.md) — Component diagram, data flow, invariants, and trade-offs
- [`docs/contracts.md`](docs/contracts.md) — Canonical input/output schemas (`dqrp.v1`), units, timestamp rules, and error codes
- [`docs/integration.md`](docs/integration.md) — Adapter boundaries and repository integration design
- [`docs/runbook.md`](docs/runbook.md) — Setup, demo, validation, diagnostics, and recovery procedures
- [`docs/adr/0001-architecture-and-determinism.md`](docs/adr/0001-architecture-and-determinism.md) — Architecture Decision Record #1
- [`reports/acceptance.md`](reports/acceptance.md) — Full acceptance evidence matrix (including explicitly marked untested optional adapters)
- [`reports/baseline_comparison.json`](reports/baseline_comparison.json) — Hand-calculated oracle comparison (`abs_diff = 0.0`) & demo parity
- [`reports/scaling_benchmark.json`](reports/scaling_benchmark.json) — Measured scaling benchmarks & synthetic cost projections
- [`reports/experiment_registry_export.json`](reports/experiment_registry_export.json) — Exported experiment registry

## Assumptions, Limitations & Provenance

- **Fixture provenance:** All datasets in `data/fixtures/` are 100% synthetic, generated deterministically by `scripts/generate_fixtures.py` using fixed seeds (`20250101` for synthetic GBM OHLCV bars; closed-form 6-bar array for the oracle).
- **Dependency licenses:** Core runtime uses Python standard library only (PSF License). Pinned optional extras (`requirements-lock.txt`): `pyarrow==20.0.0` (Apache-2.0), `dask/distributed==2025.2.0` (BSD-3-Clause), `ray==2.43.0` (Apache-2.0), `psycopg==3.2.6` (LGPL-3.0), `pytest==8.3.5` (MIT).
- **Untested optional backends in baseline offline environment:** Live Ray cluster, live Dask Distributed cluster, Parquet reading via `pyarrow`, and PostgreSQL registry via `psycopg` were not executed against live servers/packages; fail-fast `BackendUnavailableError` handling is covered by automated tests.
- **Not production-ready:** Live execution, multi-tenant auth, secrets management, and distributed object storage require dedicated security, operational, and risk review.
