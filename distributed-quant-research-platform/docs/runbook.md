# Operator & Developer Runbook

## 1. Local Setup (Clean Checkout)

Requires **Python >= 3.9** (standard library only; no third-party packages required for core functionality).

```bash
cd distributed-quant-research-platform
python3 --version
```

Optional extras (only needed if testing optional Parquet, Ray, Dask, PostgreSQL, or pytest adapters):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"          # pytest
pip install -e ".[parquet]"      # pyarrow Parquet reader
pip install -e ".[ray]"          # Ray worker adapter
pip install -e ".[dask]"         # Dask Distributed worker adapter
```

## 2. Regenerate Deterministic Fixtures

```bash
python3 scripts/generate_fixtures.py
```

Outputs valid, invalid, boundary, and oracle fixtures under `data/fixtures/` and pinned experiment configs under `config/`.

## 3. Validation & Test Commands

Run the full automated unit, integration, failure, and parity test suite:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Validate manifests and specs from the CLI:

```bash
PYTHONPATH=src python3 -m dqrp validate-manifest data/fixtures/equities_synthetic_v1/manifest.json
PYTHONPATH=src python3 -m dqrp validate-spec config/demo_experiment.json
```

## 4. End-to-End Demo & Scaling Benchmarks

Run the reproducible demo (hand-calculated oracle check + 16-trial local/distributed parity + SQLite registry export):

```bash
python3 scripts/run_demo.py
```

Run the 24-trial scaling benchmark (local 1w vs. distributed 1w/2w/4w + parity + cost projections):

```bash
python3 scripts/run_benchmarks.py
```

Run a custom CLI experiment and inspect the registry:

```bash
PYTHONPATH=src python3 -m dqrp run config/demo_experiment.json \
  --backend distributed --workers 4 \
  --output-dir reports/runs \
  --registry reports/experiment_registry.sqlite

PYTHONPATH=src python3 -m dqrp registry --registry reports/experiment_registry.sqlite list
PYTHONPATH=src python3 -m dqrp registry --registry reports/experiment_registry.sqlite export --output reports/experiment_registry_export.json
```

## 5. Diagnostics & Failure Recovery

### Inspecting Structured Logs
Every run writes append-only JSONL lifecycle events to `reports/runs/<run_id>/events.jsonl` and its full final summary to `reports/runs/<run_id>/summary.json`. Key diagnostic counters in `summary.json`:

- `worker_crashes_detected`: Unexpected worker crashes or non-zero subprocess exits detected and retried.
- `partial_artifacts_rejected`: Truncated `.tmp` files or SHA-256-mismatched artifacts quarantined to `artifacts/quarantine/`.
- `duplicate_tasks_suppressed` / `duplicate_completions_ignored`: Duplicate dispatches or completion events rejected by idempotency guards.
- `budget_stops`: Number of hard budget terminations (`max_trials`, `max_cpu_seconds`, `max_wall_seconds`, `max_memory_mb`, `max_estimated_cost_usd`).
- `artifact_cache_hits`: Verified artifacts reused during `--resume`.

### Recovery Procedures

1. **Interrupted / Cancelled Run Recovery (`--resume`):**
   Re-run the exact same `ExperimentSpec` with `--resume`. Verified artifacts matching `(trial_id, input_sha256, artifact_sha256)` are reused; any partial `.tmp` or corrupted files are automatically quarantined and recomputed cleanly.
2. **Partition Hash Mismatch (`PARTITION_HASH_MISMATCH`):**
   Do not override or ignore. Re-fetch or regenerate the versioned dataset (`python3 scripts/generate_fixtures.py` for synthetic fixtures) and verify `manifest_sha256`.
3. **Budget Exhausted (`BUDGET_EXHAUSTED`):**
   Inspect `termination_reason` and `total_cpu_seconds` / `total_wall_seconds` / `total_estimated_cost_usd` in `summary.json`. Adjust `budget` in the spec (creating a new `spec_sha256`) or narrow the `parameter_grid`.
4. **Optional Backend Missing (`BACKEND_UNAVAILABLE_ERROR`):**
   Install the requested extra (`pip install -e ".[ray]"`, `".[dask]"`, `".[parquet]"`, `".[postgres]"`) or switch to `--backend distributed` / `--backend local`.

## 6. Known Limitations

- Distributed speedup is workload-dependent: tiny trials (<5 ms each) can be slower on `distributed` than `local` due to `multiprocessing` spawn (~70 ms) and IPC serialization overhead.
- Single-host multi-process execution scales up to local CPU cores; multi-node scaling requires deploying an external Ray or Dask cluster (not exercised in offline CI).
- Cost figures use a synthetic unit rate card (`CostModel`) and do not reflect live cloud provider billing.
- Synthetic OHLCV bars and educational strategies are for research infrastructure validation only; live trading, real market feeds, and production credentials are strictly excluded.
