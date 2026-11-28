# Data & Job Contracts (`dqrp.v1`)

All schemas carry an explicit `schema_version` (`"dqrp.v1"`). Unknown schema versions fail fast with `UNSUPPORTED_SCHEMA_VERSION`.

## Time, Unit, and Identifier Semantics

- **Timestamps:** Mandatory UTC ISO-8601 strings with explicit timezone (`Z` or `+00:00`). Timezone-naive timestamps fail with `TIMEZONE_NAIVE_TIMESTAMP`.
- **Event vs. Availability Time:**
  - `event_time`: Market bar close timestamp (when the economic event occurred).
  - `availability_time`: Ingestion/publication timestamp (when the bar became queryable for research).
  - **Invariant:** `availability_time >= event_time`. Violations fail with `POINT_IN_TIME_VIOLATION`.
- **Units:**
  - Prices (`open`, `high`, `low`, `close`): quote currency per share/unit, strictly finite and `> 0`, satisfying `high >= max(open, close, low)` and `low <= min(open, close, high)`.
  - `volume`: share/contract count, finite and `>= 0`.
  - Transaction costs (`fee_bps`, `slippage_bps`): basis points (`1 bp = 0.0001`).
  - Annualization factor: `TRADING_DAYS_PER_YEAR = 252`.
  - Resource metrics: `cpu_seconds` and `wall_seconds` in seconds; `peak_memory_mb` in MiB; `estimated_cost_usd` in synthetic USD.
- **Stable Identifiers & Hashes:**
  - `manifest_sha256`: SHA-256 of canonical JSON over sorted symbols and sorted partitions.
  - `spec_sha256`: SHA-256 of canonical JSON over strategy, seed, budget, grid/trials, and cost model.
  - `input_sha256`: SHA-256 over `(schema_version, experiment_sha256, dataset_sha256, trial_index, strategy_name, params, seed, evaluation_as_of)`.
  - `trial_id`: `trial-<trial_index:04d>-<sha256[:12]>` (content-addressed).
  - `artifact_sha256`: SHA-256 of the exact committed canonical JSON artifact bytes.
  - `aggregate_sha256`: SHA-256 over canonically ordered `TrialResult.logical_payload()` objects.

## Input Contracts

### 1. `DatasetManifest`

| Field | Type | Constraints |
|---|---|---|
| `schema_version` | `string` | Must equal `"dqrp.v1"` |
| `dataset_id` | `string` | Non-empty identifier |
| `version` | `string` | Semantic version string |
| `format` | `string` | `"jsonl"`, `"csv"`, or `"parquet"` |
| `source_name` | `string` | Non-empty upstream source identifier |
| `source_lineage` | `string` | Generator/pipeline provenance string |
| `created_at` | `string` | UTC ISO-8601 timestamp |
| `expected_row_count` | `integer` | `>= 1` (used to compute `completeness_ratio`) |
| `symbols` | `string[]` | Non-empty symbol list |
| `partitions` | `DatasetPartition[]` | Non-empty; unique `partition_id`; each file verified for `size_bytes`, `content_sha256`, and `row_count` |
| `manifest_sha256` | `string` | Optional on input (computed if omitted; verified if present) |

### 2. `ExperimentSpec`

| Field | Type | Constraints |
|---|---|---|
| `schema_version` | `string` | Must equal `"dqrp.v1"` |
| `experiment_id` | `string` | Non-empty identifier |
| `name` | `string` | Human-readable name |
| `dataset_manifest_path` | `string` | Relative (to spec file) or absolute path |
| `expected_dataset_sha256` | `string \| null` | Optional 64-hex pin; fails with `DATASET_HASH_MISMATCH` on drift |
| `strategy_name` | `string` | `"sma_crossover"`, `"momentum_zscore"`, or `"mean_reversion"` |
| `base_seed` | `integer` | `>= 0` |
| `evaluation_as_of` | `string \| null` | Optional UTC ISO-8601 cutoff applied to `availability_time` |
| `parameter_grid` / `explicit_trials` | `object` / `object[]` | Exactly one must be non-empty |
| `budget` | `ResourceBudget` | `max_trials >= 1`, `max_concurrency >= 1`, positive finite CPU/wall/memory, non-negative USD cost |
| `cost_model` | `CostModel` | Non-negative synthetic unit rates (`cpu_core_hour_usd`, `memory_gb_hour_usd`, `artifact_gb_write_usd`) |
| `max_retries` | `integer` | `>= 0` (default `2`) |

### 3. `JobContract` (Immutable Single Trial)

Materialized by `ExperimentSpec.build_job_contracts(dataset_sha256)`. Contains `trial_id`, `experiment_sha256`, `dataset_sha256`, `trial_index`, `strategy_name`, `params`, `seed`, `evaluation_as_of`, `attempt`, and `input_sha256`.

## Output Contracts

- **Per-Trial Artifact (`trials/<trial_id>.json` + `<trial_id>.sha256`):** Canonical JSON containing contract metadata, `TrialMetrics` (`total_return`, `annualized_return`, `annualized_volatility`, `sharpe_ratio`, `max_drawdown`, `win_rate`, `trade_count`, `final_equity`, `turnover`, `bars_evaluated`, `mc_sharpe_p05`, `mc_sharpe_p95`), and `equity_curve`.
- **Experiment Summary (`runs/<run_id>/summary.json`):** Contains run metadata, lineage hashes, `status` (`COMPLETED`, `BUDGET_EXHAUSTED`, `CANCELLED`, `PARTIAL_FAILURE`, `FAILED`), `aggregate_sha256`, `duplicate_completed_trials`, resource usage, dataset quality metrics (`dataset_completeness_ratio`, `dataset_freshness_lag_seconds`), diagnostic `counters`, runtime `environment`, and canonically ordered `trials`.

## Validation Error Codes

| Error Code | Trigger |
|---|---|
| `MISSING_FIELDS` | Required field omitted from manifest, bar, or spec |
| `UNSUPPORTED_SCHEMA_VERSION` | `schema_version != "dqrp.v1"` |
| `UNSUPPORTED_FORMAT` / `UNSUPPORTED_STRATEGY` / `UNSUPPORTED_BACKEND` | Unknown format, strategy, or execution backend |
| `TIMEZONE_NAIVE_TIMESTAMP` / `INVALID_TIMESTAMP` | Malformed or non-UTC timestamp |
| `POINT_IN_TIME_VIOLATION` | `availability_time < event_time` |
| `INVALID_OHLC_ENVELOPE` | `high < max(open, close, low)` or `low > min(open, close, high)` |
| `PARTITION_HASH_MISMATCH` / `PARTITION_SIZE_MISMATCH` / `MANIFEST_HASH_MISMATCH` | Tampered or mismatched dataset file |
| `DATASET_HASH_MISMATCH` | Manifest hash does not match `expected_dataset_sha256` |
| `ARTIFACT_INTEGRITY_ERROR` | Truncated `.tmp` file, corrupted JSON, or sidecar SHA-256 mismatch |
| `DUPLICATE_TASK_ERROR` | Duplicate `trial_id` in job set or registry write |
| `WORKER_CRASH_ERROR` | Subprocess exit or injected worker crash |
| `BACKEND_UNAVAILABLE_ERROR` | Optional dependency (`pyarrow`, `ray`, `dask`, `psycopg`) not installed |

## Fixture Inventory

- **Valid fixtures:**
  - `data/fixtures/oracle/` — 6-bar synthetic series + independent closed-form `expected_oracle.json`.
  - `data/fixtures/equities_synthetic_v1/` — 2-partition, 504-row synthetic daily OHLCV JSONL dataset (`SYM_A`, `SYM_B`).
  - `data/fixtures/equities_synthetic_csv_v1/` — 60-row synthetic CSV dataset verifying the CSV source adapter.
- **Invalid fixtures (`data/fixtures/invalid/`):**
  - `manifest_bad_schema.json` (`UNSUPPORTED_SCHEMA_VERSION`)
  - `manifest_pit_violation.json` (`POINT_IN_TIME_VIOLATION`)
  - `spec_unsupported_strategy.json` (`UNSUPPORTED_STRATEGY`)
- **Boundary fixture (`data/fixtures/boundary/`):**
  - `manifest.json` + `flat_2bars.jsonl` — minimal 2-bar flat-price series (zero volatility, zero trades, `sharpe_ratio == 0.0`, no NaN/Inf).
