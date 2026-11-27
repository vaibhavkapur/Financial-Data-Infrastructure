# API & Schema Reference

## 1. Core Domain Schemas (`contracts.py`)

### `FeatureDefinition`
| Field | Type | Constraints / Semantics |
|---|---|---|
| `name` | `str` | `^[a-z][a-z0-9_]{1,63}$` |
| `feature_view` | `str` | Logical group bounded by `max_active_features_per_view` |
| `version` | `str` | Strict SemVer `MAJOR.MINOR.PATCH` |
| `version_id` (computed) | `str` | `<name>@<version>+<sha256[:16]>` uniquely binding transform + source + schema |
| `entity_type` | `str` | e.g., `instrument`, `account` |
| `value_type` | `ValueType` | `float64`, `int64`, `bool`, `string` |
| `unit` / `currency` | `str` / `str \| None` | Explicit unit (e.g., `ratio`, `bps`, `USD`, `count`) |
| `owner` | `str` | Mandatory owning team/engineer |
| `source` | `SourceDatasetContract` | `source_id`, `dataset_name`, `source_version`, `schema_hash`, `entitlement`, `license`, `provenance_uri` |
| `transform` | `TransformSpec` | `transform_name`, `source_columns`, `params`, `lookback_seconds`, `transform_hash` |
| `null_policy` | `NullPolicy` | `keep_null`, `reject`, `default_value` |
| `ttl_seconds` | `float` | Positive freshness SLA in seconds |
| `stale_policy` | `StalePolicy` | `reject`, `warn`, `fallback_null` |
| `quality_rules` | `QualityRuleSpec` | `min_row_count`, `max_null_ratio`, `min_value`, `max_value`, `max_late_arrival_seconds`, `allow_nan_or_inf` |
| `stage` | `LifecycleStage` | `draft`, `staging`, `active`, `deprecated`, `retired` |

### Materialized Offline Parquet Schema
| Column | Polars Dtype | Description |
|---|---|---|
| `entity_id` | `String` | Stable entity identifier (e.g., `EQ:AAPL`, `ACCT:0001`) |
| `feature_name` | `String` | Registered feature name |
| `feature_version` | `String` | Registered SemVer |
| `version_id` | `String` | Exact content-addressed fingerprint |
| `event_time` | `Datetime("us", "UTC")` | Underlying event occurrence timestamp |
| `availability_time` | `Datetime("us", "UTC")` | Knowledge availability timestamp (`>= event_time`) |
| `materialized_at` | `Datetime("us", "UTC")` | Materialization run timestamp |
| `value` | Typed (`Float64`/`Int64`/`Boolean`/`String`) | Computed causal feature value |
| `unit` | `String` | Feature unit |
| `source_version` | `String` | Upstream dataset version |
| `run_id` | `String` | Materialization or backfill run ID |
| `is_late_arrival` | `Boolean` | `availability_time > event_time` |
| `late_arrival_seconds` | `Float64` | `(availability_time - event_time)` in seconds |

## 2. Structured Error Codes (`errors.py`)

| Exception | Error Code | Trigger |
|---|---|---|
| `TimestampContractError` | `TIMESTAMP_CONTRACT_ERROR` | Naive datetime, missing timestamp columns, or `availability_time < event_time` |
| `LeakageViolationError` | `LEAKAGE_VIOLATION` | Future query, negative lag, future online record, or post-join leakage detection |
| `FeatureProliferationError` | `FEATURE_PROLIFERATION_LIMIT_EXCEEDED` | Exceeding per-view or store-wide feature budgets |
| `ImmutableVersionError` | `IMMUTABLE_VERSION_CONFLICT` | Re-registering existing `(name, version)` with different definition |
| `SchemaCompatibilityError` | `SCHEMA_COMPATIBILITY_ERROR` | Breaking change without MAJOR bump or logic change without MINOR bump |
| `VersionMismatchError` | `VERSION_MISMATCH` | Unknown version, pinned `version_id` mismatch, or tampered offline/online cache |
| `QualityGateError` | `QUALITY_GATE_FAILED` | Attempting `ACTIVE` promotion or online sync without passing `QualityReport` |
| `StaleFeatureError` | `STALE_FEATURE` | Online feature `age_seconds > ttl_seconds` under `StalePolicy.REJECT` |
| `NullPolicyViolationError` | `NULL_POLICY_VIOLATION` | Null/missing feature value under `NullPolicy.REJECT` |
| `AdapterError` | `ADAPTER_ERROR` | Missing fixture, corrupted SHA-256 checksum, or backend I/O error |

## 3. CLI Usage

```bash
.venv/bin/ffs inspect-registry data/registry/feature_registry.json
```
