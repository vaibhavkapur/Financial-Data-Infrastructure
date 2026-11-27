# Foundation Audit, Assumptions, Security/IP Review & Unsupported Features

## 1. Existing Foundations Audit (Milestone M1)

`plan.md` Section 2 references two upstream personal portfolio repositories:
- `vaibhavkapur/Machine-Learning/projects/Sagemaker-Fraud-Detector`
- `vaibhavkapur/Quantum-Trading/projects/07-quantum-order-book-predictor`

### Audit Findings
- Neither repository is checked out in the local workspace, and public web indexing did not return inspectable source commits or license files.
- Per `plan.md` Section 2 ("Confirm access, licenses, background IP and SAAT ownership terms before copying or adapting any personal repository. ... If access or licensing is unresolved, implement against clean, documented contracts instead"), **no external code was copied**.
- Instead, this repository implements clean, documented, zero-IP-encumbrance contracts covering both domains:
  1. **Order-book microstructure feature contracts** (`lob_mid_price`, `lob_spread_bps`, `lob_order_book_imbalance`, `lob_microprice`, `lob_rolling_imbalance_mean`) using level-1 bid/ask prices and sizes.
  2. **Entity/account risk & fraud velocity feature contracts** (`tx_rolling_count`, `tx_rolling_amount_sum`, `tx_amount_zscore`) using account transaction event streams.
  3. **Market bar feature contracts** (`bar_log_return`, `bar_rolling_volatility`, `bar_vwap_deviation_bps`) for OHLCV/VWAP time-series models.

## 2. Key Assumptions & Design Decisions

1. **UTC-Only Timestamps**: Naive datetimes are rejected everywhere; all timestamps are normalized to UTC microsecond precision (`Datetime("us", "UTC")`).
2. **Strict Bi-Temporal Ordering**: `availability_time >= event_time` is enforced on raw ingestion, feature records, offline joins, and online serving.
3. **Deterministic Content-Addressed Versioning**: Each feature's `version_id` (`<name>@<semver>+<sha256_16>`) hashes the feature name, view, SemVer, entity type, value type, unit, currency, null policy, default value, `transform_hash`, and source `(source_id, source_version, schema_hash)`. Any change to source or transform creates a distinct `version_id`.
4. **No Forward-Filled Future Values**: `NullPolicy` explicitly supports only `keep_null`, `reject`, and `default_value`. Backward/future fill is prohibited by design.
5. **Anti-Proliferation Governance**: Registry enforces `max_active_features_per_view`, `max_total_active_features`, mandatory `owner`, and mandatory `source.provenance_uri`.
6. **Execution Disabled**: `governance.allow_order_execution` is `false`. This system produces features only and cannot route orders.

## 3. Security, Data Licensing, IP & Deployment Approval Review

- **Data Provenance & Entitlements**: All fixtures shipped in `data/fixtures/` are 100% synthetic, deterministically generated with fixed seeds (`seed=42`), carrying `entitlement: "synthetic-internal"` and `license: "internal-synthetic"`. No proprietary or redistributed vendor market data is included.
- **Secrets Management**: Zero credentials or API keys exist in code or `config/store_config.json`. Production PostgreSQL/Redis credentials must be injected exclusively via runtime secret stores / environment variables.
- **IP & Ownership**: Clean-room implementation with no copied code from external personal repositories.
- **Deployment Approval Gate**: Prototype is scoped strictly to local reproducible execution. Connecting live market feeds, external PostgreSQL/Redis clusters, or paper/live trading systems requires separate SAAT security, data-licensing, risk, and legal/regulatory approval.

## 4. Explicit List of Unsupported Features (Out of MVP Scope)

- Distributed compute execution (Spark/Ray/Flink) — current MVP runs single-node Polars + Parquet + SQLite.
- Live streaming ingestion brokers (Kafka/Redpanda/Kinesis) — streaming replay is modeled synchronously via `OnlineFeatureStore.materialize_and_sync_from_raw`.
- Live PostgreSQL catalog server migrations — registry persists deterministically to versioned JSON with SHA-256 snapshot hashes; SQLite provides durable local SQL online serving.
- Live Redis cluster HA / sentinel / TLS pooling — `RedisOnlineBackend` provides the isolated adapter contract (`RedisClientProtocol`), tested via contract tests without requiring a live Redis server.
- Automated feature selection / auto-generated unbounded feature spaces (explicit non-goal).
- Order routing, broker execution, or live alpha performance claims.
