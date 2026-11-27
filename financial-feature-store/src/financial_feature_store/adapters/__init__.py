"""External data source and online backend adapters."""

from financial_feature_store.adapters.online_backends import (
    InMemoryOnlineBackend,
    OnlineBackend,
    RedisClientProtocol,
    RedisOnlineBackend,
    SQLiteOnlineBackend,
)
from financial_feature_store.adapters.sources import (
    FeatureSourceAdapter,
    ParquetFixtureSourceAdapter,
    SyntheticDataGenerator,
    compute_file_sha256,
    compute_schema_hash,
)

__all__ = [
    "FeatureSourceAdapter",
    "InMemoryOnlineBackend",
    "OnlineBackend",
    "ParquetFixtureSourceAdapter",
    "RedisClientProtocol",
    "RedisOnlineBackend",
    "SQLiteOnlineBackend",
    "SyntheticDataGenerator",
    "compute_file_sha256",
    "compute_schema_hash",
]
