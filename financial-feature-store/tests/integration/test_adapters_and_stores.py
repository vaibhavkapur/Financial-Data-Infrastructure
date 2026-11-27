"""Integration tests for Parquet fixture adapter, SQLite & Redis online backends, and training export."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from financial_feature_store import (
    AdapterError,
    FeatureDefinition,
    FeatureRecord,
    FeatureRegistry,
    FreshnessStatus,
    LineageTracker,
    OfflineFeatureStore,
    OnlineFeatureStore,
    ParquetFixtureSourceAdapter,
    RedisOnlineBackend,
    SQLiteOnlineBackend,
    SyntheticDataGenerator,
)


class _FakeRedisClient:
    """In-memory mock satisfying RedisClientProtocol for adapter contract testing."""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def get(self, name: str) -> bytes | str | None:
        return self._data.get(name)

    def set(self, name: str, value: str) -> bool:
        self._data[name] = value
        return True

    def delete(self, *names: str) -> int:
        deleted = 0
        for n in names:
            if n in self._data:
                del self._data[n]
                deleted += 1
        return deleted

    def scan_iter(self, match: str | None = None) -> list[str]:
        if match is None:
            return list(self._data)
        prefix = match.rstrip("*")
        return [k for k in self._data if k.startswith(prefix)]

    def ping(self) -> bool:
        return True


def test_parquet_fixture_adapter_checksum_and_load(
    tmp_path: Path,
    base_time: datetime,
) -> None:
    gen = SyntheticDataGenerator(seed=7)
    df = gen.generate_order_book_snapshots(start_time=base_time, steps=10)
    pq_path, manifest_path, contract = gen.write_fixture_bundle(
        df,
        tmp_path / "fixtures" / "lob.parquet",
        source_id="src_lob",
        dataset_name="lob_l1",
    )

    adapter = ParquetFixtureSourceAdapter(
        pq_path,
        manifest_path=manifest_path,
        required_columns=("best_bid", "best_ask", "bid_size", "ask_size"),
    )
    loaded_df, loaded_contract = adapter.load()
    assert loaded_df.height == df.height
    assert loaded_contract.schema_hash == contract.schema_hash

    # Corrupting the parquet file triggers checksum validation error
    pq_path.write_bytes(pq_path.read_bytes() + b"corrupt")
    with pytest.raises(AdapterError):
        adapter.load()


@pytest.mark.parametrize("backend_kind", ["sqlite", "redis"])
def test_online_backend_adapter_contracts_and_monotonic_guard(
    tmp_path: Path,
    base_time: datetime,
    backend_kind: str,
) -> None:
    backend: Any
    if backend_kind == "sqlite":
        backend = SQLiteOnlineBackend(tmp_path / "online.sqlite3")
    else:
        backend = RedisOnlineBackend(_FakeRedisClient())

    rec_new = FeatureRecord(
        entity_id="EQ:AAPL",
        feature_name="lob_imbalance_l1",
        feature_version="1.0.0",
        version_id="lob_imbalance_l1@1.0.0+aaaa1111",
        event_time=base_time + timedelta(seconds=20),
        availability_time=base_time + timedelta(seconds=21),
        materialized_at=base_time + timedelta(seconds=21),
        value=0.42,
        unit="ratio",
        source_version="r1",
        run_id="run-2",
    )
    rec_old = rec_new.model_copy(
        update={
            "event_time": base_time + timedelta(seconds=10),
            "availability_time": base_time + timedelta(seconds=11),
            "materialized_at": base_time + timedelta(seconds=11),
            "value": -0.99,
            "run_id": "run-1",
        }
    )

    assert backend.put_batch([rec_new]) == 1
    # Older record must NOT overwrite newer record
    assert backend.put_batch([rec_old]) == 0
    fetched = backend.get("EQ:AAPL", "lob_imbalance_l1", "1.0.0")
    assert fetched is not None
    assert fetched.value == pytest.approx(0.42)
    assert backend.health()["status"] == "ok"
    assert backend.delete_feature("lob_imbalance_l1", "1.0.0") == 1
    assert backend.get("EQ:AAPL", "lob_imbalance_l1", "1.0.0") is None


def test_sqlite_online_store_and_reproducible_training_export(
    tmp_path: Path,
    base_time: datetime,
    active_store_env: tuple[FeatureRegistry, OfflineFeatureStore, LineageTracker],
    synthetic_lob_df: pl.DataFrame,
    lob_imbalance_def: FeatureDefinition,
) -> None:
    registry, offline, lineage = active_store_env
    sqlite_backend = SQLiteOnlineBackend(tmp_path / "online.sqlite3")
    online = OnlineFeatureStore(sqlite_backend, registry, lineage=lineage)

    cutoff = base_time + timedelta(seconds=120)
    synced = online.materialize_and_sync_from_raw(
        "lob_imbalance_l1",
        synthetic_lob_df,
        as_of_time=cutoff,
    )
    assert synced == 2

    vec = online.get_online_features("EQ:AAPL", ["lob_imbalance_l1"], now=cutoff + timedelta(seconds=2))
    assert vec.all_fresh
    assert vec.features["lob_imbalance_l1"].freshness_status == FreshnessStatus.FRESH
    assert vec.features["lob_imbalance_l1"].age_seconds is not None
    assert vec.features["lob_imbalance_l1"].age_seconds >= 0.0

    # Export training dataset with chronological splits
    spine_rows = [
        {"entity_id": ent, "as_of_time": base_time + timedelta(seconds=step * 10 + 2)}
        for step in range(2, 18)
        for ent in ("EQ:AAPL", "EQ:MSFT")
    ]
    spine_df = pl.DataFrame(
        spine_rows,
        schema={"entity_id": pl.String(), "as_of_time": pl.Datetime("us", "UTC")},
    )
    exported_df, manifest = offline.export_training_dataset(
        spine_df,
        ["lob_imbalance_l1", "lob_imbalance_roll_30s"],
        export_dir=tmp_path / "exports",
        export_name="lob_training_v1",
        seed=42,
    )
    assert manifest.zero_leakage_verified
    assert manifest.row_count == exported_df.height
    assert Path(manifest.parquet_path).exists()
    assert set(exported_df["split"].unique().to_list()) == {"train", "val", "test"}

    # Verify strict chronological ordering across train < val < test
    train_max = exported_df.filter(pl.col("split") == "train")["as_of_time"].max()
    val_min = exported_df.filter(pl.col("split") == "val")["as_of_time"].min()
    val_max = exported_df.filter(pl.col("split") == "val")["as_of_time"].max()
    test_min = exported_df.filter(pl.col("split") == "test")["as_of_time"].min()
    assert train_max < val_min
    assert val_max < test_min

    # Re-exporting identical inputs produces identical data content
    exported_df_2, manifest_2 = offline.export_training_dataset(
        spine_df,
        ["lob_imbalance_l1", "lob_imbalance_roll_30s"],
        export_dir=tmp_path / "exports",
        export_name="lob_training_v1_repro",
        seed=42,
    )
    assert exported_df.equals(exported_df_2)
    assert manifest.feature_versions == manifest_2.feature_versions
    assert manifest.sha256_checksum == manifest_2.sha256_checksum
