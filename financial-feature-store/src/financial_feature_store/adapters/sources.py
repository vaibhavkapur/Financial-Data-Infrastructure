"""Source adapters and deterministic synthetic market/risk fixture generators."""

from __future__ import annotations

import hashlib
import json
import math
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, runtime_checkable

import polars as pl

from financial_feature_store.contracts import SourceDatasetContract, canonical_sha256, ensure_utc
from financial_feature_store.errors import AdapterError
from financial_feature_store.transforms import validate_raw_frame


def compute_file_sha256(path: str | Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_schema_hash(df: pl.DataFrame) -> str:
    schema_pairs = [(name, str(dtype)) for name, dtype in sorted(df.schema.items())]
    return canonical_sha256(schema_pairs)[:16]


@runtime_checkable
class FeatureSourceAdapter(Protocol):
    """Contract separating upstream data providers from feature store business logic."""

    def load(self) -> tuple[pl.DataFrame, SourceDatasetContract]:
        ...


class ParquetFixtureSourceAdapter:
    """Loads permitted/synthetic Parquet fixtures alongside their provenance manifest."""

    def __init__(
        self,
        parquet_path: str | Path,
        *,
        manifest_path: str | Path | None = None,
        required_columns: tuple[str, ...] = (),
    ) -> None:
        self.parquet_path = Path(parquet_path)
        self.manifest_path = Path(manifest_path) if manifest_path else self.parquet_path.with_suffix(".manifest.json")
        self.required_columns = required_columns

    def load(self) -> tuple[pl.DataFrame, SourceDatasetContract]:
        if not self.parquet_path.exists():
            raise AdapterError(
                f"Parquet fixture not found: {self.parquet_path}",
                details={"parquet_path": str(self.parquet_path)},
            )
        if not self.manifest_path.exists():
            raise AdapterError(
                f"Fixture provenance manifest not found: {self.manifest_path}",
                details={"manifest_path": str(self.manifest_path)},
            )

        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        actual_sha = compute_file_sha256(self.parquet_path)
        expected_sha = manifest.get("sha256_checksum")
        if expected_sha and actual_sha != expected_sha:
            raise AdapterError(
                "Fixture checksum mismatch; refusing to load potentially corrupted data",
                details={
                    "parquet_path": str(self.parquet_path),
                    "expected_sha256": expected_sha,
                    "actual_sha256": actual_sha,
                },
            )

        df = pl.read_parquet(self.parquet_path)
        validate_raw_frame(df, self.required_columns)

        actual_schema_hash = compute_schema_hash(df)
        contract = SourceDatasetContract(
            source_id=manifest["source_id"],
            dataset_name=manifest["dataset_name"],
            source_version=manifest["source_version"],
            schema_hash=actual_schema_hash,
            entitlement=manifest.get("entitlement", "synthetic-internal"),
            license=manifest.get("license", "internal-synthetic"),
            provenance_uri=str(self.parquet_path),
            currency=manifest.get("currency"),
        )
        return df, contract


class SyntheticDataGenerator:
    """Deterministic seeded generator for order-book, market-bar, and risk-event fixtures."""

    def __init__(self, seed: int = 42) -> None:
        self.seed = seed

    def generate_order_book_snapshots(
        self,
        *,
        entities: tuple[str, ...] = ("EQ:AAPL", "EQ:MSFT", "EQ:NVDA"),
        start_time: datetime = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC),
        steps: int = 60,
        step_seconds: int = 10,
        availability_delay_seconds: float = 0.25,
    ) -> pl.DataFrame:
        start_utc = ensure_utc(start_time, "start_time")
        rng = random.Random(self.seed)
        rows: list[dict[str, object]] = []
        base_prices = {ent: 100.0 + idx * 50.0 for idx, ent in enumerate(entities)}

        for ent in entities:
            mid = base_prices[ent]
            for step in range(steps):
                ev_ts = start_utc + timedelta(seconds=step * step_seconds)
                av_ts = ev_ts + timedelta(seconds=availability_delay_seconds)
                mid = max(1.0, mid * math.exp(rng.gauss(0.0, 0.0008)))
                half_spread = max(0.01, mid * rng.uniform(0.0001, 0.0004))
                bid = round(mid - half_spread, 4)
                ask = round(mid + half_spread, 4)
                bid_size = float(rng.randint(50, 1500))
                ask_size = float(rng.randint(50, 1500))
                rows.append(
                    {
                        "entity_id": ent,
                        "event_time": ev_ts,
                        "availability_time": av_ts,
                        "best_bid": bid,
                        "best_ask": ask,
                        "bid_size": bid_size,
                        "ask_size": ask_size,
                    }
                )

        return pl.DataFrame(
            rows,
            schema={
                "entity_id": pl.String(),
                "event_time": pl.Datetime("us", "UTC"),
                "availability_time": pl.Datetime("us", "UTC"),
                "best_bid": pl.Float64(),
                "best_ask": pl.Float64(),
                "bid_size": pl.Float64(),
                "ask_size": pl.Float64(),
            },
        )

    def generate_market_bars(
        self,
        *,
        entities: tuple[str, ...] = ("EQ:AAPL", "EQ:MSFT", "EQ:NVDA"),
        start_time: datetime = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC),
        steps: int = 60,
        step_seconds: int = 60,
        availability_delay_seconds: float = 1.0,
    ) -> pl.DataFrame:
        start_utc = ensure_utc(start_time, "start_time")
        rng = random.Random(self.seed + 100)
        rows: list[dict[str, object]] = []

        for idx, ent in enumerate(entities):
            price = 100.0 + idx * 50.0
            for step in range(steps):
                ev_ts = start_utc + timedelta(seconds=step * step_seconds)
                av_ts = ev_ts + timedelta(seconds=availability_delay_seconds)
                ret = rng.gauss(0.0, 0.0025)
                close = round(max(1.0, price * math.exp(ret)), 4)
                vwap = round(0.5 * (price + close) * (1.0 + rng.uniform(-0.0003, 0.0003)), 4)
                volume = float(rng.randint(1000, 50000))
                price = close
                rows.append(
                    {
                        "entity_id": ent,
                        "event_time": ev_ts,
                        "availability_time": av_ts,
                        "close": close,
                        "vwap": vwap,
                        "volume": volume,
                    }
                )

        return pl.DataFrame(
            rows,
            schema={
                "entity_id": pl.String(),
                "event_time": pl.Datetime("us", "UTC"),
                "availability_time": pl.Datetime("us", "UTC"),
                "close": pl.Float64(),
                "vwap": pl.Float64(),
                "volume": pl.Float64(),
            },
        )

    def generate_account_transactions(
        self,
        *,
        accounts: tuple[str, ...] = ("ACCT:0001", "ACCT:0002", "ACCT:0003"),
        start_time: datetime = datetime(2026, 1, 15, 14, 30, 0, tzinfo=UTC),
        steps: int = 40,
        step_seconds: int = 30,
        availability_delay_seconds: float = 0.5,
    ) -> pl.DataFrame:
        start_utc = ensure_utc(start_time, "start_time")
        rng = random.Random(self.seed + 200)
        rows: list[dict[str, object]] = []

        for acct in accounts:
            for step in range(steps):
                ev_ts = start_utc + timedelta(seconds=step * step_seconds)
                av_ts = ev_ts + timedelta(seconds=availability_delay_seconds)
                amount = round(max(5.0, rng.lognormvariate(5.0, 0.6)), 2)
                rows.append(
                    {
                        "entity_id": acct,
                        "event_time": ev_ts,
                        "availability_time": av_ts,
                        "amount": amount,
                    }
                )

        return pl.DataFrame(
            rows,
            schema={
                "entity_id": pl.String(),
                "event_time": pl.Datetime("us", "UTC"),
                "availability_time": pl.Datetime("us", "UTC"),
                "amount": pl.Float64(),
            },
        )

    def write_fixture_bundle(
        self,
        df: pl.DataFrame,
        parquet_path: str | Path,
        *,
        source_id: str,
        dataset_name: str,
        source_version: str = "2026.01.15-r1",
        currency: str | None = "USD",
    ) -> tuple[Path, Path, SourceDatasetContract]:
        """Persist synthetic fixture Parquet and provenance manifest with SHA-256 checksum."""
        pq_path = Path(parquet_path)
        pq_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(pq_path)

        sha = compute_file_sha256(pq_path)
        schema_hash = compute_schema_hash(df)
        manifest_path = pq_path.with_suffix(".manifest.json")
        manifest = {
            "source_id": source_id,
            "dataset_name": dataset_name,
            "source_version": source_version,
            "schema_hash": schema_hash,
            "sha256_checksum": sha,
            "entitlement": "synthetic-internal",
            "license": "internal-synthetic",
            "currency": currency,
            "seed": self.seed,
            "row_count": df.height,
            "generated_at": datetime.now(UTC).isoformat(),
        }
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

        contract = SourceDatasetContract(
            source_id=source_id,
            dataset_name=dataset_name,
            source_version=source_version,
            schema_hash=schema_hash,
            entitlement="synthetic-internal",
            license="internal-synthetic",
            provenance_uri=str(pq_path),
            currency=currency,
        )
        return pq_path, manifest_path, contract
