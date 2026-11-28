"""Contract validation, dataset integrity, boundary fixtures, and operational error tests."""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dqrp.contracts import (
    SCHEMA_VERSION,
    DatasetManifest,
    ExperimentSpec,
    MarketBar,
    ResourceBudget,
)
from dqrp.domain.backtest import run_strategy_backtest
from dqrp.errors import (
    BackendUnavailableError,
    ContractValidationError,
    UnsupportedOperationError,
)
from dqrp.orchestrator import ExperimentOrchestrator, RunOptions, load_experiment_spec
from dqrp.storage.dataset import validate_and_load_dataset
from dqrp.storage.registry import PostgresExperimentRegistry

FIXTURES = PROJECT_ROOT / "data" / "fixtures"


class TestContractsAndDataset(unittest.TestCase):
    def test_valid_jsonl_manifest_and_quality_metrics(self) -> None:
        loaded = validate_and_load_dataset(FIXTURES / "equities_synthetic_v1" / "manifest.json")
        self.assertEqual(loaded.manifest.dataset_id, "synthetic-equities-daily")
        self.assertEqual(len(loaded.bars), 504)
        self.assertAlmostEqual(loaded.completeness_ratio, 1.0, places=8)
        self.assertAlmostEqual(loaded.freshness_lag_seconds, 45.0, places=6)
        self.assertEqual(len(loaded.verified_partition_hashes), 2)

    def test_valid_csv_manifest_loads_identically(self) -> None:
        loaded = validate_and_load_dataset(FIXTURES / "equities_synthetic_csv_v1" / "manifest.json")
        self.assertEqual(loaded.manifest.format, "csv")
        self.assertEqual(len(loaded.bars), 60)
        self.assertEqual(loaded.bars[0].symbol, "SYM_A")

    def test_invalid_schema_version_rejected(self) -> None:
        with self.assertRaises(UnsupportedOperationError) as ctx:
            validate_and_load_dataset(FIXTURES / "invalid" / "manifest_bad_schema.json")
        self.assertEqual(ctx.exception.code, "UNSUPPORTED_SCHEMA_VERSION")

    def test_point_in_time_violation_rejected(self) -> None:
        with self.assertRaises(ContractValidationError) as ctx:
            validate_and_load_dataset(FIXTURES / "invalid" / "manifest_pit_violation.json")
        self.assertEqual(ctx.exception.code, "POINT_IN_TIME_VIOLATION")

    def test_unsupported_strategy_rejected(self) -> None:
        with self.assertRaises(UnsupportedOperationError) as ctx:
            load_experiment_spec(FIXTURES / "invalid" / "spec_unsupported_strategy.json")
        self.assertEqual(ctx.exception.code, "UNSUPPORTED_STRATEGY")

    def test_corrupted_partition_hash_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dst = Path(tmp) / "oracle"
            shutil.copytree(FIXTURES / "oracle", dst)
            # Corrupt one byte inside the data file while keeping file size identical
            data_file = dst / "oracle_bars.jsonl"
            content = data_file.read_text(encoding="utf-8")
            corrupted = content.replace("10000.0", "10001.0", 1)
            data_file.write_text(corrupted, encoding="utf-8")
            with self.assertRaises(ContractValidationError) as ctx:
                validate_and_load_dataset(dst / "manifest.json")
            self.assertEqual(ctx.exception.code, "PARTITION_HASH_MISMATCH")

    def test_market_bar_validation_errors(self) -> None:
        # Timezone-naive timestamp
        with self.assertRaises(ContractValidationError) as ctx1:
            MarketBar(
                symbol="TEST",
                event_time="2025-01-01T10:00:00",
                availability_time="2025-01-01T10:01:00Z",
                open=10.0,
                high=11.0,
                low=9.0,
                close=10.5,
                volume=100.0,
            )
        self.assertEqual(ctx1.exception.code, "TIMEZONE_NAIVE_TIMESTAMP")

        # Invalid OHLC envelope (high < close)
        with self.assertRaises(ContractValidationError) as ctx2:
            MarketBar(
                symbol="TEST",
                event_time="2025-01-01T10:00:00Z",
                availability_time="2025-01-01T10:01:00Z",
                open=10.0,
                high=9.5,
                low=9.0,
                close=10.5,
                volume=100.0,
            )
        self.assertEqual(ctx2.exception.code, "INVALID_OHLC_ENVELOPE")

    def test_boundary_flat_two_bar_dataset(self) -> None:
        loaded = validate_and_load_dataset(FIXTURES / "boundary" / "manifest.json")
        outcome = run_strategy_backtest(
            loaded.bars,
            strategy_name="sma_crossover",
            params={"symbol": "FLAT", "fast_window": 1, "slow_window": 2, "bootstrap_draws": 10},
            seed=42,
        )
        self.assertEqual(outcome.metrics.bars_evaluated, 2)
        self.assertEqual(outcome.metrics.total_return, 0.0)
        self.assertEqual(outcome.metrics.sharpe_ratio, 0.0)
        self.assertEqual(outcome.metrics.max_drawdown, 0.0)
        self.assertEqual(outcome.metrics.trade_count, 0)

    def test_dataset_hash_mismatch_rejected_before_run(self) -> None:
        spec = ExperimentSpec(
            schema_version=SCHEMA_VERSION,
            experiment_id="exp-mismatch",
            name="Mismatched Dataset Hash",
            dataset_manifest_path=str(FIXTURES / "oracle" / "manifest.json"),
            expected_dataset_sha256="a" * 64,
            strategy_name="sma_crossover",
            base_seed=1,
            budget=ResourceBudget(max_trials=1, max_concurrency=1),
            explicit_trials=({"symbol": "ORACLE", "fast_window": 2, "slow_window": 3},),
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ContractValidationError) as ctx:
                ExperimentOrchestrator().run(spec, options=RunOptions(output_dir=Path(tmp)))
            self.assertEqual(ctx.exception.code, "DATASET_HASH_MISMATCH")

    def test_missing_config_and_unavailable_optional_backends_fail_explicitly(self) -> None:
        with self.assertRaises(ContractValidationError) as ctx:
            load_experiment_spec(PROJECT_ROOT / "config" / "does_not_exist.json")
        self.assertEqual(ctx.exception.code, "SPEC_NOT_FOUND")

        spec = load_experiment_spec(PROJECT_ROOT / "config" / "oracle_experiment.json")
        with tempfile.TemporaryDirectory() as tmp:
            for optional_backend in ("ray", "dask"):
                try:
                    __import__(optional_backend)
                    continue
                except ImportError:
                    pass
                with self.assertRaises(BackendUnavailableError):
                    ExperimentOrchestrator().run(
                        spec,
                        options=RunOptions(backend=optional_backend, output_dir=Path(tmp)),
                        spec_file_path=PROJECT_ROOT / "config" / "oracle_experiment.json",
                    )

        try:
            __import__("psycopg")
        except ImportError:
            with self.assertRaises(BackendUnavailableError):
                PostgresExperimentRegistry("postgresql://localhost/nonexistent")

    def test_parquet_missing_dependency_fails_explicitly(self) -> None:
        try:
            __import__("pyarrow")
            self.skipTest("pyarrow is installed in this environment")
        except ImportError:
            pass
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            fake_pq = tmp_dir / "part-000.parquet"
            fake_pq.write_bytes(b"PAR1fakePAR1")
            from dqrp.hashing import sha256_file

            manifest = DatasetManifest(
                schema_version=SCHEMA_VERSION,
                dataset_id="parquet-test",
                version="1.0.0",
                format="parquet",
                source_name="synthetic",
                source_lineage="unit-test",
                created_at="2025-01-01T00:00:00Z",
                expected_row_count=1,
                symbols=("SYM_A",),
                partitions=(
                    {
                        "partition_id": "part-000",
                        "relative_path": "part-000.parquet",
                        "row_count": 1,
                        "content_sha256": sha256_file(fake_pq),
                        "min_event_time": "2025-01-01T00:00:00Z",
                        "max_event_time": "2025-01-01T00:00:00Z",
                        "max_availability_time": "2025-01-01T00:00:00Z",
                        "size_bytes": fake_pq.stat().st_size,
                    },
                ),
            ) if False else None
            # Build via from_dict for clean validation
            manifest_dict = {
                "schema_version": SCHEMA_VERSION,
                "dataset_id": "parquet-test",
                "version": "1.0.0",
                "format": "parquet",
                "source_name": "synthetic",
                "source_lineage": "unit-test",
                "created_at": "2025-01-01T00:00:00Z",
                "expected_row_count": 1,
                "symbols": ["SYM_A"],
                "partitions": [
                    {
                        "partition_id": "part-000",
                        "relative_path": "part-000.parquet",
                        "row_count": 1,
                        "content_sha256": sha256_file(fake_pq),
                        "min_event_time": "2025-01-01T00:00:00Z",
                        "max_event_time": "2025-01-01T00:00:00Z",
                        "max_availability_time": "2025-01-01T00:00:00Z",
                        "size_bytes": fake_pq.stat().st_size,
                    }
                ],
            }
            mpath = tmp_dir / "manifest.json"
            mpath.write_text(json.dumps(manifest_dict), encoding="utf-8")
            with self.assertRaises(BackendUnavailableError) as ctx:
                validate_and_load_dataset(mpath)
            self.assertEqual(ctx.exception.code, "PARQUET_BACKEND_UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()
