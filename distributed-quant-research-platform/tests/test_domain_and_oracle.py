"""Domain correctness, hand-calculated oracle verification, and scheduler fault unit tests.

Covers required unit test scenarios:
- Reference oracle verification within declared tolerance (1e-8)
- Worker crash detection and retry transition
- Duplicate task submission and duplicate completion suppression
- Partial / corrupted artifact detection, quarantine, and recovery
- Bounded concurrency invariant and deterministic order-independent aggregation
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dqrp.contracts import (
    SCHEMA_VERSION,
    ArtifactRecord,
    JobContract,
    ResourceBudget,
    RunStatus,
    TrialMetrics,
    TrialResult,
    TrialStatus,
)
from dqrp.domain.backtest import run_strategy_backtest
from dqrp.domain.scheduler import ExperimentStateMachine
from dqrp.errors import ArtifactIntegrityError, ContractValidationError, DuplicateTaskError
from dqrp.storage.artifacts import ArtifactStore
from dqrp.storage.dataset import validate_and_load_dataset

FIXTURES = PROJECT_ROOT / "data" / "fixtures"


def _make_contract(idx: int) -> JobContract:
    return JobContract(
        schema_version=SCHEMA_VERSION,
        trial_id=f"trial-{idx:04d}-{'a' * 12}",
        experiment_id="exp-unit",
        experiment_sha256="1" * 64,
        dataset_sha256="2" * 64,
        trial_index=idx,
        strategy_name="sma_crossover",
        params={"symbol": "ORACLE", "fast_window": 2, "slow_window": 3 + idx},
        seed=100 + idx,
        attempt=1,
    )


def _dummy_metrics(sharpe: float) -> TrialMetrics:
    return TrialMetrics(
        total_return=0.05,
        annualized_return=0.10,
        annualized_volatility=0.12,
        sharpe_ratio=sharpe,
        max_drawdown=0.04,
        win_rate=0.55,
        trade_count=4,
        final_equity=105000.0,
        turnover=4.0,
        bars_evaluated=252,
    )


class TestDomainAndOracle(unittest.TestCase):
    def test_hand_calculated_oracle_matches_within_tolerance(self) -> None:
        loaded = validate_and_load_dataset(FIXTURES / "oracle" / "manifest.json")
        oracle = json.loads((FIXTURES / "oracle" / "expected_oracle.json").read_text(encoding="utf-8"))
        tol = float(oracle["tolerance"])

        outcome = run_strategy_backtest(
            loaded.bars,
            strategy_name=oracle["strategy_name"],
            params=oracle["params"],
            seed=42,
        )
        actual = outcome.metrics.to_dict()
        for key, expected_val in oracle["expected_metrics"].items():
            self.assertAlmostEqual(
                float(actual[key]),
                float(expected_val),
                delta=tol,
                msg=f"Oracle metric '{key}' deviated beyond tolerance {tol}",
            )

    def test_point_in_time_cutoff_prevents_lookahead(self) -> None:
        loaded = validate_and_load_dataset(FIXTURES / "oracle" / "manifest.json")
        # Cut off after bar 3's availability_time -> only first 4 bars should be visible
        cutoff = loaded.bars[3].availability_time
        outcome = run_strategy_backtest(
            loaded.bars,
            strategy_name="sma_crossover",
            params={"symbol": "ORACLE", "fast_window": 2, "slow_window": 3},
            seed=42,
            evaluation_as_of=cutoff,
        )
        self.assertEqual(outcome.metrics.bars_evaluated, 4)

    def test_all_strategies_and_seeded_bootstrap_reproducibility(self) -> None:
        loaded = validate_and_load_dataset(FIXTURES / "equities_synthetic_v1" / "manifest.json")
        for strategy, params in [
            ("sma_crossover", {"symbol": "SYM_A", "fast_window": 5, "slow_window": 20, "bootstrap_draws": 100}),
            ("momentum_zscore", {"symbol": "SYM_A", "lookback_window": 10, "entry_z": 1.0, "bootstrap_draws": 100}),
            ("mean_reversion", {"symbol": "SYM_B", "lookback_window": 15, "band_std": 1.25, "bootstrap_draws": 100}),
        ]:
            out1 = run_strategy_backtest(loaded.bars, strategy_name=strategy, params=params, seed=777)
            out2 = run_strategy_backtest(loaded.bars, strategy_name=strategy, params=params, seed=777)
            self.assertEqual(out1.metrics, out2.metrics)
            self.assertIsNotNone(out1.metrics.mc_sharpe_p05)
            self.assertIsNotNone(out1.metrics.mc_sharpe_p95)
            self.assertLessEqual(out1.metrics.mc_sharpe_p05, out1.metrics.mc_sharpe_p95)

        # Invalid strategy parameter raises ContractValidationError
        with self.assertRaises(ContractValidationError):
            run_strategy_backtest(
                loaded.bars,
                strategy_name="sma_crossover",
                params={"symbol": "SYM_A", "fast_window": 20, "slow_window": 10},
                seed=1,
            )

    def test_scheduler_worker_crash_retry_and_recovery(self) -> None:
        c0 = _make_contract(0)
        sm = ExperimentStateMachine([c0], budget=ResourceBudget(max_trials=5, max_concurrency=2), max_retries=2, retry_backoff_seconds=0.05)

        dispatched = sm.next_dispatchable(now_monotonic=1.0, elapsed_wall_seconds=0.1)
        self.assertEqual(len(dispatched), 1)
        self.assertEqual(dispatched[0].attempt, 1)

        # Simulate worker crash on attempt 1
        crash_res = TrialResult(
            trial_id=c0.trial_id,
            trial_index=0,
            attempt=1,
            status=TrialStatus.FAILED,
            worker_id="dist-worker-0",
            input_sha256=c0.input_sha256,
            params=c0.params,
            seed=c0.seed,
            started_at="2025-01-01T00:00:00Z",
            completed_at="2025-01-01T00:00:01Z",
            cpu_seconds=0.01,
            wall_seconds=0.02,
            peak_memory_mb=20.0,
            error_code="WORKER_CRASH_ERROR",
            error_message="Worker exited with code -9",
            retryable=True,
        )
        tr = sm.record_result(crash_res, now_monotonic=1.02, elapsed_wall_seconds=0.12)
        self.assertEqual(tr, "RETRY_SCHEDULED")
        self.assertEqual(sm.counters["worker_crashes_detected"], 1)
        self.assertEqual(sm.counters["tasks_retried"], 1)

        # Before backoff elapses, nothing dispatches
        self.assertEqual(sm.next_dispatchable(now_monotonic=1.04, elapsed_wall_seconds=0.14), [])

        # After backoff elapses, attempt 2 dispatches and succeeds
        dispatched2 = sm.next_dispatchable(now_monotonic=1.10, elapsed_wall_seconds=0.20)
        self.assertEqual(len(dispatched2), 1)
        self.assertEqual(dispatched2[0].attempt, 2)

        ok_res = TrialResult(
            trial_id=c0.trial_id,
            trial_index=0,
            attempt=2,
            status=TrialStatus.SUCCEEDED,
            worker_id="dist-worker-1",
            input_sha256=c0.input_sha256,
            params=c0.params,
            seed=c0.seed,
            started_at="2025-01-01T00:00:02Z",
            completed_at="2025-01-01T00:00:03Z",
            cpu_seconds=0.02,
            wall_seconds=0.02,
            peak_memory_mb=22.0,
            metrics=_dummy_metrics(1.25),
            artifact=ArtifactRecord(c0.trial_id, c0.input_sha256, "f" * 64, "trials/x.json", 128),
        )
        self.assertEqual(sm.record_result(ok_res, now_monotonic=1.15, elapsed_wall_seconds=0.25), "SUCCEEDED")
        self.assertEqual(sm.final_run_status(), RunStatus.COMPLETED)
        agg = sm.aggregate()
        self.assertEqual(agg.duplicate_completed_trials, 0)
        self.assertEqual(agg.best_trial_id, c0.trial_id)

    def test_scheduler_duplicate_task_and_duplicate_completion(self) -> None:
        c0 = _make_contract(0)
        # Duplicate task in contract list rejected immediately
        with self.assertRaises(DuplicateTaskError):
            ExperimentStateMachine([c0, c0], budget=ResourceBudget(max_trials=5, max_concurrency=2))

        sm = ExperimentStateMachine([c0], budget=ResourceBudget(max_trials=5, max_concurrency=2))
        batch = sm.next_dispatchable(now_monotonic=0.0, elapsed_wall_seconds=0.0)
        self.assertEqual(len(batch), 1)
        # Guard rejects re-submission while in-flight
        self.assertTrue(sm.reject_duplicate_submission(c0.trial_id))
        self.assertEqual(sm.counters["duplicate_tasks_suppressed"], 1)

        ok_res = TrialResult(
            trial_id=c0.trial_id,
            trial_index=0,
            attempt=1,
            status=TrialStatus.SUCCEEDED,
            worker_id="w0",
            input_sha256=c0.input_sha256,
            params=c0.params,
            seed=c0.seed,
            started_at="2025-01-01T00:00:00Z",
            completed_at="2025-01-01T00:00:01Z",
            cpu_seconds=0.01,
            wall_seconds=0.01,
            peak_memory_mb=15.0,
            metrics=_dummy_metrics(1.5),
            artifact=ArtifactRecord(c0.trial_id, c0.input_sha256, "e" * 64, "trials/x.json", 100),
        )
        self.assertEqual(sm.record_result(ok_res, now_monotonic=0.1, elapsed_wall_seconds=0.1), "SUCCEEDED")
        # Duplicate completion delivery (same attempt and spoofed attempt 2) must be ignored
        self.assertEqual(sm.record_result(ok_res, now_monotonic=0.2, elapsed_wall_seconds=0.2), "DUPLICATE_IGNORED")
        dup_attempt2 = TrialResult.from_dict({**ok_res.to_dict(), "attempt": 2})
        self.assertEqual(sm.record_result(dup_attempt2, now_monotonic=0.3, elapsed_wall_seconds=0.3), "DUPLICATE_IGNORED")
        self.assertEqual(sm.counters["duplicate_completions_ignored"], 2)
        self.assertEqual(sm.counters["tasks_completed"], 1)
        agg = sm.aggregate()
        self.assertEqual(agg.duplicate_completed_trials, 0)

    def test_partial_artifact_detection_quarantine_and_recovery(self) -> None:
        c0 = _make_contract(0)
        with tempfile.TemporaryDirectory() as tmp:
            store = ArtifactStore(tmp)
            # Inject partial/corrupted artifact files
            store.write_partial_corrupt_artifact(c0)
            quarantined_tmps = store.sweep_partial_temp_files()
            self.assertEqual(len(quarantined_tmps), 1)

            with self.assertRaises(ArtifactIntegrityError):
                store.verify_and_load(c0)

            # Corrupt target + sidecar were quarantined and removed from active trials dir
            self.assertFalse(store.artifact_path(c0.trial_id).exists())
            self.assertGreaterEqual(len(list(store.quarantine_dir.iterdir())), 3)

            # Subsequent valid atomic commit succeeds and verifies cleanly
            metrics = _dummy_metrics(0.9)
            rec = store.commit_trial_artifact(
                c0.with_attempt(2),
                metrics=metrics,
                equity_curve=(("2025-01-02T21:00:00Z", 100000.0),),
            )
            verified = store.verify_and_load(c0)
            self.assertEqual(verified.record.artifact_sha256, rec.artifact_sha256)
            self.assertEqual(verified.metrics, metrics)

    def test_bounded_concurrency_and_order_independent_deterministic_aggregation(self) -> None:
        contracts = [_make_contract(i) for i in range(4)]
        budget = ResourceBudget(max_trials=4, max_concurrency=2)

        def run_with_order(completion_order: list[int]) -> str:
            sm = ExperimentStateMachine(contracts, budget=budget)
            t = 0.0
            in_flight_map = {}
            while sm.has_pending_work():
                batch = sm.next_dispatchable(t, t)
                for c in batch:
                    in_flight_map[c.trial_index] = c
                self.assertLessEqual(len(sm.in_flight_ids), budget.max_concurrency)
                # Pop next trial according to desired completion_order
                next_idx = next(idx for idx in completion_order if idx in in_flight_map)
                c = in_flight_map.pop(next_idx)
                t += 0.05
                res = TrialResult(
                    trial_id=c.trial_id,
                    trial_index=c.trial_index,
                    attempt=1,
                    status=TrialStatus.SUCCEEDED,
                    worker_id=f"w-{c.trial_index % 2}",
                    input_sha256=c.input_sha256,
                    params=c.params,
                    seed=c.seed,
                    started_at="2025-01-01T00:00:00Z",
                    completed_at="2025-01-01T00:00:01Z",
                    cpu_seconds=0.01,
                    wall_seconds=0.01,
                    peak_memory_mb=10.0,
                    metrics=_dummy_metrics(0.5 + 0.1 * c.trial_index),
                    artifact=ArtifactRecord(c.trial_id, c.input_sha256, f"{c.trial_index:064x}", "trials/t.json", 100),
                )
                sm.record_result(res, now_monotonic=t, elapsed_wall_seconds=t)
            return sm.aggregate().aggregate_sha256

        h_forward = run_with_order([0, 1, 2, 3])
        h_reverse = run_with_order([1, 0, 3, 2])
        self.assertEqual(h_forward, h_reverse)


if __name__ == "__main__":
    unittest.main()
