"""End-to-end integration, resilience, parity, replay, and CLI tests.

Covers required integration and failure scenarios:
- Budget exhaustion (max_trials and max_wall_seconds / max_estimated_cost_usd)
- Cooperative cancellation (local and distributed)
- Real subprocess hard worker crash (`os._exit(137)`), duplicate completion, partial artifact
- Local vs distributed bit-exact result parity
- Replay / resume from verified immutable artifacts + tampered cache quarantine
- CLI smoke & operational error exit codes
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dqrp.contracts import (
    SCHEMA_VERSION,
    CostModel,
    ExperimentSpec,
    ResourceBudget,
    RunStatus,
    TrialStatus,
)
from dqrp.orchestrator import ExperimentOrchestrator, RunOptions, load_experiment_spec
from dqrp.storage.registry import SqliteExperimentRegistry
from dqrp.workers.base import FaultInjectionPlan

FIXTURES = PROJECT_ROOT / "data" / "fixtures"
CONFIG = PROJECT_ROOT / "config"


def _build_small_sweep_spec(
    *,
    max_trials: int = 8,
    max_concurrency: int = 2,
    max_wall_seconds: float = 60.0,
    max_estimated_cost_usd: float = 5.0,
    max_retries: int = 2,
    bootstrap_draws: int = 80,
) -> ExperimentSpec:
    return ExperimentSpec(
        schema_version=SCHEMA_VERSION,
        experiment_id="exp-int-test",
        name="Integration Sweep",
        dataset_manifest_path=str(FIXTURES / "equities_synthetic_v1" / "manifest.json"),
        strategy_name="momentum_zscore",
        base_seed=9001,
        max_retries=max_retries,
        retry_backoff_seconds=0.01,
        budget=ResourceBudget(
            max_trials=max_trials,
            max_concurrency=max_concurrency,
            max_cpu_seconds=60.0,
            max_wall_seconds=max_wall_seconds,
            max_memory_mb=2048.0,
            max_estimated_cost_usd=max_estimated_cost_usd,
        ),
        parameter_grid={
            "symbol": ["SYM_A", "SYM_B"],
            "lookback_window": [5, 10, 20],
            "entry_z": [1.0],
            "bootstrap_draws": [bootstrap_draws],
        },
        cost_model=CostModel(),
    )


class TestIntegrationAndParity(unittest.TestCase):
    def test_local_and_distributed_result_parity(self) -> None:
        spec = _build_small_sweep_spec(max_trials=6, max_concurrency=3, bootstrap_draws=120)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reg_path = root / "registry.sqlite"
            orch = ExperimentOrchestrator()

            local_sum = orch.run(
                spec,
                options=RunOptions(backend="local", max_workers=1, output_dir=root / "runs", registry_path=reg_path),
            )
            dist_sum = orch.run(
                spec,
                options=RunOptions(backend="distributed", max_workers=3, output_dir=root / "runs", registry_path=reg_path),
            )

            self.assertEqual(local_sum.status, RunStatus.COMPLETED)
            self.assertEqual(dist_sum.status, RunStatus.COMPLETED)
            self.assertEqual(local_sum.succeeded_trials, 6)
            self.assertEqual(dist_sum.succeeded_trials, 6)
            self.assertEqual(local_sum.duplicate_completed_trials, 0)
            self.assertEqual(dist_sum.duplicate_completed_trials, 0)
            # Bit-exact parity across aggregate hash, best trial, and every individual trial artifact hash
            self.assertEqual(local_sum.aggregate_sha256, dist_sum.aggregate_sha256)
            self.assertEqual(local_sum.best_trial_id, dist_sum.best_trial_id)
            for lt, dt in zip(local_sum.trials, dist_sum.trials):
                self.assertEqual(lt.trial_id, dt.trial_id)
                self.assertEqual(lt.metrics, dt.metrics)
                self.assertEqual(lt.artifact.artifact_sha256, dt.artifact.artifact_sha256)

            reg = SqliteExperimentRegistry(reg_path)
            self.assertEqual(reg.count_duplicate_succeeded_trials(local_sum.run_id), 0)
            self.assertEqual(reg.count_duplicate_succeeded_trials(dist_sum.run_id), 0)

    def test_distributed_hard_worker_crash_duplicate_and_partial_artifact_recovery(self) -> None:
        spec = _build_small_sweep_spec(max_trials=6, max_concurrency=2, max_retries=2, bootstrap_draws=50)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reg_path = root / "registry.sqlite"
            orch = ExperimentOrchestrator()

            clean_local = orch.run(
                spec,
                options=RunOptions(backend="local", max_workers=1, output_dir=root / "clean", registry_path=reg_path),
            )

            faults = FaultInjectionPlan(
                hard_exit_attempts=((1, 1),),        # real subprocess os._exit(137) on trial 1 attempt 1
                partial_artifact_attempts=((3, 1),), # truncated/corrupt artifact on trial 3 attempt 1
                duplicate_completion_trials=(0, 2),  # duplicate completion messages for trials 0 and 2
            )
            resilient_dist = orch.run(
                spec,
                options=RunOptions(
                    backend="distributed",
                    max_workers=2,
                    output_dir=root / "faulty",
                    registry_path=reg_path,
                    fault_plan=faults,
                ),
            )

            self.assertEqual(resilient_dist.status, RunStatus.COMPLETED)
            self.assertEqual(resilient_dist.succeeded_trials, 6)
            self.assertEqual(resilient_dist.duplicate_completed_trials, 0)
            self.assertGreaterEqual(resilient_dist.counters["worker_crashes_detected"], 1)
            self.assertGreaterEqual(resilient_dist.counters["partial_artifacts_rejected"], 1)
            self.assertGreaterEqual(resilient_dist.counters["duplicate_completions_ignored"], 2)
            self.assertGreaterEqual(resilient_dist.counters["tasks_retried"], 2)
            # Final logical output must match clean local baseline bit-for-bit!
            self.assertEqual(resilient_dist.aggregate_sha256, clean_local.aggregate_sha256)

            reg = SqliteExperimentRegistry(reg_path)
            self.assertEqual(reg.count_duplicate_succeeded_trials(resilient_dist.run_id), 0)

    def test_budget_exhaustion_max_trials_and_wall_clock(self) -> None:
        # 1. max_trials budget exhaustion (2 of 6 trials allowed)
        spec_trials = _build_small_sweep_spec(max_trials=2, max_concurrency=2, bootstrap_draws=30)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            orch = ExperimentOrchestrator()
            summary = orch.run(
                spec_trials,
                options=RunOptions(
                    backend="local",
                    max_workers=2,
                    output_dir=root / "runs",
                    registry_path=root / "reg.sqlite",
                ),
            )
            self.assertEqual(summary.status, RunStatus.BUDGET_EXHAUSTED)
            self.assertEqual(summary.succeeded_trials, 2)
            self.assertEqual(summary.skipped_budget_trials, 4)
            self.assertEqual(summary.duplicate_completed_trials, 0)
            self.assertGreaterEqual(summary.counters["budget_stops"], 1)

        # 2. Wall-clock budget exhaustion mid-run with artificial delay
        spec_wall = _build_small_sweep_spec(max_trials=6, max_concurrency=1, max_wall_seconds=0.08, bootstrap_draws=10)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            orch = ExperimentOrchestrator()
            summary_w = orch.run(
                spec_wall,
                options=RunOptions(
                    backend="local",
                    max_workers=1,
                    output_dir=root / "runs",
                    fault_plan=FaultInjectionPlan(delay_seconds_per_trial=0.05),
                ),
            )
            self.assertEqual(summary_w.status, RunStatus.BUDGET_EXHAUSTED)
            self.assertLess(summary_w.succeeded_trials, 6)
            self.assertGreater(summary_w.skipped_budget_trials, 0)

    def test_cooperative_cancellation_local_and_distributed(self) -> None:
        spec = _build_small_sweep_spec(max_trials=6, max_concurrency=2, bootstrap_draws=30)
        for backend in ("local", "distributed"):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cancel_ev = threading.Event()

                def _cancel_after_first(res, scheduler) -> None:
                    if res.status == TrialStatus.SUCCEEDED:
                        cancel_ev.set()

                orch = ExperimentOrchestrator()
                summary = orch.run(
                    spec,
                    options=RunOptions(
                        backend=backend,
                        max_workers=2 if backend == "distributed" else 1,
                        output_dir=root / "runs",
                        registry_path=root / "reg.sqlite",
                        cancel_event=cancel_ev,
                        on_trial_complete=_cancel_after_first,
                        fault_plan=FaultInjectionPlan(delay_seconds_per_trial=0.04),
                    ),
                )
                self.assertEqual(summary.status, RunStatus.CANCELLED, msg=f"backend={backend}")
                self.assertGreaterEqual(summary.succeeded_trials, 1)
                self.assertGreater(summary.cancelled_trials, 0)
                self.assertEqual(summary.duplicate_completed_trials, 0)

    def test_resume_reuses_verified_artifacts_and_recomputes_tampered_ones(self) -> None:
        spec = _build_small_sweep_spec(max_trials=6, max_concurrency=2, bootstrap_draws=40)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            orch = ExperimentOrchestrator()

            # First run cancelled after 2 completed trials
            cancel_ev = threading.Event()
            completed_count = {"n": 0}

            def _stop_at_two(res, scheduler) -> None:
                if res.status == TrialStatus.SUCCEEDED:
                    completed_count["n"] += 1
                    if completed_count["n"] >= 2:
                        cancel_ev.set()

            first = orch.run(
                spec,
                options=RunOptions(
                    backend="local",
                    max_workers=1,
                    output_dir=root / "runs",
                    resume_from_artifacts=True,
                    cancel_event=cancel_ev,
                    on_trial_complete=_stop_at_two,
                ),
            )
            self.assertEqual(first.status, RunStatus.CANCELLED)
            self.assertEqual(first.succeeded_trials, 2)

            # Tamper with one of the two cached artifact files on disk
            cached_dir = (
                root
                / "runs"
                / "artifacts"
                / f"{spec.spec_sha256[:12]}-{first.dataset_sha256[:12]}"
                / "trials"
            )
            cached_files = sorted(cached_dir.glob("*.json"))
            self.assertEqual(len(cached_files), 2)
            cached_files[0].write_text("{corrupted_json", encoding="utf-8")

            # Resume run: 1 valid cached artifact should hit cache, 1 tampered should be quarantined & recomputed
            resumed = orch.run(
                spec,
                options=RunOptions(
                    backend="local",
                    max_workers=1,
                    output_dir=root / "runs",
                    resume_from_artifacts=True,
                ),
            )
            self.assertEqual(resumed.status, RunStatus.COMPLETED)
            self.assertEqual(resumed.succeeded_trials, 6)
            self.assertEqual(resumed.counters["artifact_cache_hits"], 1)

            # Compare with a completely fresh run
            fresh = orch.run(
                spec,
                options=RunOptions(backend="local", max_workers=1, output_dir=root / "fresh"),
            )
            self.assertEqual(resumed.aggregate_sha256, fresh.aggregate_sha256)

    def test_cli_validate_run_and_registry_commands(self) -> None:
        env = {"PYTHONPATH": str(PROJECT_ROOT / "src")}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reg_path = root / "cli_registry.sqlite"
            export_path = root / "registry_export.json"

            # 1. validate-manifest
            p1 = subprocess.run(
                [sys.executable, "-m", "dqrp", "validate-manifest", str(FIXTURES / "oracle" / "manifest.json")],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
            self.assertEqual(p1.returncode, 0, msg=p1.stderr)
            self.assertEqual(json.loads(p1.stdout)["status"], "VALID")

            # 2. run oracle experiment
            p2 = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "dqrp",
                    "run",
                    str(CONFIG / "oracle_experiment.json"),
                    "--backend",
                    "local",
                    "--output-dir",
                    str(root / "runs"),
                    "--registry",
                    str(reg_path),
                ],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
            self.assertEqual(p2.returncode, 0, msg=p2.stderr)
            run_out = json.loads(p2.stdout)
            self.assertEqual(run_out["status"], "COMPLETED")
            self.assertEqual(run_out["duplicate_completed_trials"], 0)

            # 3. registry export
            p3 = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "dqrp",
                    "registry",
                    "--registry",
                    str(reg_path),
                    "export",
                    "--output",
                    str(export_path),
                ],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
            self.assertEqual(p3.returncode, 0, msg=p3.stderr)
            self.assertTrue(export_path.exists())
            self.assertEqual(json.loads(export_path.read_text(encoding="utf-8"))["run_count"], 1)

            # 4. invalid manifest fails with non-zero exit code and structured JSON error
            p4 = subprocess.run(
                [sys.executable, "-m", "dqrp", "validate-manifest", str(FIXTURES / "invalid" / "manifest_bad_schema.json")],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
            self.assertNotEqual(p4.returncode, 0)
            self.assertEqual(json.loads(p4.stderr)["error_code"], "UNSUPPORTED_SCHEMA_VERSION")


if __name__ == "__main__":
    unittest.main()
