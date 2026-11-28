"""Experiment orchestrator connecting domain state machine, adapters, artifacts, and registry."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import threading
import time
from typing import Callable, Optional, Union

from dqrp.contracts import (
    SCHEMA_VERSION,
    SUPPORTED_BACKENDS,
    ExperimentSpec,
    ExperimentSummary,
    TrialResult,
    TrialStatus,
    utc_now_iso,
)
from dqrp.domain.scheduler import ExperimentStateMachine
from dqrp.errors import ContractValidationError, UnsupportedOperationError
from dqrp.hashing import sha256_canonical
from dqrp.storage.artifacts import ArtifactStore
from dqrp.storage.dataset import LoadedDataset, validate_and_load_dataset
from dqrp.storage.registry import ExperimentRegistry, SqliteExperimentRegistry
from dqrp.telemetry import StructuredLogger, collect_runtime_environment
from dqrp.workers.base import FaultInjectionPlan, WorkerAdapter
from dqrp.workers.distributed import DistributedProcessWorkerAdapter
from dqrp.workers.local import LocalWorkerAdapter
from dqrp.workers.optional_backends import DaskWorkerAdapter, RayWorkerAdapter


def load_experiment_spec(spec_path: Union[str, Path]) -> ExperimentSpec:
    """Load and validate an ExperimentSpec JSON file."""
    path = Path(spec_path)
    if not path.exists():
        raise ContractValidationError(
            f"Experiment spec not found: {path}",
            code="SPEC_NOT_FOUND",
            details={"path": str(path)},
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ContractValidationError(
            f"Invalid JSON in experiment spec {path}: {exc}",
            code="INVALID_JSON",
        ) from exc
    if not isinstance(raw, dict):
        raise ContractValidationError("Experiment spec must be a JSON object", code="INVALID_SPEC_TYPE")
    return ExperimentSpec.from_dict(raw)


def resolve_manifest_path(spec: ExperimentSpec, spec_file_path: Optional[Union[str, Path]] = None) -> Path:
    raw = Path(spec.dataset_manifest_path)
    if raw.is_absolute():
        return raw
    if spec_file_path is not None:
        candidate = (Path(spec_file_path).resolve().parent / raw).resolve()
        if candidate.exists():
            return candidate
    return raw.resolve()


@dataclass
class RunOptions:
    """Execution options for running an experiment."""

    backend: str = "local"
    max_workers: Optional[int] = None
    output_dir: Path = Path("reports/runs")
    registry_path: Optional[Path] = None
    resume_from_artifacts: bool = False
    run_id: Optional[str] = None
    poll_interval_seconds: float = 0.02
    fault_plan: Optional[FaultInjectionPlan] = None
    cancel_event: Optional[threading.Event] = None
    on_trial_complete: Optional[Callable[[TrialResult, ExperimentStateMachine], None]] = None


class ExperimentOrchestrator:
    """High-level reproducible experiment runner.

    Coordinates dataset validation, immutable JobContract generation, artifact
    verification/caching, bounded worker scheduling, retries, cancellation,
    budget enforcement, deterministic aggregation, and registry persistence.
    """

    def __init__(
        self,
        *,
        registry: Optional[ExperimentRegistry] = None,
    ) -> None:
        self._registry = registry

    def _build_worker_adapter(
        self,
        *,
        backend: str,
        dataset: LoadedDataset,
        artifact_root: Path,
        spec: ExperimentSpec,
        max_workers: int,
        fault_plan: Optional[FaultInjectionPlan],
    ) -> WorkerAdapter:
        if backend not in SUPPORTED_BACKENDS:
            raise UnsupportedOperationError(
                f"Unsupported worker backend {backend!r}; supported: {SUPPORTED_BACKENDS}",
                code="UNSUPPORTED_BACKEND",
            )
        if backend == "local":
            return LocalWorkerAdapter(
                bars=dataset.bars,
                artifact_root=artifact_root,
                cost_model=spec.cost_model,
                max_workers=max_workers,
                fault_plan=fault_plan,
            )
        if backend == "distributed":
            return DistributedProcessWorkerAdapter(
                bars=dataset.bars,
                artifact_root=artifact_root,
                cost_model=spec.cost_model,
                max_workers=max_workers,
                fault_plan=fault_plan,
            )
        if backend == "ray":
            return RayWorkerAdapter(
                bars=dataset.bars,
                artifact_root=artifact_root,
                cost_model=spec.cost_model,
                max_workers=max_workers,
                fault_plan=fault_plan,
            )
        if backend == "dask":
            return DaskWorkerAdapter(
                bars=dataset.bars,
                artifact_root=artifact_root,
                cost_model=spec.cost_model,
                max_workers=max_workers,
                fault_plan=fault_plan,
            )
        raise UnsupportedOperationError(f"Unsupported backend: {backend!r}")

    def run(
        self,
        spec: ExperimentSpec,
        *,
        options: RunOptions,
        spec_file_path: Optional[Union[str, Path]] = None,
        manifest_path_override: Optional[Union[str, Path]] = None,
    ) -> ExperimentSummary:
        """Execute an experiment end-to-end and return a deterministic ExperimentSummary."""
        manifest_path = (
            Path(manifest_path_override).resolve()
            if manifest_path_override is not None
            else resolve_manifest_path(spec, spec_file_path)
        )
        dataset = validate_and_load_dataset(manifest_path)
        contracts = spec.build_job_contracts(dataset.manifest.manifest_sha256)

        started_at = utc_now_iso()
        start_mono = time.perf_counter()

        run_id = options.run_id or self._generate_run_id(
            spec=spec,
            dataset_sha256=dataset.manifest.manifest_sha256,
            backend=options.backend,
            started_at=started_at,
        )

        # Shared artifact cache directory keyed by experiment+dataset hashes if resuming,
        # otherwise isolated per run_id so clean runs never silently reuse stale artifacts.
        if options.resume_from_artifacts:
            artifact_root = options.output_dir / "artifacts" / f"{spec.spec_sha256[:12]}-{dataset.manifest.manifest_sha256[:12]}"
        else:
            artifact_root = options.output_dir / run_id / "artifacts"

        run_dir = options.output_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        artifact_store = ArtifactStore(artifact_root)
        quarantined_tmps = artifact_store.sweep_partial_temp_files()

        logger = StructuredLogger(run_dir / "events.jsonl")
        logger.emit(
            "run_started",
            run_id=run_id,
            experiment_id=spec.experiment_id,
            experiment_sha256=spec.spec_sha256,
            dataset_id=dataset.manifest.dataset_id,
            dataset_sha256=dataset.manifest.manifest_sha256,
            backend=options.backend,
            total_trials=len(contracts),
            quarantined_temp_files=len(quarantined_tmps),
        )

        registry: Optional[ExperimentRegistry] = self._registry
        if registry is None and options.registry_path is not None:
            registry = SqliteExperimentRegistry(options.registry_path)

        effective_workers = (
            min(options.max_workers, spec.budget.max_concurrency)
            if options.max_workers is not None
            else spec.budget.max_concurrency
        )
        effective_workers = max(1, effective_workers)

        scheduler = ExperimentStateMachine(
            contracts,
            budget=spec.budget,
            max_retries=spec.max_retries,
            retry_backoff_seconds=spec.retry_backoff_seconds,
        )
        if quarantined_tmps:
            scheduler.counters["partial_artifacts_rejected"] += len(quarantined_tmps)

        # Replay / resume support: verify and adopt already-committed immutable artifacts
        if options.resume_from_artifacts:
            for contract in contracts:
                cached = artifact_store.try_load_cached(contract)
                if cached is not None:
                    now_ts = utc_now_iso()
                    cached_result = TrialResult(
                        trial_id=contract.trial_id,
                        trial_index=contract.trial_index,
                        attempt=1,
                        status=TrialStatus.SUCCEEDED,
                        worker_id="artifact-cache",
                        input_sha256=contract.input_sha256,
                        params=dict(contract.params),
                        seed=contract.seed,
                        started_at=now_ts,
                        completed_at=now_ts,
                        cpu_seconds=0.0,
                        wall_seconds=0.0,
                        peak_memory_mb=0.0,
                        estimated_cost_usd=0.0,
                        metrics=cached.metrics,
                        artifact=cached.record,
                        from_cache=True,
                    )
                    scheduler.register_cached_result(cached_result)
                    logger.emit("trial_cache_hit", run_id=run_id, trial_id=contract.trial_id)

        adapter = self._build_worker_adapter(
            backend=options.backend,
            dataset=dataset,
            artifact_root=artifact_root,
            spec=spec,
            max_workers=effective_workers,
            fault_plan=options.fault_plan,
        )

        try:
            while scheduler.has_pending_work():
                now_mono = time.perf_counter()
                elapsed_wall = now_mono - start_mono

                # Check external cooperative cancellation
                if options.cancel_event is not None and options.cancel_event.is_set() and not scheduler.cancelled:
                    to_cancel = scheduler.request_cancel("Cancellation signal received")
                    logger.emit("run_cancel_requested", run_id=run_id, in_flight=to_cancel)
                    cancelled_results = adapter.cancel(to_cancel)
                    for cr in cancelled_results:
                        scheduler.record_result(cr, now_monotonic=now_mono, elapsed_wall_seconds=elapsed_wall)
                        if registry is not None:
                            registry.record_trial_attempt(run_id, cr)

                # Check wall-clock / resource budget even while tasks are in flight
                if not scheduler.budget_exhausted and not scheduler.cancelled:
                    bcheck = scheduler.check_budget(elapsed_wall)
                    if bcheck.exhausted:
                        in_flight = scheduler.trigger_budget_exhaustion(bcheck.reason or "Budget exhausted")
                        logger.emit("budget_exhausted", run_id=run_id, reason=bcheck.reason, in_flight=in_flight)
                        cancelled_results = adapter.cancel(in_flight)
                        for cr in cancelled_results:
                            scheduler.record_result(cr, now_monotonic=now_mono, elapsed_wall_seconds=elapsed_wall)
                            if registry is not None:
                                registry.record_trial_attempt(run_id, cr)

                # Dispatch bounded concurrency batch
                batch = scheduler.next_dispatchable(now_mono, elapsed_wall)
                for contract in batch:
                    adapter.submit(contract)
                    logger.emit(
                        "trial_dispatched",
                        run_id=run_id,
                        trial_id=contract.trial_id,
                        attempt=contract.attempt,
                        input_sha256=contract.input_sha256,
                    )

                if not scheduler.has_pending_work():
                    break

                # Poll completed / crashed worker results
                if scheduler.in_flight_ids:
                    results = adapter.poll_completed(timeout_seconds=options.poll_interval_seconds)
                    now_mono = time.perf_counter()
                    elapsed_wall = now_mono - start_mono
                    for res in results:
                        transition = scheduler.record_result(
                            res,
                            now_monotonic=now_mono,
                            elapsed_wall_seconds=elapsed_wall,
                        )
                        if registry is not None and transition != "DUPLICATE_IGNORED":
                            registry.record_trial_attempt(run_id, res)
                        logger.emit(
                            "trial_event",
                            run_id=run_id,
                            trial_id=res.trial_id,
                            attempt=res.attempt,
                            status=res.status.value,
                            transition=transition,
                            worker_id=res.worker_id,
                            error_code=res.error_code,
                        )
                        if options.on_trial_complete is not None and transition != "DUPLICATE_IGNORED":
                            options.on_trial_complete(res, scheduler)
                        if scheduler.cancelled or scheduler.budget_exhausted:
                            remaining_in_flight = sorted(scheduler.in_flight_ids)
                            if remaining_in_flight:
                                extra_cancelled = adapter.cancel(remaining_in_flight)
                                for cr in extra_cancelled:
                                    scheduler.record_result(
                                        cr,
                                        now_monotonic=time.perf_counter(),
                                        elapsed_wall_seconds=time.perf_counter() - start_mono,
                                    )
                                    if registry is not None:
                                        registry.record_trial_attempt(run_id, cr)
                else:
                    wait_s = scheduler.next_retry_wait_seconds(now_mono)
                    if wait_s is not None and wait_s > 0.0:
                        time.sleep(min(wait_s, options.poll_interval_seconds))
        finally:
            adapter.shutdown()

        total_wall_seconds = round(max(1e-6, time.perf_counter() - start_mono), 6)
        completed_at = utc_now_iso()
        agg = scheduler.aggregate()
        run_status = scheduler.final_run_status()

        succeeded_trials = sum(1 for r in agg.ordered_results if r.status == TrialStatus.SUCCEEDED)
        failed_trials = sum(1 for r in agg.ordered_results if r.status == TrialStatus.FAILED)
        cancelled_trials = sum(1 for r in agg.ordered_results if r.status == TrialStatus.CANCELLED)
        skipped_budget_trials = sum(1 for r in agg.ordered_results if r.status == TrialStatus.SKIPPED_BUDGET)

        summary = ExperimentSummary(
            schema_version=SCHEMA_VERSION,
            run_id=run_id,
            experiment_id=spec.experiment_id,
            experiment_sha256=spec.spec_sha256,
            dataset_id=dataset.manifest.dataset_id,
            dataset_version=dataset.manifest.version,
            dataset_sha256=dataset.manifest.manifest_sha256,
            backend=options.backend,
            status=run_status,
            started_at=started_at,
            completed_at=completed_at,
            total_trials=len(agg.ordered_results),
            succeeded_trials=succeeded_trials,
            failed_trials=failed_trials,
            cancelled_trials=cancelled_trials,
            skipped_budget_trials=skipped_budget_trials,
            duplicate_completed_trials=agg.duplicate_completed_trials,
            aggregate_sha256=agg.aggregate_sha256,
            best_trial_id=agg.best_trial_id,
            best_sharpe_ratio=agg.best_sharpe_ratio,
            mean_sharpe_ratio=agg.mean_sharpe_ratio,
            mean_total_return=agg.mean_total_return,
            total_cpu_seconds=round(scheduler.total_cpu_seconds, 6),
            total_wall_seconds=total_wall_seconds,
            peak_memory_mb=round(scheduler.peak_memory_mb, 4),
            total_estimated_cost_usd=round(scheduler.total_estimated_cost_usd, 10),
            dataset_completeness_ratio=dataset.completeness_ratio,
            dataset_freshness_lag_seconds=dataset.freshness_lag_seconds,
            termination_reason=scheduler.termination_reason,
            counters=dict(scheduler.counters),
            environment=collect_runtime_environment(),
            trials=agg.ordered_results,
        )

        # Persist summary JSON to run directory
        summary_path = run_dir / "summary.json"
        summary_path.write_text(json.dumps(summary.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")

        if registry is not None:
            registry.record_run(summary)

        logger.emit(
            "run_completed",
            run_id=run_id,
            status=run_status.value,
            aggregate_sha256=summary.aggregate_sha256,
            succeeded_trials=succeeded_trials,
            failed_trials=failed_trials,
            cancelled_trials=cancelled_trials,
            skipped_budget_trials=skipped_budget_trials,
            total_wall_seconds=total_wall_seconds,
        )
        return summary

    @staticmethod
    def _generate_run_id(
        *,
        spec: ExperimentSpec,
        dataset_sha256: str,
        backend: str,
        started_at: str,
    ) -> str:
        short = sha256_canonical(
            {
                "experiment_sha256": spec.spec_sha256,
                "dataset_sha256": dataset_sha256,
                "backend": backend,
                "started_at": started_at,
            }
        )[:10]
        return f"run-{spec.experiment_id}-{backend}-{short}"
