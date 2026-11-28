"""Worker adapter protocol, shared deterministic trial executor, and fault injection."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import os
from pathlib import Path
import platform
import resource
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from dqrp.contracts import (
    CostModel,
    JobContract,
    MarketBar,
    TrialResult,
    TrialStatus,
    utc_now_iso,
)
from dqrp.domain.backtest import run_strategy_backtest
from dqrp.errors import (
    ArtifactIntegrityError,
    ContractValidationError,
    UnsupportedOperationError,
    WorkerCrashError,
)
from dqrp.storage.artifacts import ArtifactStore


@dataclass(frozen=True)
class WorkerCapabilities:
    """Explicit runtime capability advertisement for a worker backend."""

    backend_name: str
    is_distributed: bool
    max_workers: int
    supports_hard_crash_isolation: bool
    supports_cancellation: bool


@dataclass(frozen=True)
class FaultInjectionPlan:
    """Deterministic fault schedule used exclusively in tests and resilience drills.

    Keys are `(trial_index, attempt)` tuples so tests remain 100% deterministic.
    """

    crash_attempts: Tuple[Tuple[int, int], ...] = field(default_factory=tuple)
    hard_exit_attempts: Tuple[Tuple[int, int], ...] = field(default_factory=tuple)
    partial_artifact_attempts: Tuple[Tuple[int, int], ...] = field(default_factory=tuple)
    duplicate_completion_trials: Tuple[int, ...] = field(default_factory=tuple)
    delay_seconds_per_trial: float = 0.0
    extra_cpu_burn_iters: int = 0


def current_peak_rss_mb() -> float:
    """Return current process peak RSS in MiB (macOS returns bytes, Linux returns KiB)."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    raw = float(usage.ru_maxrss)
    if platform.system() == "Darwin":
        return round(raw / (1024.0 * 1024.0), 4)
    return round(raw / 1024.0, 4)


def execute_trial_core(
    contract: JobContract,
    *,
    bars: Sequence[MarketBar],
    artifact_root: Union[str, Path],
    cost_model: CostModel,
    worker_id: str,
    fault_plan: Optional[FaultInjectionPlan] = None,
) -> TrialResult:
    """Execute one immutable JobContract and atomically commit its verified artifact.

    Shared verbatim by LocalWorkerAdapter, DistributedProcessWorkerAdapter, and optional
    Ray/Dask adapters to guarantee local/distributed result parity.
    """
    started_at = utc_now_iso()
    wall_t0 = time.perf_counter()
    cpu_t0 = time.process_time()
    store = ArtifactStore(artifact_root)

    try:
        key = (contract.trial_index, contract.attempt)
        if fault_plan is not None:
            if fault_plan.delay_seconds_per_trial > 0.0:
                time.sleep(fault_plan.delay_seconds_per_trial)
            if fault_plan.extra_cpu_burn_iters > 0:
                acc = 0.0
                for k in range(fault_plan.extra_cpu_burn_iters):
                    acc += ((k * 17 + contract.seed) % 97) * 0.0001
                _ = acc
            if key in fault_plan.hard_exit_attempts:
                os._exit(137)
            if key in fault_plan.crash_attempts:
                raise WorkerCrashError(
                    f"Injected worker crash on trial_index={contract.trial_index} attempt={contract.attempt}",
                    details={"trial_id": contract.trial_id, "attempt": contract.attempt},
                )
            if key in fault_plan.partial_artifact_attempts:
                store.write_partial_corrupt_artifact(contract)
                # Verification must detect and quarantine the corrupted/partial artifact
                store.verify_and_load(contract)

        outcome = run_strategy_backtest(
            bars,
            strategy_name=contract.strategy_name,
            params=contract.params,
            seed=contract.seed,
            evaluation_as_of=contract.evaluation_as_of,
        )
        artifact_record = store.commit_trial_artifact(
            contract,
            metrics=outcome.metrics,
            equity_curve=outcome.equity_curve,
        )
        wall_s = max(1e-6, time.perf_counter() - wall_t0)
        cpu_s = max(0.0, time.process_time() - cpu_t0)
        peak_mb = current_peak_rss_mb()
        cost_usd = cost_model.estimate_trial_cost_usd(
            cpu_seconds=cpu_s,
            wall_seconds=wall_s,
            peak_memory_mb=peak_mb,
            artifact_bytes=artifact_record.size_bytes,
        )
        return TrialResult(
            trial_id=contract.trial_id,
            trial_index=contract.trial_index,
            attempt=contract.attempt,
            status=TrialStatus.SUCCEEDED,
            worker_id=worker_id,
            input_sha256=contract.input_sha256,
            params=dict(contract.params),
            seed=contract.seed,
            started_at=started_at,
            completed_at=utc_now_iso(),
            cpu_seconds=round(cpu_s, 6),
            wall_seconds=round(wall_s, 6),
            peak_memory_mb=peak_mb,
            estimated_cost_usd=cost_usd,
            metrics=outcome.metrics,
            artifact=artifact_record,
            retryable=False,
        )
    except (WorkerCrashError, ArtifactIntegrityError) as exc:
        wall_s = max(1e-6, time.perf_counter() - wall_t0)
        cpu_s = max(0.0, time.process_time() - cpu_t0)
        peak_mb = current_peak_rss_mb()
        cost_usd = cost_model.estimate_trial_cost_usd(
            cpu_seconds=cpu_s,
            wall_seconds=wall_s,
            peak_memory_mb=peak_mb,
            artifact_bytes=0,
        )
        return TrialResult(
            trial_id=contract.trial_id,
            trial_index=contract.trial_index,
            attempt=contract.attempt,
            status=TrialStatus.FAILED,
            worker_id=worker_id,
            input_sha256=contract.input_sha256,
            params=dict(contract.params),
            seed=contract.seed,
            started_at=started_at,
            completed_at=utc_now_iso(),
            cpu_seconds=round(cpu_s, 6),
            wall_seconds=round(wall_s, 6),
            peak_memory_mb=peak_mb,
            estimated_cost_usd=cost_usd,
            error_code=exc.code,
            error_message=str(exc),
            retryable=True,
        )
    except (ContractValidationError, UnsupportedOperationError) as exc:
        wall_s = max(1e-6, time.perf_counter() - wall_t0)
        cpu_s = max(0.0, time.process_time() - cpu_t0)
        return TrialResult(
            trial_id=contract.trial_id,
            trial_index=contract.trial_index,
            attempt=contract.attempt,
            status=TrialStatus.FAILED,
            worker_id=worker_id,
            input_sha256=contract.input_sha256,
            params=dict(contract.params),
            seed=contract.seed,
            started_at=started_at,
            completed_at=utc_now_iso(),
            cpu_seconds=round(cpu_s, 6),
            wall_seconds=round(wall_s, 6),
            peak_memory_mb=current_peak_rss_mb(),
            estimated_cost_usd=0.0,
            error_code=exc.code,
            error_message=str(exc),
            retryable=False,
        )
    except Exception as exc:  # pragma: no cover - defensive fallback
        wall_s = max(1e-6, time.perf_counter() - wall_t0)
        cpu_s = max(0.0, time.process_time() - cpu_t0)
        return TrialResult(
            trial_id=contract.trial_id,
            trial_index=contract.trial_index,
            attempt=contract.attempt,
            status=TrialStatus.FAILED,
            worker_id=worker_id,
            input_sha256=contract.input_sha256,
            params=dict(contract.params),
            seed=contract.seed,
            started_at=started_at,
            completed_at=utc_now_iso(),
            cpu_seconds=round(cpu_s, 6),
            wall_seconds=round(wall_s, 6),
            peak_memory_mb=current_peak_rss_mb(),
            estimated_cost_usd=0.0,
            error_code="UNEXPECTED_WORKER_ERROR",
            error_message=f"{type(exc).__name__}: {exc}",
            retryable=True,
        )


class WorkerAdapter(ABC):
    """Execution backend interface used by the orchestrator."""

    @property
    @abstractmethod
    def capabilities(self) -> WorkerCapabilities:
        raise NotImplementedError

    @abstractmethod
    def submit(self, contract: JobContract) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll_completed(self, timeout_seconds: float = 0.05) -> List[TrialResult]:
        raise NotImplementedError

    @abstractmethod
    def cancel(self, trial_ids: Sequence[str]) -> List[TrialResult]:
        raise NotImplementedError

    @abstractmethod
    def shutdown(self) -> None:
        raise NotImplementedError
