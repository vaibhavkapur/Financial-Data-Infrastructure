"""Optional Ray and Dask worker adapters with explicit capability checks.

If the optional dependency (`ray` or `dask.distributed`) is not installed, constructing
the adapter raises `BackendUnavailableError` explicitly (never silently falling back).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from dqrp.contracts import CostModel, JobContract, MarketBar, TrialResult, TrialStatus, utc_now_iso
from dqrp.errors import BackendUnavailableError
from dqrp.workers.base import (
    FaultInjectionPlan,
    WorkerAdapter,
    WorkerCapabilities,
    execute_trial_core,
)


class RayWorkerAdapter(WorkerAdapter):
    """Adapter for running immutable JobContracts on a Ray cluster/local Ray runtime."""

    def __init__(
        self,
        *,
        bars: Sequence[MarketBar],
        artifact_root: Union[str, Path],
        cost_model: CostModel,
        max_workers: int = 2,
        fault_plan: Optional[FaultInjectionPlan] = None,
    ) -> None:
        try:
            import ray  # type: ignore
        except ImportError as exc:
            raise BackendUnavailableError(
                "Ray backend requested but optional package 'ray' is not installed. "
                "Install with: pip install -e '.[ray]' or run with --backend distributed.",
                code="RAY_BACKEND_UNAVAILABLE",
            ) from exc

        self._ray = ray
        self._started_ray = False
        if not ray.is_initialized():
            ray.init(num_cpus=max(1, int(max_workers)), ignore_reinit_error=True, log_to_driver=False)
            self._started_ray = True
        self._bars_ref = ray.put([b.to_dict() for b in bars])
        self._artifact_root_str = str(Path(artifact_root).resolve())
        self._cost_model_dict = cost_model.to_dict()
        self._max_workers = max(1, int(max_workers))
        self._fault_plan = fault_plan
        self._in_flight: Dict[str, Tuple[Any, JobContract]] = {}

        @ray.remote
        def _ray_run(
            contract_dict: Dict[str, Any],
            bars_dicts: List[Dict[str, Any]],
            artifact_root_s: str,
            cost_dict: Dict[str, Any],
            plan: Optional[FaultInjectionPlan],
        ) -> Dict[str, Any]:
            c = JobContract.from_dict(contract_dict)
            res = execute_trial_core(
                c,
                bars=tuple(MarketBar.from_dict(b) for b in bars_dicts),
                artifact_root=Path(artifact_root_s),
                cost_model=CostModel.from_dict(cost_dict),
                worker_id="ray-worker",
                fault_plan=plan,
            )
            return res.to_dict()

        self._remote_fn = _ray_run

    @property
    def capabilities(self) -> WorkerCapabilities:
        return WorkerCapabilities(
            backend_name="ray",
            is_distributed=True,
            max_workers=self._max_workers,
            supports_hard_crash_isolation=True,
            supports_cancellation=True,
        )

    def submit(self, contract: JobContract) -> None:
        ref = self._remote_fn.remote(
            contract.to_dict(),
            self._bars_ref,
            self._artifact_root_str,
            self._cost_model_dict,
            self._fault_plan,
        )
        self._in_flight[contract.trial_id] = (ref, contract)

    def poll_completed(self, timeout_seconds: float = 0.05) -> List[TrialResult]:
        if not self._in_flight:
            return []
        refs = [pair[0] for pair in self._in_flight.values()]
        ready, _ = self._ray.wait(refs, num_returns=len(refs), timeout=timeout_seconds)
        ready_set = set(ready)
        results: List[TrialResult] = []
        for tid, (ref, contract) in list(self._in_flight.items()):
            if ref in ready_set:
                self._in_flight.pop(tid, None)
                try:
                    raw = self._ray.get(ref)
                    results.append(TrialResult.from_dict(raw))
                except Exception as exc:
                    now = utc_now_iso()
                    results.append(
                        TrialResult(
                            trial_id=contract.trial_id,
                            trial_index=contract.trial_index,
                            attempt=contract.attempt,
                            status=TrialStatus.FAILED,
                            worker_id="ray-worker",
                            input_sha256=contract.input_sha256,
                            params=dict(contract.params),
                            seed=contract.seed,
                            started_at=now,
                            completed_at=now,
                            cpu_seconds=0.0,
                            wall_seconds=0.0,
                            peak_memory_mb=0.0,
                            error_code="WORKER_CRASH_ERROR",
                            error_message=str(exc),
                            retryable=True,
                        )
                    )
        return results

    def cancel(self, trial_ids: Sequence[str]) -> List[TrialResult]:
        cancelled: List[TrialResult] = []
        now = utc_now_iso()
        for tid in trial_ids:
            pair = self._in_flight.pop(tid, None)
            if pair is None:
                continue
            ref, contract = pair
            self._ray.cancel(ref, force=True)
            cancelled.append(
                TrialResult(
                    trial_id=contract.trial_id,
                    trial_index=contract.trial_index,
                    attempt=contract.attempt,
                    status=TrialStatus.CANCELLED,
                    worker_id="ray-worker",
                    input_sha256=contract.input_sha256,
                    params=dict(contract.params),
                    seed=contract.seed,
                    started_at=now,
                    completed_at=now,
                    cpu_seconds=0.0,
                    wall_seconds=0.0,
                    peak_memory_mb=0.0,
                    error_code="CANCELLED",
                    error_message="Ray task cancelled",
                )
            )
        return cancelled

    def shutdown(self) -> None:
        self._in_flight.clear()
        if self._started_ray:
            self._ray.shutdown()


class DaskWorkerAdapter(WorkerAdapter):
    """Adapter for running immutable JobContracts on a Dask Distributed cluster."""

    def __init__(
        self,
        *,
        bars: Sequence[MarketBar],
        artifact_root: Union[str, Path],
        cost_model: CostModel,
        max_workers: int = 2,
        fault_plan: Optional[FaultInjectionPlan] = None,
    ) -> None:
        try:
            from dask.distributed import Client  # type: ignore
        except ImportError as exc:
            raise BackendUnavailableError(
                "Dask backend requested but optional package 'dask[distributed]' is not installed. "
                "Install with: pip install -e '.[dask]' or run with --backend distributed.",
                code="DASK_BACKEND_UNAVAILABLE",
            ) from exc

        self._max_workers = max(1, int(max_workers))
        self._client = Client(n_workers=self._max_workers, threads_per_worker=1, processes=True)
        self._bars_future = self._client.scatter([b.to_dict() for b in bars], broadcast=True)
        self._artifact_root_str = str(Path(artifact_root).resolve())
        self._cost_model_dict = cost_model.to_dict()
        self._fault_plan = fault_plan
        self._in_flight: Dict[str, Tuple[Any, JobContract]] = {}

    @property
    def capabilities(self) -> WorkerCapabilities:
        return WorkerCapabilities(
            backend_name="dask",
            is_distributed=True,
            max_workers=self._max_workers,
            supports_hard_crash_isolation=True,
            supports_cancellation=True,
        )

    @staticmethod
    def _dask_run(
        contract_dict: Dict[str, Any],
        bars_dicts: List[Dict[str, Any]],
        artifact_root_s: str,
        cost_dict: Dict[str, Any],
        plan: Optional[FaultInjectionPlan],
    ) -> Dict[str, Any]:
        c = JobContract.from_dict(contract_dict)
        res = execute_trial_core(
            c,
            bars=tuple(MarketBar.from_dict(b) for b in bars_dicts),
            artifact_root=Path(artifact_root_s),
            cost_model=CostModel.from_dict(cost_dict),
            worker_id="dask-worker",
            fault_plan=plan,
        )
        return res.to_dict()

    def submit(self, contract: JobContract) -> None:
        fut = self._client.submit(
            self._dask_run,
            contract.to_dict(),
            self._bars_future,
            self._artifact_root_str,
            self._cost_model_dict,
            self._fault_plan,
            key=f"{contract.trial_id}-attempt-{contract.attempt}",
        )
        self._in_flight[contract.trial_id] = (fut, contract)

    def poll_completed(self, timeout_seconds: float = 0.05) -> List[TrialResult]:
        if not self._in_flight:
            return []
        results: List[TrialResult] = []
        for tid, (fut, contract) in list(self._in_flight.items()):
            if fut.done():
                self._in_flight.pop(tid, None)
                try:
                    raw = fut.result()
                    results.append(TrialResult.from_dict(raw))
                except Exception as exc:
                    now = utc_now_iso()
                    results.append(
                        TrialResult(
                            trial_id=contract.trial_id,
                            trial_index=contract.trial_index,
                            attempt=contract.attempt,
                            status=TrialStatus.FAILED,
                            worker_id="dask-worker",
                            input_sha256=contract.input_sha256,
                            params=dict(contract.params),
                            seed=contract.seed,
                            started_at=now,
                            completed_at=now,
                            cpu_seconds=0.0,
                            wall_seconds=0.0,
                            peak_memory_mb=0.0,
                            error_code="WORKER_CRASH_ERROR",
                            error_message=str(exc),
                            retryable=True,
                        )
                    )
        return results

    def cancel(self, trial_ids: Sequence[str]) -> List[TrialResult]:
        cancelled: List[TrialResult] = []
        now = utc_now_iso()
        for tid in trial_ids:
            pair = self._in_flight.pop(tid, None)
            if pair is None:
                continue
            fut, contract = pair
            self._client.cancel(fut, force=True)
            cancelled.append(
                TrialResult(
                    trial_id=contract.trial_id,
                    trial_index=contract.trial_index,
                    attempt=contract.attempt,
                    status=TrialStatus.CANCELLED,
                    worker_id="dask-worker",
                    input_sha256=contract.input_sha256,
                    params=dict(contract.params),
                    seed=contract.seed,
                    started_at=now,
                    completed_at=now,
                    cpu_seconds=0.0,
                    wall_seconds=0.0,
                    peak_memory_mb=0.0,
                    error_code="CANCELLED",
                    error_message="Dask task cancelled",
                )
            )
        return cancelled

    def shutdown(self) -> None:
        self._in_flight.clear()
        self._client.close()
