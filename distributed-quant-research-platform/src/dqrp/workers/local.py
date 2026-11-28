"""Local in-process deterministic reference worker adapter."""

from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import Deque, List, Optional, Sequence, Set, Union

from dqrp.contracts import CostModel, JobContract, MarketBar, TrialResult, TrialStatus, utc_now_iso
from dqrp.workers.base import (
    FaultInjectionPlan,
    WorkerAdapter,
    WorkerCapabilities,
    execute_trial_core,
)


class LocalWorkerAdapter(WorkerAdapter):
    """Single-process bounded worker adapter serving as the reference oracle backend."""

    def __init__(
        self,
        *,
        bars: Sequence[MarketBar],
        artifact_root: Union[str, Path],
        cost_model: CostModel,
        max_workers: int = 1,
        fault_plan: Optional[FaultInjectionPlan] = None,
    ) -> None:
        self._bars = tuple(bars)
        self._artifact_root = Path(artifact_root)
        self._cost_model = cost_model
        self._max_workers = max(1, int(max_workers))
        self._fault_plan = fault_plan
        self._queue: Deque[JobContract] = deque()
        self._cancelled_ids: Set[str] = set()

    @property
    def capabilities(self) -> WorkerCapabilities:
        return WorkerCapabilities(
            backend_name="local",
            is_distributed=False,
            max_workers=self._max_workers,
            supports_hard_crash_isolation=False,
            supports_cancellation=True,
        )

    def submit(self, contract: JobContract) -> None:
        self._queue.append(contract)

    def poll_completed(self, timeout_seconds: float = 0.05) -> List[TrialResult]:
        if not self._queue:
            return []
        # Execute one queued task per poll step so orchestrator can enforce budgets
        # and cooperative cancellation cleanly between trials.
        contract = self._queue.popleft()
        if contract.trial_id in self._cancelled_ids:
            now = utc_now_iso()
            return [
                TrialResult(
                    trial_id=contract.trial_id,
                    trial_index=contract.trial_index,
                    attempt=contract.attempt,
                    status=TrialStatus.CANCELLED,
                    worker_id="local-worker-0",
                    input_sha256=contract.input_sha256,
                    params=dict(contract.params),
                    seed=contract.seed,
                    started_at=now,
                    completed_at=now,
                    cpu_seconds=0.0,
                    wall_seconds=0.0,
                    peak_memory_mb=0.0,
                    error_code="CANCELLED",
                    error_message="Cancelled prior to local execution",
                )
            ]

        res = execute_trial_core(
            contract,
            bars=self._bars,
            artifact_root=self._artifact_root,
            cost_model=self._cost_model,
            worker_id="local-worker-0",
            fault_plan=self._fault_plan,
        )
        results = [res]
        if (
            self._fault_plan is not None
            and res.status == TrialStatus.SUCCEEDED
            and contract.trial_index in self._fault_plan.duplicate_completion_trials
        ):
            # Inject duplicate completion delivery to verify scheduler idempotency
            results.append(res)
        return results

    def cancel(self, trial_ids: Sequence[str]) -> List[TrialResult]:
        target = set(trial_ids)
        self._cancelled_ids.update(target)
        remaining: Deque[JobContract] = deque()
        cancelled_results: List[TrialResult] = []
        now = utc_now_iso()
        while self._queue:
            contract = self._queue.popleft()
            if contract.trial_id in target:
                cancelled_results.append(
                    TrialResult(
                        trial_id=contract.trial_id,
                        trial_index=contract.trial_index,
                        attempt=contract.attempt,
                        status=TrialStatus.CANCELLED,
                        worker_id="local-worker-0",
                        input_sha256=contract.input_sha256,
                        params=dict(contract.params),
                        seed=contract.seed,
                        started_at=now,
                        completed_at=now,
                        cpu_seconds=0.0,
                        wall_seconds=0.0,
                        peak_memory_mb=0.0,
                        error_code="CANCELLED",
                        error_message="Cancelled while queued on local worker",
                    )
                )
            else:
                remaining.append(contract)
        self._queue = remaining
        return cancelled_results

    def shutdown(self) -> None:
        self._queue.clear()
