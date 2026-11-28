"""Pure experiment scheduling, retry, budget, cancellation, and aggregation state machine.

This module performs zero I/O and depends only on contracts, hashing, and standard types.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Set, Tuple

from dqrp.contracts import (
    JobContract,
    ResourceBudget,
    RunStatus,
    TrialResult,
    TrialStatus,
)
from dqrp.errors import DuplicateTaskError
from dqrp.hashing import compute_aggregate_hash


@dataclass
class TrialSlot:
    """Mutable scheduling state for a single trial within an experiment run."""

    contract: JobContract
    status: TrialStatus = TrialStatus.PENDING
    attempts: int = 0
    next_eligible_time: float = 0.0
    result: Optional[TrialResult] = None
    history: List[TrialResult] = field(default_factory=list)


@dataclass(frozen=True)
class BudgetCheck:
    """Result of evaluating hard budget limits."""

    exhausted: bool
    reason: Optional[str] = None


@dataclass(frozen=True)
class AggregateResult:
    """Deterministic aggregation over canonically ordered trials."""

    ordered_results: Tuple[TrialResult, ...]
    aggregate_sha256: str
    best_trial_id: Optional[str]
    best_sharpe_ratio: Optional[float]
    mean_sharpe_ratio: Optional[float]
    mean_total_return: Optional[float]
    duplicate_completed_trials: int


class ExperimentStateMachine:
    """Deterministic finite-state scheduler with bounded concurrency and idempotency.

    Guarantees:
    - Bounded scheduling: len(in_flight) <= budget.max_concurrency at every step.
    - No duplicate completed trials: once a trial_id reaches SUCCEEDED, any duplicate
      submission or completion for that trial_id is rejected/ignored and counted.
    - Retries: retryable failures (e.g., worker crash or partial artifact) are retried
      up to max_retries with bounded backoff.
    - Hard budget enforcement: max_trials, max_cpu_seconds, max_wall_seconds,
      max_memory_mb, and max_estimated_cost_usd.
    - Deterministic aggregation: results are sorted by (trial_index, trial_id), making
      aggregate_sha256 independent of worker scheduling or completion order.
    """

    def __init__(
        self,
        contracts: Sequence[JobContract],
        *,
        budget: ResourceBudget,
        max_retries: int = 2,
        retry_backoff_seconds: float = 0.01,
    ) -> None:
        self.budget = budget
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

        self._slots: Dict[str, TrialSlot] = {}
        self._order: List[str] = []
        self._in_flight: Set[str] = set()
        self._completed_ids: Set[str] = set()
        self._seen_completion_keys: Set[Tuple[str, int]] = set()

        self.total_cpu_seconds: float = 0.0
        self.peak_memory_mb: float = 0.0
        self.total_estimated_cost_usd: float = 0.0
        self.trials_dispatched_unique: Set[str] = set()

        self.cancelled: bool = False
        self.budget_exhausted: bool = False
        self.termination_reason: Optional[str] = None

        self.counters: Dict[str, int] = {
            "tasks_scheduled": 0,
            "tasks_completed": 0,
            "tasks_retried": 0,
            "tasks_failed": 0,
            "tasks_cancelled": 0,
            "tasks_skipped_budget": 0,
            "duplicate_tasks_suppressed": 0,
            "duplicate_completions_ignored": 0,
            "worker_crashes_detected": 0,
            "partial_artifacts_rejected": 0,
            "artifact_cache_hits": 0,
            "budget_stops": 0,
        }

        for contract in contracts:
            if contract.trial_id in self._slots:
                self.counters["duplicate_tasks_suppressed"] += 1
                raise DuplicateTaskError(
                    f"Duplicate trial_id in experiment contract list: {contract.trial_id}",
                    details={"trial_id": contract.trial_id},
                )
            self._slots[contract.trial_id] = TrialSlot(contract=contract)
            self._order.append(contract.trial_id)

    @property
    def in_flight_ids(self) -> Set[str]:
        return set(self._in_flight)

    @property
    def completed_ids(self) -> Set[str]:
        return set(self._completed_ids)

    def get_slot(self, trial_id: str) -> TrialSlot:
        return self._slots[trial_id]

    def register_cached_result(self, result: TrialResult) -> bool:
        """Accept a pre-verified immutable artifact from cache before scheduling."""
        slot = self._slots[result.trial_id]
        if result.trial_id in self._completed_ids:
            self.counters["duplicate_completions_ignored"] += 1
            return False
        slot.status = TrialStatus.SUCCEEDED
        slot.attempts = max(slot.attempts, result.attempt)
        slot.result = result
        slot.history.append(result)
        self._completed_ids.add(result.trial_id)
        self.counters["tasks_completed"] += 1
        self.counters["artifact_cache_hits"] += 1
        return True

    def check_budget(self, elapsed_wall_seconds: float) -> BudgetCheck:
        if elapsed_wall_seconds >= self.budget.max_wall_seconds:
            return BudgetCheck(
                True,
                f"Wall-clock budget exceeded: {elapsed_wall_seconds:.4f}s >= {self.budget.max_wall_seconds:.4f}s",
            )
        if self.total_cpu_seconds >= self.budget.max_cpu_seconds:
            return BudgetCheck(
                True,
                f"CPU budget exceeded: {self.total_cpu_seconds:.4f}s >= {self.budget.max_cpu_seconds:.4f}s",
            )
        if self.total_estimated_cost_usd > self.budget.max_estimated_cost_usd + 1e-12:
            return BudgetCheck(
                True,
                f"Cost budget exceeded: ${self.total_estimated_cost_usd:.6f} > ${self.budget.max_estimated_cost_usd:.6f}",
            )
        if self.peak_memory_mb > self.budget.max_memory_mb:
            return BudgetCheck(
                True,
                f"Memory budget exceeded: {self.peak_memory_mb:.2f}MB > {self.budget.max_memory_mb:.2f}MB",
            )
        return BudgetCheck(False, None)

    def request_cancel(self, reason: str = "User or orchestrator requested cancellation") -> List[str]:
        """Transition pending/retry-wait trials to CANCELLED; return in-flight IDs to cancel."""
        if not self.cancelled:
            self.cancelled = True
            self.termination_reason = self.termination_reason or reason
        for tid in self._order:
            slot = self._slots[tid]
            if slot.status in (TrialStatus.PENDING, TrialStatus.RETRY_WAIT):
                slot.status = TrialStatus.CANCELLED
                self.counters["tasks_cancelled"] += 1
        return sorted(self._in_flight)

    def trigger_budget_exhaustion(self, reason: str) -> List[str]:
        """Halt scheduling due to budget exhaustion; mark pending trials SKIPPED_BUDGET."""
        if not self.budget_exhausted:
            self.budget_exhausted = True
            self.termination_reason = self.termination_reason or reason
            self.counters["budget_stops"] += 1
        for tid in self._order:
            slot = self._slots[tid]
            if slot.status in (TrialStatus.PENDING, TrialStatus.RETRY_WAIT):
                slot.status = TrialStatus.SKIPPED_BUDGET
                self.counters["tasks_skipped_budget"] += 1
        return sorted(self._in_flight)

    def next_dispatchable(self, now_monotonic: float, elapsed_wall_seconds: float) -> List[JobContract]:
        """Select up to bounded concurrency slots of ready trials for dispatch."""
        if self.cancelled or self.budget_exhausted:
            return []

        budget_check = self.check_budget(elapsed_wall_seconds)
        if budget_check.exhausted:
            self.trigger_budget_exhaustion(budget_check.reason or "Budget exhausted")
            return []

        available_slots = self.budget.max_concurrency - len(self._in_flight)
        if available_slots <= 0:
            return []

        to_dispatch: List[JobContract] = []
        for tid in self._order:
            if len(to_dispatch) >= available_slots:
                break
            slot = self._slots[tid]
            if tid in self._completed_ids or tid in self._in_flight:
                continue
            if slot.status == TrialStatus.RETRY_WAIT:
                if now_monotonic < slot.next_eligible_time:
                    continue
                slot.status = TrialStatus.PENDING
            if slot.status != TrialStatus.PENDING:
                continue

            # Enforce max_trials limit on unique trials executed (excluding already-started retries)
            if tid not in self.trials_dispatched_unique and len(self.trials_dispatched_unique) >= self.budget.max_trials:
                self.trigger_budget_exhaustion(
                    f"Trial count budget reached: max_trials={self.budget.max_trials}"
                )
                break

            slot.attempts += 1
            slot.status = TrialStatus.RUNNING
            contract = slot.contract.with_attempt(slot.attempts)
            slot.contract = contract
            self._in_flight.add(tid)
            self.trials_dispatched_unique.add(tid)
            self.counters["tasks_scheduled"] += 1
            to_dispatch.append(contract)

        return to_dispatch

    def reject_duplicate_submission(self, trial_id: str) -> bool:
        """Explicit guard for callers/adapters attempting to re-submit an active or done trial."""
        if trial_id in self._completed_ids or trial_id in self._in_flight:
            self.counters["duplicate_tasks_suppressed"] += 1
            return True
        return False

    def record_result(
        self,
        result: TrialResult,
        *,
        now_monotonic: float,
        elapsed_wall_seconds: float,
    ) -> str:
        """Process a worker result event and return state transition label.

        Handles duplicate deliveries, worker crashes, partial artifact rejections,
        retries, cancellation acknowledgments, and budget checks.
        """
        tid = result.trial_id
        if tid not in self._slots:
            raise KeyError(f"Unknown trial_id: {tid}")
        slot = self._slots[tid]

        # Idempotency guard: ignore duplicate completion of an already SUCCEEDED trial
        if tid in self._completed_ids:
            self.counters["duplicate_completions_ignored"] += 1
            return "DUPLICATE_IGNORED"

        completion_key = (tid, result.attempt)
        if completion_key in self._seen_completion_keys:
            self.counters["duplicate_completions_ignored"] += 1
            return "DUPLICATE_IGNORED"
        self._seen_completion_keys.add(completion_key)

        self._in_flight.discard(tid)
        slot.history.append(result)

        self.total_cpu_seconds += max(0.0, result.cpu_seconds)
        self.peak_memory_mb = max(self.peak_memory_mb, max(0.0, result.peak_memory_mb))
        self.total_estimated_cost_usd += max(0.0, result.estimated_cost_usd)

        if result.error_code == "WORKER_CRASH_ERROR":
            self.counters["worker_crashes_detected"] += 1
        elif result.error_code == "ARTIFACT_INTEGRITY_ERROR":
            self.counters["partial_artifacts_rejected"] += 1

        if result.status == TrialStatus.SUCCEEDED:
            slot.status = TrialStatus.SUCCEEDED
            slot.result = result
            self._completed_ids.add(tid)
            self.counters["tasks_completed"] += 1
            budget_check = self.check_budget(elapsed_wall_seconds)
            if budget_check.exhausted:
                self.trigger_budget_exhaustion(budget_check.reason or "Budget exhausted")
            return "SUCCEEDED"

        if result.status == TrialStatus.CANCELLED or self.cancelled:
            slot.status = TrialStatus.CANCELLED
            slot.result = result
            self.counters["tasks_cancelled"] += 1
            return "CANCELLED"

        # Failure path: decide retry vs terminal failure
        budget_check = self.check_budget(elapsed_wall_seconds)
        if budget_check.exhausted:
            self.trigger_budget_exhaustion(budget_check.reason or "Budget exhausted")
            slot.status = TrialStatus.FAILED
            slot.result = result
            self.counters["tasks_failed"] += 1
            return "FAILED_BUDGET"

        if result.retryable and slot.attempts <= self.max_retries:
            slot.status = TrialStatus.RETRY_WAIT
            backoff = self.retry_backoff_seconds * (2 ** (slot.attempts - 1))
            slot.next_eligible_time = now_monotonic + backoff
            self.counters["tasks_retried"] += 1
            return "RETRY_SCHEDULED"

        slot.status = TrialStatus.FAILED
        slot.result = result
        self.counters["tasks_failed"] += 1
        return "FAILED"

    def has_pending_work(self) -> bool:
        if self._in_flight:
            return True
        if self.cancelled or self.budget_exhausted:
            return False
        for tid in self._order:
            if self._slots[tid].status in (TrialStatus.PENDING, TrialStatus.RETRY_WAIT):
                return True
        return False

    def next_retry_wait_seconds(self, now_monotonic: float) -> Optional[float]:
        waits = [
            max(0.0, self._slots[tid].next_eligible_time - now_monotonic)
            for tid in self._order
            if self._slots[tid].status == TrialStatus.RETRY_WAIT
        ]
        return min(waits) if waits else None

    def final_run_status(self) -> RunStatus:
        if self.cancelled:
            return RunStatus.CANCELLED
        if self.budget_exhausted:
            return RunStatus.BUDGET_EXHAUSTED
        statuses = [self._slots[tid].status for tid in self._order]
        if all(s == TrialStatus.SUCCEEDED for s in statuses):
            return RunStatus.COMPLETED
        if any(s == TrialStatus.SUCCEEDED for s in statuses):
            return RunStatus.PARTIAL_FAILURE
        return RunStatus.FAILED

    def aggregate(self) -> AggregateResult:
        """Build canonically ordered trial results and deterministic aggregate hash."""
        ordered_results: List[TrialResult] = []
        seen_succeeded: Set[str] = set()
        duplicate_completed = 0

        for tid in sorted(self._order, key=lambda t: (self._slots[t].contract.trial_index, t)):
            slot = self._slots[tid]
            if slot.result is not None:
                res = slot.result
            else:
                # Synthesize deterministic terminal record for unstarted cancelled/budget-skipped trials
                res = TrialResult(
                    trial_id=slot.contract.trial_id,
                    trial_index=slot.contract.trial_index,
                    attempt=max(1, slot.attempts),
                    status=slot.status,
                    worker_id="scheduler",
                    input_sha256=slot.contract.input_sha256,
                    params=dict(slot.contract.params),
                    seed=slot.contract.seed,
                    started_at="",
                    completed_at="",
                    cpu_seconds=0.0,
                    wall_seconds=0.0,
                    peak_memory_mb=0.0,
                    estimated_cost_usd=0.0,
                    error_code=slot.status.value,
                    error_message=self.termination_reason,
                )
            if res.status == TrialStatus.SUCCEEDED:
                if res.trial_id in seen_succeeded:
                    duplicate_completed += 1
                seen_succeeded.add(res.trial_id)
            ordered_results.append(res)

        logical_payloads = [r.logical_payload() for r in ordered_results]
        agg_hash = compute_aggregate_hash(logical_payloads)

        succeeded = [r for r in ordered_results if r.status == TrialStatus.SUCCEEDED and r.metrics is not None]
        if succeeded:
            best = max(
                succeeded,
                key=lambda r: (r.metrics.sharpe_ratio, -r.trial_index),  # deterministic tie-break
            )
            best_trial_id: Optional[str] = best.trial_id
            best_sharpe: Optional[float] = best.metrics.sharpe_ratio
            mean_sharpe: Optional[float] = round(
                sum(r.metrics.sharpe_ratio for r in succeeded) / float(len(succeeded)), 10
            )
            mean_return: Optional[float] = round(
                sum(r.metrics.total_return for r in succeeded) / float(len(succeeded)), 10
            )
        else:
            best_trial_id = None
            best_sharpe = None
            mean_sharpe = None
            mean_return = None

        return AggregateResult(
            ordered_results=tuple(ordered_results),
            aggregate_sha256=agg_hash,
            best_trial_id=best_trial_id,
            best_sharpe_ratio=best_sharpe,
            mean_sharpe_ratio=mean_sharpe,
            mean_total_return=mean_return,
            duplicate_completed_trials=duplicate_completed,
        )
