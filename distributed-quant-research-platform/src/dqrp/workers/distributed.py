"""Multi-process isolated distributed worker adapter with crash detection and bounded pool.

Uses multiprocessing 'spawn' workers with dedicated IPC queues so each worker runs in
its own OS process with independent CPU cores, independent memory space, and true
process-crash isolation (detecting abrupt `os._exit` / SIGKILL worker deaths cleanly).
"""

from __future__ import annotations

from dataclasses import dataclass
import multiprocessing as mp
from pathlib import Path
import queue
import time
from typing import Dict, List, Optional, Sequence, Tuple, Union

from dqrp.contracts import (
    CostModel,
    JobContract,
    MarketBar,
    TrialResult,
    TrialStatus,
    utc_now_iso,
)
from dqrp.workers.base import (
    FaultInjectionPlan,
    WorkerAdapter,
    WorkerCapabilities,
    execute_trial_core,
)


def _worker_process_main(
    worker_id: str,
    bars_dicts: List[Dict[str, object]],
    artifact_root_str: str,
    cost_model_dict: Dict[str, object],
    fault_plan: Optional[FaultInjectionPlan],
    task_queue: "mp.Queue[Optional[Dict[str, object]]]",
    result_queue: "mp.Queue[Tuple[str, Dict[str, object]]]",
) -> None:
    """Entry point executed inside each spawned OS worker process."""
    bars = tuple(MarketBar.from_dict(b) for b in bars_dicts)
    cost_model = CostModel.from_dict(cost_model_dict)
    artifact_root = Path(artifact_root_str)

    while True:
        item = task_queue.get()
        if item is None:
            return
        contract = JobContract.from_dict(item)
        res = execute_trial_core(
            contract,
            bars=bars,
            artifact_root=artifact_root,
            cost_model=cost_model,
            worker_id=worker_id,
            fault_plan=fault_plan,
        )
        result_queue.put((worker_id, res.to_dict()))
        if (
            fault_plan is not None
            and res.status == TrialStatus.SUCCEEDED
            and contract.trial_index in fault_plan.duplicate_completion_trials
        ):
            result_queue.put((worker_id, res.to_dict()))


@dataclass
class _WorkerSlot:
    worker_id: str
    process: mp.Process
    task_queue: "mp.Queue[Optional[Dict[str, object]]]"
    active_contract: Optional[JobContract] = None
    active_started_at: str = ""
    active_started_mono: float = 0.0


class DistributedProcessWorkerAdapter(WorkerAdapter):
    """Bounded multi-process worker pool with per-worker crash monitoring and respawning."""

    def __init__(
        self,
        *,
        bars: Sequence[MarketBar],
        artifact_root: Union[str, Path],
        cost_model: CostModel,
        max_workers: int = 2,
        fault_plan: Optional[FaultInjectionPlan] = None,
    ) -> None:
        self._bars_dicts = [b.to_dict() for b in bars]
        self._artifact_root_str = str(Path(artifact_root).resolve())
        self._cost_model_dict = cost_model.to_dict()
        self._max_workers = max(1, int(max_workers))
        self._fault_plan = fault_plan
        self._ctx = mp.get_context("spawn")
        self._result_queue: "mp.Queue[Tuple[str, Dict[str, object]]]" = self._ctx.Queue()
        self._workers: Dict[str, _WorkerSlot] = {}
        self._pending_buffer: List[JobContract] = []
        self._respawn_seq = 0
        self._closed = False

        for idx in range(self._max_workers):
            self._spawn_worker(f"dist-worker-{idx}")

    def _spawn_worker(self, worker_id: str) -> _WorkerSlot:
        task_q: "mp.Queue[Optional[Dict[str, object]]]" = self._ctx.Queue()
        proc = self._ctx.Process(
            target=_worker_process_main,
            args=(
                worker_id,
                self._bars_dicts,
                self._artifact_root_str,
                self._cost_model_dict,
                self._fault_plan,
                task_q,
                self._result_queue,
            ),
            name=worker_id,
            daemon=True,
        )
        proc.start()
        slot = _WorkerSlot(worker_id=worker_id, process=proc, task_queue=task_q)
        self._workers[worker_id] = slot
        return slot

    @property
    def capabilities(self) -> WorkerCapabilities:
        return WorkerCapabilities(
            backend_name="distributed",
            is_distributed=True,
            max_workers=self._max_workers,
            supports_hard_crash_isolation=True,
            supports_cancellation=True,
        )

    def _dispatch_to_idle(self) -> None:
        if not self._pending_buffer:
            return
        for slot in self._workers.values():
            if not self._pending_buffer:
                break
            if slot.active_contract is None and slot.process.is_alive():
                contract = self._pending_buffer.pop(0)
                slot.active_contract = contract
                slot.active_started_at = utc_now_iso()
                slot.active_started_mono = time.perf_counter()
                slot.task_queue.put(contract.to_dict())

    def submit(self, contract: JobContract) -> None:
        self._pending_buffer.append(contract)
        self._dispatch_to_idle()

    def _check_crashed_workers(self) -> List[TrialResult]:
        crashed_results: List[TrialResult] = []
        dead_ids: List[str] = []
        for wid, slot in list(self._workers.items()):
            if not slot.process.is_alive():
                dead_ids.append(wid)
                if slot.active_contract is not None:
                    c = slot.active_contract
                    wall_s = max(1e-6, time.perf_counter() - slot.active_started_mono)
                    crashed_results.append(
                        TrialResult(
                            trial_id=c.trial_id,
                            trial_index=c.trial_index,
                            attempt=c.attempt,
                            status=TrialStatus.FAILED,
                            worker_id=wid,
                            input_sha256=c.input_sha256,
                            params=dict(c.params),
                            seed=c.seed,
                            started_at=slot.active_started_at or utc_now_iso(),
                            completed_at=utc_now_iso(),
                            cpu_seconds=0.0,
                            wall_seconds=round(wall_s, 6),
                            peak_memory_mb=0.0,
                            estimated_cost_usd=0.0,
                            error_code="WORKER_CRASH_ERROR",
                            error_message=(
                                f"Worker process '{wid}' exited unexpectedly with code {slot.process.exitcode}"
                            ),
                            retryable=True,
                        )
                    )
                    slot.active_contract = None
                slot.process.join(timeout=0.5)
                try:
                    slot.task_queue.close()
                except Exception:
                    pass
        for wid in dead_ids:
            self._workers.pop(wid, None)
            if not self._closed:
                self._respawn_seq += 1
                self._spawn_worker(f"dist-worker-r{self._respawn_seq}")
        return crashed_results

    def poll_completed(self, timeout_seconds: float = 0.05) -> List[TrialResult]:
        self._dispatch_to_idle()
        results: List[TrialResult] = []

        # Wait up to timeout_seconds for the first completed message, then drain non-blocking
        try:
            wid, raw_dict = self._result_queue.get(timeout=max(0.005, timeout_seconds))
            res = TrialResult.from_dict(raw_dict)
            if wid in self._workers and self._workers[wid].active_contract is not None:
                if self._workers[wid].active_contract.trial_id == res.trial_id:
                    self._workers[wid].active_contract = None
            results.append(res)
        except queue.Empty:
            pass

        while True:
            try:
                wid, raw_dict = self._result_queue.get_nowait()
                res = TrialResult.from_dict(raw_dict)
                if wid in self._workers and self._workers[wid].active_contract is not None:
                    if self._workers[wid].active_contract.trial_id == res.trial_id:
                        self._workers[wid].active_contract = None
                results.append(res)
            except queue.Empty:
                break

        crashed = self._check_crashed_workers()
        results.extend(crashed)
        self._dispatch_to_idle()
        return results

    def cancel(self, trial_ids: Sequence[str]) -> List[TrialResult]:
        target = set(trial_ids)
        cancelled: List[TrialResult] = []
        now = utc_now_iso()

        # Cancel buffered tasks that have not started yet
        remaining: List[JobContract] = []
        for contract in self._pending_buffer:
            if contract.trial_id in target:
                cancelled.append(
                    TrialResult(
                        trial_id=contract.trial_id,
                        trial_index=contract.trial_index,
                        attempt=contract.attempt,
                        status=TrialStatus.CANCELLED,
                        worker_id="dist-pool",
                        input_sha256=contract.input_sha256,
                        params=dict(contract.params),
                        seed=contract.seed,
                        started_at=now,
                        completed_at=now,
                        cpu_seconds=0.0,
                        wall_seconds=0.0,
                        peak_memory_mb=0.0,
                        error_code="CANCELLED",
                        error_message="Cancelled before worker dispatch",
                    )
                )
            else:
                remaining.append(contract)
        self._pending_buffer = remaining

        # Terminate workers currently running any targeted trial_id
        for wid, slot in list(self._workers.items()):
            if slot.active_contract is not None and slot.active_contract.trial_id in target:
                c = slot.active_contract
                wall_s = max(0.0, time.perf_counter() - slot.active_started_mono)
                if slot.process.is_alive():
                    slot.process.terminate()
                    slot.process.join(timeout=1.0)
                slot.active_contract = None
                cancelled.append(
                    TrialResult(
                        trial_id=c.trial_id,
                        trial_index=c.trial_index,
                        attempt=c.attempt,
                        status=TrialStatus.CANCELLED,
                        worker_id=wid,
                        input_sha256=c.input_sha256,
                        params=dict(c.params),
                        seed=c.seed,
                        started_at=slot.active_started_at or now,
                        completed_at=utc_now_iso(),
                        cpu_seconds=0.0,
                        wall_seconds=round(wall_s, 6),
                        peak_memory_mb=0.0,
                        error_code="CANCELLED",
                        error_message="Worker terminated on cancellation request",
                    )
                )
                self._workers.pop(wid, None)
                if not self._closed:
                    self._respawn_seq += 1
                    self._spawn_worker(f"dist-worker-r{self._respawn_seq}")

        return cancelled

    def shutdown(self) -> None:
        self._closed = True
        self._pending_buffer.clear()
        for slot in list(self._workers.values()):
            try:
                if slot.process.is_alive():
                    slot.task_queue.put(None)
            except Exception:
                pass
        for slot in list(self._workers.values()):
            slot.process.join(timeout=1.5)
            if slot.process.is_alive():
                slot.process.terminate()
                slot.process.join(timeout=0.5)
            try:
                slot.task_queue.close()
            except Exception:
                pass
        self._workers.clear()
        try:
            self._result_queue.close()
        except Exception:
            pass
