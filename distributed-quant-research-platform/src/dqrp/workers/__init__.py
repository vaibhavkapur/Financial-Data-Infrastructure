"""Worker execution adapters: local, multi-process distributed, and optional Ray/Dask."""

from __future__ import annotations

from dqrp.workers.base import (
    FaultInjectionPlan,
    WorkerAdapter,
    WorkerCapabilities,
    execute_trial_core,
)
from dqrp.workers.distributed import DistributedProcessWorkerAdapter
from dqrp.workers.local import LocalWorkerAdapter
from dqrp.workers.optional_backends import DaskWorkerAdapter, RayWorkerAdapter

__all__ = [
    "DaskWorkerAdapter",
    "DistributedProcessWorkerAdapter",
    "FaultInjectionPlan",
    "LocalWorkerAdapter",
    "RayWorkerAdapter",
    "WorkerAdapter",
    "WorkerCapabilities",
    "execute_trial_core",
]
