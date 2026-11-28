"""Distributed Quant Research Platform (dqrp).

Reproducible, budgeted parallel quantitative research platform operating on
versioned datasets with immutable input/artifact hashes, bounded scheduling,
retry/cancellation controls, and deterministic result aggregation.
"""

from __future__ import annotations

from dqrp.contracts import (
    SCHEMA_VERSION,
    ArtifactRecord,
    CostModel,
    DatasetManifest,
    DatasetPartition,
    ExperimentSpec,
    ExperimentSummary,
    JobContract,
    MarketBar,
    ResourceBudget,
    RunStatus,
    TrialMetrics,
    TrialResult,
    TrialStatus,
)
from dqrp.errors import (
    ArtifactIntegrityError,
    BackendUnavailableError,
    BudgetExceededError,
    ContractValidationError,
    DqrpError,
    DuplicateTaskError,
    UnsupportedOperationError,
    WorkerCrashError,
)

__all__ = [
    "SCHEMA_VERSION",
    "ArtifactIntegrityError",
    "ArtifactRecord",
    "BackendUnavailableError",
    "BudgetExceededError",
    "ContractValidationError",
    "CostModel",
    "DatasetManifest",
    "DatasetPartition",
    "DqrpError",
    "DuplicateTaskError",
    "ExperimentSpec",
    "ExperimentSummary",
    "JobContract",
    "MarketBar",
    "ResourceBudget",
    "RunStatus",
    "TrialMetrics",
    "TrialResult",
    "TrialStatus",
    "UnsupportedOperationError",
    "WorkerCrashError",
]

__version__ = "0.1.0"
