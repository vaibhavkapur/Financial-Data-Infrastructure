"""Structured error hierarchy for dqrp."""

from __future__ import annotations

from typing import Any, Dict, Optional


class DqrpError(Exception):
    """Base error with machine-readable code and structured context."""

    code: str = "DQRP_ERROR"

    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        self.details: Dict[str, Any] = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_code": self.code,
            "message": str(self),
            "details": self.details,
        }


class ContractValidationError(DqrpError):
    """Raised when an input manifest, spec, budget, or record fails validation."""

    code = "CONTRACT_VALIDATION_ERROR"


class ArtifactIntegrityError(DqrpError):
    """Raised when a partial, truncated, or hash-mismatched artifact is detected."""

    code = "ARTIFACT_INTEGRITY_ERROR"


class DuplicateTaskError(DqrpError):
    """Raised when a duplicate trial submission or completion is detected."""

    code = "DUPLICATE_TASK_ERROR"


class WorkerCrashError(DqrpError):
    """Raised when a worker process terminates unexpectedly or loses heartbeat."""

    code = "WORKER_CRASH_ERROR"


class BudgetExceededError(DqrpError):
    """Raised when a job exceeds its configured compute, wall, trial, or cost budget."""

    code = "BUDGET_EXCEEDED_ERROR"


class UnsupportedOperationError(DqrpError):
    """Raised for unsupported formats, strategies, or schema versions."""

    code = "UNSUPPORTED_OPERATION_ERROR"


class BackendUnavailableError(DqrpError):
    """Raised when an optional execution or storage backend is not installed/configured."""

    code = "BACKEND_UNAVAILABLE_ERROR"
