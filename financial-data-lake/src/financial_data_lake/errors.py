"""Structured domain errors for the Financial Data Lake."""

from __future__ import annotations

from typing import Any


class DataLakeError(Exception):
    """Base error with machine-readable code and structured context."""

    code: str = "DATA_LAKE_ERROR"

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict[str, Any] = context or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.code,
            "message": self.message,
            "context": self.context,
        }


class EntitlementError(DataLakeError):
    """Raised when access to a dataset violates license, role, purpose, expiry, or redistribution rules."""

    code = "ENTITLEMENT_DENIED"


class SchemaEvolutionError(DataLakeError):
    """Raised when a proposed schema change breaks backward compatibility or violates contract rules."""

    code = "SCHEMA_EVOLUTION_REJECTED"


class SchemaValidationError(DataLakeError):
    """Raised when incoming records fail schema or domain contract validation."""

    code = "SCHEMA_VALIDATION_FAILED"


class ImmutabilityViolationError(DataLakeError):
    """Raised when an operation attempts to overwrite or destructively rewrite an immutable artifact."""

    code = "IMMUTABILITY_VIOLATION"


class CorruptPartitionError(DataLakeError):
    """Raised when a persisted partition fails SHA-256 checksum or Parquet integrity verification."""

    code = "CORRUPT_PARTITION"


class QualityCheckError(DataLakeError):
    """Raised when a critical data quality rule fails during normalization or curation."""

    code = "QUALITY_CHECK_FAILED"


class FixtureVerificationError(DataLakeError):
    """Raised when an input fixture fails manifest checksum or provenance validation."""

    code = "FIXTURE_VERIFICATION_FAILED"


class SnapshotError(DataLakeError):
    """Raised when a requested dataset snapshot does not exist or fails reproducibility checks."""

    code = "SNAPSHOT_ERROR"


class RecoveryError(DataLakeError):
    """Raised when backfill, replay, or partition recovery cannot safely complete."""

    code = "RECOVERY_ERROR"
