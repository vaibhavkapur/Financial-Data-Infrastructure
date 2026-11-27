"""Structured domain errors for the Financial Feature Store."""

from __future__ import annotations

from typing import Any


class FeatureStoreError(Exception):
    """Base error with machine-readable code and structured context."""

    code: str = "FEATURE_STORE_ERROR"

    def __init__(self, message: str, *, code: str | None = None, details: dict[str, Any] | None = None) -> None:
        self.message = message
        if code is not None:
            self.code = code
        self.details: dict[str, Any] = details or {}
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        if not self.details:
            return f"[{self.code}] {self.message}"
        return f"[{self.code}] {self.message} | details={self.details}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class TimestampContractError(FeatureStoreError):
    code = "TIMESTAMP_CONTRACT_ERROR"


class LeakageViolationError(FeatureStoreError):
    code = "LEAKAGE_VIOLATION"


class RegistryError(FeatureStoreError):
    code = "REGISTRY_ERROR"


class FeatureProliferationError(RegistryError):
    code = "FEATURE_PROLIFERATION_LIMIT_EXCEEDED"


class ImmutableVersionError(RegistryError):
    code = "IMMUTABLE_VERSION_CONFLICT"


class SchemaCompatibilityError(RegistryError):
    code = "SCHEMA_COMPATIBILITY_ERROR"


class VersionMismatchError(FeatureStoreError):
    code = "VERSION_MISMATCH"


class QualityGateError(FeatureStoreError):
    code = "QUALITY_GATE_FAILED"


class StaleFeatureError(FeatureStoreError):
    code = "STALE_FEATURE"


class NullPolicyViolationError(FeatureStoreError):
    code = "NULL_POLICY_VIOLATION"


class BackfillError(FeatureStoreError):
    code = "BACKFILL_ERROR"


class ExportError(FeatureStoreError):
    code = "EXPORT_ERROR"


class AdapterError(FeatureStoreError):
    code = "ADAPTER_ERROR"
