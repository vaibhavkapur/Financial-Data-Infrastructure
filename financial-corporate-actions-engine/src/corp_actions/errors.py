"""Structured exception hierarchy for the corporate actions engine."""

from __future__ import annotations

from typing import Any, Dict, Optional


class CorporateActionsError(Exception):
    """Base error with machine-readable code and structured context."""

    code: str = "CORP_ACTIONS_ERROR"

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


class ConfigurationError(CorporateActionsError):
    code = "CONFIGURATION_ERROR"


class SchemaVersionError(CorporateActionsError):
    code = "SCHEMA_VERSION_ERROR"


class ValidationError(CorporateActionsError):
    code = "VALIDATION_ERROR"


class UnsupportedActionError(CorporateActionsError):
    code = "UNSUPPORTED_ACTION"


class SameDayConflictError(CorporateActionsError):
    code = "SAME_DAY_CONFLICT"


class MissingMarketDataError(CorporateActionsError):
    code = "MISSING_MARKET_DATA"


class MissingFxRateError(CorporateActionsError):
    code = "MISSING_FX_RATE"


class StorageError(CorporateActionsError):
    code = "STORAGE_ERROR"


class DependencyUnavailableError(CorporateActionsError):
    code = "DEPENDENCY_UNAVAILABLE"
