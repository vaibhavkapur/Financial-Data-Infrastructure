"""Structured error hierarchy for Security Master Platform.

Every domain and operational error carries a stable machine-readable ``code``,
an HTTP status hint, and structured ``details`` so callers never receive silent
successes or opaque exceptions.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class SecurityMasterError(Exception):
    """Base error for all Security Master Platform failures."""

    code: str = "SECURITY_MASTER_ERROR"
    http_status: int = 500

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.message = message
        self.details: Dict[str, Any] = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


class ConfigError(SecurityMasterError):
    """Missing, malformed, or unsupported configuration."""

    code = "CONFIG_ERROR"
    http_status = 500


class ValidationError(SecurityMasterError):
    """Schema, field format, check-digit, or temporal window validation failure."""

    code = "VALIDATION_ERROR"
    http_status = 400


class LicensingError(SecurityMasterError):
    """Attempted ingestion or exposure of an unlicensed identifier namespace."""

    code = "LICENSING_VIOLATION"
    http_status = 403


class UnsupportedEntityError(SecurityMasterError):
    """Explicit rejection of unsupported asset classes, event types, or corporate actions."""

    code = "UNSUPPORTED_ENTITY"
    http_status = 422


class AmbiguousMappingError(SecurityMasterError):
    """Query matched multiple distinct canonical identities and cannot be silently resolved."""

    code = "AMBIGUOUS_MAPPING"
    http_status = 409


class ConflictBlockedError(SecurityMasterError):
    """Requested identity is blocked by an unresolved data-quality conflict ticket."""

    code = "CONFLICT_BLOCKED"
    http_status = 409


class PreObservationLookupError(SecurityMasterError):
    """Requested as-of observation timestamp precedes when any matching record was observed."""

    code = "PRE_OBSERVATION_LOOKUP"
    http_status = 422


class NotFoundError(SecurityMasterError):
    """No matching instrument, listing, identifier, or conflict ticket exists."""

    code = "NOT_FOUND"
    http_status = 404
