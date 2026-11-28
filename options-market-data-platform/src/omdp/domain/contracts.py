"""Canonical option IDs, event contracts, provenance structures, and domain errors.

All domain structures are immutable (frozen dataclasses) and carry deterministic
canonical JSON serialization and SHA-256 content hashes for bitemporal lineage
and exact replay verification.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class DomainError(Exception):
    """Base exception for domain-level violations."""


class ContractValidationError(DomainError):
    """Raised when an input contract violates schema or canonical ID rules."""


class UnsupportedInputError(DomainError):
    """Raised when an unsupported schema version, currency, or instrument feature is supplied."""


class SnapshotConsistencyError(DomainError):
    """Raised when a chain snapshot violates structural consistency invariants."""

    def __init__(self, message: str, code: str = "SNAPSHOT_INCONSISTENT") -> None:
        super().__init__(message)
        self.code = code


class OptionRight(str, Enum):
    CALL = "C"
    PUT = "P"


class ValidationStatus(str, Enum):
    VALID = "VALID"
    FLAGGED = "FLAGGED"
    REJECTED = "REJECTED"


class DataQualityFlag(str, Enum):
    EXPIRED_OPTION = "EXPIRED_OPTION"
    STRIKE_DECIMAL_MISMATCH = "STRIKE_DECIMAL_MISMATCH"
    CROSSED_QUOTE = "CROSSED_QUOTE"
    LOCKED_QUOTE = "LOCKED_QUOTE"
    INVALID_PRICE = "INVALID_PRICE"
    INVALID_SIZE = "INVALID_SIZE"
    STALE_QUOTE = "STALE_QUOTE"
    DELAYED_QUOTE = "DELAYED_QUOTE"
    FUTURE_DATED_QUOTE = "FUTURE_DATED_QUOTE"
    MISSING_CONTRACT = "MISSING_CONTRACT"
    MISSING_UNDERLYING = "MISSING_UNDERLYING"
    STALE_UNDERLYING = "STALE_UNDERLYING"
    CROSSED_UNDERLYING = "CROSSED_UNDERLYING"
    MISSING_RATE_CURVE = "MISSING_RATE_CURVE"
    UNSUPPORTED_EXERCISE_STYLE = "UNSUPPORTED_EXERCISE_STYLE"


class IVStatus(str, Enum):
    COMPUTED = "COMPUTED"
    EXCLUDED_INVALID_QUOTE = "EXCLUDED_INVALID_QUOTE"
    EXCLUDED_STALE_QUOTE = "EXCLUDED_STALE_QUOTE"
    EXCLUDED_EXPIRED = "EXCLUDED_EXPIRED"
    EXCLUDED_MISSING_UNDERLYING = "EXCLUDED_MISSING_UNDERLYING"
    EXCLUDED_MISSING_RATE = "EXCLUDED_MISSING_RATE"
    EXCLUDED_UNSUPPORTED_STYLE = "EXCLUDED_UNSUPPORTED_STYLE"
    ARBITRAGE_BOUND_VIOLATION = "ARBITRAGE_BOUND_VIOLATION"
    SOLVER_NON_CONVERGENT = "SOLVER_NON_CONVERGENT"


_SYMBOL_RE = re.compile(r"^[A-Z0-9._-]{1,12}$")
_CANONICAL_ID_RE = re.compile(r"^OPT:([A-Z0-9._-]{1,12}):(\d{8}):([CP]):(\d+\.\d{4})$")
_OCC_RE = re.compile(r"^([A-Z0-9 ]{6})(\d{6})([CP])(\d{8})$")


def parse_utc_timestamp(value: str | datetime) -> datetime:
    """Parse an ISO-8601 timestamp string or datetime into an aware UTC datetime."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ContractValidationError(f"Timestamp must include timezone (UTC): {value!r}")
        return value.astimezone(timezone.utc)
    if not isinstance(value, str) or not value.strip():
        raise ContractValidationError(f"Invalid timestamp string: {value!r}")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ContractValidationError(f"Malformed ISO-8601 timestamp '{value}': {exc}") from exc
    if dt.tzinfo is None:
        raise ContractValidationError(f"Timestamp must specify explicit UTC offset or 'Z': '{value}'")
    return dt.astimezone(timezone.utc)


def format_utc_timestamp(dt: datetime) -> str:
    """Format an aware UTC datetime in canonical ISO-8601 format with Z suffix."""
    utc_dt = dt.astimezone(timezone.utc)
    if utc_dt.microsecond:
        return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_decimal(value: Any, field_name: str) -> Decimal:
    """Convert string/int/Decimal to exact Decimal; rejects floats with NaN/Inf."""
    if isinstance(value, bool):
        raise ContractValidationError(f"Field '{field_name}' cannot be boolean")
    if isinstance(value, Decimal):
        dec = value
    elif isinstance(value, (int, str)):
        try:
            dec = Decimal(str(value).strip())
        except InvalidOperation as exc:
            raise ContractValidationError(f"Invalid decimal for '{field_name}': {value!r}") from exc
    elif isinstance(value, float):
        try:
            dec = Decimal(str(value))
        except InvalidOperation as exc:
            raise ContractValidationError(f"Invalid numeric value for '{field_name}': {value!r}") from exc
    else:
        raise ContractValidationError(f"Unsupported type for decimal '{field_name}': {type(value).__name__}")
    if not dec.is_finite():
        raise ContractValidationError(f"Field '{field_name}' must be finite, got {value!r}")
    return dec


def canonical_hash(payload: Dict[str, Any]) -> str:
    """Deterministic SHA-256 hash over canonical compact sorted JSON."""
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def decimal_places(value: Decimal) -> int:
    """Return the number of significant fractional digits after stripping trailing zeros."""
    normalized = value.normalize()
    exp = normalized.as_tuple().exponent
    if isinstance(exp, int) and exp < 0:
        return -exp
    return 0


@dataclass(frozen=True)
class CanonicalOptionID:
    """Canonical option contract identifier.

    Canonical string form: ``OPT:<UNDERLYING>:<YYYYMMDD>:<C|P>:<STRIKE_4DP>``
    Example: ``OPT:AAPL:20260619:C:150.0000``
    """

    underlying_symbol: str
    expiry_date: date
    option_right: OptionRight
    strike: Decimal

    def __post_init__(self) -> None:
        sym = self.underlying_symbol.strip().upper()
        if not _SYMBOL_RE.match(sym):
            raise ContractValidationError(f"Invalid underlying_symbol: {self.underlying_symbol!r}")
        object.__setattr__(self, "underlying_symbol", sym)
        if not isinstance(self.expiry_date, date):
            raise ContractValidationError(f"expiry_date must be a date, got {type(self.expiry_date).__name__}")
        right = self.option_right if isinstance(self.option_right, OptionRight) else OptionRight(str(self.option_right).upper())
        object.__setattr__(self, "option_right", right)
        strike_dec = parse_decimal(self.strike, "strike")
        if strike_dec <= Decimal("0"):
            raise ContractValidationError(f"Option strike must be positive, got {strike_dec}")
        if decimal_places(strike_dec) > 4:
            raise ContractValidationError(
                f"CanonicalOptionID supports at most 4 strike decimal places, got {strike_dec}"
            )
        quantized = strike_dec.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        object.__setattr__(self, "strike", quantized)

    @property
    def canonical(self) -> str:
        return (
            f"OPT:{self.underlying_symbol}:{self.expiry_date.strftime('%Y%m%d')}:"
            f"{self.option_right.value}:{self.strike:.4f}"
        )

    @property
    def occ_symbol(self) -> Optional[str]:
        """Standard 21-char OCC symbol when underlying <= 6 chars and strike fits 1/1000 scaling."""
        if len(self.underlying_symbol) > 6 or decimal_places(self.strike) > 3:
            return None
        scaled = int((self.strike * Decimal("1000")).to_integral_exact())
        if scaled <= 0 or scaled > 99_999_999:
            return None
        return (
            f"{self.underlying_symbol:<6}"
            f"{self.expiry_date.strftime('%y%m%d')}"
            f"{self.option_right.value}"
            f"{scaled:08d}"
        )

    def __str__(self) -> str:
        return self.canonical

    @classmethod
    def parse(cls, value: str) -> "CanonicalOptionID":
        match = _CANONICAL_ID_RE.match(value.strip())
        if not match:
            raise ContractValidationError(f"Invalid canonical option_id format: {value!r}")
        sym, ymd, right, strike_str = match.groups()
        exp = date(int(ymd[0:4]), int(ymd[4:6]), int(ymd[6:8]))
        return cls(
            underlying_symbol=sym,
            expiry_date=exp,
            option_right=OptionRight(right),
            strike=Decimal(strike_str),
        )

    @classmethod
    def from_occ(cls, occ: str) -> "CanonicalOptionID":
        match = _OCC_RE.match(occ)
        if not match:
            raise ContractValidationError(f"Invalid 21-char OCC symbol: {occ!r}")
        root, yymmdd, right, scaled_str = match.groups()
        exp = date(2000 + int(yymmdd[0:2]), int(yymmdd[2:4]), int(yymmdd[4:6]))
        strike = Decimal(int(scaled_str)) / Decimal("1000")
        return cls(
            underlying_symbol=root.strip(),
            expiry_date=exp,
            option_right=OptionRight(right),
            strike=strike,
        )


@dataclass(frozen=True)
class ContractMetadata:
    option_id: str
    contract_version: int
    underlying_symbol: str
    expiry_date: date
    option_right: OptionRight
    strike: Decimal
    strike_scale_decimals: int
    contract_multiplier: int
    exercise_style: str
    currency: str
    source_id: str
    schema_version: str
    ingestion_time: datetime
    occ_symbol: Optional[str] = None
    record_hash: str = ""

    def __post_init__(self) -> None:
        if self.contract_version < 1:
            raise ContractValidationError(f"contract_version must be >= 1, got {self.contract_version}")
        if self.strike_scale_decimals < 0 or self.strike_scale_decimals > 4:
            raise ContractValidationError(
                f"strike_scale_decimals must be in [0, 4], got {self.strike_scale_decimals}"
            )
        if self.contract_multiplier <= 0:
            raise ContractValidationError(f"contract_multiplier must be > 0, got {self.contract_multiplier}")
        cid = CanonicalOptionID(
            underlying_symbol=self.underlying_symbol,
            expiry_date=self.expiry_date,
            option_right=self.option_right,
            strike=self.strike,
        )
        if self.option_id and self.option_id != cid.canonical:
            raise ContractValidationError(
                f"option_id '{self.option_id}' does not match canonical ID '{cid.canonical}'"
            )
        object.__setattr__(self, "option_id", cid.canonical)
        object.__setattr__(self, "underlying_symbol", cid.underlying_symbol)
        object.__setattr__(self, "option_right", cid.option_right)
        object.__setattr__(self, "strike", cid.strike)
        object.__setattr__(self, "exercise_style", self.exercise_style.strip().upper())
        object.__setattr__(self, "currency", self.currency.strip().upper())
        object.__setattr__(self, "ingestion_time", parse_utc_timestamp(self.ingestion_time))
        object.__setattr__(self, "occ_symbol", self.occ_symbol or cid.occ_symbol)
        if decimal_places(self.strike) > self.strike_scale_decimals:
            raise ContractValidationError(
                f"Contract strike {self.strike} exceeds declared strike_scale_decimals={self.strike_scale_decimals}"
            )
        if not self.record_hash:
            object.__setattr__(self, "record_hash", canonical_hash(self.to_hash_payload()))

    def to_hash_payload(self) -> Dict[str, Any]:
        return {
            "option_id": self.option_id,
            "contract_version": self.contract_version,
            "occ_symbol": self.occ_symbol,
            "underlying_symbol": self.underlying_symbol,
            "expiry_date": self.expiry_date.isoformat(),
            "option_right": self.option_right.value,
            "strike": f"{self.strike:.4f}",
            "strike_scale_decimals": self.strike_scale_decimals,
            "contract_multiplier": self.contract_multiplier,
            "exercise_style": self.exercise_style,
            "currency": self.currency,
            "source_id": self.source_id,
            "schema_version": self.schema_version,
            "ingestion_time": format_utc_timestamp(self.ingestion_time),
        }

    def to_dict(self) -> Dict[str, Any]:
        d = self.to_hash_payload()
        d["record_hash"] = self.record_hash
        return d


@dataclass(frozen=True)
class ExpiryCalendarEntry:
    calendar_id: str
    calendar_version: int
    expiry_date: date
    expiry_timestamp_utc: datetime
    is_trading_day: bool
    settlement_type: str
    source_id: str
    schema_version: str
    record_hash: str = ""

    def __post_init__(self) -> None:
        if self.calendar_version < 1:
            raise ContractValidationError(f"calendar_version must be >= 1, got {self.calendar_version}")
        object.__setattr__(self, "expiry_timestamp_utc", parse_utc_timestamp(self.expiry_timestamp_utc))
        object.__setattr__(self, "settlement_type", self.settlement_type.strip().upper())
        if not self.record_hash:
            object.__setattr__(self, "record_hash", canonical_hash(self.to_hash_payload()))

    def to_hash_payload(self) -> Dict[str, Any]:
        return {
            "calendar_id": self.calendar_id,
            "calendar_version": self.calendar_version,
            "expiry_date": self.expiry_date.isoformat(),
            "expiry_timestamp_utc": format_utc_timestamp(self.expiry_timestamp_utc),
            "is_trading_day": self.is_trading_day,
            "settlement_type": self.settlement_type,
            "source_id": self.source_id,
            "schema_version": self.schema_version,
        }

    def to_dict(self) -> Dict[str, Any]:
        d = self.to_hash_payload()
        d["record_hash"] = self.record_hash
        return d


@dataclass(frozen=True)
class RateCurvePoint:
    curve_id: str
    curve_version: int
    as_of_time: datetime
    tenor_days: int
    risk_free_rate: float
    dividend_yield: float
    source_id: str
    schema_version: str
    record_hash: str = ""

    def __post_init__(self) -> None:
        if self.curve_version < 1:
            raise ContractValidationError(f"curve_version must be >= 1, got {self.curve_version}")
        if self.tenor_days <= 0:
            raise ContractValidationError(f"tenor_days must be > 0, got {self.tenor_days}")
        object.__setattr__(self, "as_of_time", parse_utc_timestamp(self.as_of_time))
        object.__setattr__(self, "risk_free_rate", float(self.risk_free_rate))
        object.__setattr__(self, "dividend_yield", float(self.dividend_yield))
        if not self.record_hash:
            object.__setattr__(self, "record_hash", canonical_hash(self.to_hash_payload()))

    def to_hash_payload(self) -> Dict[str, Any]:
        return {
            "curve_id": self.curve_id,
            "curve_version": self.curve_version,
            "as_of_time": format_utc_timestamp(self.as_of_time),
            "tenor_days": self.tenor_days,
            "risk_free_rate": round(self.risk_free_rate, 10),
            "dividend_yield": round(self.dividend_yield, 10),
            "source_id": self.source_id,
            "schema_version": self.schema_version,
        }

    def to_dict(self) -> Dict[str, Any]:
        d = self.to_hash_payload()
        d["record_hash"] = self.record_hash
        return d


@dataclass(frozen=True)
class UnderlyingQuote:
    underlying_symbol: str
    quote_version: int
    event_time: datetime
    ingestion_time: datetime
    bid: Decimal
    ask: Decimal
    last_price: Optional[Decimal]
    source_id: str
    source_sequence: int
    schema_version: str
    record_hash: str = ""

    def __post_init__(self) -> None:
        sym = self.underlying_symbol.strip().upper()
        if not _SYMBOL_RE.match(sym):
            raise ContractValidationError(f"Invalid underlying_symbol: {self.underlying_symbol!r}")
        if self.quote_version < 1:
            raise ContractValidationError(f"quote_version must be >= 1, got {self.quote_version}")
        object.__setattr__(self, "underlying_symbol", sym)
        object.__setattr__(self, "event_time", parse_utc_timestamp(self.event_time))
        object.__setattr__(self, "ingestion_time", parse_utc_timestamp(self.ingestion_time))
        object.__setattr__(self, "bid", parse_decimal(self.bid, "bid"))
        object.__setattr__(self, "ask", parse_decimal(self.ask, "ask"))
        if self.last_price is not None:
            object.__setattr__(self, "last_price", parse_decimal(self.last_price, "last_price"))
        if not self.record_hash:
            object.__setattr__(self, "record_hash", canonical_hash(self.to_hash_payload()))

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")

    @property
    def is_crossed(self) -> bool:
        return self.bid > self.ask

    @property
    def is_valid_price(self) -> bool:
        return self.bid > Decimal("0") and self.ask > Decimal("0") and not self.is_crossed

    def to_hash_payload(self) -> Dict[str, Any]:
        return {
            "underlying_symbol": self.underlying_symbol,
            "quote_version": self.quote_version,
            "event_time": format_utc_timestamp(self.event_time),
            "ingestion_time": format_utc_timestamp(self.ingestion_time),
            "bid": str(self.bid),
            "ask": str(self.ask),
            "last_price": str(self.last_price) if self.last_price is not None else None,
            "source_id": self.source_id,
            "source_sequence": self.source_sequence,
            "schema_version": self.schema_version,
        }

    def to_dict(self) -> Dict[str, Any]:
        d = self.to_hash_payload()
        d["record_hash"] = self.record_hash
        return d


@dataclass(frozen=True)
class OptionQuote:
    snapshot_id: str
    option_id: str
    quote_version: int
    underlying_symbol: str
    reported_strike: Decimal
    event_time: datetime
    ingestion_time: datetime
    bid: Decimal
    ask: Decimal
    bid_size: int
    ask_size: int
    source_id: str
    source_sequence: int
    schema_version: str
    record_hash: str = ""

    def __post_init__(self) -> None:
        if not self.snapshot_id.strip():
            raise ContractValidationError("snapshot_id must be non-empty")
        if self.quote_version < 1:
            raise ContractValidationError(f"quote_version must be >= 1, got {self.quote_version}")
        # Validate canonical option_id syntax
        CanonicalOptionID.parse(self.option_id)
        sym = self.underlying_symbol.strip().upper()
        if not _SYMBOL_RE.match(sym):
            raise ContractValidationError(f"Invalid underlying_symbol: {self.underlying_symbol!r}")
        object.__setattr__(self, "underlying_symbol", sym)
        object.__setattr__(self, "reported_strike", parse_decimal(self.reported_strike, "reported_strike"))
        object.__setattr__(self, "event_time", parse_utc_timestamp(self.event_time))
        object.__setattr__(self, "ingestion_time", parse_utc_timestamp(self.ingestion_time))
        object.__setattr__(self, "bid", parse_decimal(self.bid, "bid"))
        object.__setattr__(self, "ask", parse_decimal(self.ask, "ask"))
        if not self.record_hash:
            object.__setattr__(self, "record_hash", canonical_hash(self.to_hash_payload()))

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")

    @property
    def spread(self) -> Decimal:
        return self.ask - self.bid

    def to_hash_payload(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "option_id": self.option_id,
            "quote_version": self.quote_version,
            "underlying_symbol": self.underlying_symbol,
            "reported_strike": str(self.reported_strike),
            "event_time": format_utc_timestamp(self.event_time),
            "ingestion_time": format_utc_timestamp(self.ingestion_time),
            "bid": str(self.bid),
            "ask": str(self.ask),
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "source_id": self.source_id,
            "source_sequence": self.source_sequence,
            "schema_version": self.schema_version,
        }

    def to_dict(self) -> Dict[str, Any]:
        d = self.to_hash_payload()
        d["record_hash"] = self.record_hash
        return d


@dataclass(frozen=True)
class ValidatedOptionQuote:
    quote: OptionQuote
    status: ValidationStatus
    flags: Tuple[DataQualityFlag, ...]
    reasons: Tuple[str, ...]
    ingestion_delay_seconds: float
    staleness_seconds: float

    @property
    def is_usable_for_iv(self) -> bool:
        return self.status != ValidationStatus.REJECTED

    def to_dict(self) -> Dict[str, Any]:
        return {
            "quote": self.quote.to_dict(),
            "status": self.status.value,
            "flags": [f.value for f in self.flags],
            "reasons": list(self.reasons),
            "ingestion_delay_seconds": round(self.ingestion_delay_seconds, 6),
            "staleness_seconds": round(self.staleness_seconds, 6),
        }


@dataclass(frozen=True)
class ChainSnapshot:
    snapshot_id: str
    underlying_symbol: str
    snapshot_time: datetime
    ingestion_time: datetime
    source_id: str
    schema_version: str
    config_hash: str
    quotes: Tuple[ValidatedOptionQuote, ...]
    snapshot_hash: str = ""

    def __post_init__(self) -> None:
        sym = self.underlying_symbol.strip().upper()
        object.__setattr__(self, "underlying_symbol", sym)
        object.__setattr__(self, "snapshot_time", parse_utc_timestamp(self.snapshot_time))
        object.__setattr__(self, "ingestion_time", parse_utc_timestamp(self.ingestion_time))
        # Ensure deterministic canonical ordering of quotes inside snapshot
        ordered = tuple(
            sorted(
                self.quotes,
                key=lambda vq: (
                    CanonicalOptionID.parse(vq.quote.option_id).expiry_date.isoformat(),
                    CanonicalOptionID.parse(vq.quote.option_id).strike,
                    CanonicalOptionID.parse(vq.quote.option_id).option_right.value,
                    vq.quote.option_id,
                    vq.quote.quote_version,
                ),
            )
        )
        object.__setattr__(self, "quotes", ordered)
        if not self.snapshot_hash:
            object.__setattr__(self, "snapshot_hash", canonical_hash(self.to_hash_payload()))

    @property
    def quote_count(self) -> int:
        return len(self.quotes)

    @property
    def valid_quote_count(self) -> int:
        return sum(1 for q in self.quotes if q.status == ValidationStatus.VALID)

    @property
    def flagged_quote_count(self) -> int:
        return sum(1 for q in self.quotes if q.status == ValidationStatus.FLAGGED)

    @property
    def rejected_quote_count(self) -> int:
        return sum(1 for q in self.quotes if q.status == ValidationStatus.REJECTED)

    def to_hash_payload(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "underlying_symbol": self.underlying_symbol,
            "snapshot_time": format_utc_timestamp(self.snapshot_time),
            "ingestion_time": format_utc_timestamp(self.ingestion_time),
            "source_id": self.source_id,
            "schema_version": self.schema_version,
            "config_hash": self.config_hash,
            "quotes": [
                {
                    "option_id": vq.quote.option_id,
                    "quote_version": vq.quote.quote_version,
                    "record_hash": vq.quote.record_hash,
                    "status": vq.status.value,
                    "flags": [f.value for f in vq.flags],
                }
                for vq in self.quotes
            ],
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "underlying_symbol": self.underlying_symbol,
            "snapshot_time": format_utc_timestamp(self.snapshot_time),
            "ingestion_time": format_utc_timestamp(self.ingestion_time),
            "source_id": self.source_id,
            "schema_version": self.schema_version,
            "config_hash": self.config_hash,
            "snapshot_hash": self.snapshot_hash,
            "quote_count": self.quote_count,
            "valid_quote_count": self.valid_quote_count,
            "flagged_quote_count": self.flagged_quote_count,
            "rejected_quote_count": self.rejected_quote_count,
            "quotes": [vq.to_dict() for vq in self.quotes],
        }


@dataclass(frozen=True)
class FeatureProvenance:
    snapshot_id: str
    snapshot_hash: str
    option_id: str
    contract_version: Optional[int]
    contract_hash: Optional[str]
    option_quote_version: int
    option_quote_hash: str
    underlying_symbol: str
    underlying_quote_version: Optional[int]
    underlying_quote_hash: Optional[str]
    rate_curve_id: Optional[str]
    rate_curve_version: Optional[int]
    rate_curve_hashes: Tuple[str, ...]
    calendar_id: Optional[str]
    calendar_version: Optional[int]
    calendar_hash: Optional[str]
    config_version: str
    config_hash: str
    schema_version: str
    solver_name: Optional[str] = None
    solver_iterations: Optional[int] = None
    price_residual: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "snapshot_hash": self.snapshot_hash,
            "option_id": self.option_id,
            "contract_version": self.contract_version,
            "contract_hash": self.contract_hash,
            "option_quote_version": self.option_quote_version,
            "option_quote_hash": self.option_quote_hash,
            "underlying_symbol": self.underlying_symbol,
            "underlying_quote_version": self.underlying_quote_version,
            "underlying_quote_hash": self.underlying_quote_hash,
            "rate_curve_id": self.rate_curve_id,
            "rate_curve_version": self.rate_curve_version,
            "rate_curve_hashes": list(self.rate_curve_hashes),
            "calendar_id": self.calendar_id,
            "calendar_version": self.calendar_version,
            "calendar_hash": self.calendar_hash,
            "config_version": self.config_version,
            "config_hash": self.config_hash,
            "schema_version": self.schema_version,
            "solver_name": self.solver_name,
            "solver_iterations": self.solver_iterations,
            "price_residual": self.price_residual,
        }


@dataclass(frozen=True)
class DerivedOptionFeature:
    snapshot_id: str
    option_id: str
    underlying_symbol: str
    valuation_time: datetime
    mid_price: Optional[float]
    spread: Optional[float]
    time_to_expiry_years: Optional[float]
    underlying_mid: Optional[float]
    risk_free_rate: Optional[float]
    dividend_yield: Optional[float]
    forward_price: Optional[float]
    spot_moneyness: Optional[float]
    log_forward_moneyness: Optional[float]
    implied_vol: Optional[float]
    delta: Optional[float]
    vega: Optional[float]
    iv_status: IVStatus
    quality_flags: Tuple[DataQualityFlag, ...]
    notes: Tuple[str, ...]
    provenance: FeatureProvenance
    feature_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "valuation_time", parse_utc_timestamp(self.valuation_time))
        if not self.feature_id:
            object.__setattr__(self, "feature_id", canonical_hash(self.to_hash_payload()))

    def to_hash_payload(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "option_id": self.option_id,
            "underlying_symbol": self.underlying_symbol,
            "valuation_time": format_utc_timestamp(self.valuation_time),
            "mid_price": round(self.mid_price, 10) if self.mid_price is not None else None,
            "spread": round(self.spread, 10) if self.spread is not None else None,
            "time_to_expiry_years": (
                round(self.time_to_expiry_years, 10) if self.time_to_expiry_years is not None else None
            ),
            "underlying_mid": round(self.underlying_mid, 10) if self.underlying_mid is not None else None,
            "risk_free_rate": round(self.risk_free_rate, 10) if self.risk_free_rate is not None else None,
            "dividend_yield": round(self.dividend_yield, 10) if self.dividend_yield is not None else None,
            "forward_price": round(self.forward_price, 10) if self.forward_price is not None else None,
            "spot_moneyness": round(self.spot_moneyness, 10) if self.spot_moneyness is not None else None,
            "log_forward_moneyness": (
                round(self.log_forward_moneyness, 10) if self.log_forward_moneyness is not None else None
            ),
            "implied_vol": round(self.implied_vol, 10) if self.implied_vol is not None else None,
            "delta": round(self.delta, 10) if self.delta is not None else None,
            "vega": round(self.vega, 10) if self.vega is not None else None,
            "iv_status": self.iv_status.value,
            "quality_flags": [f.value for f in self.quality_flags],
            "provenance": self.provenance.to_dict(),
        }

    def to_dict(self) -> Dict[str, Any]:
        d = self.to_hash_payload()
        d["feature_id"] = self.feature_id
        d["notes"] = list(self.notes)
        return d


@dataclass(frozen=True)
class QualityReport:
    snapshot_id: str
    underlying_symbol: str
    snapshot_time: datetime
    generated_at: datetime
    total_quotes: int
    valid_quotes: int
    flagged_quotes: int
    rejected_quotes: int
    computed_iv_count: int
    completeness_ratio: float
    iv_coverage_ratio: float
    max_quote_staleness_seconds: float
    max_ingestion_delay_seconds: float
    flag_counts: Dict[str, int]
    iv_status_counts: Dict[str, int]
    snapshot_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "underlying_symbol": self.underlying_symbol,
            "snapshot_time": format_utc_timestamp(self.snapshot_time),
            "generated_at": format_utc_timestamp(self.generated_at),
            "total_quotes": self.total_quotes,
            "valid_quotes": self.valid_quotes,
            "flagged_quotes": self.flagged_quotes,
            "rejected_quotes": self.rejected_quotes,
            "computed_iv_count": self.computed_iv_count,
            "completeness_ratio": round(self.completeness_ratio, 6),
            "iv_coverage_ratio": round(self.iv_coverage_ratio, 6),
            "max_quote_staleness_seconds": round(self.max_quote_staleness_seconds, 6),
            "max_ingestion_delay_seconds": round(self.max_ingestion_delay_seconds, 6),
            "flag_counts": dict(sorted(self.flag_counts.items())),
            "iv_status_counts": dict(sorted(self.iv_status_counts.items())),
            "snapshot_hash": self.snapshot_hash,
        }
