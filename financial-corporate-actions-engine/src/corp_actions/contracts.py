"""Versioned canonical data contracts for inputs, outputs, and diagnostics."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

SCHEMA_VERSION = "1.0.0"


class ActionType(str, Enum):
    SPLIT = "SPLIT"
    REVERSE_SPLIT = "REVERSE_SPLIT"
    CASH_DIVIDEND = "CASH_DIVIDEND"
    SPECIAL_DIVIDEND = "SPECIAL_DIVIDEND"
    SYMBOL_CHANGE = "SYMBOL_CHANGE"
    MERGER = "MERGER"
    SPINOFF = "SPINOFF"


SUPPORTED_ACTION_TYPES = frozenset(
    {
        ActionType.SPLIT,
        ActionType.REVERSE_SPLIT,
        ActionType.CASH_DIVIDEND,
        ActionType.SPECIAL_DIVIDEND,
        ActionType.SYMBOL_CHANGE,
    }
)

UNSUPPORTED_ACTION_TYPES = frozenset(
    {
        ActionType.MERGER,
        ActionType.SPINOFF,
    }
)


class RevisionType(str, Enum):
    NEW = "NEW"
    CORRECTION = "CORRECTION"
    CANCELLATION = "CANCELLATION"


def serialize_value(val: Any) -> Any:
    """Convert domain values into deterministic JSON-serializable primitives."""
    if isinstance(val, Decimal):
        return format(val, "f")
    if isinstance(val, datetime):
        return val.isoformat().replace("+00:00", "Z")
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, Enum):
        return val.value
    if isinstance(val, tuple):
        return [serialize_value(v) for v in val]
    if isinstance(val, list):
        return [serialize_value(v) for v in val]
    if isinstance(val, dict):
        return {str(k): serialize_value(v) for k, v in sorted(val.items())}
    return val


def dataclass_to_dict(obj: Any) -> Dict[str, Any]:
    return {k: serialize_value(v) for k, v in asdict(obj).items()}


@dataclass(frozen=True)
class RawPriceBar:
    """Unadjusted historical OHLCV bar with bi-temporal metadata."""

    security_id: str
    symbol: str
    trade_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    currency: str
    event_time: datetime
    ingestion_time: datetime
    source: str
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(frozen=True)
class PositionSnapshot:
    """Starting or point-in-time portfolio position & cash snapshot."""

    account_id: str
    security_id: str
    symbol: str
    as_of_date: date
    quantity: Decimal
    cost_basis_per_share: Decimal
    currency: str
    cash_balance: Decimal
    event_time: datetime
    ingestion_time: datetime
    source: str
    schema_version: str = SCHEMA_VERSION

    @property
    def total_cost_basis(self) -> Decimal:
        return self.quantity * self.cost_basis_per_share

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(frozen=True)
class FxRate:
    """Spot FX conversion rate: 1 unit of base_currency = rate units of quote_currency."""

    base_currency: str
    quote_currency: str
    rate_date: date
    rate: Decimal
    event_time: datetime
    ingestion_time: datetime
    source: str
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(frozen=True)
class CorporateActionEvent:
    """Versioned corporate action event carrying event, ingestion, and availability times."""

    action_id: str
    version: int
    revision_type: RevisionType
    action_type: ActionType
    security_id: str
    symbol: str
    announcement_date: date
    ex_date: date
    event_time: datetime
    ingestion_time: datetime
    availability_time: datetime
    source: str
    lineage_id: str
    record_date: Optional[date] = None
    pay_date: Optional[date] = None
    ratio_numerator: Optional[Decimal] = None
    ratio_denominator: Optional[Decimal] = None
    cash_amount: Optional[Decimal] = None
    currency: Optional[str] = None
    tax_withholding_rate: Decimal = Decimal("0")
    new_symbol: Optional[str] = None
    dividend_basis: Optional[str] = None  # "post_split" or "pre_split"
    schema_version: str = SCHEMA_VERSION
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def split_ratio(self) -> Optional[Decimal]:
        if self.ratio_numerator is not None and self.ratio_denominator is not None:
            return self.ratio_numerator / self.ratio_denominator
        return None

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(frozen=True)
class AdjustmentFactorPoint:
    """Cumulative adjustment factors effective on a given trade_date."""

    security_id: str
    trade_date: date
    active_symbol: str
    cumulative_split_price_factor: Decimal
    cumulative_split_volume_factor: Decimal
    cumulative_dividend_factor: Decimal
    cumulative_total_return_factor: Decimal
    applied_action_ids: Tuple[str, ...]

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(frozen=True)
class AdjustedPriceBar:
    """Separated raw, split-adjusted, and total-return adjusted price bar."""

    security_id: str
    raw_symbol: str
    active_symbol: str
    trade_date: date
    currency: str
    # Raw (unadjusted) values
    raw_open: Decimal
    raw_high: Decimal
    raw_low: Decimal
    raw_close: Decimal
    raw_volume: Decimal
    # Split-adjusted values (splits & symbol continuity only)
    split_adj_open: Decimal
    split_adj_high: Decimal
    split_adj_low: Decimal
    split_adj_close: Decimal
    split_adj_volume: Decimal
    # Total-return adjusted values (splits + regular & special dividends)
    tr_adj_open: Decimal
    tr_adj_high: Decimal
    tr_adj_low: Decimal
    tr_adj_close: Decimal
    # Factors & lineage
    split_price_factor: Decimal
    split_volume_factor: Decimal
    dividend_factor: Decimal
    total_return_factor: Decimal
    applied_action_ids: Tuple[str, ...]
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(frozen=True)
class PositionLedgerEntry:
    """Daily state of a position and its dividend cash flows."""

    account_id: str
    security_id: str
    active_symbol: str
    trade_date: date
    currency: str
    quantity: Decimal
    cost_basis_per_share: Decimal
    total_cost_basis: Decimal
    raw_close: Decimal
    market_value: Decimal
    dividend_accrued_today_gross: Decimal
    dividend_tax_today: Decimal
    dividend_accrued_today_net: Decimal
    dividend_settled_today_net: Decimal
    pending_receivable_cash: Decimal
    settled_cash_balance: Decimal
    cumulative_gross_dividends: Decimal
    cumulative_net_dividends: Decimal
    total_equity_plus_net_cash: Decimal
    applied_action_ids: Tuple[str, ...]
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(frozen=True)
class QuarantineRecord:
    """Explicit record for unsupported or quarantined actions (never silent)."""

    action_id: str
    version: int
    action_type: str
    security_id: str
    ex_date: date
    reason_code: str
    message: str
    remediation: str
    availability_time: datetime
    source: str

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(frozen=True)
class ReconciliationResult:
    """Total-return reconciliation & data-quality diagnostic summary."""

    security_id: str
    account_id: Optional[str]
    as_of_time: datetime
    start_date: date
    end_date: date
    tr_series_return: Decimal
    portfolio_gross_return: Decimal
    return_discrepancy: Decimal
    tolerance: Decimal
    reconciled: bool
    split_jump_checks_passed: bool
    completeness_ratio: Decimal
    max_freshness_lag_seconds: float
    freshness_sla_breaches: int
    quarantined_action_count: int
    applied_action_count: int
    notes: Tuple[str, ...]

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(frozen=True)
class ReplayDiff:
    """Deterministic diff produced by replaying between two PIT as-of timestamps."""

    security_id: str
    from_as_of: datetime
    to_as_of: datetime
    added_actions: Tuple[str, ...]
    corrected_actions: Tuple[str, ...]
    cancelled_actions: Tuple[str, ...]
    quarantined_actions: Tuple[str, ...]
    changed_dates: Tuple[date, ...]
    price_deltas: Dict[str, Dict[str, str]]
    cash_delta_end: Optional[Decimal]
    quantity_delta_end: Optional[Decimal]
    from_digest: str
    to_digest: str

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(frozen=True)
class SnapshotResult:
    """Complete immutable output of a PIT as-of query."""

    run_id: str
    security_id: str
    as_of_time: datetime
    config_version: str
    schema_version: str
    raw_series: Tuple[RawPriceBar, ...]
    adjusted_series: Tuple[AdjustedPriceBar, ...]
    factor_schedule: Tuple[AdjustmentFactorPoint, ...]
    position_ledger: Tuple[PositionLedgerEntry, ...]
    effective_actions: Tuple[CorporateActionEvent, ...]
    quarantined_actions: Tuple[QuarantineRecord, ...]
    reconciliation: Optional[ReconciliationResult]
    output_digest: str
    diagnostics: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "security_id": self.security_id,
            "as_of_time": serialize_value(self.as_of_time),
            "config_version": self.config_version,
            "schema_version": self.schema_version,
            "raw_series": [r.to_dict() for r in self.raw_series],
            "adjusted_series": [a.to_dict() for a in self.adjusted_series],
            "factor_schedule": [f.to_dict() for f in self.factor_schedule],
            "position_ledger": [p.to_dict() for p in self.position_ledger],
            "effective_actions": [e.to_dict() for e in self.effective_actions],
            "quarantined_actions": [q.to_dict() for q in self.quarantined_actions],
            "reconciliation": self.reconciliation.to_dict() if self.reconciliation else None,
            "output_digest": self.output_digest,
            "diagnostics": serialize_value(self.diagnostics),
        }
