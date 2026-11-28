"""Strict contract validation for prices, positions, FX rates, and corporate actions."""

from __future__ import annotations

from decimal import Decimal
from typing import Iterable, List, Optional

from corp_actions.contracts import (
    SCHEMA_VERSION,
    SUPPORTED_ACTION_TYPES,
    UNSUPPORTED_ACTION_TYPES,
    ActionType,
    CorporateActionEvent,
    FxRate,
    PositionSnapshot,
    RawPriceBar,
    RevisionType,
)
from corp_actions.errors import SchemaVersionError, UnsupportedActionError, ValidationError


def _require_tz_aware(dt, field_name: str, context: str) -> None:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValidationError(
            f"{context}: {field_name} must be timezone-aware (UTC recommended)",
            details={"field": field_name, "context": context},
        )


def _require_schema_version(version: str, context: str) -> None:
    if version != SCHEMA_VERSION:
        raise SchemaVersionError(
            f"{context}: unsupported schema_version '{version}' (expected '{SCHEMA_VERSION}')",
            details={"schema_version": version, "expected": SCHEMA_VERSION},
        )


def _require_iso_currency(ccy: Optional[str], field_name: str, context: str) -> None:
    if not ccy or len(ccy) != 3 or not ccy.isalpha() or ccy != ccy.upper():
        raise ValidationError(
            f"{context}: {field_name} must be a 3-letter uppercase ISO currency code, got {ccy!r}",
            details={"field": field_name, "value": ccy},
        )


def validate_price_bar(bar: RawPriceBar) -> None:
    ctx = f"RawPriceBar({bar.security_id}@{bar.trade_date})"
    _require_schema_version(bar.schema_version, ctx)
    if not bar.security_id.strip():
        raise ValidationError(f"{ctx}: security_id must be non-empty")
    if not bar.symbol.strip():
        raise ValidationError(f"{ctx}: symbol must be non-empty")
    _require_iso_currency(bar.currency, "currency", ctx)
    for name, val in [
        ("open", bar.open),
        ("high", bar.high),
        ("low", bar.low),
        ("close", bar.close),
    ]:
        if val <= Decimal("0"):
            raise ValidationError(
                f"{ctx}: {name} must be strictly positive, got {val}",
                details={"field": name, "value": str(val)},
            )
    if bar.high < bar.low:
        raise ValidationError(f"{ctx}: high ({bar.high}) < low ({bar.low})")
    if bar.high < max(bar.open, bar.close) or bar.low > min(bar.open, bar.close):
        raise ValidationError(f"{ctx}: OHLC invariant violated (O={bar.open}, H={bar.high}, L={bar.low}, C={bar.close})")
    if bar.volume < Decimal("0"):
        raise ValidationError(f"{ctx}: volume must be non-negative, got {bar.volume}")
    _require_tz_aware(bar.event_time, "event_time", ctx)
    _require_tz_aware(bar.ingestion_time, "ingestion_time", ctx)
    if not bar.source.strip():
        raise ValidationError(f"{ctx}: source must be non-empty")


def validate_position_snapshot(pos: PositionSnapshot) -> None:
    ctx = f"PositionSnapshot({pos.account_id}/{pos.security_id}@{pos.as_of_date})"
    _require_schema_version(pos.schema_version, ctx)
    if not pos.account_id.strip() or not pos.security_id.strip() or not pos.symbol.strip():
        raise ValidationError(f"{ctx}: account_id, security_id, and symbol must be non-empty")
    _require_iso_currency(pos.currency, "currency", ctx)
    if pos.quantity < Decimal("0"):
        raise ValidationError(
            f"{ctx}: negative (short) quantity {pos.quantity} is not supported in this prototype"
        )
    if pos.cost_basis_per_share < Decimal("0"):
        raise ValidationError(f"{ctx}: cost_basis_per_share must be non-negative")
    _require_tz_aware(pos.event_time, "event_time", ctx)
    _require_tz_aware(pos.ingestion_time, "ingestion_time", ctx)


def validate_fx_rate(fx: FxRate) -> None:
    ctx = f"FxRate({fx.base_currency}/{fx.quote_currency}@{fx.rate_date})"
    _require_schema_version(fx.schema_version, ctx)
    _require_iso_currency(fx.base_currency, "base_currency", ctx)
    _require_iso_currency(fx.quote_currency, "quote_currency", ctx)
    if fx.rate <= Decimal("0"):
        raise ValidationError(f"{ctx}: FX rate must be strictly positive, got {fx.rate}")
    _require_tz_aware(fx.event_time, "event_time", ctx)
    _require_tz_aware(fx.ingestion_time, "ingestion_time", ctx)


def validate_corporate_action(
    event: CorporateActionEvent,
    *,
    allow_unsupported: bool = True,
) -> None:
    """Validate canonical corporate action schema and domain invariants.

    When allow_unsupported=True, MERGER and SPINOFF pass structural validation so the
    engine can quarantine and report them explicitly rather than crashing ingestion.
    When allow_unsupported=False, UnsupportedActionError is raised immediately.
    """
    ctx = f"CorporateActionEvent({event.action_id} v{event.version})"
    _require_schema_version(event.schema_version, ctx)
    if not event.action_id.strip():
        raise ValidationError(f"{ctx}: action_id must be non-empty")
    if event.version < 1:
        raise ValidationError(f"{ctx}: version must be >= 1, got {event.version}")
    if event.revision_type == RevisionType.NEW and event.version != 1:
        raise ValidationError(f"{ctx}: revision_type NEW requires version == 1")
    if event.revision_type in {RevisionType.CORRECTION, RevisionType.CANCELLATION} and event.version < 2:
        raise ValidationError(f"{ctx}: revision_type {event.revision_type.value} requires version >= 2")
    if not event.security_id.strip() or not event.symbol.strip():
        raise ValidationError(f"{ctx}: security_id and symbol must be non-empty")
    if not event.source.strip() or not event.lineage_id.strip():
        raise ValidationError(f"{ctx}: source and lineage_id must be non-empty")

    _require_tz_aware(event.event_time, "event_time", ctx)
    _require_tz_aware(event.ingestion_time, "ingestion_time", ctx)
    _require_tz_aware(event.availability_time, "availability_time", ctx)

    if event.pay_date is not None and event.pay_date < event.ex_date:
        raise ValidationError(
            f"{ctx}: pay_date ({event.pay_date}) cannot precede ex_date ({event.ex_date})",
            details={"ex_date": event.ex_date.isoformat(), "pay_date": event.pay_date.isoformat()},
        )
    if event.record_date is not None and event.pay_date is not None and event.pay_date < event.record_date:
        raise ValidationError(
            f"{ctx}: pay_date ({event.pay_date}) cannot precede record_date ({event.record_date})"
        )

    if event.action_type in UNSUPPORTED_ACTION_TYPES:
        if not allow_unsupported:
            raise UnsupportedActionError(
                f"{ctx}: action_type {event.action_type.value} requires tax/valuation assumptions and is unsupported",
                details={
                    "action_id": event.action_id,
                    "action_type": event.action_type.value,
                    "security_id": event.security_id,
                },
            )
        return

    if event.action_type not in SUPPORTED_ACTION_TYPES:
        raise UnsupportedActionError(
            f"{ctx}: unknown action_type {event.action_type}",
            details={"action_type": str(event.action_type)},
        )

    if event.revision_type == RevisionType.CANCELLATION:
        return

    if event.action_type in {ActionType.SPLIT, ActionType.REVERSE_SPLIT}:
        if event.ratio_numerator is None or event.ratio_denominator is None:
            raise ValidationError(f"{ctx}: split requires ratio_numerator and ratio_denominator")
        if event.ratio_numerator <= Decimal("0") or event.ratio_denominator <= Decimal("0"):
            raise ValidationError(
                f"{ctx}: split numerator/denominator must be > 0, got {event.ratio_numerator}/{event.ratio_denominator}"
            )
        ratio = event.ratio_numerator / event.ratio_denominator
        if event.action_type == ActionType.SPLIT and ratio <= Decimal("1"):
            raise ValidationError(
                f"{ctx}: SPLIT requires ratio > 1 (got {ratio}); use REVERSE_SPLIT for ratio < 1"
            )
        if event.action_type == ActionType.REVERSE_SPLIT and ratio >= Decimal("1"):
            raise ValidationError(
                f"{ctx}: REVERSE_SPLIT requires ratio < 1 (got {ratio}); use SPLIT for ratio > 1"
            )

    elif event.action_type in {ActionType.CASH_DIVIDEND, ActionType.SPECIAL_DIVIDEND}:
        if event.cash_amount is None or event.cash_amount <= Decimal("0"):
            raise ValidationError(f"{ctx}: dividend requires cash_amount > 0, got {event.cash_amount}")
        _require_iso_currency(event.currency, "currency", ctx)
        if event.pay_date is None:
            raise ValidationError(f"{ctx}: dividend requires pay_date")
        if event.tax_withholding_rate < Decimal("0") or event.tax_withholding_rate >= Decimal("1"):
            raise ValidationError(
                f"{ctx}: tax_withholding_rate must be in [0, 1), got {event.tax_withholding_rate}"
            )
        if event.dividend_basis is not None and event.dividend_basis not in {"post_split", "pre_split"}:
            raise ValidationError(
                f"{ctx}: dividend_basis must be 'post_split' or 'pre_split', got {event.dividend_basis!r}"
            )

    elif event.action_type == ActionType.SYMBOL_CHANGE:
        if not event.new_symbol or not event.new_symbol.strip():
            raise ValidationError(f"{ctx}: SYMBOL_CHANGE requires non-empty new_symbol")
        if event.new_symbol == event.symbol:
            raise ValidationError(f"{ctx}: SYMBOL_CHANGE new_symbol must differ from symbol ({event.symbol})")


def validate_batch(
    *,
    prices: Iterable[RawPriceBar] = (),
    positions: Iterable[PositionSnapshot] = (),
    fx_rates: Iterable[FxRate] = (),
    actions: Iterable[CorporateActionEvent] = (),
    allow_unsupported: bool = True,
) -> List[str]:
    """Validate a full batch of records and return list of any warning messages."""
    warnings: List[str] = []
    for bar in prices:
        validate_price_bar(bar)
    for pos in positions:
        validate_position_snapshot(pos)
    for fx in fx_rates:
        validate_fx_rate(fx)
    for action in actions:
        validate_corporate_action(action, allow_unsupported=allow_unsupported)
        if action.action_type in UNSUPPORTED_ACTION_TYPES:
            warnings.append(
                f"Action {action.action_id} ({action.action_type.value}) will be quarantined on processing"
            )
    return warnings
