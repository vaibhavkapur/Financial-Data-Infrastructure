"""Pure exact-Decimal adjustment factor calculations and PIT action resolution."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, localcontext
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from corp_actions.config import EngineConfig
from corp_actions.contracts import (
    UNSUPPORTED_ACTION_TYPES,
    ActionType,
    AdjustmentFactorPoint,
    CorporateActionEvent,
    FxRate,
    QuarantineRecord,
    RawPriceBar,
    RevisionType,
)
from corp_actions.errors import (
    MissingFxRateError,
    MissingMarketDataError,
    SameDayConflictError,
    UnsupportedActionError,
    ValidationError,
)

ACTION_PRECEDENCE: Dict[ActionType, int] = {
    ActionType.SYMBOL_CHANGE: 10,
    ActionType.SPLIT: 20,
    ActionType.REVERSE_SPLIT: 20,
    ActionType.CASH_DIVIDEND: 30,
    ActionType.SPECIAL_DIVIDEND: 40,
    ActionType.MERGER: 90,
    ActionType.SPINOFF: 90,
}


@dataclass(frozen=True)
class ResolvedActions:
    """Result of resolving versioned corporate actions at a PIT as-of timestamp."""

    effective: Tuple[CorporateActionEvent, ...]
    pending_future_ex: Tuple[CorporateActionEvent, ...]
    cancelled: Tuple[CorporateActionEvent, ...]
    quarantined: Tuple[QuarantineRecord, ...]


def resolve_fx_rate(
    base_currency: str,
    quote_currency: str,
    rate_date: date,
    fx_rates: Sequence[FxRate],
    *,
    as_of_time: Optional[datetime] = None,
) -> Decimal:
    """Resolve spot FX rate converting 1 unit of base_currency into quote_currency.

    Checks direct quote (base/quote), then inverse quote (quote/base), falling back
    to the latest rate on or before rate_date (still respecting as_of_time if supplied).
    """
    if base_currency == quote_currency:
        return Decimal("1")

    eligible = [
        fx
        for fx in fx_rates
        if fx.rate_date <= rate_date
        and (as_of_time is None or fx.ingestion_time <= as_of_time)
    ]

    direct = [
        fx
        for fx in eligible
        if fx.base_currency == base_currency and fx.quote_currency == quote_currency
    ]
    if direct:
        latest = max(direct, key=lambda r: (r.rate_date, r.ingestion_time))
        return latest.rate

    inverse = [
        fx
        for fx in eligible
        if fx.base_currency == quote_currency and fx.quote_currency == base_currency
    ]
    if inverse:
        latest = max(inverse, key=lambda r: (r.rate_date, r.ingestion_time))
        return Decimal("1") / latest.rate

    raise MissingFxRateError(
        f"No FX rate available for {base_currency}/{quote_currency} on or before {rate_date}",
        details={
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "rate_date": rate_date.isoformat(),
        },
    )


def resolve_pit_actions(
    events: Sequence[CorporateActionEvent],
    *,
    as_of_time: datetime,
    effective_cutoff_date: date,
    strict_unsupported: bool = False,
) -> ResolvedActions:
    """Resolve bi-temporal corporate action revisions strictly without hindsight.

    Invariants:
    1. Only versions with availability_time <= as_of_time are visible.
    2. For each action_id, only the highest visible version applies.
    3. Actions whose latest visible version is CANCELLATION are excluded and recorded.
    4. Unsupported action types (MERGER, SPINOFF) are explicitly quarantined (or raise
       UnsupportedActionError when strict_unsupported=True).
    5. Supported actions with ex_date > effective_cutoff_date are separated into
       pending_future_ex so historical snapshots never prematurely apply un-exed events.
    """
    latest_by_id: Dict[str, CorporateActionEvent] = {}
    for ev in events:
        if ev.availability_time > as_of_time:
            continue
        prev = latest_by_id.get(ev.action_id)
        if prev is None or (ev.version, ev.availability_time) > (prev.version, prev.availability_time):
            latest_by_id[ev.action_id] = ev

    effective: List[CorporateActionEvent] = []
    pending_future_ex: List[CorporateActionEvent] = []
    cancelled: List[CorporateActionEvent] = []
    quarantined: List[QuarantineRecord] = []

    for ev in latest_by_id.values():
        if ev.revision_type == RevisionType.CANCELLATION:
            cancelled.append(ev)
            continue

        if ev.action_type in UNSUPPORTED_ACTION_TYPES:
            if strict_unsupported:
                raise UnsupportedActionError(
                    f"Unsupported corporate action {ev.action_id} ({ev.action_type.value}) on {ev.ex_date}",
                    details={
                        "action_id": ev.action_id,
                        "version": ev.version,
                        "action_type": ev.action_type.value,
                        "security_id": ev.security_id,
                        "ex_date": ev.ex_date.isoformat(),
                    },
                )
            quarantined.append(
                QuarantineRecord(
                    action_id=ev.action_id,
                    version=ev.version,
                    action_type=ev.action_type.value,
                    security_id=ev.security_id,
                    ex_date=ev.ex_date,
                    reason_code="UNSUPPORTED_ACTION_TYPE",
                    message=(
                        f"{ev.action_type.value} requires external tax-lot allocation and "
                        f"deal-valuation assumptions not supported by automated adjustment."
                    ),
                    remediation=(
                        "Flag position for manual corporate-actions desk review; apply custom "
                        "valuation/tax election outside automated split/dividend pipeline."
                    ),
                    availability_time=ev.availability_time,
                    source=ev.source,
                )
            )
            continue

        if ev.ex_date > effective_cutoff_date:
            pending_future_ex.append(ev)
        else:
            effective.append(ev)

    sort_key = lambda e: (e.ex_date, ACTION_PRECEDENCE[e.action_type], e.action_id, e.version)
    effective.sort(key=sort_key)
    pending_future_ex.sort(key=sort_key)
    cancelled.sort(key=sort_key)
    quarantined.sort(key=lambda q: (q.ex_date, q.action_id, q.version))

    return ResolvedActions(
        effective=tuple(effective),
        pending_future_ex=tuple(pending_future_ex),
        cancelled=tuple(cancelled),
        quarantined=tuple(quarantined),
    )


def _validate_same_day_actions(day_actions: Sequence[CorporateActionEvent]) -> None:
    """Detect conflicting duplicate actions on the same (security_id, ex_date)."""
    splits = [a for a in day_actions if a.action_type in {ActionType.SPLIT, ActionType.REVERSE_SPLIT}]
    if len(splits) > 1:
        raise SameDayConflictError(
            f"Conflicting multiple split actions on {splits[0].security_id} @ {splits[0].ex_date}: "
            f"{[s.action_id for s in splits]}",
            details={
                "security_id": splits[0].security_id,
                "ex_date": splits[0].ex_date.isoformat(),
                "action_ids": [s.action_id for s in splits],
            },
        )
    sym_changes = [a for a in day_actions if a.action_type == ActionType.SYMBOL_CHANGE]
    if len(sym_changes) > 1:
        raise SameDayConflictError(
            f"Conflicting multiple symbol changes on {sym_changes[0].security_id} @ {sym_changes[0].ex_date}: "
            f"{[s.action_id for s in sym_changes]}",
            details={
                "security_id": sym_changes[0].security_id,
                "ex_date": sym_changes[0].ex_date.isoformat(),
                "action_ids": [s.action_id for s in sym_changes],
            },
        )


def compute_factor_schedule(
    raw_bars: Sequence[RawPriceBar],
    effective_actions: Sequence[CorporateActionEvent],
    fx_rates: Sequence[FxRate],
    config: EngineConfig,
    *,
    as_of_time: Optional[datetime] = None,
) -> Tuple[AdjustmentFactorPoint, ...]:
    """Compute cumulative backward adjustment factors per trade_date in exact Decimal.

    Backward adjustment convention (CRSP / S&P standard):
    - On the latest date (after all ex-dates), cumulative factors equal 1.
    - For every trade_date strictly before an action's ex_date:
      * Split (ratio = num/den):
            split_price_factor *= (1 / ratio)
            split_volume_factor *= ratio
      * Dividend (gross cash D converted to security quote currency):
            reference_prior_close = prior_close / same_day_split_ratio
            dividend_factor *= (reference_prior_close - D_post_split) / reference_prior_close
      * Total return factor = split_price_factor * dividend_factor
    - Symbol changes do not alter price/volume factors; they update active_symbol from ex_date onward.
    """
    if not raw_bars:
        return ()

    ordered_bars = sorted(raw_bars, key=lambda b: b.trade_date)
    security_id = ordered_bars[0].security_id
    quote_currency = ordered_bars[0].currency
    dates = [b.trade_date for b in ordered_bars]
    close_by_date: Dict[date, Decimal] = {b.trade_date: b.close for b in ordered_bars}

    actions_by_ex: Dict[date, List[CorporateActionEvent]] = defaultdict(list)
    for action in effective_actions:
        if action.security_id != security_id:
            continue
        actions_by_ex[action.ex_date].append(action)

    # Pre-compute single-day step factors at each ex_date
    ex_step_factors: Dict[date, Tuple[Decimal, Decimal, Decimal, Tuple[str, ...]]] = {}

    with localcontext() as ctx:
        ctx.prec = config.decimal_precision
        ctx.rounding = config.rounding_mode

        for ex_d in sorted(actions_by_ex.keys()):
            day_actions = sorted(
                actions_by_ex[ex_d],
                key=lambda a: (ACTION_PRECEDENCE[a.action_type], a.action_id, a.version),
            )
            _validate_same_day_actions(day_actions)

            step_split_ratio = Decimal("1")
            step_split_price = Decimal("1")
            step_split_vol = Decimal("1")
            step_div_factor = Decimal("1")
            applied_ids: List[str] = []

            # 1. Process splits first so same-day dividends know the post-split reference price
            for act in day_actions:
                if act.action_type in {ActionType.SPLIT, ActionType.REVERSE_SPLIT}:
                    ratio = act.split_ratio
                    assert ratio is not None
                    step_split_ratio = ratio
                    step_split_price = Decimal("1") / ratio
                    step_split_vol = ratio
                    applied_ids.append(f"{act.action_id}@v{act.version}")
                elif act.action_type == ActionType.SYMBOL_CHANGE:
                    applied_ids.append(f"{act.action_id}@v{act.version}")

            # 2. Process dividends (regular + special) on this ex_date
            div_actions = [
                a
                for a in day_actions
                if a.action_type in {ActionType.CASH_DIVIDEND, ActionType.SPECIAL_DIVIDEND}
            ]
            if div_actions:
                prior_dates = [d for d in dates if d < ex_d]
                if not prior_dates:
                    raise MissingMarketDataError(
                        f"Cannot compute dividend factor for {security_id} on {ex_d}: no prior close bar",
                        details={"security_id": security_id, "ex_date": ex_d.isoformat()},
                    )
                prior_close = close_by_date[prior_dates[-1]]
                post_split_ref_close = prior_close / step_split_ratio

                total_post_split_div = Decimal("0")
                for div_act in div_actions:
                    assert div_act.cash_amount is not None and div_act.currency is not None
                    fx_date = (
                        div_act.ex_date
                        if config.fx_conversion_date_policy == "ex_date"
                        else (div_act.pay_date or div_act.ex_date)
                    )
                    fx = resolve_fx_rate(
                        div_act.currency,
                        quote_currency,
                        fx_date,
                        fx_rates,
                        as_of_time=as_of_time,
                    )
                    gross_quote_div = div_act.cash_amount * fx
                    basis = div_act.dividend_basis or config.same_day_dividend_basis
                    if basis == "pre_split" and step_split_ratio != Decimal("1"):
                        gross_quote_div = gross_quote_div / step_split_ratio
                    total_post_split_div += gross_quote_div
                    applied_ids.append(f"{div_act.action_id}@v{div_act.version}")

                if total_post_split_div >= post_split_ref_close:
                    raise ValidationError(
                        f"Total gross dividend ({total_post_split_div}) >= reference prior close "
                        f"({post_split_ref_close}) for {security_id} on {ex_d}",
                        details={
                            "security_id": security_id,
                            "ex_date": ex_d.isoformat(),
                            "dividend": str(total_post_split_div),
                            "reference_close": str(post_split_ref_close),
                        },
                    )
                step_div_factor = (post_split_ref_close - total_post_split_div) / post_split_ref_close

            ex_step_factors[ex_d] = (
                step_split_price,
                step_split_vol,
                step_div_factor,
                tuple(applied_ids),
            )

        # Resolve active symbol forward through time
        sym_events = sorted(
            [a for a in effective_actions if a.security_id == security_id and a.action_type == ActionType.SYMBOL_CHANGE],
            key=lambda a: a.ex_date,
        )

        def active_symbol_for(d: date, fallback: str) -> str:
            sym = ordered_bars[0].symbol
            for se in sym_events:
                if d >= se.ex_date and se.new_symbol:
                    sym = se.new_symbol
            return sym or fallback

        # Accumulate backward factors for each trade_date from all ex_dates strictly > trade_date
        schedule: List[AdjustmentFactorPoint] = []
        sorted_ex_dates = sorted(ex_step_factors.keys())

        for bar in ordered_bars:
            cum_split_price = Decimal("1")
            cum_split_vol = Decimal("1")
            cum_div = Decimal("1")
            applied_for_bar: List[str] = []

            for ex_d in sorted_ex_dates:
                if bar.trade_date < ex_d:
                    sp, sv, df, ids = ex_step_factors[ex_d]
                    cum_split_price *= sp
                    cum_split_vol *= sv
                    cum_div *= df
                    applied_for_bar.extend(ids)
                elif bar.trade_date == ex_d:
                    # Include symbol-change or same-day action tags on ex_date for audit lineage
                    _, _, _, ids = ex_step_factors[ex_d]
                    for aid in ids:
                        if aid not in applied_for_bar:
                            applied_for_bar.append(aid)

            cum_tr = cum_split_price * cum_div
            schedule.append(
                AdjustmentFactorPoint(
                    security_id=security_id,
                    trade_date=bar.trade_date,
                    active_symbol=active_symbol_for(bar.trade_date, bar.symbol),
                    cumulative_split_price_factor=cum_split_price.normalize(),
                    cumulative_split_volume_factor=cum_split_vol.normalize(),
                    cumulative_dividend_factor=cum_div.normalize(),
                    cumulative_total_return_factor=cum_tr.normalize(),
                    applied_action_ids=tuple(applied_for_bar),
                )
            )

    return tuple(schedule)
