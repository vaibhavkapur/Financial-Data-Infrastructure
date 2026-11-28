"""Pure state transitions applying corporate actions to positions and dividend cash."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, localcontext
from typing import Dict, List, Optional, Sequence, Tuple

from corp_actions.config import EngineConfig
from corp_actions.contracts import (
    ActionType,
    CorporateActionEvent,
    FxRate,
    PositionLedgerEntry,
    PositionSnapshot,
    RawPriceBar,
)
from corp_actions.domain.factors import ACTION_PRECEDENCE, _validate_same_day_actions, resolve_fx_rate


def apply_actions_to_position(
    initial_position: PositionSnapshot,
    raw_bars: Sequence[RawPriceBar],
    effective_actions: Sequence[CorporateActionEvent],
    fx_rates: Sequence[FxRate],
    config: EngineConfig,
    *,
    as_of_time: Optional[datetime] = None,
) -> Tuple[PositionLedgerEntry, ...]:
    """Deterministically evolve a position and cash balance over the trading calendar.

    Semantics:
    - Splits / Reverse Splits (on ex_date):
        quantity *= ratio
        cost_basis_per_share /= ratio  (total cost basis preserved!)
    - Cash / Special Dividends:
        * On ex_date (entitlement determined on shares held):
          - If dividend_basis == 'post_split' (default), applied to post-split shares on ex_date.
          - If dividend_basis == 'pre_split', applied to pre-split shares entering ex_date.
          - Accrues gross dividend (converted via FX to position currency), deducts
            withholding tax, adds net dividend to pending_receivable_cash scheduled for pay_date.
        * On pay_date:
          - Pending receivable settles into settled_cash_balance.
    - Symbol Change (on ex_date):
        Updates active_symbol without changing shares or cash.
    """
    ordered_bars = sorted(
        [b for b in raw_bars if b.security_id == initial_position.security_id and b.trade_date >= initial_position.as_of_date],
        key=lambda b: b.trade_date,
    )
    if not ordered_bars:
        return ()

    actions_by_ex: Dict[date, List[CorporateActionEvent]] = defaultdict(list)
    for act in effective_actions:
        if act.security_id == initial_position.security_id and act.ex_date >= initial_position.as_of_date:
            actions_by_ex[act.ex_date].append(act)

    # Track scheduled settlements: pay_date -> list of net cash amounts
    settlements_by_pay_date: Dict[date, List[Decimal]] = defaultdict(list)

    ledger: List[PositionLedgerEntry] = []

    with localcontext() as ctx:
        ctx.prec = config.decimal_precision
        ctx.rounding = config.rounding_mode

        qty = initial_position.quantity
        cost_per_share = initial_position.cost_basis_per_share
        settled_cash = initial_position.cash_balance
        pending_receivable = Decimal("0")
        cum_gross_div = Decimal("0")
        cum_net_div = Decimal("0")
        active_symbol = initial_position.symbol

        for bar in ordered_bars:
            d = bar.trade_date
            day_actions = sorted(
                actions_by_ex.get(d, ()),
                key=lambda a: (ACTION_PRECEDENCE[a.action_type], a.action_id, a.version),
            )
            if day_actions:
                _validate_same_day_actions(day_actions)

            applied_today: List[str] = []
            pre_split_qty = qty
            div_accrued_gross = Decimal("0")
            div_tax_today = Decimal("0")
            div_accrued_net = Decimal("0")
            div_settled_net = Decimal("0")

            # 1. Symbol changes and splits first
            for act in day_actions:
                if act.action_type == ActionType.SYMBOL_CHANGE:
                    if act.new_symbol:
                        active_symbol = act.new_symbol
                    applied_today.append(f"{act.action_id}@v{act.version}")
                elif act.action_type in {ActionType.SPLIT, ActionType.REVERSE_SPLIT}:
                    ratio = act.split_ratio
                    assert ratio is not None
                    qty = qty * ratio
                    cost_per_share = cost_per_share / ratio
                    applied_today.append(f"{act.action_id}@v{act.version}")

            # 2. Dividends accruing on ex_date
            for act in day_actions:
                if act.action_type in {ActionType.CASH_DIVIDEND, ActionType.SPECIAL_DIVIDEND}:
                    assert act.cash_amount is not None and act.currency is not None
                    fx_date = (
                        act.ex_date
                        if config.fx_conversion_date_policy == "ex_date"
                        else (act.pay_date or act.ex_date)
                    )
                    fx = resolve_fx_rate(
                        act.currency,
                        initial_position.currency,
                        fx_date,
                        fx_rates,
                        as_of_time=as_of_time,
                    )
                    basis = act.dividend_basis or config.same_day_dividend_basis
                    entitled_shares = pre_split_qty if basis == "pre_split" else qty
                    gross = entitled_shares * act.cash_amount * fx
                    tax = gross * act.tax_withholding_rate
                    net = gross - tax

                    div_accrued_gross += gross
                    div_tax_today += tax
                    div_accrued_net += net
                    cum_gross_div += gross
                    cum_net_div += net
                    pending_receivable += net
                    pay_d = act.pay_date or act.ex_date
                    settlements_by_pay_date[pay_d].append(net)
                    applied_today.append(f"{act.action_id}@v{act.version}")

            # 3. Settle any receivables whose pay_date <= current trade_date
            due_dates = sorted(pd for pd in settlements_by_pay_date.keys() if pd <= d)
            for pd in due_dates:
                for amount in settlements_by_pay_date.pop(pd):
                    pending_receivable -= amount
                    settled_cash += amount
                    div_settled_net += amount

            q_qty = qty.quantize(config.quantity_quantize)
            q_cost = cost_per_share.quantize(config.price_quantize)
            q_total_cost = (qty * cost_per_share).quantize(config.cash_quantize)
            q_mv = (qty * bar.close).quantize(config.cash_quantize)
            q_acc_gross = div_accrued_gross.quantize(config.cash_quantize)
            q_tax = div_tax_today.quantize(config.cash_quantize)
            q_acc_net = div_accrued_net.quantize(config.cash_quantize)
            q_set_net = div_settled_net.quantize(config.cash_quantize)
            q_pend = pending_receivable.quantize(config.cash_quantize)
            q_cash = settled_cash.quantize(config.cash_quantize)
            q_cum_gross = cum_gross_div.quantize(config.cash_quantize)
            q_cum_net = cum_net_div.quantize(config.cash_quantize)
            q_total_wealth = (qty * bar.close + settled_cash + pending_receivable).quantize(
                config.cash_quantize
            )

            ledger.append(
                PositionLedgerEntry(
                    account_id=initial_position.account_id,
                    security_id=initial_position.security_id,
                    active_symbol=active_symbol,
                    trade_date=d,
                    currency=initial_position.currency,
                    quantity=q_qty,
                    cost_basis_per_share=q_cost,
                    total_cost_basis=q_total_cost,
                    raw_close=bar.close.quantize(config.price_quantize),
                    market_value=q_mv,
                    dividend_accrued_today_gross=q_acc_gross,
                    dividend_tax_today=q_tax,
                    dividend_accrued_today_net=q_acc_net,
                    dividend_settled_today_net=q_set_net,
                    pending_receivable_cash=q_pend,
                    settled_cash_balance=q_cash,
                    cumulative_gross_dividends=q_cum_gross,
                    cumulative_net_dividends=q_cum_net,
                    total_equity_plus_net_cash=q_total_wealth,
                    applied_action_ids=tuple(applied_today),
                )
            )

    return tuple(ledger)
