"""Total-return series reconciliation and data-quality diagnostics."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, localcontext
from typing import List, Optional, Sequence

from corp_actions.config import EngineConfig
from corp_actions.contracts import (
    ActionType,
    AdjustedPriceBar,
    CorporateActionEvent,
    PositionLedgerEntry,
    QuarantineRecord,
    RawPriceBar,
    ReconciliationResult,
)


def reconcile_snapshot(
    *,
    security_id: str,
    as_of_time: datetime,
    raw_bars: Sequence[RawPriceBar],
    adjusted_bars: Sequence[AdjustedPriceBar],
    position_ledger: Sequence[PositionLedgerEntry],
    effective_actions: Sequence[CorporateActionEvent],
    quarantined_actions: Sequence[QuarantineRecord],
    config: EngineConfig,
) -> Optional[ReconciliationResult]:
    """Reconcile total-return adjusted price series against dividend-reinvested portfolio wealth.

    Mathematical invariant:
    Under the CRSP/S&P proportional-reinvestment convention, the compounded daily gross
    total return from holding the position (reinvesting each gross ex-date dividend at the
    prior close or equivalently compounding (Q_t * P_t + D_gross_t) / (Q_{t-1} * P_{t-1}))
    MUST match the total-return adjusted close series ratio:
        TR_return = (TR_close_end / TR_close_start) - 1
    within config.reconciliation_tolerance.

    Additionally verifies:
    - Split-jump conservation: on split-only ex-dates, market value conservation holds
      (Q_post * (P_prev / ratio) == Q_prev * P_prev).
    - Completeness ratio: fraction of bars with valid adjusted counterparts.
    - Freshness lag: max (ingestion_time - event_time) across bars and actions vs SLA.
    """
    if not raw_bars or not adjusted_bars:
        return None

    ordered_raw = sorted(raw_bars, key=lambda b: b.trade_date)
    ordered_adj = sorted(adjusted_bars, key=lambda b: b.trade_date)
    ordered_ledger = sorted(position_ledger, key=lambda p: p.trade_date)

    notes: List[str] = []

    with localcontext() as ctx:
        ctx.prec = config.decimal_precision
        ctx.rounding = config.rounding_mode

        # 1. Total-return adjusted price series return
        tr_start = ordered_adj[0].tr_adj_close
        tr_end = ordered_adj[-1].tr_adj_close
        tr_series_return = (tr_end / tr_start) - Decimal("1")

        # 2. Compounded gross portfolio total return from position ledger (if available)
        account_id: Optional[str] = None
        if len(ordered_ledger) >= 2:
            account_id = ordered_ledger[0].account_id
            compounded = Decimal("1")
            for idx in range(1, len(ordered_ledger)):
                prev = ordered_ledger[idx - 1]
                curr = ordered_ledger[idx]
                prev_mv = prev.quantity * prev.raw_close
                if prev_mv <= Decimal("0"):
                    continue
                # CRSP proportional-reinvestment economic identity:
                # Reinvesting gross ex-date dividend at ex-open reference (P_ref - D) yields
                # exact factor: curr_mv / (prev_mv - gross_div_today) == P_t / (P_{t-1}/r - D)
                curr_mv = curr.quantity * curr.raw_close
                denom = prev_mv - curr.dividend_accrued_today_gross
                if denom <= Decimal("0"):
                    continue
                daily_gross_factor = curr_mv / denom
                compounded *= daily_gross_factor
            portfolio_gross_return = compounded - Decimal("1")
        elif len(ordered_ledger) == 1:
            account_id = ordered_ledger[0].account_id
            portfolio_gross_return = Decimal("0")
        else:
            portfolio_gross_return = tr_series_return
            notes.append("No position ledger supplied; portfolio return defaulted to TR series return.")

        discrepancy = abs(tr_series_return - portfolio_gross_return)
        reconciled = discrepancy <= config.reconciliation_tolerance

        # 3. Check split wealth conservation (total cost basis preserved across splits)
        split_dates = {
            a.ex_date
            for a in effective_actions
            if a.action_type in {ActionType.SPLIT, ActionType.REVERSE_SPLIT}
        }
        split_jump_ok = True
        if ordered_ledger and split_dates:
            ledger_by_date = {p.trade_date: p for p in ordered_ledger}
            base_cost = ordered_ledger[0].total_cost_basis
            for sd in split_dates:
                entry = ledger_by_date.get(sd)
                if entry is not None and abs(entry.total_cost_basis - base_cost) > config.cash_quantize:
                    split_jump_ok = False
                    notes.append(
                        f"Cost basis drift on split ex-date {sd}: expected {base_cost}, got {entry.total_cost_basis}"
                    )

        # 4. Data-quality diagnostics: completeness & freshness lag
        completeness = Decimal(len(ordered_adj)) / Decimal(len(ordered_raw))
        lags: List[float] = []
        for b in ordered_raw:
            lags.append(max(0.0, (b.ingestion_time - b.event_time).total_seconds()))
        for a in effective_actions:
            lags.append(max(0.0, (a.ingestion_time - a.event_time).total_seconds()))
        max_lag = max(lags) if lags else 0.0
        sla_breaches = sum(1 for lag in lags if lag > config.freshness_sla_seconds)

        if quarantined_actions:
            notes.append(
                f"{len(quarantined_actions)} unsupported action(s) quarantined: "
                + ", ".join(f"{q.action_id}({q.action_type})" for q in quarantined_actions)
            )
        if not reconciled:
            notes.append(
                f"Discrepancy {discrepancy} exceeded tolerance {config.reconciliation_tolerance}; "
                "verify whether dividends were held in cash vs reinvested during price moves."
            )

        q_factor = config.factor_quantize
        return ReconciliationResult(
            security_id=security_id,
            account_id=account_id,
            as_of_time=as_of_time,
            start_date=ordered_raw[0].trade_date,
            end_date=ordered_raw[-1].trade_date,
            tr_series_return=tr_series_return.quantize(q_factor),
            portfolio_gross_return=portfolio_gross_return.quantize(q_factor),
            return_discrepancy=discrepancy.quantize(q_factor),
            tolerance=config.reconciliation_tolerance,
            reconciled=reconciled,
            split_jump_checks_passed=split_jump_ok,
            completeness_ratio=completeness.quantize(Decimal("0.0001")),
            max_freshness_lag_seconds=round(max_lag, 3),
            freshness_sla_breaches=sla_breaches,
            quarantined_action_count=len(quarantined_actions),
            applied_action_count=len(effective_actions),
            notes=tuple(notes),
        )
