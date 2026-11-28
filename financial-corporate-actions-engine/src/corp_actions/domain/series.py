"""Build clearly separated raw, split-adjusted, and total-return adjusted price series."""

from __future__ import annotations

from decimal import localcontext
from typing import Dict, List, Sequence, Tuple

from corp_actions.config import EngineConfig
from corp_actions.contracts import AdjustedPriceBar, AdjustmentFactorPoint, RawPriceBar
from corp_actions.errors import MissingMarketDataError


def build_adjusted_series(
    raw_bars: Sequence[RawPriceBar],
    factor_schedule: Sequence[AdjustmentFactorPoint],
    config: EngineConfig,
) -> Tuple[AdjustedPriceBar, ...]:
    """Produce immutable AdjustedPriceBars preserving raw alongside adjusted columns."""
    if not raw_bars:
        return ()

    factors_by_date: Dict = {fp.trade_date: fp for fp in factor_schedule}
    ordered_bars = sorted(raw_bars, key=lambda b: b.trade_date)
    adjusted: List[AdjustedPriceBar] = []

    with localcontext() as ctx:
        ctx.prec = config.decimal_precision
        ctx.rounding = config.rounding_mode

        for bar in ordered_bars:
            fp = factors_by_date.get(bar.trade_date)
            if fp is None:
                raise MissingMarketDataError(
                    f"Missing adjustment factor point for {bar.security_id} on {bar.trade_date}"
                )
            sp = fp.cumulative_split_price_factor
            sv = fp.cumulative_split_volume_factor
            tr = fp.cumulative_total_return_factor

            adjusted.append(
                AdjustedPriceBar(
                    security_id=bar.security_id,
                    raw_symbol=bar.symbol,
                    active_symbol=fp.active_symbol,
                    trade_date=bar.trade_date,
                    currency=bar.currency,
                    raw_open=bar.open.quantize(config.price_quantize),
                    raw_high=bar.high.quantize(config.price_quantize),
                    raw_low=bar.low.quantize(config.price_quantize),
                    raw_close=bar.close.quantize(config.price_quantize),
                    raw_volume=bar.volume.quantize(config.quantity_quantize),
                    split_adj_open=(bar.open * sp).quantize(config.price_quantize),
                    split_adj_high=(bar.high * sp).quantize(config.price_quantize),
                    split_adj_low=(bar.low * sp).quantize(config.price_quantize),
                    split_adj_close=(bar.close * sp).quantize(config.price_quantize),
                    split_adj_volume=(bar.volume * sv).quantize(config.quantity_quantize),
                    tr_adj_open=(bar.open * tr).quantize(config.price_quantize),
                    tr_adj_high=(bar.high * tr).quantize(config.price_quantize),
                    tr_adj_low=(bar.low * tr).quantize(config.price_quantize),
                    tr_adj_close=(bar.close * tr).quantize(config.price_quantize),
                    split_price_factor=sp,
                    split_volume_factor=sv,
                    dividend_factor=fp.cumulative_dividend_factor,
                    total_return_factor=tr,
                    applied_action_ids=fp.applied_action_ids,
                )
            )

    return tuple(adjusted)
