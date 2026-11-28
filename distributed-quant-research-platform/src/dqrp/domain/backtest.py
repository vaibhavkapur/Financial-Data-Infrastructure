"""Pure deterministic quantitative backtesting engine (isolated from I/O).

Design invariants:
- Pure function over MarketBar sequences, strategy name, params, and integer seed.
- Point-in-time safe: filters bars by availability_time <= evaluation_as_of when provided,
  sorts deterministically by (event_time, symbol), and never peeks at future bars.
- Signal at bar t (computed from closes up to and including bar t) determines position held
  over the return from close[t] to close[t+1] (close-to-close execution with explicit
  transaction costs in basis points).
- Stochastic uncertainty quantification (optional bootstrap Sharpe CI) uses an isolated
  random.Random(seed) instance so results are 100% reproducible across processes and runs.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import random
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from dqrp.contracts import MarketBar, TrialMetrics, parse_utc_timestamp
from dqrp.errors import ContractValidationError, UnsupportedOperationError

TRADING_DAYS_PER_YEAR = 252


@dataclass(frozen=True)
class BacktestOutcome:
    """Complete pure output of a single strategy evaluation."""

    metrics: TrialMetrics
    equity_curve: Tuple[Tuple[str, float], ...]
    bar_returns: Tuple[float, ...]


def _validate_positive_int_param(params: Mapping[str, Any], key: str, default: Optional[int] = None) -> int:
    raw = params.get(key, default)
    if raw is None or isinstance(raw, bool) or not isinstance(raw, int) or raw < 1:
        raise ContractValidationError(
            f"Strategy parameter '{key}' must be an integer >= 1, got {raw!r}",
            code="INVALID_STRATEGY_PARAM",
            details={"param": key, "value": raw},
        )
    return int(raw)


def _validate_nonneg_float_param(params: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    raw = params.get(key, default)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ContractValidationError(
            f"Strategy parameter '{key}' must be numeric, got {raw!r}",
            code="INVALID_STRATEGY_PARAM",
            details={"param": key, "value": raw},
        )
    val = float(raw)
    if math.isnan(val) or math.isinf(val) or val < 0.0:
        raise ContractValidationError(
            f"Strategy parameter '{key}' must be finite and >= 0, got {raw!r}",
            code="INVALID_STRATEGY_PARAM",
            details={"param": key, "value": raw},
        )
    return val


def _filter_and_sort_bars(
    bars: Sequence[MarketBar],
    *,
    symbol: Optional[str] = None,
    evaluation_as_of: Optional[str] = None,
) -> List[MarketBar]:
    as_of_dt = parse_utc_timestamp(evaluation_as_of, field_name="evaluation_as_of") if evaluation_as_of else None
    selected: List[Tuple[Any, MarketBar]] = []
    for bar in bars:
        if symbol is not None and bar.symbol != symbol:
            continue
        if as_of_dt is not None:
            av_dt = parse_utc_timestamp(bar.availability_time, field_name="availability_time")
            if av_dt > as_of_dt:
                continue
        ev_dt = parse_utc_timestamp(bar.event_time, field_name="event_time")
        selected.append((ev_dt, bar))
    selected.sort(key=lambda pair: (pair[0], pair[1].symbol))
    return [pair[1] for pair in selected]


def _compute_sma_crossover_targets(closes: Sequence[float], params: Mapping[str, Any]) -> List[float]:
    fast = _validate_positive_int_param(params, "fast_window")
    slow = _validate_positive_int_param(params, "slow_window")
    if fast >= slow:
        raise ContractValidationError(
            f"sma_crossover requires fast_window ({fast}) < slow_window ({slow})",
            code="INVALID_STRATEGY_PARAM",
            details={"fast_window": fast, "slow_window": slow},
        )
    allow_short = bool(params.get("allow_short", False))
    targets: List[float] = []
    for i in range(len(closes)):
        if i + 1 < slow:
            targets.append(0.0)
            continue
        fast_sma = sum(closes[i - fast + 1 : i + 1]) / float(fast)
        slow_sma = sum(closes[i - slow + 1 : i + 1]) / float(slow)
        if fast_sma > slow_sma:
            targets.append(1.0)
        elif fast_sma < slow_sma and allow_short:
            targets.append(-1.0)
        else:
            targets.append(0.0)
    return targets


def _compute_momentum_zscore_targets(closes: Sequence[float], params: Mapping[str, Any]) -> List[float]:
    lookback = _validate_positive_int_param(params, "lookback_window")
    if lookback < 2:
        raise ContractValidationError(
            "momentum_zscore requires lookback_window >= 2",
            code="INVALID_STRATEGY_PARAM",
        )
    entry_z = _validate_nonneg_float_param(params, "entry_z", default=1.0)
    exit_z = _validate_nonneg_float_param(params, "exit_z", default=0.25)
    allow_short = bool(params.get("allow_short", True))

    bar_rets = [0.0]
    for i in range(1, len(closes)):
        bar_rets.append((closes[i] / closes[i - 1]) - 1.0)

    targets: List[float] = []
    pos = 0.0
    for i in range(len(closes)):
        if i < lookback:
            targets.append(0.0)
            continue
        window = bar_rets[i - lookback + 1 : i + 1]
        mean = sum(window) / float(lookback)
        var = sum((r - mean) ** 2 for r in window) / float(lookback)
        std = math.sqrt(var)
        if std <= 1e-12:
            z = 0.0
        else:
            cum_ret = (closes[i] / closes[i - lookback]) - 1.0
            z = cum_ret / (std * math.sqrt(float(lookback)))
        if z >= entry_z:
            pos = 1.0
        elif allow_short and z <= -entry_z:
            pos = -1.0
        elif abs(z) <= exit_z:
            pos = 0.0
        targets.append(pos)
    return targets


def _compute_mean_reversion_targets(closes: Sequence[float], params: Mapping[str, Any]) -> List[float]:
    lookback = _validate_positive_int_param(params, "lookback_window")
    if lookback < 2:
        raise ContractValidationError(
            "mean_reversion requires lookback_window >= 2",
            code="INVALID_STRATEGY_PARAM",
        )
    band_std = _validate_nonneg_float_param(params, "band_std", default=1.5)
    allow_short = bool(params.get("allow_short", False))

    targets: List[float] = []
    pos = 0.0
    for i in range(len(closes)):
        if i + 1 < lookback:
            targets.append(0.0)
            continue
        window = closes[i - lookback + 1 : i + 1]
        mean = sum(window) / float(lookback)
        var = sum((c - mean) ** 2 for c in window) / float(lookback)
        std = math.sqrt(var)
        if std <= 1e-12:
            targets.append(pos)
            continue
        z = (closes[i] - mean) / std
        if z <= -band_std:
            pos = 1.0
        elif allow_short and z >= band_std:
            pos = -1.0
        elif abs(z) <= 0.25:
            pos = 0.0
        targets.append(pos)
    return targets


def _bootstrap_sharpe_ci(
    returns: Sequence[float],
    *,
    seed: int,
    draws: int,
) -> Tuple[Optional[float], Optional[float]]:
    """Seeded non-parametric bootstrap 5th/95th percentile annualized Sharpe."""
    if draws <= 0 or len(returns) < 2:
        return None, None
    rng = random.Random(int(seed))
    n = len(returns)
    sharpes: List[float] = []
    sqrt_ann = math.sqrt(float(TRADING_DAYS_PER_YEAR))
    for _ in range(draws):
        sample_sum = 0.0
        sample_sq = 0.0
        for _ in range(n):
            r = returns[rng.randrange(n)]
            sample_sum += r
            sample_sq += r * r
        mean = sample_sum / n
        var = max(0.0, (sample_sq / n) - (mean * mean))
        std = math.sqrt(var)
        sharpes.append(0.0 if std <= 1e-12 else (mean / std) * sqrt_ann)
    sharpes.sort()
    idx_05 = max(0, min(draws - 1, int(0.05 * (draws - 1))))
    idx_95 = max(0, min(draws - 1, int(0.95 * (draws - 1))))
    return round(sharpes[idx_05], 8), round(sharpes[idx_95], 8)


def run_strategy_backtest(
    bars: Sequence[MarketBar],
    *,
    strategy_name: str,
    params: Mapping[str, Any],
    seed: int,
    evaluation_as_of: Optional[str] = None,
) -> BacktestOutcome:
    """Execute a pure, deterministic backtest over canonical MarketBars.

    Parameters
    ----------
    bars:
        Sequence of validated MarketBar records.
    strategy_name:
        One of 'sma_crossover', 'momentum_zscore', 'mean_reversion'.
    params:
        Strategy hyperparameters plus optional 'symbol', 'initial_capital',
        'fee_bps', 'slippage_bps', and 'bootstrap_draws'.
    seed:
        Deterministic trial seed used for optional bootstrap uncertainty draws.
    evaluation_as_of:
        Optional UTC ISO-8601 point-in-time cutoff on availability_time.
    """
    symbol = params.get("symbol")
    filtered = _filter_and_sort_bars(
        bars,
        symbol=str(symbol) if symbol is not None else None,
        evaluation_as_of=evaluation_as_of,
    )
    if len(filtered) < 2:
        raise ContractValidationError(
            f"At least 2 bars are required for backtest; got {len(filtered)}",
            code="INSUFFICIENT_BARS",
            details={"bars_found": len(filtered), "symbol": symbol, "evaluation_as_of": evaluation_as_of},
        )

    # Ensure single-symbol series when no symbol filter was specified and multiple exist
    symbols_present = sorted({b.symbol for b in filtered})
    if len(symbols_present) > 1:
        # Aggregate per-symbol equally weighted or require explicit symbol;
        # deterministic default: pick lexicographically first if symbol omitted only when single expected,
        # otherwise fail explicitly to prevent undocumented assumptions.
        raise ContractValidationError(
            f"Multi-symbol dataset ({symbols_present}) requires explicit 'symbol' parameter in trial params",
            code="AMBIGUOUS_SYMBOL",
            details={"symbols": symbols_present},
        )

    initial_capital = _validate_nonneg_float_param(params, "initial_capital", default=100000.0)
    if initial_capital <= 0.0:
        raise ContractValidationError("initial_capital must be > 0", code="INVALID_STRATEGY_PARAM")
    fee_bps = _validate_nonneg_float_param(params, "fee_bps", default=0.0)
    slippage_bps = _validate_nonneg_float_param(params, "slippage_bps", default=0.0)
    bootstrap_draws = int(params.get("bootstrap_draws", 0))
    if isinstance(params.get("bootstrap_draws", 0), bool) or bootstrap_draws < 0:
        raise ContractValidationError("bootstrap_draws must be an integer >= 0", code="INVALID_STRATEGY_PARAM")

    closes = [b.close for b in filtered]
    if strategy_name == "sma_crossover":
        targets = _compute_sma_crossover_targets(closes, params)
    elif strategy_name == "momentum_zscore":
        targets = _compute_momentum_zscore_targets(closes, params)
    elif strategy_name == "mean_reversion":
        targets = _compute_mean_reversion_targets(closes, params)
    else:
        raise UnsupportedOperationError(
            f"Unsupported strategy {strategy_name!r}",
            code="UNSUPPORTED_STRATEGY",
            details={"strategy_name": strategy_name},
        )

    cost_rate = (fee_bps + slippage_bps) / 10000.0
    equity = initial_capital
    peak_equity = equity
    max_drawdown = 0.0
    prev_pos = 0.0
    trade_count = 0
    turnover = 0.0
    winning_periods = 0
    active_periods = 0

    equity_curve: List[Tuple[str, float]] = [(filtered[0].event_time, round(equity, 8))]
    strat_returns: List[float] = []

    for i in range(len(filtered) - 1):
        target_pos = targets[i]
        delta_pos = abs(target_pos - prev_pos)
        if delta_pos > 1e-12:
            trade_count += 1
            turnover += delta_pos
        asset_ret = (closes[i + 1] / closes[i]) - 1.0
        period_ret = (target_pos * asset_ret) - (delta_pos * cost_rate)
        strat_returns.append(period_ret)

        if abs(target_pos) > 1e-12 or delta_pos > 1e-12:
            active_periods += 1
            if period_ret > 0.0:
                winning_periods += 1

        equity = equity * (1.0 + period_ret)
        if equity > peak_equity:
            peak_equity = equity
        dd = 0.0 if peak_equity <= 0.0 else (peak_equity - equity) / peak_equity
        if dd > max_drawdown:
            max_drawdown = dd
        equity_curve.append((filtered[i + 1].event_time, round(equity, 8)))
        prev_pos = target_pos

    n_periods = len(strat_returns)
    total_return = (equity / initial_capital) - 1.0
    if equity > 0.0 and n_periods > 0:
        annualized_return = (equity / initial_capital) ** (TRADING_DAYS_PER_YEAR / float(n_periods)) - 1.0
    else:
        annualized_return = -1.0

    mean_ret = sum(strat_returns) / float(n_periods)
    variance = sum((r - mean_ret) ** 2 for r in strat_returns) / float(n_periods)
    std_ret = math.sqrt(variance)
    ann_vol = std_ret * math.sqrt(float(TRADING_DAYS_PER_YEAR))
    sharpe = 0.0 if std_ret <= 1e-12 else (mean_ret / std_ret) * math.sqrt(float(TRADING_DAYS_PER_YEAR))
    win_rate = 0.0 if active_periods == 0 else winning_periods / float(active_periods)

    mc_p05, mc_p95 = _bootstrap_sharpe_ci(strat_returns, seed=seed, draws=bootstrap_draws)

    metrics = TrialMetrics(
        total_return=round(total_return, 10),
        annualized_return=round(annualized_return, 10),
        annualized_volatility=round(ann_vol, 10),
        sharpe_ratio=round(sharpe, 10),
        max_drawdown=round(max_drawdown, 10),
        win_rate=round(win_rate, 10),
        trade_count=trade_count,
        final_equity=round(equity, 8),
        turnover=round(turnover, 10),
        bars_evaluated=len(filtered),
        mc_sharpe_p05=mc_p05,
        mc_sharpe_p95=mc_p95,
    )
    return BacktestOutcome(
        metrics=metrics,
        equity_curve=tuple(equity_curve),
        bar_returns=tuple(round(r, 10) for r in strat_returns),
    )
