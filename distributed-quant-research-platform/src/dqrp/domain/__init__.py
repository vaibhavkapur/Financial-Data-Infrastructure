"""Pure, I/O-free domain algorithms and scheduling state machine."""

from __future__ import annotations

from dqrp.domain.backtest import BacktestOutcome, TRADING_DAYS_PER_YEAR, run_strategy_backtest
from dqrp.domain.scheduler import (
    AggregateResult,
    BudgetCheck,
    ExperimentStateMachine,
    TrialSlot,
)

__all__ = [
    "AggregateResult",
    "BacktestOutcome",
    "BudgetCheck",
    "ExperimentStateMachine",
    "TRADING_DAYS_PER_YEAR",
    "TrialSlot",
    "run_strategy_backtest",
]
