"""Pure domain algorithms for corporate-action adjustments, positions, series, and reconciliation."""

from corp_actions.domain.factors import (
    ACTION_PRECEDENCE,
    ResolvedActions,
    compute_factor_schedule,
    resolve_fx_rate,
    resolve_pit_actions,
)
from corp_actions.domain.positions import apply_actions_to_position
from corp_actions.domain.reconcile import reconcile_snapshot
from corp_actions.domain.series import build_adjusted_series

__all__ = [
    "ACTION_PRECEDENCE",
    "ResolvedActions",
    "apply_actions_to_position",
    "build_adjusted_series",
    "compute_factor_schedule",
    "reconcile_snapshot",
    "resolve_fx_rate",
    "resolve_pit_actions",
]
