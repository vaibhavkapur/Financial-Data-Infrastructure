"""Optional Polars DataFrame adapter for analytics and quant research pipelines."""

from __future__ import annotations

from typing import Any, Sequence

from corp_actions.contracts import AdjustedPriceBar, PositionLedgerEntry
from corp_actions.errors import DependencyUnavailableError


def polars_available() -> bool:
    try:
        import polars  # type: ignore  # noqa: F401
        return True
    except ImportError:
        return False


def _require_polars() -> Any:
    try:
        import polars as pl  # type: ignore
        return pl
    except ImportError as exc:
        raise DependencyUnavailableError(
            "Polars adapter requires optional dependency 'polars>=1.0.0'. "
            "Install with `pip install -e '.[polars]'`.",
            details={"missing_package": "polars"},
        ) from exc


def adjusted_series_to_polars(bars: Sequence[AdjustedPriceBar]) -> Any:
    """Convert separated raw/split-adjusted/TR-adjusted price bars into a Polars DataFrame.

    Decimal columns are preserved as exact strings alongside optional float64 research views
    or Polars Decimal types to prevent silent precision loss.
    """
    pl = _require_polars()
    rows = [b.to_dict() for b in bars]
    return pl.DataFrame(rows)


def position_ledger_to_polars(entries: Sequence[PositionLedgerEntry]) -> Any:
    """Convert daily position and cash ledger entries into a Polars DataFrame."""
    pl = _require_polars()
    rows = [e.to_dict() for e in entries]
    return pl.DataFrame(rows)
