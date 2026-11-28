"""Optional Polars analytics adapter with deterministic record export fallback."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

from omdp.domain.contracts import DerivedOptionFeature, format_utc_timestamp


class OptionalDependencyError(ImportError):
    """Raised when an optional dependency (such as polars) is requested but not installed."""


def is_polars_available() -> bool:
    try:
        import polars  # noqa: F401
        return True
    except ImportError:
        return False


def features_to_records(features: Sequence[DerivedOptionFeature]) -> List[Dict[str, Any]]:
    """Flatten derived features into columnar-friendly dictionaries preserving input provenance."""
    rows: List[Dict[str, Any]] = []
    for f in features:
        rows.append(
            {
                "feature_id": f.feature_id,
                "snapshot_id": f.snapshot_id,
                "option_id": f.option_id,
                "underlying_symbol": f.underlying_symbol,
                "valuation_time": format_utc_timestamp(f.valuation_time),
                "mid_price": f.mid_price,
                "spread": f.spread,
                "time_to_expiry_years": f.time_to_expiry_years,
                "underlying_mid": f.underlying_mid,
                "risk_free_rate": f.risk_free_rate,
                "dividend_yield": f.dividend_yield,
                "forward_price": f.forward_price,
                "spot_moneyness": f.spot_moneyness,
                "log_forward_moneyness": f.log_forward_moneyness,
                "implied_vol": f.implied_vol,
                "delta": f.delta,
                "vega": f.vega,
                "iv_status": f.iv_status.value,
                "quality_flags": [flag.value for flag in f.quality_flags],
                "option_quote_version": f.provenance.option_quote_version,
                "option_quote_hash": f.provenance.option_quote_hash,
                "underlying_quote_version": f.provenance.underlying_quote_version,
                "contract_version": f.provenance.contract_version,
                "rate_curve_version": f.provenance.rate_curve_version,
                "config_hash": f.provenance.config_hash,
            }
        )
    return rows


def features_to_polars_dataframe(features: Sequence[DerivedOptionFeature]) -> Any:
    """Export DerivedOptionFeature records to a Polars DataFrame if polars is installed."""
    try:
        import polars as pl
    except ImportError as exc:
        raise OptionalDependencyError(
            "Optional dependency 'polars' is not installed. Install with `pip install .[analytics]` "
            "or use `features_to_records()` for zero-dependency dict export."
        ) from exc
    return pl.DataFrame(features_to_records(features))
