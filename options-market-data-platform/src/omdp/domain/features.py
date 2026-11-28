"""Pure domain feature derivation engine with full input-version provenance.

Computes timestamped option chain features:
- Mid price & bid-ask spread
- Time-to-expiry (ACT/365F) from calendar or default expiry UTC close
- Linearly interpolated risk-free rate and dividend yield from versioned rate curves
- Forward price, spot moneyness (S/K), and log-forward moneyness ln(K/F)
- Implied volatility, delta, and vega via Black-Scholes-Merton inversion
- Full FeatureProvenance linking every derived feature to input versions & SHA-256 hashes
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from omdp.config import PlatformConfig
from omdp.domain.contracts import (
    ChainSnapshot,
    ContractMetadata,
    DataQualityFlag,
    DerivedOptionFeature,
    ExpiryCalendarEntry,
    FeatureProvenance,
    IVStatus,
    RateCurvePoint,
    UnderlyingQuote,
    ValidatedOptionQuote,
    ValidationStatus,
)
from omdp.domain.pricing import solve_implied_volatility
from omdp.domain.validation import resolve_expiry_timestamp

_SECONDS_PER_YEAR_365 = 365.0 * 86400.0


def select_latest_quote_per_option(
    quotes: Sequence[ValidatedOptionQuote],
) -> Tuple[ValidatedOptionQuote, ...]:
    """Within a snapshot, if multiple quote_versions exist for an option_id, keep the highest version."""
    latest: Dict[str, ValidatedOptionQuote] = {}
    for vq in quotes:
        oid = vq.quote.option_id
        prev = latest.get(oid)
        if prev is None or vq.quote.quote_version > prev.quote.quote_version:
            latest[oid] = vq
    return tuple(latest[oid] for oid in sorted(latest.keys()))


def select_underlying_quote(
    underlying_quotes: Sequence[UnderlyingQuote],
    snapshot_time: datetime,
    config: PlatformConfig,
) -> Tuple[Optional[UnderlyingQuote], Tuple[DataQualityFlag, ...], Tuple[str, ...]]:
    """Select the latest non-future underlying quote as of snapshot_time and check skew/crossed state."""
    eligible = [
        uq
        for uq in underlying_quotes
        if (uq.event_time - snapshot_time).total_seconds() <= config.validation.max_future_clock_skew_seconds
    ]
    if not eligible:
        return (
            None,
            (DataQualityFlag.MISSING_UNDERLYING,),
            ("No underlying quote available at or before snapshot_time",),
        )

    # Sort deterministically by (event_time, quote_version, source_sequence)
    chosen = sorted(
        eligible,
        key=lambda u: (u.event_time, u.quote_version, u.source_sequence),
    )[-1]

    flags: List[DataQualityFlag] = []
    notes: List[str] = []

    if not chosen.is_valid_price:
        flags.append(DataQualityFlag.CROSSED_UNDERLYING)
        notes.append(f"Underlying quote invalid/crossed: bid={chosen.bid}, ask={chosen.ask}")

    skew = abs((snapshot_time - chosen.event_time).total_seconds())
    if skew > config.validation.max_underlying_skew_seconds:
        flags.append(DataQualityFlag.STALE_UNDERLYING)
        notes.append(
            f"Underlying quote skew {skew:.2f}s exceeds max_underlying_skew_seconds="
            f"{config.validation.max_underlying_skew_seconds}s"
        )

    return chosen, tuple(flags), tuple(notes)


def interpolate_rate_curve(
    rate_points: Sequence[RateCurvePoint],
    time_to_expiry_years: float,
) -> Optional[Tuple[float, float, str, int, Tuple[str, ...]]]:
    """Linearly interpolate (risk_free_rate, dividend_yield) by tenor_days with flat clamping at ends."""
    if not rate_points:
        return None
    ordered = sorted(rate_points, key=lambda p: p.tenor_days)
    target_days = time_to_expiry_years * 365.0
    curve_id = ordered[0].curve_id
    curve_version = ordered[0].curve_version

    if target_days <= ordered[0].tenor_days:
        p = ordered[0]
        return p.risk_free_rate, p.dividend_yield, curve_id, curve_version, (p.record_hash,)
    if target_days >= ordered[-1].tenor_days:
        p = ordered[-1]
        return p.risk_free_rate, p.dividend_yield, curve_id, curve_version, (p.record_hash,)

    for left, right in zip(ordered, ordered[1:]):
        if left.tenor_days <= target_days <= right.tenor_days:
            if right.tenor_days == left.tenor_days:
                return left.risk_free_rate, left.dividend_yield, curve_id, curve_version, (left.record_hash,)
            weight = (target_days - left.tenor_days) / float(right.tenor_days - left.tenor_days)
            r = left.risk_free_rate + weight * (right.risk_free_rate - left.risk_free_rate)
            q = left.dividend_yield + weight * (right.dividend_yield - left.dividend_yield)
            return r, q, curve_id, curve_version, (left.record_hash, right.record_hash)

    return None


def derive_chain_features(
    snapshot: ChainSnapshot,
    contracts_by_id: Mapping[str, ContractMetadata],
    underlying_quotes: Sequence[UnderlyingQuote],
    rate_points: Sequence[RateCurvePoint],
    calendar_by_date: Mapping[str, ExpiryCalendarEntry],
    config: PlatformConfig,
) -> Tuple[DerivedOptionFeature, ...]:
    """Derive timestamped option features for every contract in a ChainSnapshot with full provenance."""
    latest_quotes = select_latest_quote_per_option(snapshot.quotes)
    underlying, u_flags, u_notes = select_underlying_quote(
        underlying_quotes=underlying_quotes,
        snapshot_time=snapshot.snapshot_time,
        config=config,
    )

    derived: List[DerivedOptionFeature] = []
    for vq in latest_quotes:
        q = vq.quote
        contract = contracts_by_id.get(q.option_id)
        combined_flags: List[DataQualityFlag] = list(vq.flags)
        combined_notes: List[str] = list(vq.reasons)

        for uf in u_flags:
            if uf not in combined_flags:
                combined_flags.append(uf)
        combined_notes.extend(u_notes)

        mid_price: Optional[float] = None
        spread: Optional[float] = None
        if DataQualityFlag.INVALID_PRICE not in vq.flags and DataQualityFlag.CROSSED_QUOTE not in vq.flags:
            mid_price = float(q.mid)
            spread = float(q.spread)

        expiry_ts: Optional[datetime] = None
        calendar_entry: Optional[ExpiryCalendarEntry] = None
        tte: Optional[float] = None
        if contract is not None:
            expiry_ts, calendar_entry = resolve_expiry_timestamp(contract, calendar_by_date, config)
            raw_tte = (expiry_ts - snapshot.snapshot_time).total_seconds() / _SECONDS_PER_YEAR_365
            tte = max(0.0, raw_tte)

        underlying_mid: Optional[float] = (
            float(underlying.mid) if (underlying is not None and underlying.is_valid_price) else None
        )

        # Interpolate rate curve when T > 0
        rate_info = None
        r_val: Optional[float] = None
        q_val: Optional[float] = None
        if tte is not None and tte > 0.0:
            rate_info = interpolate_rate_curve(rate_points, tte)
            if rate_info is None:
                if DataQualityFlag.MISSING_RATE_CURVE not in combined_flags:
                    combined_flags.append(DataQualityFlag.MISSING_RATE_CURVE)
                combined_notes.append("No rate curve points available for interpolation")
            else:
                r_val, q_val = rate_info[0], rate_info[1]

        forward_price: Optional[float] = None
        spot_moneyness: Optional[float] = None
        log_fwd_moneyness: Optional[float] = None
        if contract is not None and underlying_mid is not None:
            k_float = float(contract.strike)
            spot_moneyness = underlying_mid / k_float
            if tte is not None and tte > 0.0 and r_val is not None and q_val is not None:
                forward_price = underlying_mid * math.exp((r_val - q_val) * tte)
                log_fwd_moneyness = math.log(k_float / forward_price)

        # Determine IV eligibility & run solver
        implied_vol: Optional[float] = None
        delta: Optional[float] = None
        vega: Optional[float] = None
        solver_name: Optional[str] = None
        solver_iterations: Optional[int] = None
        price_residual: Optional[float] = None

        if DataQualityFlag.EXPIRED_OPTION in vq.flags or (tte is not None and tte <= 0.0):
            iv_status = IVStatus.EXCLUDED_EXPIRED
        elif DataQualityFlag.UNSUPPORTED_EXERCISE_STYLE in vq.flags:
            iv_status = IVStatus.EXCLUDED_UNSUPPORTED_STYLE
        elif DataQualityFlag.STALE_QUOTE in vq.flags and config.validation.exclude_stale_quotes_from_iv:
            iv_status = IVStatus.EXCLUDED_STALE_QUOTE
        elif vq.status == ValidationStatus.REJECTED or contract is None or mid_price is None or mid_price <= 0.0:
            iv_status = IVStatus.EXCLUDED_INVALID_QUOTE
        elif (
            underlying is None
            or not underlying.is_valid_price
            or DataQualityFlag.STALE_UNDERLYING in u_flags
            or underlying_mid is None
        ):
            iv_status = IVStatus.EXCLUDED_MISSING_UNDERLYING
        elif rate_info is None or r_val is None or q_val is None or tte is None:
            iv_status = IVStatus.EXCLUDED_MISSING_RATE
        else:
            solve_res = solve_implied_volatility(
                market_price=mid_price,
                spot=underlying_mid,
                strike=float(contract.strike),
                time_to_expiry=tte,
                risk_free_rate=r_val,
                dividend_yield=q_val,
                option_right=contract.option_right,
                min_vol=config.pricing.iv_min_vol,
                max_vol=config.pricing.iv_max_vol,
                price_tolerance=config.pricing.iv_price_tolerance,
                vol_tolerance=config.pricing.iv_vol_tolerance,
                max_iterations=config.pricing.iv_max_iterations,
            )
            solver_name = solve_res.solver_name
            solver_iterations = solve_res.iterations
            price_residual = solve_res.price_residual
            if solve_res.converged and solve_res.implied_vol is not None:
                implied_vol = solve_res.implied_vol
                delta = solve_res.delta
                vega = solve_res.vega
                iv_status = IVStatus.COMPUTED
            elif solve_res.arbitrage_violation:
                iv_status = IVStatus.ARBITRAGE_BOUND_VIOLATION
                combined_notes.append(solve_res.message)
            else:
                iv_status = IVStatus.SOLVER_NON_CONVERGENT
                combined_notes.append(solve_res.message)

        provenance = FeatureProvenance(
            snapshot_id=snapshot.snapshot_id,
            snapshot_hash=snapshot.snapshot_hash,
            option_id=q.option_id,
            contract_version=contract.contract_version if contract else None,
            contract_hash=contract.record_hash if contract else None,
            option_quote_version=q.quote_version,
            option_quote_hash=q.record_hash,
            underlying_symbol=snapshot.underlying_symbol,
            underlying_quote_version=underlying.quote_version if underlying else None,
            underlying_quote_hash=underlying.record_hash if underlying else None,
            rate_curve_id=rate_info[2] if rate_info else None,
            rate_curve_version=rate_info[3] if rate_info else None,
            rate_curve_hashes=rate_info[4] if rate_info else (),
            calendar_id=calendar_entry.calendar_id if calendar_entry else None,
            calendar_version=calendar_entry.calendar_version if calendar_entry else None,
            calendar_hash=calendar_entry.record_hash if calendar_entry else None,
            config_version=config.config_version,
            config_hash=config.config_hash(),
            schema_version=snapshot.schema_version,
            solver_name=solver_name,
            solver_iterations=solver_iterations,
            price_residual=price_residual,
        )

        derived.append(
            DerivedOptionFeature(
                snapshot_id=snapshot.snapshot_id,
                option_id=q.option_id,
                underlying_symbol=snapshot.underlying_symbol,
                valuation_time=snapshot.snapshot_time,
                mid_price=mid_price,
                spread=spread,
                time_to_expiry_years=tte,
                underlying_mid=underlying_mid,
                risk_free_rate=r_val,
                dividend_yield=q_val,
                forward_price=forward_price,
                spot_moneyness=spot_moneyness,
                log_forward_moneyness=log_fwd_moneyness,
                implied_vol=implied_vol,
                delta=delta,
                vega=vega,
                iv_status=iv_status,
                quality_flags=tuple(combined_flags),
                notes=tuple(combined_notes),
                provenance=provenance,
            )
        )

    return tuple(derived)
