"""Analytical Black-Scholes-Merton European option pricing and implied volatility solver.

Pure mathematical domain module with zero I/O dependencies.
Uses closed-form normal CDF via math.erf and a safeguarded Newton-Raphson +
Brent root finder over a bounded volatility bracket.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, Tuple

from omdp.domain.contracts import OptionRight

_SQRT_2 = math.sqrt(2.0)
_INV_SQRT_2PI = 1.0 / math.sqrt(2.0 * math.pi)


def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function N(x)."""
    return 0.5 * (1.0 + math.erf(x / _SQRT_2))


def norm_pdf(x: float) -> float:
    """Standard normal probability density function n(x)."""
    return _INV_SQRT_2PI * math.exp(-0.5 * x * x)


@dataclass(frozen=True)
class BSPriceResult:
    price: float
    delta: float
    vega: float


@dataclass(frozen=True)
class IVSolveResult:
    implied_vol: Optional[float]
    delta: Optional[float]
    vega: Optional[float]
    converged: bool
    arbitrage_violation: bool
    solver_name: str
    iterations: int
    price_residual: Optional[float]
    message: str


def bs_price_and_greeks(
    spot: float,
    strike: float,
    time_to_expiry: float,
    risk_free_rate: float,
    dividend_yield: float,
    volatility: float,
    option_right: OptionRight,
) -> BSPriceResult:
    """Compute European Black-Scholes-Merton price, delta, and vega (per 1.0 vol)."""
    if spot <= 0.0 or strike <= 0.0 or time_to_expiry <= 0.0 or volatility <= 0.0:
        raise ValueError(
            f"spot, strike, time_to_expiry, and volatility must be > 0 "
            f"(got S={spot}, K={strike}, T={time_to_expiry}, sigma={volatility})"
        )

    sqrt_t = math.sqrt(time_to_expiry)
    vol_sqrt_t = volatility * sqrt_t
    df_q = math.exp(-dividend_yield * time_to_expiry)
    df_r = math.exp(-risk_free_rate * time_to_expiry)

    d1 = (
        math.log(spot / strike)
        + (risk_free_rate - dividend_yield + 0.5 * volatility * volatility) * time_to_expiry
    ) / vol_sqrt_t
    d2 = d1 - vol_sqrt_t

    if option_right == OptionRight.CALL:
        price = spot * df_q * norm_cdf(d1) - strike * df_r * norm_cdf(d2)
        delta = df_q * norm_cdf(d1)
    else:
        price = strike * df_r * norm_cdf(-d2) - spot * df_q * norm_cdf(-d1)
        delta = df_q * (norm_cdf(d1) - 1.0)

    vega = spot * df_q * norm_pdf(d1) * sqrt_t
    return BSPriceResult(price=price, delta=delta, vega=vega)


def no_arbitrage_bounds(
    spot: float,
    strike: float,
    time_to_expiry: float,
    risk_free_rate: float,
    dividend_yield: float,
    option_right: OptionRight,
) -> Tuple[float, float]:
    """Return European no-arbitrage (lower_bound, upper_bound) for option price."""
    df_q = math.exp(-dividend_yield * time_to_expiry)
    df_r = math.exp(-risk_free_rate * time_to_expiry)
    if option_right == OptionRight.CALL:
        lower = max(0.0, spot * df_q - strike * df_r)
        upper = spot * df_q
    else:
        lower = max(0.0, strike * df_r - spot * df_q)
        upper = strike * df_r
    return lower, upper


def solve_implied_volatility(
    market_price: float,
    spot: float,
    strike: float,
    time_to_expiry: float,
    risk_free_rate: float,
    dividend_yield: float,
    option_right: OptionRight,
    min_vol: float = 1e-6,
    max_vol: float = 5.0,
    price_tolerance: float = 1e-10,
    vol_tolerance: float = 1e-9,
    max_iterations: int = 100,
) -> IVSolveResult:
    """Invert European Black-Scholes price for implied volatility.

    Uses safeguarded Newton-Raphson within a shrinking bracket, falling back to
    bisection/Brent step whenever vega is tiny or Newton steps leave the bracket.
    """
    if spot <= 0.0 or strike <= 0.0 or time_to_expiry <= 0.0 or market_price <= 0.0:
        return IVSolveResult(
            implied_vol=None,
            delta=None,
            vega=None,
            converged=False,
            arbitrage_violation=True,
            solver_name="bs_safeguarded_newton_brent",
            iterations=0,
            price_residual=None,
            message="Non-positive pricing input",
        )

    lower_bound, upper_bound = no_arbitrage_bounds(
        spot, strike, time_to_expiry, risk_free_rate, dividend_yield, option_right
    )
    # Allow tiny numerical epsilon around intrinsic value only if price is strictly above lower_bound - 1e-12
    if market_price < lower_bound - 1e-12 or market_price >= upper_bound:
        return IVSolveResult(
            implied_vol=None,
            delta=None,
            vega=None,
            converged=False,
            arbitrage_violation=True,
            solver_name="bs_safeguarded_newton_brent",
            iterations=0,
            price_residual=None,
            message=(
                f"Market price {market_price:.6f} violates European no-arbitrage bounds "
                f"[{lower_bound:.6f}, {upper_bound:.6f})"
            ),
        )

    low = min_vol
    high = max_vol
    f_low = (
        bs_price_and_greeks(spot, strike, time_to_expiry, risk_free_rate, dividend_yield, low, option_right).price
        - market_price
    )
    f_high = (
        bs_price_and_greeks(spot, strike, time_to_expiry, risk_free_rate, dividend_yield, high, option_right).price
        - market_price
    )

    if abs(f_low) <= price_tolerance:
        res = bs_price_and_greeks(spot, strike, time_to_expiry, risk_free_rate, dividend_yield, low, option_right)
        return IVSolveResult(
            implied_vol=low,
            delta=res.delta,
            vega=res.vega,
            converged=True,
            arbitrage_violation=False,
            solver_name="bs_safeguarded_newton_brent",
            iterations=1,
            price_residual=abs(f_low),
            message="Converged at lower volatility bound",
        )

    if f_low > 0.0:
        return IVSolveResult(
            implied_vol=None,
            delta=None,
            vega=None,
            converged=False,
            arbitrage_violation=True,
            solver_name="bs_safeguarded_newton_brent",
            iterations=0,
            price_residual=abs(f_low),
            message=f"Market price {market_price:.6f} is below minimum-vol model price",
        )

    if f_high < 0.0:
        return IVSolveResult(
            implied_vol=None,
            delta=None,
            vega=None,
            converged=False,
            arbitrage_violation=False,
            solver_name="bs_safeguarded_newton_brent",
            iterations=0,
            price_residual=abs(f_high),
            message=f"Implied volatility exceeds configured max_vol={max_vol}",
        )

    # Brenner-Subrahmanyam-inspired clamped initial guess
    forward = spot * math.exp((risk_free_rate - dividend_yield) * time_to_expiry)
    guess = math.sqrt(2.0 * math.pi / time_to_expiry) * (market_price / max(forward, strike))
    sigma = min(max(guess, low + 0.05 * (high - low)), high - 0.05 * (high - low))
    if not (low < sigma < high):
        sigma = 0.5 * (low + high)

    for iteration in range(1, max_iterations + 1):
        res = bs_price_and_greeks(
            spot, strike, time_to_expiry, risk_free_rate, dividend_yield, sigma, option_right
        )
        diff = res.price - market_price
        if abs(diff) <= price_tolerance or (high - low) <= vol_tolerance:
            return IVSolveResult(
                implied_vol=sigma,
                delta=res.delta,
                vega=res.vega,
                converged=True,
                arbitrage_violation=False,
                solver_name="bs_safeguarded_newton_brent",
                iterations=iteration,
                price_residual=abs(diff),
                message="Converged",
            )

        if diff < 0.0:
            low = sigma
        else:
            high = sigma

        # Attempt Newton-Raphson step if vega is well-conditioned
        next_sigma: Optional[float] = None
        if res.vega > 1e-12:
            candidate = sigma - diff / res.vega
            if low < candidate < high:
                next_sigma = candidate

        # Fallback to bisection when Newton leaves the bracket
        if next_sigma is None:
            next_sigma = 0.5 * (low + high)

        if abs(next_sigma - sigma) <= vol_tolerance:
            final_res = bs_price_and_greeks(
                spot, strike, time_to_expiry, risk_free_rate, dividend_yield, next_sigma, option_right
            )
            final_diff = abs(final_res.price - market_price)
            return IVSolveResult(
                implied_vol=next_sigma,
                delta=final_res.delta,
                vega=final_res.vega,
                converged=True,
                arbitrage_violation=False,
                solver_name="bs_safeguarded_newton_brent",
                iterations=iteration,
                price_residual=final_diff,
                message="Converged by vol tolerance",
            )

        sigma = next_sigma

    final_res = bs_price_and_greeks(
        spot, strike, time_to_expiry, risk_free_rate, dividend_yield, sigma, option_right
    )
    return IVSolveResult(
        implied_vol=None,
        delta=None,
        vega=None,
        converged=False,
        arbitrage_violation=False,
        solver_name="bs_safeguarded_newton_brent",
        iterations=max_iterations,
        price_residual=abs(final_res.price - market_price),
        message=f"Did not converge within {max_iterations} iterations",
    )
