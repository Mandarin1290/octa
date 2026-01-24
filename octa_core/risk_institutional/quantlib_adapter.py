from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _require_quantlib():
    try:
        import QuantLib as ql  # type: ignore

        return ql
    except Exception as e:
        raise RuntimeError("QuantLib_not_available") from e


def build_flat_yield_curve(rate: float, day_count: str = "Actual365Fixed", calendar: str = "TARGET") -> Any:
    """Build a flat yield curve handle.

    Used for discounting and stress/shock scenarios.
    """

    ql = _require_quantlib()

    dc = getattr(ql, day_count)()
    getattr(ql, calendar)()
    today = ql.Date.todaysDate()
    ql.Settings.instance().evaluationDate = today

    curve = ql.FlatForward(today, ql.QuoteHandle(ql.SimpleQuote(float(rate))), dc)
    return ql.YieldTermStructureHandle(curve)


def price_european_option_black_scholes(
    spot: float,
    strike: float,
    vol: float,
    r: float,
    q: float,
    t: float,
    call_put: str = "call",
) -> Dict[str, float]:
    """Price European option using Black-Scholes-Merton with QuantLib.

    Returns price and basic Greeks (delta, gamma, vega).
    """

    ql = _require_quantlib()

    if t <= 0:
        raise ValueError("t_must_be_positive")

    today = ql.Date.todaysDate()
    ql.Settings.instance().evaluationDate = today

    dc = ql.Actual365Fixed()

    maturity = today + int(round(float(t) * 365))
    payoff = ql.PlainVanillaPayoff(ql.Option.Call if call_put.lower().startswith("c") else ql.Option.Put, float(strike))
    exercise = ql.EuropeanExercise(maturity)
    option = ql.VanillaOption(payoff, exercise)

    spot_h = ql.QuoteHandle(ql.SimpleQuote(float(spot)))
    r_ts = ql.YieldTermStructureHandle(ql.FlatForward(today, float(r), dc))
    q_ts = ql.YieldTermStructureHandle(ql.FlatForward(today, float(q), dc))
    vol_ts = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(today, ql.TARGET(), float(vol), dc))

    process = ql.BlackScholesMertonProcess(spot_h, q_ts, r_ts, vol_ts)
    engine = ql.AnalyticEuropeanEngine(process)
    option.setPricingEngine(engine)

    return {
        "price": float(option.NPV()),
        "delta": float(option.delta()),
        "gamma": float(option.gamma()),
        "vega": float(option.vega()),
    }


def curve_shock_scenarios(curve_handle: Any, shocks_bp_list: List[float]) -> List[Tuple[float, Any]]:
    """Create parallel-shifted yield curves from a base curve.

    shocks_bp_list: list of shocks in basis points, e.g. [-50, 0, +50]
    Returns list of (shock_bp, shocked_curve_handle)
    """

    ql = _require_quantlib()

    base = curve_handle
    out: List[Tuple[float, Any]] = []
    for bp in shocks_bp_list:
        spread = ql.QuoteHandle(ql.SimpleQuote(float(bp) / 10000.0))
        z = ql.ZeroSpreadedTermStructure(base, spread)
        out.append((float(bp), ql.YieldTermStructureHandle(z)))
    return out


__all__ = [
    "build_flat_yield_curve",
    "price_european_option_black_scholes",
    "curve_shock_scenarios",
]
