from statistics import mean, pstdev
from typing import Any, Dict, List, Optional


def max_drawdown(returns: List[float]) -> float:
    """Compute max drawdown from a series of period returns (simple returns).
    Returns a positive fraction (e.g., 0.2 for 20% drawdown).
    """
    if not returns:
        return 0.0
    equity = 1.0
    peak = equity
    max_dd = 0.0
    for r in returns:
        equity *= 1 + float(r)
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak
        if dd > max_dd:
            max_dd = dd
    return max_dd


def tail_risk(returns: List[float], threshold: float = -0.05) -> float:
    """Return fraction of returns below threshold (tail probability)."""
    if not returns:
        return 0.0
    cnt = sum(1 for r in returns if float(r) <= threshold)
    return cnt / len(returns)


def corr(xs: List[float], ys: List[float]) -> float:
    """Pearson correlation; returns 0 for insufficient data."""
    if not xs or not ys or len(xs) != len(ys) or len(xs) < 2:
        return 0.0
    xbar = mean(xs)
    ybar = mean(ys)
    sx = pstdev(xs)
    sy = pstdev(ys)
    if sx == 0 or sy == 0:
        # if both series are constant and identical, treat as perfectly correlated
        if all(x == xs[0] for x in xs) and all(y == ys[0] for y in ys) and xs == ys:
            return 1.0
        return 0.0
    cov = sum((xi - xbar) * (yi - ybar) for xi, yi in zip(xs, ys, strict=False)) / len(
        xs
    )
    return cov / (sx * sy)


def liquidity_feasible(
    expected_turnover: float, aum: float, available_liquid: float, buffer: float = 1.0
) -> bool:
    """Check if turnover * aum can be covered by available_liquid * buffer."""
    required = abs(float(expected_turnover)) * float(aum)
    return float(available_liquid) * float(buffer) >= required


def run_pre_risk(
    returns: List[float],
    signal_returns: Optional[List[float]] = None,
    existing_returns: Optional[List[float]] = None,
    expected_turnover: float = 0.0,
    aum: float = 1.0,
    available_liquid: float = 0.0,
    max_drawdown_allowed: float = 0.5,
    tail_threshold: float = -0.05,
    max_tail_prob: float = 0.05,
    max_correlation: float = 0.8,
    liquidity_buffer: float = 1.0,
) -> Dict[str, Any]:
    """Run pre-risk checks and return dict with `passed` (bool), `reasons`, `details`."""
    reasons: List[str] = []
    details: Dict[str, Any] = {}

    md = max_drawdown(returns)
    details["max_drawdown"] = md
    if md > max_drawdown_allowed:
        reasons.append("drawdown_exceeded")

    tr = tail_risk(returns, threshold=tail_threshold)
    details["tail_prob"] = tr
    if tr > max_tail_prob:
        reasons.append("excessive_tail_risk")

    if signal_returns is not None and existing_returns is not None:
        c = corr(signal_returns, existing_returns)
        details["correlation"] = c
        if abs(c) > max_correlation:
            reasons.append("correlation_breach")

    liq_ok = liquidity_feasible(
        expected_turnover, aum, available_liquid, buffer=liquidity_buffer
    )
    details["liquidity_ok"] = liq_ok
    if not liq_ok:
        reasons.append("liquidity_insufficient")

    passed = len(reasons) == 0
    return {"passed": passed, "reasons": reasons, "details": details}
