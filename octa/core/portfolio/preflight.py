"""Portfolio pre-flight overlay.

Deterministic, fail-closed checks that run BEFORE any order is submitted:

1. Per-symbol exposure cap
2. Gross / net portfolio exposure caps
3. Correlation gate (max pairwise corr)
4. Tail risk check (empirical CVaR)

Any *unknown* or *error* state triggers a BLOCK — fail-closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence

from .correlation import _corr


@dataclass(frozen=True)
class PreflightConfig:
    max_symbol_exposure_pct: float = 0.10
    max_gross_exposure_pct: float = 1.50
    max_net_exposure_pct: float = 1.00
    max_pairwise_correlation: float = 0.85
    cvar_confidence: float = 0.95
    max_cvar_pct: float = 0.05
    min_returns_for_cvar: int = 20


@dataclass(frozen=True)
class PreflightResult:
    ok: bool
    blocked_symbols: list[str]
    checks: Dict[str, Any]
    reason: str


def _empirical_cvar(returns: Sequence[float], confidence: float) -> Optional[float]:
    """Compute empirical CVaR (expected shortfall) at given confidence.

    Returns None if insufficient data.  Returns a positive number representing
    the expected loss magnitude in the worst (1-confidence) fraction of returns.
    """
    if not returns or len(returns) < 2:
        return None
    sorted_returns = sorted(returns)
    cutoff_idx = max(1, int(len(sorted_returns) * (1.0 - confidence)))
    tail = sorted_returns[:cutoff_idx]
    if not tail:
        return None
    avg_tail_loss = sum(tail) / len(tail)
    return -avg_tail_loss if avg_tail_loss < 0 else 0.0


def run_preflight(
    *,
    positions: Mapping[str, float],
    nav: float,
    returns_by_symbol: Mapping[str, Sequence[float]],
    config: Optional[PreflightConfig] = None,
) -> PreflightResult:
    """Run deterministic portfolio pre-flight checks.

    Parameters
    ----------
    positions : Mapping[str, float]
        Current or proposed position sizes (in currency units) keyed by symbol.
    nav : float
        Net asset value for normalisation.
    returns_by_symbol : Mapping[str, Sequence[float]]
        Historical return series per symbol for correlation and CVaR.
    config : PreflightConfig, optional
        Override default thresholds.

    Returns
    -------
    PreflightResult
        ``ok=True`` if all checks pass.  ``ok=False`` with details otherwise.
    """
    cfg = config or PreflightConfig()
    checks: Dict[str, Any] = {}
    blocked: list[str] = []
    reasons: list[str] = []

    if nav <= 0:
        return PreflightResult(
            ok=False,
            blocked_symbols=list(positions.keys()),
            checks={"nav": nav},
            reason="NAV_INVALID",
        )

    # ---- 1. Per-symbol exposure cap ----
    symbol_exposures: Dict[str, float] = {}
    symbol_violations: list[str] = []
    for sym, size in sorted(positions.items()):
        exposure_pct = abs(size) / nav
        symbol_exposures[sym] = round(exposure_pct, 6)
        if exposure_pct > cfg.max_symbol_exposure_pct:
            symbol_violations.append(sym)
            blocked.append(sym)
    checks["symbol_exposures"] = symbol_exposures
    checks["symbol_cap_pct"] = cfg.max_symbol_exposure_pct
    checks["symbol_violations"] = symbol_violations
    if symbol_violations:
        reasons.append("SYMBOL_EXPOSURE_EXCEEDED")

    # ---- 2. Gross / net exposure ----
    gross = sum(abs(v) for v in positions.values()) / nav
    net = sum(v for v in positions.values()) / nav
    checks["gross_exposure_pct"] = round(gross, 6)
    checks["net_exposure_pct"] = round(net, 6)
    checks["gross_cap_pct"] = cfg.max_gross_exposure_pct
    checks["net_cap_pct"] = cfg.max_net_exposure_pct

    if gross > cfg.max_gross_exposure_pct:
        reasons.append("GROSS_EXPOSURE_EXCEEDED")
    if abs(net) > cfg.max_net_exposure_pct:
        reasons.append("NET_EXPOSURE_EXCEEDED")

    # ---- 3. Correlation gate ----
    symbols_with_returns = [s for s in positions if s in returns_by_symbol]
    max_corr = 0.0
    max_corr_pair: Optional[tuple[str, str]] = None
    for i, sym_a in enumerate(symbols_with_returns):
        for sym_b in symbols_with_returns[i + 1:]:
            corr = abs(_corr(
                list(returns_by_symbol[sym_a]),
                list(returns_by_symbol[sym_b]),
            ))
            if corr > max_corr:
                max_corr = corr
                max_corr_pair = (sym_a, sym_b)

    checks["max_pairwise_correlation"] = round(max_corr, 6)
    checks["max_corr_pair"] = max_corr_pair
    checks["corr_threshold"] = cfg.max_pairwise_correlation

    if max_corr > cfg.max_pairwise_correlation:
        reasons.append("CORRELATION_EXCEEDED")

    # Fail-closed: if we have positions but no returns data at all
    symbols_missing_returns = [s for s in positions if s not in returns_by_symbol]
    if symbols_missing_returns and len(positions) > 1:
        checks["symbols_missing_returns"] = symbols_missing_returns
        reasons.append("UNKNOWN_CORRELATION")

    # ---- 4. Tail risk (CVaR) ----
    portfolio_returns: list[float] = []
    # Build simple weighted portfolio returns
    if returns_by_symbol and symbols_with_returns:
        min_len = min(len(returns_by_symbol[s]) for s in symbols_with_returns)
        if min_len >= cfg.min_returns_for_cvar:
            total_weight = sum(abs(positions.get(s, 0.0)) for s in symbols_with_returns)
            if total_weight > 0:
                for t in range(min_len):
                    port_ret = 0.0
                    for s in symbols_with_returns:
                        w = abs(positions.get(s, 0.0)) / total_weight
                        port_ret += w * returns_by_symbol[s][t]
                    portfolio_returns.append(port_ret)

    if portfolio_returns:
        cvar = _empirical_cvar(portfolio_returns, cfg.cvar_confidence)
        checks["cvar"] = round(cvar, 6) if cvar is not None else None
        checks["cvar_confidence"] = cfg.cvar_confidence
        checks["cvar_cap_pct"] = cfg.max_cvar_pct
        if cvar is not None and cvar > cfg.max_cvar_pct:
            reasons.append("TAIL_RISK_EXCEEDED")
    else:
        checks["cvar"] = None
        checks["cvar_confidence"] = cfg.cvar_confidence
        # Fail-closed if we have meaningful positions but can't compute tail risk
        if len(positions) > 1 and sum(abs(v) for v in positions.values()) > 0:
            reasons.append("UNKNOWN_TAIL_RISK")

    ok = len(reasons) == 0
    reason = ",".join(reasons) if reasons else "PREFLIGHT_OK"

    return PreflightResult(
        ok=ok,
        blocked_symbols=sorted(set(blocked)),
        checks=checks,
        reason=reason,
    )
