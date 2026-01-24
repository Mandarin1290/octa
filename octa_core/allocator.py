from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

from .correlation import correlation_matrix, rolling_returns
from .strategy import StrategyOutput


@dataclass
class PortfolioIntent:
    targets: Dict[str, float]
    attribution: Dict[str, Dict[str, float]]


def allocate(
    strategy_results: List[Tuple[str, StrategyOutput]],
    current_portfolio: Dict[str, float],
    risk_budgets: Dict[str, float],
    prices: Dict[str, List[float]],
    asset_classes: Dict[str, str],
    gross_cap: float = 1.0,
    net_cap: float = 1.0,
) -> PortfolioIntent:
    """Allocate target exposures given multiple strategy outputs.

    - strategy_results: list of (strategy_id, StrategyOutput)
    - risk_budgets: map strategy_id -> budget multiplier (0..1)
    - prices: map symbol -> price series for vol/corr
    - asset_classes: map symbol -> class
    Returns PortfolioIntent with targets (symbol->exposure) and attribution per strategy.
    """
    # start with raw exposures per strategy adjusted by their risk budget
    strat_adj: Dict[str, Dict[str, float]] = {}
    for sid, so in strategy_results:
        budget = float(risk_budgets.get(sid, 1.0))
        adj = {sym: float(v) * budget for sym, v in so.exposures.items()}
        strat_adj[sid] = adj

    # compute vol per asset (std of returns)
    vols: Dict[str, float] = {}
    for s, series in prices.items():
        r = rolling_returns(series)
        if not r:
            vols[s] = 0.0
        else:
            mean = sum(r) / len(r)
            variance = sum((x - mean) ** 2 for x in r) / len(r)
            vols[s] = math.sqrt(variance)

    # correlation-aware scaling: compute average absolute correlation per asset
    corr = correlation_matrix(prices) if prices else {}
    avg_abs_corr: Dict[str, float] = {}
    for s in corr:
        vals = [abs(corr[s].get(other, 0.0)) for other in corr if other != s]
        avg_abs_corr[s] = (sum(vals) / len(vals)) if vals else 0.0

    # aggregate exposures with volatility scaling and correlation downweight
    combined: Dict[str, float] = defaultdict(float)
    attribution: Dict[str, Dict[str, float]] = defaultdict(dict)
    for sid, exposures in strat_adj.items():
        for sym, raw in exposures.items():
            vol = vols.get(sym, 0.0)
            # normalized volatility scaling: avoid huge multipliers for tiny vols
            vol_scale = 1.0 / (1.0 + vol * 100.0) if vol >= 0.0 else 1.0
            corr_scale = 1.0 / (1.0 + avg_abs_corr.get(sym, 0.0))
            scaled = raw * vol_scale * corr_scale
            combined[sym] += scaled
            attribution[sid][sym] = scaled

    # enforce gross/net caps
    # gross = sum(abs(exposures)) cap to gross_cap
    gross = sum(abs(v) for v in combined.values())
    if gross > gross_cap and gross > 0:
        scale_down = gross_cap / gross
        for s in list(combined.keys()):
            combined[s] = combined[s] * scale_down
            # scale attribution accordingly
            for sid in attribution:
                if s in attribution[sid]:
                    attribution[sid][s] *= scale_down

    # net cap enforcement per asset class
    class_totals: Dict[str, float] = defaultdict(float)
    for s, v in combined.items():
        cls = asset_classes.get(s, "UNKNOWN")
        class_totals[cls] += v
    for cls, total in class_totals.items():
        if abs(total) > net_cap and total != 0:
            # scale down all assets in this class proportionally
            factor = net_cap / abs(total)
            for s, v in list(combined.items()):
                if asset_classes.get(s, "UNKNOWN") == cls:
                    combined[s] = v * factor
                    for sid in attribution:
                        if s in attribution[sid]:
                            attribution[sid][s] *= factor

    # round small values to zero
    for s in list(combined.keys()):
        if abs(combined[s]) < 1e-6:
            combined.pop(s, None)

    return PortfolioIntent(
        targets=dict(combined), attribution={k: v for k, v in attribution.items()}
    )
