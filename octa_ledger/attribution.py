from __future__ import annotations

from collections import defaultdict
from typing import Any, DefaultDict, Dict, List


def attribute_trades(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute attribution from list of trades.

    Each trade: {strategy, asset, pnl, fees, impact}
    Returns totals by strategy and by asset, and cost breakdown.
    """
    strat_pnl: DefaultDict[str, float] = defaultdict(float)
    asset_pnl: DefaultDict[str, float] = defaultdict(float)
    cost_fees = 0.0
    cost_impact = 0.0
    gross_pnl = 0.0
    for t in trades:
        s = t.get("strategy")
        a = t.get("asset")
        s_str = str(s or "")
        a_str = str(a or "")
        pnl = float(t.get("pnl", 0.0))
        fees = float(t.get("fees", 0.0))
        impact = float(t.get("impact", 0.0))
        strat_pnl[s_str] += pnl
        asset_pnl[a_str] += pnl
        cost_fees += fees
        cost_impact += impact
        gross_pnl += pnl

    total_costs = cost_fees + cost_impact
    net_pnl = gross_pnl - total_costs

    return {
        "by_strategy": dict(strat_pnl),
        "by_asset": dict(asset_pnl),
        "costs": {"fees": cost_fees, "impact": cost_impact, "total": total_costs},
        "gross_pnl": gross_pnl,
        "net_pnl": net_pnl,
    }


def reconcile(attribution: Dict[str, Any]) -> bool:
    """Verify that strategy and asset totals reconcile to gross_pnl and net_pnl."""
    by_strategy = attribution.get("by_strategy", {})
    by_asset = attribution.get("by_asset", {})
    gross = attribution.get("gross_pnl", 0.0)
    net = attribution.get("net_pnl", 0.0)
    costs = attribution.get("costs", {}).get("total", 0.0)

    # sum strategy and asset
    sum_strat = sum(by_strategy.values())
    sum_asset = sum(by_asset.values())

    ok = (
        abs(sum_strat - gross) < 1e-8
        and abs(sum_asset - gross) < 1e-8
        and abs(gross - (net + costs)) < 1e-8
    )
    return ok
