from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional


@dataclass(frozen=True)
class CostConfig:
    fee_bps: float = 1.0
    spread_bps: float = 0.5
    slippage_bps: float = 0.5
    min_cost_bps: float = 0.0
    max_cost_bps: float = 20.0
    stress_multiplier: float = 1.0


@dataclass(frozen=True)
class CostsBreakdown:
    total_cost_bps: float
    fee_cost_bps: float
    spread_cost_bps: float
    slippage_cost_bps: float
    per_trade_bps: List[float]
    diagnostics: Dict[str, Any]


def estimate_costs(
    trades: Iterable[Mapping[str, Any]],
    market_ctx: Mapping[str, Any],
    cost_cfg: CostConfig,
) -> CostsBreakdown:
    per_trade: List[float] = []
    fee_total = 0.0
    spread_total = 0.0
    slippage_total = 0.0

    vol = _as_float(market_ctx.get("volatility"), default=0.0)
    liquidity = _as_float(market_ctx.get("liquidity"), default=1.0)
    size_frac_default = _as_float(market_ctx.get("order_size_frac"), default=0.01)

    for trade in trades:
        size_frac = _as_float(trade.get("size_frac"), default=size_frac_default)
        fee = cost_cfg.fee_bps
        spread = cost_cfg.spread_bps * _spread_multiplier(trade, market_ctx)
        slip = cost_cfg.slippage_bps * (1.0 + vol) * (size_frac / max(liquidity, 1e-6))
        total = (fee + spread + slip) * cost_cfg.stress_multiplier
        total = max(cost_cfg.min_cost_bps, min(cost_cfg.max_cost_bps, total))
        per_trade.append(total)
        fee_total += fee
        spread_total += spread
        slippage_total += slip

    total_cost = sum(per_trade)
    return CostsBreakdown(
        total_cost_bps=float(total_cost),
        fee_cost_bps=float(fee_total),
        spread_cost_bps=float(spread_total),
        slippage_cost_bps=float(slippage_total),
        per_trade_bps=per_trade,
        diagnostics={
            "trade_count": len(per_trade),
            "volatility": vol,
            "liquidity": liquidity,
        },
    )


def apply_costs(pnl_series: Iterable[float], costs: CostsBreakdown) -> List[float]:
    pnl = list(pnl_series)
    if not pnl or not costs.per_trade_bps:
        return pnl
    total_cost_return = (sum(costs.per_trade_bps) / 10000.0)
    per_bar = total_cost_return / max(len(pnl), 1)
    return [float(r - per_bar) for r in pnl]


def _spread_multiplier(trade: Mapping[str, Any], market_ctx: Mapping[str, Any]) -> float:
    high = _as_float(market_ctx.get("high"), default=None)
    low = _as_float(market_ctx.get("low"), default=None)
    price = _as_float(trade.get("price"), default=None)
    if high is None or low is None or price is None or price == 0:
        return 1.0
    spread_proxy = max(0.0, (high - low) / price)
    return min(3.0, max(0.5, 1.0 + spread_proxy))


def _as_float(value: Any, *, default: Optional[float]) -> Optional[float]:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default
