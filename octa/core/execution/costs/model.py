from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional


COST_MODEL_VERSION = "v1"

# ── Per-asset-class fee schedules (canonical defaults) ───────────────
ASSET_CLASS_FEE_SCHEDULE: Dict[str, Dict[str, float]] = {
    "equity":  {"fee_bps": 1.0, "spread_bps": 0.5, "slippage_bps": 0.5},
    "stock":   {"fee_bps": 1.0, "spread_bps": 0.5, "slippage_bps": 0.5},
    "etf":     {"fee_bps": 0.8, "spread_bps": 0.3, "slippage_bps": 0.3},
    "forex":   {"fee_bps": 0.3, "spread_bps": 0.2, "slippage_bps": 0.2},
    "crypto":  {"fee_bps": 5.0, "spread_bps": 3.0, "slippage_bps": 2.0},
    "future":  {"fee_bps": 1.5, "spread_bps": 0.5, "slippage_bps": 0.5},
    "option":  {"fee_bps": 3.0, "spread_bps": 2.0, "slippage_bps": 1.0},
    "index":   {"fee_bps": 0.5, "spread_bps": 0.2, "slippage_bps": 0.2},
}


@dataclass(frozen=True)
class CostConfig:
    fee_bps: float = 1.0
    spread_bps: float = 0.5
    slippage_bps: float = 0.5
    min_cost_bps: float = 0.0
    max_cost_bps: float = 20.0
    stress_multiplier: float = 1.0
    borrow_annual_bps: float = 50.0
    fx_cost_bps: float = 2.0

    @classmethod
    def for_asset_class(cls, asset_class: str, **overrides: Any) -> "CostConfig":
        key = str(asset_class).lower().strip()
        schedule = ASSET_CLASS_FEE_SCHEDULE.get(key, ASSET_CLASS_FEE_SCHEDULE["equity"])
        params = dict(schedule)
        params.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**params)


def cost_model_fingerprint(cfg: CostConfig) -> str:
    blob = json.dumps(
        {
            "version": COST_MODEL_VERSION,
            "config": asdict(cfg),
            "implementation": "octa/core/execution/costs/model.py",
            "entrypoints": ["estimate_costs", "apply_costs", "cost_model_fingerprint"],
        },
        sort_keys=True,
    )
    return hashlib.sha256(blob.encode()).hexdigest()


@dataclass(frozen=True)
class CostsBreakdown:
    total_cost_bps: float
    fee_cost_bps: float
    spread_cost_bps: float
    slippage_cost_bps: float
    per_trade_bps: List[float]
    diagnostics: Dict[str, Any]
    borrow_cost_bps: float = 0.0
    fx_cost_bps: float = 0.0
    gross_pnl_bps: float = 0.0
    net_pnl_bps: float = 0.0
    cost_model_version: str = COST_MODEL_VERSION
    cost_model_fingerprint: str = ""


def estimate_costs(
    trades: Iterable[Mapping[str, Any]],
    market_ctx: Mapping[str, Any],
    cost_cfg: CostConfig,
    *,
    gross_pnl_bps: float = 0.0,
) -> CostsBreakdown:
    per_trade: List[float] = []
    fee_total = 0.0
    spread_total = 0.0
    slippage_total = 0.0
    borrow_total = 0.0
    fx_total = 0.0

    vol = _as_float(market_ctx.get("volatility"), default=0.0)
    liquidity = _as_float(market_ctx.get("liquidity"), default=1.0)
    size_frac_default = _as_float(market_ctx.get("order_size_frac"), default=0.01)
    is_fx_conversion = bool(market_ctx.get("fx_conversion", False))

    for trade in trades:
        size_frac = _as_float(trade.get("size_frac"), default=size_frac_default)
        fee = cost_cfg.fee_bps
        spread = cost_cfg.spread_bps * _spread_multiplier(trade, market_ctx)
        slip = cost_cfg.slippage_bps * (1.0 + vol) * (size_frac / max(liquidity, 1e-6))
        total = (fee + spread + slip) * cost_cfg.stress_multiplier
        total = max(cost_cfg.min_cost_bps, min(cost_cfg.max_cost_bps, total))

        # Borrow cost for short positions (daily rate from annual)
        is_short = _as_float(trade.get("side"), default=1.0)
        if is_short is not None and is_short < 0:
            holding_days = _as_float(trade.get("holding_days"), default=1.0) or 1.0
            borrow = (cost_cfg.borrow_annual_bps / 252.0) * holding_days
            borrow_total += borrow
            total += borrow

        # FX conversion cost
        if is_fx_conversion:
            fx_total += cost_cfg.fx_cost_bps
            total += cost_cfg.fx_cost_bps

        per_trade.append(total)
        fee_total += fee
        spread_total += spread
        slippage_total += slip

    total_cost = sum(per_trade)
    fp = cost_model_fingerprint(cost_cfg)
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
        borrow_cost_bps=float(borrow_total),
        fx_cost_bps=float(fx_total),
        gross_pnl_bps=float(gross_pnl_bps),
        net_pnl_bps=float(gross_pnl_bps - total_cost),
        cost_model_version=COST_MODEL_VERSION,
        cost_model_fingerprint=fp,
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
