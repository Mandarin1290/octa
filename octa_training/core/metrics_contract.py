from __future__ import annotations

from datetime import datetime
from typing import List, Optional

try:
    from pydantic.v1 import BaseModel, Field, validator
except Exception:  # pragma: no cover
    from pydantic.v1 import BaseModel, Field, validator


class MetricsSummaryLite(BaseModel):
    sharpe: Optional[float] = None
    sharpe_is: Optional[float] = None
    max_drawdown: Optional[float] = None
    n_trades: Optional[int] = None
    is_start: Optional[str] = None
    is_end: Optional[str] = None
    oos_start: Optional[str] = None
    oos_end: Optional[str] = None
    is_ret_count: Optional[int] = None
    oos_ret_count: Optional[int] = None
    is_ret_mean: Optional[float] = None
    oos_ret_mean: Optional[float] = None
    is_ret_std: Optional[float] = None
    oos_ret_std: Optional[float] = None


class MetricsMetadata(BaseModel):
    horizon: Optional[str] = None
    bar_size: Optional[str] = None
    cost_bps: Optional[float] = None
    spread_bps: Optional[float] = None
    sample_start: Optional[datetime] = None
    sample_end: Optional[datetime] = None


class MetricsSummary(BaseModel):
    sharpe: Optional[float] = None
    sortino: Optional[float] = None
    # Optional in-sample Sharpe proxy computed across WF train windows.
    sharpe_is_mean: Optional[float] = None
    # Sharpe stability across WF splits (OOS).
    sharpe_wf_std: Optional[float] = None
    # IS/OOS degradation ratio proxy.
    sharpe_oos_over_is: Optional[float] = None
    max_drawdown: Optional[float] = None
    calmar: Optional[float] = None
    profit_factor: Optional[float] = None
    cagr: Optional[float] = None
    # Cost coverage: Net PnL / Gross PnL (both measured on strategy return stream).
    net_to_gross: Optional[float] = None
    cvar_99: Optional[float] = None
    # Derived tail-risk proxy in sigma units: abs(CVaR_99) / std(returns).
    # Frequency-agnostic gate signal.
    cvar_99_sigma: Optional[float] = None
    # Hard kill-switch tail-risk metric: CVaR 95% vs daily volatility.
    cvar_95: Optional[float] = None
    daily_vol: Optional[float] = None
    cvar_95_over_daily_vol: Optional[float] = None
    # Monte Carlo gate p05 profit-factor — persisted from mandatory_monte_carlo_gate().
    mc_pf_p05: Optional[float] = None
    turnover: Optional[float] = None
    # Turnover normalized to a per-day estimate using inferred bars/day.
    turnover_per_day: Optional[float] = None
    # Average gross exposure proxy (mean(|position|)).
    avg_gross_exposure: Optional[float] = None
    hit_rate: Optional[float] = None
    avg_trade: Optional[float] = None
    # Average net return per trade (fraction of capital per trade).
    avg_net_trade_return: Optional[float] = None
    n_trades: int = 0
    fold_metrics: List[MetricsSummaryLite] = Field(default_factory=list)
    metadata: Optional[MetricsMetadata] = None

    @validator("max_drawdown")
    def check_max_drawdown(cls, v):
        if v is None:
            return v
        if not (0 <= v <= 10):
            raise ValueError("max_drawdown must be in [0,10] (fraction or percent)")
        return v

    @validator("n_trades")
    def check_n_trades(cls, v):
        if v is None:
            return 0
        if v < 0:
            raise ValueError("n_trades must be >= 0")
        return v
