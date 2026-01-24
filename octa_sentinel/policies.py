from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class GateLevel(int):
    L0 = 0
    L1 = 1
    L2 = 2
    L3 = 3


class DrawdownPolicy(BaseModel):
    max_portfolio_drawdown: float = Field(
        0.05, description="fractional drawdown e.g., 0.05 for 5%"
    )
    max_daily_loss: float = Field(0.02)


class ExposurePolicy(BaseModel):
    max_per_asset: float = Field(0.1, description="fractional of NAV")
    max_gross_exposure: float = Field(1.0)
    max_net_exposure: float = Field(0.5)


class LeveragePolicy(BaseModel):
    max_leverage_equity: float = Field(2.0)
    max_leverage_future: float = Field(10.0)


class OperationalPolicy(BaseModel):
    audit_failure_level: int = Field(3)
    broker_disconnect_level: int = Field(2)
    data_integrity_failure_level: int = Field(3)


class SentinelPolicy(BaseModel):
    schema_version: int = Field(..., ge=1)
    name: str
    drawdown: DrawdownPolicy = Field(
        default_factory=lambda: DrawdownPolicy(
            max_portfolio_drawdown=0.05, max_daily_loss=0.02
        )
    )
    exposure: ExposurePolicy = Field(
        default_factory=lambda: ExposurePolicy(
            max_per_asset=0.1, max_gross_exposure=1.0, max_net_exposure=0.5
        )
    )
    leverage: LeveragePolicy = Field(
        default_factory=lambda: LeveragePolicy(
            max_leverage_equity=2.0, max_leverage_future=10.0
        )
    )
    operational: OperationalPolicy = Field(
        default_factory=lambda: OperationalPolicy(
            audit_failure_level=3,
            broker_disconnect_level=2,
            data_integrity_failure_level=3,
        )
    )
    policy_version: Optional[str] = None
