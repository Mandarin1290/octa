from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SizingResult:
    position_size: float
    capital_required: float
    expected_sl_loss: float


@dataclass(frozen=True)
class FixedFractionalSizing:
    risk_pct: float = 0.01

    def size(self, entry_price: float, stop_loss: float | None, equity: float) -> SizingResult:
        if equity <= 0 or stop_loss is None or entry_price <= 0:
            return SizingResult(0.0, 0.0, 0.0)
        per_unit_loss = abs(entry_price - stop_loss)
        if per_unit_loss <= 0:
            return SizingResult(0.0, 0.0, 0.0)
        risk_amount = equity * self.risk_pct
        size = risk_amount / per_unit_loss
        capital_required = size * entry_price
        return SizingResult(size, capital_required, risk_amount)


@dataclass(frozen=True)
class VolatilityAdjustedSizing:
    base_risk_pct: float = 0.01
    min_vol: float = 0.005

    def size(
        self, entry_price: float, stop_loss: float | None, equity: float, volatility: float
    ) -> SizingResult:
        if volatility <= 0:
            volatility = self.min_vol
        scale = min(1.0, self.min_vol / volatility)
        risk_pct = self.base_risk_pct * scale
        return FixedFractionalSizing(risk_pct=risk_pct).size(entry_price, stop_loss, equity)


@dataclass(frozen=True)
class MaxLossSizing:
    max_risk_pct: float = 0.01

    def size(self, entry_price: float, stop_loss: float | None, equity: float) -> SizingResult:
        return FixedFractionalSizing(risk_pct=self.max_risk_pct).size(entry_price, stop_loss, equity)
