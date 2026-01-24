from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, cast


class IneligibleAsset(Exception):
    pass


@dataclass
class LiquidityProfile:
    symbol: str
    adv: Optional[float]  # average daily volume in contracts/shares per day
    price: float  # current price
    spread_bps: float = 1.0  # typical spread in basis points
    vol: Optional[float] = None  # daily vol estimate (fraction)
    tick_size: Optional[float] = None
    contract_multiplier: float = 1.0  # for futures


class CapacityEngine:
    """Simple, conservative capacity engine.

    Notes / assumptions:
    - If ADV is missing or <=0 the asset is considered ineligible (SAFE: block orders).
    - Capacity is limited by the minimum of:
      * max %ADV per day
      * max impact-derived size (linear proxy)
    - Stress reduces allowed sizes by `stress_multiplier`.
    """

    def __init__(
        self,
        max_pct_adv_per_day: float = 0.10,
        max_participation_rate: float = 0.10,
        max_impact_bps: float = 50.0,
        stress_multiplier: float = 3.0,
    ) -> None:
        self.max_pct_adv_per_day = float(max_pct_adv_per_day)
        self.max_participation_rate = float(max_participation_rate)
        self.max_impact_bps = float(max_impact_bps)
        self.stress_multiplier = float(stress_multiplier)

    def _require_adv(self, prof: LiquidityProfile) -> None:
        if not prof.adv or prof.adv <= 0:
            raise IneligibleAsset(f"Missing or zero ADV for {prof.symbol}")

    def compute_daily_adv_notional(self, prof: LiquidityProfile) -> float:
        self._require_adv(prof)
        adv = cast(float, prof.adv)  # prof.adv validated by _require_adv
        return adv * prof.price * prof.contract_multiplier

    def compute_max_notional(
        self, prof: LiquidityProfile, stress: bool = False
    ) -> float:
        """Return max notional allowed (in currency units) for this asset under given settings.

        If asset is ineligible (ADV missing) raises `IneligibleAsset`.
        """
        self._require_adv(prof)
        daily_adv_notional = self.compute_daily_adv_notional(prof)

        cap_by_pct_adv = daily_adv_notional * self.max_pct_adv_per_day

        # simple linear impact proxy: if taking fraction f of ADV would cause impact ~ f*10000 bps
        # so max size for given max_impact_bps is:
        cap_by_impact = (self.max_impact_bps / 10000.0) * daily_adv_notional

        cap = min(cap_by_pct_adv, cap_by_impact)
        if stress and self.stress_multiplier > 1.0:
            cap = cap / self.stress_multiplier
        return cap

    def compute_slice_limits(
        self, prof: LiquidityProfile, execution_window_hours: float = 1.0
    ) -> float:
        """Return recommended max slice notional for an execution window (e.g., 1 hour).

        Slice limit = ADV_in_window * participation_rate * price * contract_multiplier
        """
        self._require_adv(prof)
        adv_in_window = cast(float, prof.adv) * (execution_window_hours / 24.0)
        return (
            adv_in_window
            * self.max_participation_rate
            * prof.price
            * prof.contract_multiplier
        )

    def recommended_time_to_liquidate_days(self, prof: LiquidityProfile) -> float:
        """Estimate time (days) to liquidate the computed max_notional at normal ADV.

        Returns days (float)."""
        self._require_adv(prof)
        daily_adv_notional = self.compute_daily_adv_notional(prof)
        max_notional = self.compute_max_notional(prof, stress=False)
        if daily_adv_notional == 0:
            return float("inf")
        return max_notional / daily_adv_notional


__all__ = ["CapacityEngine", "LiquidityProfile", "IneligibleAsset"]
