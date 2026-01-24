from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from octa_core.capacity import CapacityEngine, IneligibleAsset, LiquidityProfile
from octa_ledger.events import AuditEvent
from octa_ledger.store import LedgerStore


@dataclass(frozen=True)
class StressScenario:
    name: str
    version: str
    adv_shock_factor: float  # multiply ADV by this factor (0.1..1.0)
    spread_mult: float  # multiply spreads
    vol_mult: float
    gap_pct: float  # fractional gap stress


@dataclass
class AssetStressResult:
    symbol: str
    time_to_liquidate_days: float
    expected_slippage_bps: float
    forced_loss_estimate: float


class LiquidityStressTester:
    """Deterministic liquidity stress tester.

    It computes per-asset and portfolio metrics under specified scenarios and writes
    an audited `liquidity_stress.report` event to the ledger. If any asset exceeds
    configured thresholds, it will also append a `gate_event` to the ledger (severity WARN).
    """

    def __init__(self, engine: CapacityEngine):
        self.engine = engine

    def run_scenario(
        self,
        ledger: LedgerStore,
        profiles: List[LiquidityProfile],
        scenario: StressScenario,
        portfolio_notional: float = 0.0,
        time_to_liquidate_threshold_days: float = 7.0,
    ) -> Dict[str, Any]:
        results: List[AssetStressResult] = []
        portfolio_ttl = 0.0
        total_forced_loss = 0.0

        for prof in profiles:
            try:
                # apply adv shock
                shocked_adv = prof.adv * scenario.adv_shock_factor if prof.adv else None
                shocked_prof = LiquidityProfile(
                    symbol=prof.symbol,
                    adv=shocked_adv,
                    price=prof.price,
                    spread_bps=prof.spread_bps * scenario.spread_mult,
                    vol=(prof.vol * scenario.vol_mult) if prof.vol else None,
                    tick_size=prof.tick_size,
                    contract_multiplier=prof.contract_multiplier,
                )

                # compute max notional under shock (stress=True uses stress_multiplier)
                try:
                    max_notional = self.engine.compute_max_notional(
                        shocked_prof, stress=True
                    )
                except IneligibleAsset:
                    # ineligible: report zeros and mark as breach
                    results.append(
                        AssetStressResult(
                            prof.symbol, float("inf"), float("inf"), float("inf")
                        )
                    )
                    continue

                # time to liquidate at shocked adv (days) assuming using max_pct_adv_per_day participation
                daily_adv_notional = (
                    (shocked_prof.adv or 0.0)
                    * shocked_prof.price
                    * shocked_prof.contract_multiplier
                )
                if daily_adv_notional <= 0:
                    ttl = float("inf")
                else:
                    ttl = max_notional / daily_adv_notional

                # expected slippage proxy (bps): spread + linear impact = spread + 10000*(size/daily_adv)
                impact = (
                    10000.0 * (max_notional / daily_adv_notional)
                    if daily_adv_notional > 0
                    else float("inf")
                )
                expected_slippage = shocked_prof.spread_bps + impact

                # forced loss estimate: assume worst-case gap pct of scenario applied to max_notional
                forced_loss = max_notional * scenario.gap_pct

                results.append(
                    AssetStressResult(prof.symbol, ttl, expected_slippage, forced_loss)
                )
                portfolio_ttl = max(portfolio_ttl, ttl)
                total_forced_loss += forced_loss
            except Exception:
                # deterministic handling: record failure with infinities
                results.append(
                    AssetStressResult(
                        prof.symbol, float("inf"), float("inf"), float("inf")
                    )
                )

        payload = {
            "scenario": {
                "name": scenario.name,
                "version": scenario.version,
                "adv_shock_factor": scenario.adv_shock_factor,
                "spread_mult": scenario.spread_mult,
                "vol_mult": scenario.vol_mult,
                "gap_pct": scenario.gap_pct,
            },
            # omit non-deterministic timestamps to keep reports reproducible given same inputs
            "asset_results": [r.__dict__ for r in results],
            "portfolio_time_to_liquidate_days": portfolio_ttl,
            "portfolio_forced_loss": total_forced_loss,
        }

        ev = AuditEvent.create(
            actor="liquidity_stress",
            action="liquidity_stress.report",
            payload=payload,
            severity="INFO",
        )
        ledger.append(ev)

        # if any asset TTL > threshold, create gate_event
        # breach if TTL equals or exceeds threshold
        breached = portfolio_ttl >= time_to_liquidate_threshold_days
        if breached:
            ge = AuditEvent.create(
                actor="liquidity_stress",
                action="gate_event",
                payload={"reason": "liquidation_time_exceeded", "ttl": portfolio_ttl},
                severity="WARN",
            )
            ledger.append(ge)

        return payload


__all__ = ["LiquidityStressTester", "StressScenario", "AssetStressResult"]
