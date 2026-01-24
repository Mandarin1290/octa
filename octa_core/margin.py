"""Margin, leverage and financing model.

Conservative, multi-asset margin calculator suitable for paper mode.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass
class InstrumentSpec:
    instrument_id: str
    instrument_type: str  # 'equity'|'etf'|'future'|'fx'|'bond'
    contract_multiplier: float = 1.0
    tick_size: float = 0.0
    margin_initial_rate: float = 0.5  # fraction of notional
    margin_maintenance_rate: float = 0.3
    haircut: float = 0.0


@dataclass
class MarginConfig:
    borrow_rate_annual: float = 0.05
    funding_rate_annual: float = 0.0
    conservative_margin_multiplier: float = 1.5


class PortfolioMarginCalculator:
    def __init__(
        self,
        equity: float,
        specs: Dict[str, InstrumentSpec],
        config: MarginConfig | None = None,
    ):
        self.equity = float(equity)
        self.specs = specs
        self.config = config or MarginConfig()

    def compute(self, positions: List[Dict]) -> Dict:
        """Compute margin and financing metrics.

        positions: list of {instrument_id, quantity, price, side: 'long'|'short'}
        returns dictionary with gross, net, leverage, margins, financing costs, flags
        """
        gross = 0.0
        net = 0.0
        initial_margin = 0.0
        maintenance_margin = 0.0
        borrow_cost = 0.0
        funding_cost = 0.0

        for p in positions:
            iid = p["instrument_id"]
            qty = float(p.get("quantity", 0.0))
            price = float(p.get("price", 0.0))
            side = p.get("side", "long")
            spec = self.specs.get(iid)

            if spec is None:
                # unknown instrument: apply conservative assumptions
                notional = abs(qty) * price
                init_rate = 0.5 * self.config.conservative_margin_multiplier
                maint_rate = 0.3 * self.config.conservative_margin_multiplier
            else:
                mult = spec.contract_multiplier if spec.contract_multiplier else 1.0
                notional = abs(qty) * price * mult
                init_rate = spec.margin_initial_rate
                maint_rate = spec.margin_maintenance_rate
                # apply haircut for bonds
                if spec.instrument_type == "bond":
                    notional = notional * (1.0 - spec.haircut)

            gross += notional
            signed = notional * (1.0 if side == "long" else -1.0)
            net += signed

            initial_margin += notional * init_rate
            maintenance_margin += notional * maint_rate

            # financing: borrow costs for shorts
            if side == "short":
                borrow_cost += notional * self.config.borrow_rate_annual

            # funding proxy (applies to futures/carry)
            funding_cost += notional * self.config.funding_rate_annual

        leverage = gross / max(1e-9, self.equity)
        margin_utilization = initial_margin / max(1e-9, self.equity)
        headroom = self.equity - initial_margin

        breach_flags = {
            "initial_margin_breached": margin_utilization >= 1.0,
            "maintenance_margin_breached": maintenance_margin >= self.equity,
        }

        return {
            "gross_exposure": gross,
            "net_exposure": net,
            "leverage": leverage,
            "initial_margin": initial_margin,
            "maintenance_margin": maintenance_margin,
            "margin_utilization": margin_utilization,
            "headroom": headroom,
            "borrow_cost_annual": borrow_cost,
            "funding_cost_annual": funding_cost,
            "breach_flags": breach_flags,
        }
