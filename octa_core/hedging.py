from statistics import mean, variance
from typing import Dict, List, Tuple


def _cov(x: List[float], y: List[float]) -> float:
    n = len(x)
    if n < 2:
        return 0.0
    mx = mean(x)
    my = mean(y)
    return sum((xi - mx) * (yi - my) for xi, yi in zip(x, y, strict=False)) / (n - 1)


def _var(x: List[float]) -> float:
    if len(x) < 2:
        return 0.0
    return variance(x)


class HedgeEngine:
    """Simple, explainable cross-asset hedging engine.

    - Supports equity beta hedge (index futures), duration hedge (rate futures), FX hedge (spot/forward proxy).
    - Tracks effectiveness as variance reduction.
    - Adjusts hedge ratio by regime ('calm'|'volatile'|'stress').
    - Calls sentinel_api.set_gate(level, reason) when hedge is ineffective.
    """

    def __init__(
        self, audit_fn=None, sentinel_api=None, effectiveness_threshold: float = 0.01
    ):
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api
        self.effectiveness_threshold = effectiveness_threshold

    def _compute_beta(self, asset_r: List[float], hedge_r: List[float]) -> float:
        v = _var(hedge_r)
        if v <= 0:
            return 0.0
        return _cov(asset_r, hedge_r) / v

    def _regime_scale(self, regime: str) -> float:
        # dynamic sizing: conservative in stress, aggressive in calm
        return {"calm": 1.2, "normal": 1.0, "volatile": 0.75, "stress": 0.5}.get(
            regime, 1.0
        )

    def compute_hedge_positions(
        self,
        exposures: Dict[str, float],
        market_returns: Dict[str, List[float]],
        regime: str = "normal",
    ) -> Dict[str, float]:
        """Return hedge positions keyed by hedge instrument name.

        exposures: e.g. {'EQ': 1000000.0, 'RATES': 500000.0, 'FX': 200000.0}
        market_returns: must include asset returns and hedge proxy returns, e.g.
          'EQ', 'EQ_FUT', 'RATES', 'RATES_FUT', 'FX', 'FX_PROXY'
        Positions are sized so that sign reduces exposure (position = -ratio * exposure).
        """
        positions: Dict[str, float] = {}
        scale = self._regime_scale(regime)

        # Equity beta hedge
        if "EQ" in exposures and "EQ" in market_returns and "EQ_FUT" in market_returns:
            beta = self._compute_beta(market_returns["EQ"], market_returns["EQ_FUT"])
            ratio = beta * scale
            positions["EQ_FUT"] = -ratio * exposures["EQ"]
            self.audit_fn(
                "hedge_compute", {"instrument": "EQ_FUT", "beta": beta, "ratio": ratio}
            )

        # Duration hedge (rates)
        if (
            "RATES" in exposures
            and "RATES" in market_returns
            and "RATES_FUT" in market_returns
        ):
            beta = self._compute_beta(
                market_returns["RATES"], market_returns["RATES_FUT"]
            )
            ratio = beta * scale
            positions["RATES_FUT"] = -ratio * exposures["RATES"]
            self.audit_fn(
                "hedge_compute",
                {"instrument": "RATES_FUT", "beta": beta, "ratio": ratio},
            )

        # FX hedge
        if (
            "FX" in exposures
            and "FX" in market_returns
            and "FX_PROXY" in market_returns
        ):
            beta = self._compute_beta(market_returns["FX"], market_returns["FX_PROXY"])
            ratio = beta * scale
            positions["FX_PROXY"] = -ratio * exposures["FX"]
            self.audit_fn(
                "hedge_compute",
                {"instrument": "FX_PROXY", "beta": beta, "ratio": ratio},
            )

        return positions

    def evaluate_hedge_effectiveness(
        self,
        exposure: float,
        asset_r: List[float],
        hedge_r: List[float],
        hedge_ratio: float,
    ) -> Tuple[float, float]:
        """Return (variance_unhedged, variance_net) where net includes the hedge sized as hedge_ratio*exposure.

        hedge_ratio is the multiplier applied to exposure to size hedge (positive means long hedge which could increase or decrease risk depending on sign convention).
        """
        n = min(len(asset_r), len(hedge_r))
        if n < 2:
            return 0.0, 0.0
        unhedged = [exposure * r for r in asset_r[:n]]
        hedge_pnl = [-hedge_ratio * exposure * hr for hr in hedge_r[:n]]
        net = [u + h for u, h in zip(unhedged, hedge_pnl, strict=False)]
        var_un = _var(unhedged)
        var_net = _var(net)
        return var_un, var_net

    def assess_and_enforce(
        self,
        exposures: Dict[str, float],
        market_returns: Dict[str, List[float]],
        regime: str = "normal",
    ) -> Dict[str, Dict[str, float]]:
        """Compute positions, evaluate variance reduction, and call sentinel if ineffective.

        Returns a report mapping instrument -> {"ratio":.., "var_unhedged":.., "var_net":.., "reduction":..}
        """
        positions = self.compute_hedge_positions(exposures, market_returns, regime)
        report: Dict[str, Dict[str, float]] = {}

        for instr, pos in positions.items():
            # determine corresponding asset key
            if instr == "EQ_FUT":
                asset_key = "EQ"
                hedge_key = "EQ_FUT"
            elif instr == "RATES_FUT":
                asset_key = "RATES"
                hedge_key = "RATES_FUT"
            elif instr == "FX_PROXY":
                asset_key = "FX"
                hedge_key = "FX_PROXY"
            else:
                continue

            asset_r = market_returns.get(asset_key, [])
            hedge_r = market_returns.get(hedge_key, [])
            if not asset_r or not hedge_r:
                continue

            # hedge_ratio per unit exposure
            ratio = (
                pos / exposures.get(asset_key, 1.0)
                if exposures.get(asset_key, 0) != 0
                else 0.0
            )
            var_un, var_net = self.evaluate_hedge_effectiveness(
                exposures[asset_key], asset_r, hedge_r, abs(ratio)
            )
            reduction = 1.0 - (var_net / var_un) if var_un > 0 else 0.0
            report[instr] = {
                "ratio": ratio,
                "var_unhedged": var_un,
                "var_net": var_net,
                "reduction": reduction,
            }
            self.audit_fn(
                "hedge_assess",
                {"instrument": instr, "ratio": ratio, "reduction": reduction},
            )

            if reduction < self.effectiveness_threshold:
                if self.sentinel_api is not None:
                    reason = f"ineffective_hedge:{instr}:reduction={reduction:.6f}"
                    try:
                        self.sentinel_api.set_gate(3, reason)
                    except Exception:
                        pass
                # also log via audit
                self.audit_fn(
                    "hedge_ineffective", {"instrument": instr, "reduction": reduction}
                )

        return report
