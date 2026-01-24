from typing import Dict, Optional


class MultiAssetRiskEngine:
    """Aggregate capacity & margin across asset classes.

    - Unified exposure view across equities, futures, fx, rates, vol
    - Worst-case margin estimator (across stress scenarios)
    - Stress-adjusted leverage ratio
    - Sentinel gates when portfolio-level margin breach occurs
    """

    DEFAULT_MARGIN_RATES = {
        "equities": 0.08,
        "futures": 0.05,
        "fx": 0.02,
        "rates": 0.04,
        "vol": 0.10,
    }

    # stress scenarios apply multiplicative stress factors per asset class
    DEFAULT_STRESS_SCENARIOS = {
        "base": {"equities": 1.0, "futures": 1.0, "fx": 1.0, "rates": 1.0, "vol": 1.0},
        "equity_crash": {
            "equities": 3.0,
            "futures": 2.0,
            "fx": 1.2,
            "rates": 1.0,
            "vol": 1.5,
        },
        "rates_shock": {
            "equities": 1.2,
            "futures": 1.5,
            "fx": 1.0,
            "rates": 2.5,
            "vol": 1.3,
        },
        "fx_shock": {
            "equities": 1.1,
            "futures": 1.0,
            "fx": 3.0,
            "rates": 1.0,
            "vol": 1.1,
        },
    }

    def __init__(
        self,
        audit_fn=None,
        sentinel_api=None,
        margin_rates: Optional[Dict[str, float]] = None,
        stress_scenarios: Optional[Dict[str, Dict[str, float]]] = None,
    ):
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api
        self.margin_rates = margin_rates or dict(self.DEFAULT_MARGIN_RATES)
        self.stress_scenarios = stress_scenarios or dict(self.DEFAULT_STRESS_SCENARIOS)

    def unified_exposure(self, exposures: Dict[str, float]) -> float:
        """Sum absolute exposures across asset classes."""
        return sum(abs(exposures.get(k, 0.0)) for k in self.margin_rates.keys())

    def margins_by_class(self, exposures: Dict[str, float]) -> Dict[str, float]:
        return {
            k: abs(exposures.get(k, 0.0)) * self.margin_rates.get(k, 0.0)
            for k in self.margin_rates.keys()
        }

    def margin_for_scenario(
        self, exposures: Dict[str, float], scenario: Dict[str, float]
    ) -> float:
        total = 0.0
        for k, rate in self.margin_rates.items():
            factor = scenario.get(k, 1.0)
            total += abs(exposures.get(k, 0.0)) * rate * factor
        return total

    def worst_case_margin(self, exposures: Dict[str, float]) -> float:
        margins = {
            name: self.margin_for_scenario(exposures, scenario)
            for name, scenario in self.stress_scenarios.items()
        }
        worst = max(margins.values()) if margins else 0.0
        self.audit_fn("worst_case_margin", {"per_scenario": margins, "worst": worst})
        return worst

    def stress_adjusted_leverage(
        self, exposures: Dict[str, float], capital: float
    ) -> float:
        total_exposure = self.unified_exposure(exposures)
        worst_margin = self.worst_case_margin(exposures)
        # worst-case margin governs: it must be reserved; leverage computed vs remaining capital
        available = max(0.0, capital - worst_margin)
        if available <= 0.0:
            return float("inf")
        return total_exposure / available

    def assess_and_enforce(
        self, exposures: Dict[str, float], capital: float, leverage_limit: float = 10.0
    ) -> Dict[str, float]:
        """Return report and enforce sentinel gate if breach.

        Report includes: total_exposure, worst_margin, leverage, leverage_limit, breach(bool)
        """
        total_exposure = self.unified_exposure(exposures)
        worst_margin = self.worst_case_margin(exposures)
        leverage = self.stress_adjusted_leverage(exposures, capital)
        breach = False

        if worst_margin > capital or leverage > leverage_limit:
            breach = True
            reason = f"portfolio_margin_breach:worst_margin={worst_margin:.2f}:capital={capital:.2f}:leverage={leverage:.2f}"
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(3, reason)
            except Exception:
                pass
            self.audit_fn("margin_breach", {"reason": reason})

        return {
            "total_exposure": total_exposure,
            "worst_margin": worst_margin,
            "leverage": leverage,
            "leverage_limit": leverage_limit,
            "breach": breach,
        }
