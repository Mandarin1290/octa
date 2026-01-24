from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Callable, Dict, List


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class StressResult:
    scenario: str
    redemption_pct: float
    liquidation_timeline_days: float
    forced_slippage_pct: float
    capital_loss_estimate: float
    notes: Dict[str, float] | None = None


class RedemptionStressEngine:
    """Simple deterministic redemption stress model.

    Input assumptions:
      - portfolio_liquidity: mapping asset_id -> {'weight':w, 'liquidity_days':d, 'slippage_per_day': s}
      - liquidation proceeds degrade with slippage per extra day of pressured selling

    Outputs deterministic metrics used to feed sentinel/risk gates.
    """

    def __init__(
        self,
        audit_fn: Callable[[str, Dict], None] | None = None,
        max_safe_slippage: float = 0.1,
    ):
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.max_safe_slippage = float(max_safe_slippage)

    def _simulate_redemption(
        self, portfolio_liquidity: Dict[str, Dict], redemption_pct: float
    ) -> StressResult:
        # redemption_pct in (0,1]
        remaining = redemption_pct
        # sort assets by liquidity (fastest first)
        assets = sorted(
            portfolio_liquidity.items(),
            key=lambda kv: kv[1].get("liquidity_days", 9999.0),
        )

        total_loss = 0.0
        total_slippage = 0.0
        days_required = 0.0
        for _aid, info in assets:
            if remaining <= 0:
                break
            w = float(info.get("weight", 0.0))
            available = w
            take = min(available, remaining)
            base_days = float(info.get("liquidity_days", 9999.0))
            # extra pressure ratio: fraction of redemption relative to available
            pressure = take / max(available, 1e-12)
            # days to sell scaled by pressure
            days = base_days * (1.0 + pressure)
            days_required = max(days_required, days)
            # slippage per day param
            s_per_day = float(info.get("slippage_per_day", 0.001))
            forced_slip = s_per_day * days
            # capital loss estimate: take * forced_slip (approx)
            loss = take * forced_slip
            total_loss += loss
            total_slippage = max(total_slippage, forced_slip)
            remaining -= take

        # if not fully covered by portfolio weights, assume remainder forced into illiquid sale with heavy slippage
        if remaining > 0:
            heavy_slip = self.max_safe_slippage * 2.0
            total_slippage = max(total_slippage, heavy_slip)
            total_loss += remaining * heavy_slip
            days_required = max(days_required, 30.0)

        result = StressResult(
            scenario=f"{int(redemption_pct * 100)}%_redemption",
            redemption_pct=redemption_pct,
            liquidation_timeline_days=days_required,
            forced_slippage_pct=total_slippage,
            capital_loss_estimate=total_loss,
            notes={"uncovered_fraction": remaining},
        )
        self.audit_fn("redemption_stress.result", asdict(result))
        return result

    def run_scenarios(
        self, portfolio_liquidity: Dict[str, Dict], scenarios: List[float] | None = None
    ) -> List[StressResult]:
        if scenarios is None:
            scenarios = [0.10, 0.25]
        results = []
        for s in scenarios:
            res = self._simulate_redemption(portfolio_liquidity, s)
            results.append(res)
        # correlated market stress: increase slippage_per_day by factor and rerun 10% scenario
        stressed_port = {}
        for aid, info in portfolio_liquidity.items():
            stressed = dict(info)
            stressed["slippage_per_day"] = (
                float(info.get("slippage_per_day", 0.001)) * 5.0
            )
            stressed_port[aid] = stressed
        stressed_res = self._simulate_redemption(stressed_port, 0.10)
        stressed_res.scenario = "10%_redemption_plus_market_stress"
        results.append(stressed_res)
        return results

    def check_sentinels(
        self,
        results: List[StressResult],
        slippage_threshold: float = 0.1,
        loss_threshold: float = 0.05,
    ) -> Dict[str, bool]:
        # simple boolean triggers
        sent = {"slippage_breach": False, "loss_breach": False}
        for r in results:
            if r.forced_slippage_pct >= slippage_threshold:
                sent["slippage_breach"] = True
            if r.capital_loss_estimate >= loss_threshold:
                sent["loss_breach"] = True
        if sent["slippage_breach"] or sent["loss_breach"]:
            self.audit_fn(
                "redemption_stress.sentinel",
                {
                    "slippage_breach": sent["slippage_breach"],
                    "loss_breach": sent["loss_breach"],
                },
            )
        return sent
