import copy
import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


def _utc_now_iso():
    return datetime.utcnow().isoformat() + "Z"


def _canonical_serialize(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _compute_hash(obj: Any) -> str:
    return hashlib.sha256(_canonical_serialize(obj).encode("utf-8")).hexdigest()


@dataclass
class MarketContext:
    positions: Dict[str, float]
    prices: Dict[str, float]
    exposure: float
    liquidity: float  # 0..1
    risk_limit: float
    kill_switch: bool = False
    audit_log: List[Dict[str, Any]] = field(default_factory=list)

    def log(self, actor: str, action: str, details: Optional[Dict[str, Any]] = None):
        self.audit_log.append(
            {
                "ts": _utc_now_iso(),
                "actor": actor,
                "action": action,
                "details": details or {},
            }
        )


class SimpleRiskSystem:
    """A minimal risk system that estimates loss and enforces exposure reductions.

    - expected_loss: naive percent move * exposure
    - if expected_loss > risk_limit -> attempt to reduce exposure using available liquidity
    - if expected_loss > 2 * risk_limit -> engage kill switch
    """

    def assess_expected_loss(self, ctx: MarketContext, shock_pct: float) -> float:
        return abs(shock_pct) * ctx.exposure

    def enforce(self, ctx: MarketContext, expected_loss: float) -> None:
        ctx.log(
            "risk_system",
            "assess",
            {"expected_loss": expected_loss, "risk_limit": ctx.risk_limit},
        )
        if expected_loss > 2 * ctx.risk_limit:
            ctx.kill_switch = True
            ctx.log("risk_system", "kill_switch", {"expected_loss": expected_loss})
            return

        if expected_loss > ctx.risk_limit:
            # calculate ideal reduction fraction (proportional to excess)
            excess = expected_loss - ctx.risk_limit
            frac = min(0.95, excess / max(1e-9, expected_loss))
            # limited by liquidity: can only liquidate a fraction proportional to liquidity
            sellable_frac = frac * ctx.liquidity
            reduction = ctx.exposure * sellable_frac
            ctx.exposure = max(0.0, ctx.exposure - reduction)
            ctx.log(
                "risk_system",
                "reduce_exposure",
                {
                    "reduction": reduction,
                    "new_exposure": ctx.exposure,
                    "sellable_frac": sellable_frac,
                },
            )


class MarketCrashSimulator:
    """Simulate extreme market crash scenarios.

    Scenarios:
    - '1987': sudden intraday equity crash (deep single-session drop, partial liquidity)
    - '2008': liquidity freeze (difficulty to liquidate; price moves amplified by forced selling)
    - 'correlation_one': multi-asset correlation collapse to 1 (systemic simultaneous moves)
    """

    def __init__(self):
        self.risk = SimpleRiskSystem()

    def _apply_price_shock(
        self, ctx: MarketContext, shock_map: Dict[str, float]
    ) -> None:
        for k, pct in shock_map.items():
            if k in ctx.prices:
                old = ctx.prices[k]
                ctx.prices[k] = old * (1.0 + pct)
        # recompute exposure simplistically as sum(notional * price) across positions
        total = 0.0
        for sym, notional in ctx.positions.items():
            price = ctx.prices.get(sym, 1.0)
            total += abs(notional) * price
        ctx.exposure = total
        ctx.log(
            "simulator", "price_shock", {"prices": ctx.prices, "exposure": ctx.exposure}
        )

    def simulate(
        self, scenario: str, context_payload: Dict[str, Any], seed: Optional[int] = None
    ) -> Dict[str, Any]:
        rng = random.Random(seed)
        # isolate
        ctx = MarketContext(**copy.deepcopy(context_payload))  # type: ignore[arg-type]
        ctx.log("simulator", "start", {"scenario": scenario, "seed": seed})

        if scenario == "1987":
            # single-session deep drop: pick asset drops between -20% and -45%
            shock = rng.uniform(-0.45, -0.20)
            liquidity_hit = max(0.05, ctx.liquidity * rng.uniform(0.2, 0.6))
            self._apply_price_shock(ctx, {s: shock for s in ctx.positions.keys()})
            ctx.liquidity = liquidity_hit
            ctx.log("simulator", "liquidity", {"liquidity": ctx.liquidity})

        elif scenario == "2008":
            # liquidity freeze with cascading forced selling
            # initial moderate price move
            base_shock = rng.uniform(-0.10, -0.25)
            # low liquidity
            ctx.liquidity = min(ctx.liquidity * 0.2, 0.15)
            # cascading amplifier based on lack of liquidity
            amplifier = 1.0 + (1.0 - ctx.liquidity) * rng.uniform(0.5, 1.5)
            shock = base_shock * amplifier
            self._apply_price_shock(ctx, {s: shock for s in ctx.positions.keys()})
            ctx.log(
                "simulator",
                "liquidity",
                {"liquidity": ctx.liquidity, "amplifier": amplifier},
            )

        elif scenario == "correlation_one":
            # assets become perfectly correlated: draw a systemic shock > historical norms
            shock = rng.uniform(-0.60, -0.25)  # extreme
            self._apply_price_shock(ctx, {s: shock for s in ctx.positions.keys()})
            # liquidity collapses due to cross-asset margin calls
            ctx.liquidity = max(0.01, ctx.liquidity * rng.uniform(0.05, 0.3))
            ctx.log("simulator", "liquidity", {"liquidity": ctx.liquidity})

        else:
            raise ValueError(f"unknown scenario: {scenario}")

        # assess expected loss as average shock magnitude * exposure (naive approximation)
        # derive shock_pct from price changes
        # compute simple average pct change
        pct_changes = []
        for s in ctx.positions.keys():
            orig = context_payload["prices"].get(s, 1.0)
            new = ctx.prices.get(s, orig)
            pct_changes.append((new - orig) / max(1e-9, orig))
        avg_shock = sum(pct_changes) / max(1, len(pct_changes))
        expected_loss = self.risk.assess_expected_loss(ctx, abs(avg_shock))

        # enforce risk system (may reduce exposure or trigger kill switch)
        self.risk.enforce(ctx, expected_loss)

        # build deterministic result hash
        result_content = {
            "scenario": scenario,
            "seed": seed,
            "final_exposure": ctx.exposure,
            "kill_switch": ctx.kill_switch,
            "liquidity": ctx.liquidity,
            "prices": ctx.prices,
        }
        res_hash = _compute_hash(result_content)
        ctx.log("simulator", "finish", {"result_hash": res_hash})

        return {"context": ctx, "result": {**result_content, "hash": res_hash}}


__all__ = ["MarketCrashSimulator", "MarketContext", "SimpleRiskSystem"]
