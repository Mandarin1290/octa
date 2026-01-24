import copy
import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


@dataclass
class LiquidityContext:
    positions: Dict[str, float]
    prices: Dict[str, float]
    cash: float
    liquidity: float  # 0..1 overall market liquidity
    spread: float  # current bid-ask spread expressed as fraction
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


class LiquidityProtector:
    """Risk-first protector that slows liquidation to prioritize capital preservation.

    - `max_liquidation_frac_per_step` caps fraction of position sold per step.
    - If selling would cause capital to fall below `capital_preservation_floor`, stop further liquidation.
    """

    def __init__(
        self,
        max_liquidation_frac_per_step: float = 0.2,
        capital_preservation_floor: float = 0.5,
    ):
        self.max_liquidation_frac_per_step = max_liquidation_frac_per_step
        self.capital_floor = capital_preservation_floor

    def attempt_liquidate(
        self, ctx: LiquidityContext, market_slippage: float
    ) -> Dict[str, Any]:
        results: Dict[str, Any] = {"sold": {}, "proceeds": 0.0, "stopped": False}
        total_cash = ctx.cash
        for sym, qty in list(ctx.positions.items()):
            if qty == 0:
                continue
            sell_qty = min(qty, abs(qty) * self.max_liquidation_frac_per_step)
            # effective price considers spread and slippage
            mid = ctx.prices.get(sym, 1.0)
            effective_price = mid * (1.0 - market_slippage - ctx.spread / 2.0)
            proceeds = sell_qty * effective_price
            # check capital preservation: don't reduce cash below floor * (current total notional + cash)
            total_notional = sum(
                abs(q) * ctx.prices.get(s, 1.0) for s, q in ctx.positions.items()
            )
            preservation_target = self.capital_floor * (total_notional + total_cash)
            if (total_cash + proceeds) < preservation_target:
                ctx.log(
                    "protector",
                    "preservation_stop",
                    {
                        "sym": sym,
                        "sell_qty": sell_qty,
                        "proceeds": proceeds,
                        "preservation_target": preservation_target,
                    },
                )
                results["stopped"] = True
                break
            # apply sale
            ctx.positions[sym] = qty - sell_qty if qty > 0 else qty + sell_qty
            total_cash += proceeds
            results["sold"][sym] = sell_qty
            results["proceeds"] += proceeds
            ctx.log(
                "protector",
                "liquidated",
                {"sym": sym, "sell_qty": sell_qty, "proceeds": proceeds},
            )
        ctx.cash = total_cash
        return results


class LiquidityDrainSimulator:
    """Simulate liquidity drain and fire-sale pressure scenarios.

    Scenarios:
    - 'spread_explosion'
    - 'zero_bid'
    - 'forced_liquidation'
    """

    def __init__(self):
        self.protector = LiquidityProtector()

    def simulate(
        self,
        scenario: str,
        ctx_payload: Dict[str, Any],
        seed: Optional[int] = None,
        steps: int = 5,
    ) -> Dict[str, Any]:
        rng = random.Random(seed)
        ctx: LiquidityContext = LiquidityContext(**copy.deepcopy(ctx_payload))
        ctx.log("sim", "start", {"scenario": scenario, "seed": seed})

        market_slippage = 0.0
        if scenario == "spread_explosion":
            # spreads widen dramatically and slippage increases
            ctx.spread = min(1.0, ctx.spread * rng.uniform(5.0, 20.0))
            market_slippage = rng.uniform(0.05, 0.25)
            ctx.liquidity = max(0.0, ctx.liquidity * rng.uniform(0.1, 0.4))
            ctx.log(
                "sim",
                "spread_explosion",
                {
                    "spread": ctx.spread,
                    "slippage": market_slippage,
                    "liquidity": ctx.liquidity,
                },
            )

        elif scenario == "zero_bid":
            # some symbols lose bids (cannot sell), simulate by setting liquidity to near-zero for those symbols
            market_slippage = rng.uniform(0.1, 0.5)
            # choose subset of symbols to be zero-bid
            syms = list(ctx.positions.keys())
            num = max(1, len(syms) // 3)
            zeroed = rng.sample(syms, num)
            for z in zeroed:
                # mark price as untradable by setting effective price to zero when selling
                ctx.prices[z] = 0.0
            ctx.liquidity = max(0.0, ctx.liquidity * rng.uniform(0.01, 0.2))
            ctx.log(
                "sim",
                "zero_bid",
                {
                    "zeroed": zeroed,
                    "liquidity": ctx.liquidity,
                    "slippage": market_slippage,
                },
            )

        elif scenario == "forced_liquidation":
            # cascading forced selling increases slippage over steps
            ctx.log("sim", "forced_start", {"liquidity": ctx.liquidity})
            market_slippage = 0.02

        else:
            raise ValueError(f"unknown scenario: {scenario}")

        total_loss = 0.0
        for step in range(steps):
            # increase slippage if forced scenario
            if scenario == "forced_liquidation":
                market_slippage += rng.uniform(0.01, 0.05)
                ctx.spread = min(1.0, ctx.spread + rng.uniform(0.01, 0.05))

            res = self.protector.attempt_liquidate(ctx, market_slippage)
            ctx.log("sim", "step", {"step": step, "res": res})
            # compute mark-to-market loss (simplified)
            notional = sum(
                abs(q) * ctx.prices.get(s, 1.0) for s, q in ctx.positions.items()
            )
            loss = max(
                0.0,
                notional
                - sum(
                    abs(q) * ctx_payload["prices"].get(s, 1.0)
                    for s, q in ctx_payload["positions"].items()
                ),
            )
            total_loss = loss
            # break early if protector stopped further liquidation
            if res.get("stopped"):
                break

        result = {
            "final_positions": ctx.positions,
            "cash": ctx.cash,
            "spread": ctx.spread,
            "liquidity": ctx.liquidity,
            "loss": total_loss,
        }
        result["hash"] = _hash(result)
        ctx.log("sim", "finish", {"result_hash": result["hash"]})
        return {"context": ctx, "result": result}


__all__ = ["LiquidityDrainSimulator", "LiquidityContext", "LiquidityProtector"]
