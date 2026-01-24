from typing import Any, Dict, Optional

from octa_capital.aum_state import AUMState
from octa_capital.hard_close import HardCloseEngine
from octa_capital.liquidity_buckets import LiquidityBuckets
from octa_capital.scaling_impact import ScalingImpactAnalyzer
from octa_capital.soft_close import SoftCloseEngine
from octa_capital.tiers import CapitalTierEngine
from octa_core.aum_allocator import AUMAwareAllocator


class CapitalScalingDashboard:
    """Deterministic, auditable dashboard exposing capital-scaling metrics.

    Fields produced:
      - current_aum
      - capital_tier
      - capacity_utilization (0..1)
      - soft/hard close active flags
      - liquidity_buckets (weights by bucket)
      - scaling_headroom
    """

    def __init__(
        self,
        aum_state: AUMState,
        tier_engine: CapitalTierEngine,
        soft_close: SoftCloseEngine,
        hard_close: HardCloseEngine,
        liquidity: LiquidityBuckets,
        scaling: ScalingImpactAnalyzer,
        allocator: Optional[AUMAwareAllocator] = None,
    ):
        self.aum_state = aum_state
        self.tier_engine = tier_engine
        self.soft_close = soft_close
        self.hard_close = hard_close
        self.liquidity = liquidity
        self.scaling = scaling
        self.allocator = allocator

    def build(
        self,
        positions: Dict[str, Dict[str, Any]] | None = None,
        expected_returns: Dict[str, float] | None = None,
        base_aum: float = 1_000_000.0,
        hurdle_rate: float = 0.0001,
    ) -> Dict[str, Any]:
        # current AUM
        current = float(self.aum_state.get_current_total())

        # capital tier
        tier = self.tier_engine.get_current_tier()
        tier_val = tier.value if tier is not None else None

        # capacity utilization: if allocator present, compute deploy budget vs sum of caps
        capacity_util = None
        if self.allocator is not None:
            exp = (
                expected_returns
                or {k: 1.0 for k in (positions or {}).keys()}
                or {"_nominal": 1.0}
            )
            alloc = self.allocator.allocate(exp, aum_total=current)
            deployed = sum(alloc.values())
            total_caps = 0.0
            for k in exp.keys():
                spec = self.allocator.capacity_specs.get(k)
                if spec:
                    total_caps += (
                        current
                        * spec.base_fraction_of_aum
                        * max(0.0, min(1.0, spec.scale_fn(current)))
                    )
                else:
                    total_caps += 0.01 * current
            if total_caps > 0:
                capacity_util = float(deployed) / float(total_caps)
            else:
                capacity_util = None

        # soft/hard close status
        soft_active = bool(self.soft_close.active)
        hard_active = bool(self.hard_close.active)

        # liquidity buckets
        bucket_weights = {}
        worst_bucket = None
        if positions is not None:
            bw, worst = self.liquidity.aggregate_portfolio(positions)
            bucket_weights = {b.name: w for b, w in bw.items()}
            worst_bucket = worst.name

        # scaling headroom: break-even AUM minus current
        hist_returns = [
            p.get("historical_return", 0.0) for p in (positions or {}).values()
        ]
        be = (
            self.scaling.compute_break_even(
                historical_returns=hist_returns,
                base_aum=base_aum,
                hurdle_rate=hurdle_rate,
            )
            if self.scaling is not None
            else None
        )
        headroom = None
        if be is not None:
            headroom = be - current

        return {
            "current_aum": current,
            "capital_tier": tier_val,
            "capacity_utilization": capacity_util,
            "soft_close_active": soft_active,
            "hard_close_active": hard_active,
            "liquidity_buckets": bucket_weights,
            "worst_bucket": worst_bucket,
            "scaling_break_even_aum": be,
            "scaling_headroom": headroom,
        }
