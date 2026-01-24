from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Tuple


class Bucket(Enum):
    T0 = "T0"
    T5 = "T+5"
    T20 = "T+20"
    ILLIQUID = "ILLIQUID"


@dataclass
class AssetLiquidity:
    asset_id: str
    liquidity_days: float  # estimated days to liquidate
    illiquid_flag: bool = False
    metadata: Dict[str, Any] | None = None


class LiquidityBuckets:
    """Classifies assets into liquidity buckets and aggregates portfolio liquidity.

    Asset classification prefers explicit `illiquid_flag`, then `liquidity_days` thresholds:
      - <=1 day -> T0
      - <=5 days -> T+5
      - <=20 days -> T+20
      - >20 days -> Illiquid

    Stress adjustments multiply `liquidity_days` by (1 + stress_factor), downgrading buckets.
    """

    def __init__(self, thresholds: Dict[str, float] | None = None):
        t = thresholds or {"t0": 1.0, "t5": 5.0, "t20": 20.0}
        self.t0 = float(t.get("t0", 1.0))
        self.t5 = float(t.get("t5", 5.0))
        self.t20 = float(t.get("t20", 20.0))

    def classify_asset(self, asset: AssetLiquidity) -> Bucket:
        if asset.illiquid_flag:
            return Bucket.ILLIQUID
        d = float(asset.liquidity_days)
        if d <= self.t0:
            return Bucket.T0
        if d <= self.t5:
            return Bucket.T5
        if d <= self.t20:
            return Bucket.T20
        return Bucket.ILLIQUID

    def aggregate_portfolio(
        self, positions: Dict[str, Dict[str, Any]]
    ) -> Tuple[Dict[Bucket, float], Bucket]:
        """positions: {asset_id: {weight: float, liquidity_days: float, illiquid_flag: bool}}

        Returns (bucket_weights, worst_bucket)
        """
        bucket_weights: Dict[Bucket, float] = {b: 0.0 for b in Bucket}
        for aid, pos in positions.items():
            asset = AssetLiquidity(
                asset_id=aid,
                liquidity_days=pos.get("liquidity_days", 9999.0),
                illiquid_flag=bool(pos.get("illiquid_flag", False)),
            )
            b = self.classify_asset(asset)
            w = float(pos.get("weight", 0.0))
            bucket_weights[b] += w

        # determine worst-case bucket with any weight
        order = [Bucket.ILLIQUID, Bucket.T20, Bucket.T5, Bucket.T0]
        worst = Bucket.T0
        for b in order:
            if bucket_weights.get(b, 0.0) > 0.0:
                worst = b
                break

        return bucket_weights, worst

    def stress_adjusted_aggregate(
        self, positions: Dict[str, Dict[str, Any]], stress_factor: float
    ) -> Tuple[Dict[Bucket, float], Bucket]:
        """Apply stress_factor (>=0) which multiplies liquidity_days by (1+stress_factor) and recompute aggregation."""
        stressed_positions = {}
        for aid, pos in positions.items():
            ld = float(pos.get("liquidity_days", 9999.0))
            ill = bool(pos.get("illiquid_flag", False))
            stressed_ld = ld * (1.0 + float(stress_factor))
            stressed_positions[aid] = {
                **pos,
                "liquidity_days": stressed_ld,
                "illiquid_flag": ill,
            }

        return self.aggregate_portfolio(stressed_positions)
