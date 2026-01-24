from __future__ import annotations

from statistics import mean, stdev
from typing import Any, Dict

from octa_core.strategy import StrategyInput, StrategyOutput, StrategySpec


def run_strategy(spec: StrategySpec, inp: StrategyInput, state: Dict) -> StrategyOutput:
    # volatility-scaled trend: compute short and long moving averages per symbol
    exposures: Dict[str, float] = {}
    rationale: Dict[str, Any] = {}
    for sym, prices in inp.prices.items():
        if len(prices) < 5:
            exposures[sym] = 0.0
            rationale[sym] = {"reason": "insufficient_data"}
            continue
        # short/long windows
        short_w = 3
        long_w = 5
        short_ma = mean(prices[-short_w:])
        long_ma = mean(prices[-long_w:])
        returns = [
            (prices[i + 1] - prices[i]) / prices[i] for i in range(len(prices) - 1)
        ]
        vol = stdev(returns) if len(returns) > 1 else 0.0
        # signal direction
        sig = 1.0 if short_ma > long_ma else -1.0 if short_ma < long_ma else 0.0
        scale = 1.0 / (vol * 100.0) if vol > 0 else 0.5
        raw = sig * scale
        # clamp to [-1,1] and apply risk budget
        val = max(-1.0, min(1.0, raw * spec.risk_budget))
        exposures[sym] = val
        rationale[sym] = {"short_ma": short_ma, "long_ma": long_ma, "vol": vol}

    # confidence proportional to average absolute exposure
    if exposures:
        conf = sum(abs(v) for v in exposures.values()) / len(exposures)
        conf = max(0.0, min(1.0, conf))
    else:
        conf = 0.0

    return StrategyOutput(exposures=exposures, confidence=conf, rationale=rationale)
