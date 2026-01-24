"""Regime fit engine: measures strategy ↔ market regime compatibility.

Deterministic, rule-based regime tagging and per-regime performance aggregation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Tuple


def _mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _var(xs: List[float], ddof: int = 0) -> float:
    if not xs:
        return 0.0
    m = _mean(xs)
    s = sum((x - m) ** 2 for x in xs)
    denom = len(xs) - ddof
    return s / denom if denom > 0 else 0.0


def _normal_cdf(x: float) -> float:
    import math

    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _two_sided_z_pvalue(
    mean1: float, mean2: float, var1: float, var2: float, n1: int, n2: int
) -> float:
    denom = math.sqrt((var1 / max(1, n1)) + (var2 / max(1, n2)))
    if denom == 0.0:
        return 1.0
    z = (mean2 - mean1) / denom
    p = 2.0 * (1.0 - _normal_cdf(abs(z)))
    return max(0.0, min(1.0, p))


@dataclass
class RegimeStats:
    mean: float
    std: float
    count: int


class RegimeFitEngine:
    """Engine to tag regimes and assess strategy performance per regime.

    Methods:
    - `tag_regimes(market_indicator)`: deterministic tertile buckets 'LOW', 'MID', 'HIGH'
    - `performance_by_regime(strategy_returns, regimes)`: per-regime mean/std/count
    - `compatibility_score(latest_market_value, market_indicator, perf_by_regime)`: score 0..1 for current regime
    - `deterioration_alert(perf_by_regime, overall_returns, alpha=0.05, threshold_std=0.5)`: flags deterioration when regime mean significantly below overall
    """

    def tag_regimes(self, market_indicator: List[float]) -> List[str]:
        # deterministic tertile thresholds
        if not market_indicator:
            return []
        sorted_vals = sorted(market_indicator)
        n = len(sorted_vals)
        q1 = sorted_vals[max(0, int(n * 0.3333) - 1)]
        q2 = sorted_vals[max(0, int(n * 0.6666) - 1)]

        tags = []
        for v in market_indicator:
            if v <= q1:
                tags.append("LOW")
            elif v <= q2:
                tags.append("MID")
            else:
                tags.append("HIGH")
        return tags

    def performance_by_regime(
        self, strategy_returns: List[float], regimes: List[str]
    ) -> Dict[str, RegimeStats]:
        groups: Dict[str, List[float]] = {}
        for r, tag in zip(strategy_returns, regimes, strict=False):
            groups.setdefault(tag, []).append(r)

        out: Dict[str, RegimeStats] = {}
        for tag, vals in groups.items():
            m = _mean(vals)
            sd = math.sqrt(_var(vals))
            out[tag] = RegimeStats(mean=m, std=sd, count=len(vals))
        return out

    def compatibility_score(
        self,
        latest_market_value: float,
        market_indicator: List[float],
        perf_by_regime: Dict[str, RegimeStats],
    ) -> Tuple[float, str, float]:
        # determine current regime deterministically
        tags = self.tag_regimes(market_indicator)
        if not tags:
            return 0.0, "UNKNOWN", 0.0
        # find tag for latest_market_value using same thresholds
        sorted_vals = sorted(market_indicator)
        n = len(sorted_vals)
        q1 = sorted_vals[max(0, int(n * 0.3333) - 1)]
        q2 = sorted_vals[max(0, int(n * 0.6666) - 1)]
        if latest_market_value <= q1:
            cur = "LOW"
        elif latest_market_value <= q2:
            cur = "MID"
        else:
            cur = "HIGH"

        # score based on mean performance in current regime relative to other regimes
        if not perf_by_regime:
            return 0.0, cur, 0.0
        means = {k: v.mean for k, v in perf_by_regime.items()}
        # if regime not observed for this strategy, score 0
        if cur not in means:
            return 0.0, cur, 0.0

        min_mean = min(means.values())
        max_mean = max(means.values())
        if max_mean == min_mean:
            score = 1.0 if means[cur] >= 0 else 0.5
        else:
            score = (means[cur] - min_mean) / (max_mean - min_mean)

        # confidence = function of count in regime
        cnt = perf_by_regime[cur].count
        confidence = min(1.0, cnt / 100.0)
        return float(max(0.0, min(1.0, score))), cur, float(confidence)

    def deterioration_alert(
        self,
        perf_by_regime: Dict[str, RegimeStats],
        overall_returns: List[float],
        alpha: float = 0.05,
        threshold_std: float = 0.5,
    ) -> List[str]:
        alerts: List[str] = []
        overall_mean = _mean(overall_returns)
        overall_var = _var(overall_returns)
        overall_std = math.sqrt(overall_var)

        for tag, stats in perf_by_regime.items():
            # if regime mean is substantially below overall_mean by threshold_std * overall_std
            if stats.count < 3:
                continue
            if stats.mean < overall_mean - threshold_std * overall_std:
                # test significance
                p = _two_sided_z_pvalue(
                    overall_mean,
                    stats.mean,
                    overall_var,
                    stats.std**2,
                    len(overall_returns),
                    stats.count,
                )
                if p < alpha:
                    alerts.append(f"REGIME_{tag}_DETERIORATION:p={p:.4f}")
        return alerts
