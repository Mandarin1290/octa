"""Performance Stability Analyzer: rolling volatility, skew, kurtosis and stability score.

Links optionally to `AlphaDecayDetector` and `RegimeFitEngine` for combined monitoring.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _var(xs: List[float], ddof: int = 0) -> float:
    if not xs:
        return 0.0
    m = _mean(xs)
    s = sum((x - m) ** 2 for x in xs)
    denom = len(xs) - ddof
    return s / denom if denom > 0 else 0.0


def _std(xs: List[float]) -> float:
    return math.sqrt(_var(xs))


def _skew(xs: List[float]) -> float:
    n = len(xs)
    if n < 3:
        return 0.0
    m = _mean(xs)
    s = math.sqrt(_var(xs))
    if s == 0:
        return 0.0
    return sum(((x - m) / s) ** 3 for x in xs) / n


def _kurtosis(xs: List[float]) -> float:
    n = len(xs)
    if n < 4:
        return 0.0
    m = _mean(xs)
    s2 = _var(xs)
    if s2 == 0:
        return 0.0
    s = math.sqrt(s2)
    # excess kurtosis
    return sum(((x - m) / s) ** 4 for x in xs) / n - 3.0


@dataclass
class StabilityReport:
    stability_score: float
    vol_ratio: float
    skew_change: float
    kurtosis_change: float
    alpha_decay: Optional[Dict[str, Any]] = None
    regime_fit: Optional[Dict[str, Any]] = None


class PerformanceStabilityAnalyzer:
    """Analyzes returns for stability degradation.

    Methods:
    - `analyze(returns)` returns `StabilityReport` using baseline vs recent windows.
    - optional: pass `alpha_detector`, `regime_engine`, and `market_indicator` to augment report.
    """

    def __init__(self, baseline_window: int = 120, recent_window: int = 30):
        self.baseline_window = baseline_window
        self.recent_window = recent_window

    def _slice_windows(self, returns: List[float]):
        n = len(returns)
        if n == 0:
            return [], []
        bw = min(self.baseline_window, max(1, n - self.recent_window))
        rw = (
            min(self.recent_window, n - bw)
            if n - bw > 0
            else min(self.recent_window, n)
        )
        baseline = returns[:bw]
        recent = returns[-rw:] if rw > 0 else returns[-1:]
        return baseline, recent

    def analyze(
        self,
        returns: List[float],
        alpha_detector=None,
        regime_engine=None,
        market_indicator: Optional[List[float]] = None,
        latest_market_value: Optional[float] = None,
    ) -> StabilityReport:
        baseline, recent = self._slice_windows(returns)

        b_std = _std(baseline) if baseline else 0.0
        r_std = _std(recent) if recent else 0.0
        eps = 1e-12

        vol_ratio = (r_std / (b_std + eps)) if (b_std + eps) > 0 else float("inf")

        # changes in skew and kurtosis
        b_skew = _skew(baseline)
        r_skew = _skew(recent)
        skew_change = abs(r_skew - b_skew)

        b_kurt = _kurtosis(baseline)
        r_kurt = _kurtosis(recent)
        kurt_change = abs(r_kurt - b_kurt)

        # volatility score: log2 ratio mapping
        try:
            lr = math.log2(max(1e-12, vol_ratio))
        except Exception:
            lr = 0.0
        # map lr to [0,1] via sigmoid centered at 0.5
        vol_score = (
            1.0 / (1.0 + math.exp(-3.0 * (lr - 0.5))) if not math.isnan(lr) else 0.0
        )

        # normalize skew/kurtosis changes by their sampling variability
        nb = max(1, len(baseline))
        skew_sigma = math.sqrt(6.0 / nb)
        kurt_sigma = math.sqrt(24.0 / nb)
        skew_z = skew_change / (skew_sigma + eps)
        kurt_z = kurt_change / (kurt_sigma + eps)
        # map z-scores to [0,1] with a sigmoid centered at 1.0 (moderate change)
        avg_z = (skew_z + kurt_z) / 2.0
        moment_score = 1.0 / (1.0 + math.exp(-0.7 * (avg_z - 1.0)))

        # combine scores: weight volatility heavier
        stability_score = vol_score * 0.65 + moment_score * 0.35

        alpha_info = None
        if alpha_detector is not None:
            try:
                alpha_info = alpha_detector.detect_decay(returns)
                alpha_info = {
                    k: getattr(alpha_info, k)
                    for k in ("decay_score", "confidence", "changepoint_index")
                }
            except Exception:
                alpha_info = None

        regime_info = None
        if (
            regime_engine is not None
            and market_indicator is not None
            and latest_market_value is not None
        ):
            try:
                tags = regime_engine.tag_regimes(market_indicator)
                perf = regime_engine.performance_by_regime(returns, tags)
                score, cur, conf = regime_engine.compatibility_score(
                    latest_market_value, market_indicator, perf
                )
                regime_info = {
                    "compatibility_score": score,
                    "current_regime": cur,
                    "confidence": conf,
                }
            except Exception:
                regime_info = None

        # clamp
        stability_score = max(0.0, min(1.0, stability_score))

        return StabilityReport(
            stability_score=stability_score,
            vol_ratio=vol_ratio,
            skew_change=skew_change,
            kurtosis_change=kurt_change,
            alpha_decay=alpha_info,
            regime_fit=regime_info,
        )
