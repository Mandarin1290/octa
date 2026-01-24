from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class Regime:
    volatility: str  # low / normal / high
    trend: str  # up / down / range
    correlation_stress: str  # normal / elevated
    metrics: Dict[str, float]


class RegimeDetector:
    """Deterministic, explainable regime detector.

    Rules (transparent):
    - Volatility: compute short_vol and long_vol (std of log returns). If short > 1.5*long -> high;
      if short < 0.7*long -> low; else normal.
    - Trend: linear slope (ordinary least squares) on recent prices. If slope_norm > thresh -> up;
      if < -thresh -> down; else range. slope_norm = slope / mean_price to make threshold scale-free.
    - Correlation stress: compute mean pairwise Pearson correlation of returns across the universe.
      If mean_corr > 0.6 -> elevated else normal.

    All computations use only data up to index `idx` (no lookahead).
    """

    def __init__(
        self,
        short_window: int = 20,
        long_window: int = 100,
        trend_window: int = 30,
        vol_high_mult: float = 1.5,
        vol_low_mult: float = 0.7,
        trend_thresh: float = 0.001,
        corr_thresh: float = 0.6,
    ):
        self.short_window = short_window
        self.long_window = long_window
        self.trend_window = trend_window
        self.vol_high_mult = vol_high_mult
        self.vol_low_mult = vol_low_mult
        self.trend_thresh = trend_thresh
        self.corr_thresh = corr_thresh

    @staticmethod
    def _log_returns(prices: List[float]) -> List[float]:
        if len(prices) < 2:
            return []
        res = []
        for i in range(1, len(prices)):
            if prices[i - 1] <= 0 or prices[i] <= 0:
                res.append(0.0)
            else:
                res.append(math.log(prices[i] / prices[i - 1]))
        return res

    @staticmethod
    def _std(x: List[float]) -> float:
        if len(x) < 2:
            return 0.0
        return statistics.pstdev(x)

    @staticmethod
    def _mean_pairwise_corr(series: List[List[float]]) -> float:
        # series: list of return series (equal length)
        n = len(series)
        if n < 2:
            return 0.0
        L = len(series[0])
        if L < 2:
            return 0.0

        # compute Pearson correlation for each pair
        def corr(a: List[float], b: List[float]) -> float:
            ma = statistics.mean(a)
            mb = statistics.mean(b)
            ca = [x - ma for x in a]
            cb = [x - mb for x in b]
            denom = math.sqrt(sum(x * x for x in ca) * sum(y * y for y in cb))
            if denom == 0:
                return 0.0
            return sum(x * y for x, y in zip(ca, cb, strict=False)) / denom

        total = 0.0
        count = 0
        for i in range(n):
            for j in range(i + 1, n):
                total += corr(series[i], series[j])
                count += 1
        return total / count if count else 0.0

    @staticmethod
    def _ols_slope(y: List[float]) -> float:
        # simple OLS slope against time indices 0..n-1
        n = len(y)
        if n < 2:
            return 0.0
        x_mean = (n - 1) / 2.0
        y_mean = statistics.mean(y)
        num = sum((i - x_mean) * (yi - y_mean) for i, yi in enumerate(y))
        den = sum((i - x_mean) ** 2 for i in range(n))
        if den == 0:
            return 0.0
        return num / den

    def detect_at_index(self, prices: Dict[str, List[float]], idx: int) -> Regime:
        """Detect regime at a given index (0-based). Uses only historical data up to idx (inclusive)."""
        # prepare slices
        sliced: Dict[str, List[float]] = {}
        for sym, series in prices.items():
            if idx < 0:
                sliced[sym] = []
            else:
                sliced[sym] = series[: idx + 1]

        # volatility: compute log-returns for each symbol and aggregate short/long vol as mean across universe
        short_rs = []
        long_rs = []
        for s in sliced.values():
            if len(s) < 2:
                continue
            # short
            short_slice = s[-self.short_window :]
            long_slice = s[-self.long_window :]
            short_ret = self._log_returns(short_slice)
            long_ret = self._log_returns(long_slice)
            short_rs.append(self._std(short_ret))
            long_rs.append(self._std(long_ret))

        mean_short_vol = statistics.mean(short_rs) if short_rs else 0.0
        mean_long_vol = statistics.mean(long_rs) if long_rs else 0.0

        vol_label = "normal"
        if mean_long_vol > 0 and mean_short_vol > self.vol_high_mult * mean_long_vol:
            vol_label = "high"
        elif mean_long_vol > 0 and mean_short_vol < self.vol_low_mult * mean_long_vol:
            vol_label = "low"

        # trend: use the first symbol's recent prices by default (deterministic selection: sorted keys)
        symbols = sorted(sliced.keys())
        trend_label = "range"
        slope = 0.0
        primary = []
        if symbols:
            primary = sliced[symbols[0]]
            recent = primary[-self.trend_window :]
            slope = self._ols_slope(recent)
            mean_price = statistics.mean(recent) if recent else 0.0
            slope_norm = slope / mean_price if mean_price else 0.0
            if slope_norm > self.trend_thresh:
                trend_label = "up"
            elif slope_norm < -self.trend_thresh:
                trend_label = "down"

        # correlation stress: compute mean pairwise corr across returns over short window
        ret_matrix = []
        for s in sliced.values():
            window = s[-self.short_window :]
            r = self._log_returns(window)
            if len(r) >= 2:
                ret_matrix.append(r)
        mean_corr = self._mean_pairwise_corr(ret_matrix)
        corr_label = "elevated" if mean_corr > self.corr_thresh else "normal"

        metrics = {
            "mean_short_vol": mean_short_vol,
            "mean_long_vol": mean_long_vol,
            "slope": slope,
            "slope_norm": (
                slope / statistics.mean(primary[-self.trend_window :])
                if symbols and primary
                else 0.0
            ),
            "mean_corr": mean_corr,
        }

        return Regime(
            volatility=vol_label,
            trend=trend_label,
            correlation_stress=corr_label,
            metrics=metrics,
        )


__all__ = ["RegimeDetector", "Regime"]
