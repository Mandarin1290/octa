from __future__ import annotations

import math
from typing import Dict, List


def rolling_returns(prices: List[float]) -> List[float]:
    if not prices or len(prices) < 2:
        return []
    return [(prices[i + 1] - prices[i]) / prices[i] for i in range(len(prices) - 1)]


def pairwise_correlation(returns1: List[float], returns2: List[float]) -> float:
    # compute Pearson correlation for equal-length lists
    if not returns1 or not returns2:
        return 0.0
    n = min(len(returns1), len(returns2))
    r1 = returns1[-n:]
    r2 = returns2[-n:]
    mean1 = sum(r1) / n
    mean2 = sum(r2) / n
    num = sum((a - mean1) * (b - mean2) for a, b in zip(r1, r2, strict=False))
    den1 = math.sqrt(sum((a - mean1) ** 2 for a in r1))
    den2 = math.sqrt(sum((b - mean2) ** 2 for b in r2))
    if den1 == 0 or den2 == 0:
        return 0.0
    return num / (den1 * den2)


def correlation_matrix(prices: Dict[str, List[float]]) -> Dict[str, Dict[str, float]]:
    symbols = list(prices.keys())
    rets = {s: rolling_returns(prices[s]) for s in symbols}
    mat: Dict[str, Dict[str, float]] = {s: {} for s in symbols}
    for i, s1 in enumerate(symbols):
        for j, s2 in enumerate(symbols):
            if i == j:
                mat[s1][s2] = 1.0
            else:
                mat[s1][s2] = pairwise_correlation(rets[s1], rets[s2])
    return mat
