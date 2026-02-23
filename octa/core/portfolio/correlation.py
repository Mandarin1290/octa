
from __future__ import annotations

from typing import Mapping, Sequence

from dataclasses import dataclass
from statistics import mean, pstdev


def ewma_correlation_matrix(returns: Mapping[str, Sequence[float]]) -> dict[tuple[str, str], float]:
    if not returns:
        return {}
    try:
        return correlation_matrix(returns)
    except Exception:
        return {}


def _corr(a: Sequence[float], b: Sequence[float]) -> float:
    if len(a) != len(b) or len(a) < 2:
        return 0.0
    mean_a = mean(a)
    mean_b = mean(b)
    cov = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b))
    var_a = sum((x - mean_a) ** 2 for x in a)
    var_b = sum((y - mean_b) ** 2 for y in b)
    if var_a == 0 or var_b == 0:
        return 0.0
    return cov / (var_a ** 0.5 * var_b ** 0.5)


def correlation_matrix(returns: Mapping[str, Sequence[float]]) -> dict[tuple[str, str], float]:
    symbols = list(returns)
    matrix: dict[tuple[str, str], float] = {}
    for idx, sym_a in enumerate(symbols):
        for sym_b in symbols[idx:]:
            corr = _corr(returns[sym_a], returns[sym_b])
            matrix[(sym_a, sym_b)] = corr
            matrix[(sym_b, sym_a)] = corr
    return matrix


def cluster_exposure_score(matrix: Mapping[tuple[str, str], float], threshold: float = 0.7) -> float:
    if not matrix:
        return 0.0
    correlations = [value for key, value in matrix.items() if key[0] != key[1] and value >= threshold]
    if not correlations:
        return 0.0
    return mean(correlations)


def correlation_adjusted_exposure(correlation_score: float) -> float:
    return max(0.0, 1.0 - correlation_score)
