"""Correlation breakdown detector.

Deterministic rolling correlation with simple shrinkage and explainable outputs.
"""

from __future__ import annotations

import math
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


def _shrink_covariance(cov: np.ndarray) -> np.ndarray:
    """Apply a deterministic shrinkage toward a scalar identity prior.

    This is a simplified, deterministic alternative to Ledoit-Wolf.
    """
    n = cov.shape[0]
    var_avg = np.trace(cov) / n
    prior = np.eye(n) * var_avg

    off_diag = cov - np.diag(np.diag(cov))
    beta = np.sum(off_diag**2)
    denom = np.sum(cov**2)
    alpha = float(beta / denom) if denom > 0 else 0.0
    alpha = max(0.0, min(1.0, alpha))
    shrunk = alpha * prior + (1.0 - alpha) * cov
    return shrunk


def _cov_to_corr(cov: np.ndarray) -> np.ndarray:
    d = np.sqrt(np.diag(cov))
    denom = np.outer(d, d)
    with np.errstate(divide="ignore", invalid="ignore"):
        corr = cov / denom
    corr[denom == 0] = 0.0
    corr = np.clip(corr, -1.0, 1.0)
    return corr


def _avg_pairwise_corr(corr: np.ndarray) -> float:
    n = corr.shape[0]
    if n < 2:
        return 0.0
    iu = np.triu_indices(n, k=1)
    vals = corr[iu]
    return float(np.nanmean(vals))


def compute_rolling_corr(
    returns: pd.DataFrame, window: int = 60
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute a deterministic shrunk covariance and correlation over rolling windows.

    Returns (corr_last, corr_prev) where corr_prev is the correlation matrix for
    the immediately preceding window (if available) otherwise same as corr_last.
    """
    if returns.shape[0] < window:
        raise ValueError("not enough rows for requested window")

    data = returns.values
    # last window
    last = data[-window:]
    cov_last = np.cov(last, rowvar=False, bias=False)
    cov_last = _shrink_covariance(cov_last)
    corr_last = _cov_to_corr(cov_last)

    # previous window if available
    if returns.shape[0] >= 2 * window:
        prev = data[-2 * window : -window]
        cov_prev = np.cov(prev, rowvar=False, bias=False)
        cov_prev = _shrink_covariance(cov_prev)
        corr_prev = _cov_to_corr(cov_prev)
    else:
        corr_prev = corr_last.copy()

    cols = list(returns.columns)
    corr_last_df = pd.DataFrame(corr_last, index=cols, columns=cols)
    corr_prev_df = pd.DataFrame(corr_prev, index=cols, columns=cols)
    return corr_last_df, corr_prev_df


def correlation_metrics(
    corr_last: pd.DataFrame, corr_prev: pd.DataFrame
) -> Dict[str, float]:
    corr_last.shape[0]
    avg_last = _avg_pairwise_corr(corr_last.values)
    avg_prev = _avg_pairwise_corr(corr_prev.values)
    delta = avg_last - avg_prev
    jump_rate = float(delta)
    max_corr = float(np.nanmax(np.triu(corr_last.values, k=1)))
    return {
        "avg_pairwise": avg_last,
        "prev_avg": avg_prev,
        "delta": jump_rate,
        "max_pairwise": max_corr,
    }


def top_correlated_pairs(
    corr: pd.DataFrame, top_k: int = 5
) -> List[Tuple[str, str, float]]:
    n = corr.shape[0]
    if n < 2:
        return []
    iu = np.triu_indices(n, k=1)
    pairs = []
    cols = list(corr.columns)
    vals = corr.values[iu]
    entries = []
    for idx, (i, j) in enumerate(zip(iu[0], iu[1], strict=False)):
        entries.append((cols[i], cols[j], float(vals[idx])))
    for i, j, v in sorted(entries, key=lambda x: -abs(x[2])):
        pairs.append((i, j, v))
        if len(pairs) >= top_k:
            break
    return pairs


def normalize_score(x: float, low: float, high: float) -> float:
    if math.isfinite(x):
        if x <= low:
            return 0.0
        if x >= high:
            return 1.0
        return float((x - low) / (high - low))
    return 0.0


def detect_breakdown(
    returns: pd.DataFrame,
    window: int = 60,
    thresholds: Dict[str, float] | None = None,
) -> Dict:
    """Detect correlation breakdown and produce explainable outputs.

    thresholds keys (defaults):
      - avg_pairwise: 0.35
      - max_pairwise: 0.7
      - delta: 0.05

    Returns a dict:
      - score: 0..1 composite
      - metrics: dict of raw metrics
      - top_pairs: list of (asset_i, asset_j, corr)
      - recommended_compression: float in (0.1..1.0)
    """
    if thresholds is None:
        # Slightly more sensitive defaults to detect rapid correlation breakdowns
        thresholds = {"avg_pairwise": 0.20, "max_pairwise": 0.6, "delta": 0.02}

    corr_last, corr_prev = compute_rolling_corr(returns, window=window)
    metrics = correlation_metrics(corr_last, corr_prev)

    s_avg = normalize_score(metrics["avg_pairwise"], thresholds["avg_pairwise"], 0.8)
    s_max = normalize_score(metrics["max_pairwise"], thresholds["max_pairwise"], 0.95)
    s_delta = normalize_score(
        metrics["delta"], thresholds["delta"], thresholds["delta"] * 4 + 1e-9
    )

    # Weighted composite: prioritize average pairwise and delta
    score = float(np.clip(0.5 * s_avg + 0.3 * s_delta + 0.2 * s_max, 0.0, 1.0))

    top_pairs = top_correlated_pairs(corr_last, top_k=10)

    recommended_compression = max(0.1, 1.0 - 0.9 * score)

    return {
        "score": score,
        "metrics": metrics,
        "top_pairs": top_pairs,
        "recommended_compression": recommended_compression,
        "corr_matrix": corr_last,
    }
