"""Regime label assignment for regime-ensemble training (v0.1.0).

Priority: CRISIS > BEAR > BULL > NEUTRAL

Rules (applied in priority order):
  CRISIS: rolling_return_20d < -0.15 AND rolling_vol_20d > 2.0 * vol_252d_baseline
  BEAR:   rolling_return_20d < -0.05 AND rolling_vol_20d > 1.3 * vol_252d_baseline
  BULL:   rolling_return_20d > +0.05
  NEUTRAL: else

Requires ≥252 bars for a reliable vol_252d_baseline. Returns empty dict if insufficient data.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, List, Optional


REGIME_CRISIS = "crisis"
REGIME_BEAR = "bear"
REGIME_BULL = "bull"
REGIME_NEUTRAL = "neutral"

# Priority ordering: lower index = higher priority
_REGIME_PRIORITY = [REGIME_CRISIS, REGIME_BEAR, REGIME_BULL, REGIME_NEUTRAL]

# Minimum bars required for a reliable 252d vol baseline
_MIN_BARS_REQUIRED = 252


@dataclass
class RegimeLabelConfig:
    """Thresholds for regime classification."""
    crisis_return_threshold: float = -0.15
    crisis_vol_multiplier: float = 2.0
    bear_return_threshold: float = -0.05
    bear_vol_multiplier: float = 1.3
    bull_return_threshold: float = 0.05
    rolling_return_window: int = 20
    rolling_vol_window: int = 20
    baseline_vol_window: int = 252
    min_rows: Dict[str, int] = field(default_factory=lambda: {
        REGIME_BULL: 252,
        REGIME_BEAR: 126,
        REGIME_CRISIS: 63,
    })


def classify_regimes(
    df: pd.DataFrame,
    cfg: Optional[RegimeLabelConfig] = None,
    close_col: str = "close",
) -> pd.Series:
    """Assign a regime label to each bar in df.

    Parameters
    ----------
    df : DataFrame with DatetimeIndex, must contain `close_col`
    cfg : RegimeLabelConfig; defaults used if None
    close_col : column name for close prices

    Returns
    -------
    pd.Series of dtype str, index aligned to df.index
    Labels: 'crisis' | 'bear' | 'bull' | 'neutral'
    Empty Series if fewer than _MIN_BARS_REQUIRED rows.
    """
    if cfg is None:
        cfg = RegimeLabelConfig()

    if len(df) < _MIN_BARS_REQUIRED:
        return pd.Series(dtype=str)

    if close_col not in df.columns:
        raise KeyError(f"classify_regimes: column '{close_col}' not found in df")

    close = df[close_col].astype(float)

    # Returns
    returns = close.pct_change().fillna(0.0)

    # Rolling 20d cumulative return (sum of daily returns as proxy)
    rolling_ret = returns.rolling(window=cfg.rolling_return_window, min_periods=1).sum()

    # Rolling 20d volatility
    rolling_vol = returns.rolling(window=cfg.rolling_vol_window, min_periods=1).std().fillna(0.0)

    # 252d baseline volatility (trailing, so no look-ahead)
    baseline_vol = returns.rolling(window=cfg.baseline_vol_window, min_periods=cfg.baseline_vol_window).std()
    # For the first 252 bars, forward-fill using the expanding std (no future data)
    expanding_vol = returns.expanding(min_periods=20).std()
    baseline_vol = baseline_vol.fillna(expanding_vol).fillna(rolling_vol).fillna(0.0)

    # Classify in priority order
    labels = pd.Series(REGIME_NEUTRAL, index=df.index, dtype=str)

    # BULL (lowest priority among non-neutral)
    bull_mask = rolling_ret > cfg.bull_return_threshold
    labels[bull_mask] = REGIME_BULL

    # BEAR (overrides BULL)
    bear_mask = (
        (rolling_ret < cfg.bear_return_threshold)
        & (rolling_vol > cfg.bear_vol_multiplier * baseline_vol)
    )
    labels[bear_mask] = REGIME_BEAR

    # CRISIS (highest priority — overrides BEAR)
    crisis_mask = (
        (rolling_ret < cfg.crisis_return_threshold)
        & (rolling_vol > cfg.crisis_vol_multiplier * baseline_vol)
    )
    labels[crisis_mask] = REGIME_CRISIS

    return labels


def get_regime_splits(
    df: pd.DataFrame,
    labels: pd.Series,
    cfg: Optional[RegimeLabelConfig] = None,
) -> Dict[str, pd.DataFrame]:
    """Split df into per-regime sub-DataFrames, filtering regimes below min_rows.

    Parameters
    ----------
    df : full DataFrame (same index as labels)
    labels : Series from classify_regimes()
    cfg : RegimeLabelConfig for min_rows thresholds

    Returns
    -------
    dict mapping regime name → sub-DataFrame
    Only regimes meeting min_rows[regime] are included.
    """
    if cfg is None:
        cfg = RegimeLabelConfig()

    if labels.empty or len(df) == 0:
        return {}

    result: Dict[str, pd.DataFrame] = {}
    for regime in _REGIME_PRIORITY:
        mask = labels == regime
        subset = df.loc[mask]
        min_rows = cfg.min_rows.get(regime, 1)
        if len(subset) >= min_rows:
            result[regime] = subset

    return result


def regime_distribution(labels: pd.Series) -> Dict[str, float]:
    """Return fraction of bars in each regime (for shadow execution scoring).

    Parameters
    ----------
    labels : Series from classify_regimes()

    Returns
    -------
    dict mapping regime → fraction in [0, 1]
    """
    if labels.empty:
        return {r: 0.0 for r in _REGIME_PRIORITY}

    n = len(labels)
    counts = labels.value_counts()
    return {r: float(counts.get(r, 0)) / n for r in _REGIME_PRIORITY}
