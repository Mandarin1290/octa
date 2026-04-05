"""Multi-timeframe and regime consistency features.

These proxy higher-timeframe signals using rolling windows on single-timeframe data.
All inputs must be pre-shifted (shift(1) from caller) — leakage-safe.
"""
from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def build_multiframe_features(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    ret: pd.Series,
) -> Dict[str, pd.Series]:
    """Return multi-timeframe feature dict. All inputs are shift(1) from caller (leakage-safe)."""
    feats: Dict[str, pd.Series] = {}
    eps = 1e-9

    # 1. Volatility regime: short-window vol vs long-window vol
    # Ratio > 1 = expanding vol (regime stress); < 1 = vol compression (range-bound)
    vol_5 = ret.rolling(window=5, min_periods=2).std()
    vol_60 = ret.rolling(window=60, min_periods=10).std().clip(lower=eps)
    feats["mf_vol_regime_ratio"] = (vol_5 / vol_60).clip(0.0, 5.0)

    # 2. Trend consistency: fraction of up-bars in last 10 bars
    # 0 = all down, 1 = all up. Near 0.5 = choppy/indecisive.
    up_bars = (ret > 0.0).astype(float)
    feats["mf_trend_consistency_10"] = up_bars.rolling(window=10, min_periods=3).mean()

    # 3. Range position: where current close sits within rolling 20-bar high-low range
    # Equivalent to %B for price alone (without BB width adjustment)
    roll_high = high.rolling(window=20, min_periods=5).max()
    roll_low = low.rolling(window=20, min_periods=5).min()
    roll_range = (roll_high - roll_low).clip(lower=eps)
    feats["mf_range_position_20"] = ((close - roll_low) / roll_range).clip(0.0, 1.0)

    # 4. Return autocorrelation (lag-1, 20-bar rolling)
    # Positive = momentum (returns tend to continue)
    # Negative = mean-reversion (returns tend to reverse)
    # Uses pandas vectorized rolling correlation — fast.
    feats["mf_ret_autocorr_20"] = (
        ret.rolling(window=20, min_periods=10).corr(ret.shift(1)).clip(-1.0, 1.0).fillna(0.0)
    )

    return feats
