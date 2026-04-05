"""Robustness features — signals designed to generalise under regime change.

These features measure risk-adjusted quality of returns rather than raw momentum,
making them less prone to IS/OOS degradation caused by overfitting to bull runs.
All inputs must be pre-shifted (shift(1) from caller) — leakage-safe.
"""
from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def build_robustness_features(
    close: pd.Series,
    ret: pd.Series,
    vol: pd.Series,
) -> Dict[str, pd.Series]:
    """Return robustness feature dict. All inputs are shift(1) from caller (leakage-safe)."""
    feats: Dict[str, pd.Series] = {}
    eps = 1e-9

    # 1. Volatility-normalised return: per-bar Sharpe-like score
    # Stable OOS because it adapts to the current vol regime rather than absolute return levels
    vol_safe = vol.clip(lower=eps)
    feats["rb_vol_normalized_ret"] = (ret / vol_safe).clip(-5.0, 5.0)

    # 2. Rolling drawdown depth: how far below the rolling 60-bar peak
    # Negative values: -0.1 = 10% below peak. Signal for drawdown recovery trades.
    roll_max = close.rolling(window=60, min_periods=10).max().clip(lower=eps)
    feats["rb_drawdown_depth"] = ((close - roll_max) / roll_max).clip(-1.0, 0.0)

    # 3. Return dispersion (IQR over 20 bars): width of the return distribution
    # Low IQR = stable/predictable returns; High IQR = erratic/regime-unstable
    q75 = ret.rolling(window=20, min_periods=5).quantile(0.75)
    q25 = ret.rolling(window=20, min_periods=5).quantile(0.25)
    feats["rb_ret_iqr_20"] = (q75 - q25).clip(lower=0.0)

    # 4. Calmar-proxy: rolling mean return / rolling max drawdown depth
    # Penalises strategies that look good in IS but suffer large drawdowns
    roll_mean_ret = ret.rolling(window=20, min_periods=5).mean()
    roll_dd = feats["rb_drawdown_depth"].rolling(window=20, min_periods=5).min().abs().clip(lower=eps)
    feats["rb_calmar_proxy_20"] = (roll_mean_ret / roll_dd).clip(-3.0, 3.0)

    return feats
