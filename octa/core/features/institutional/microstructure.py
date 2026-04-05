"""Microstructure features — proxy signals for order flow and liquidity.

All inputs must be pre-shifted (shift(1) from caller) so no look-ahead leakage.
"""
from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def build_microstructure_features(
    close: pd.Series,
    open_: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
    has_usable_volume: bool = True,
) -> Dict[str, pd.Series]:
    """Return microstructure feature dict. All inputs are shift(1) from caller (leakage-safe)."""
    feats: Dict[str, pd.Series] = {}
    eps = 1e-9

    hl_range = (high - low).clip(lower=eps)

    # 1. Close position in bar: where price settled within the bar's range (0=low, 1=high)
    # Strong indicator of order-flow imbalance — buyers dominated when close > 0.7
    feats["ms_close_position"] = ((close - low) / hl_range).clip(0.0, 1.0)

    # 2. Bar body efficiency: |open-close| vs high-low range
    # Low ratio = indecision; high ratio = directional conviction
    body = (close - open_).abs()
    feats["ms_bar_body_ratio"] = (body / hl_range).clip(0.0, 1.0)

    # 3. Typical price deviation from short-term mean
    # Captures micro mean-reversion tendency
    typical = (high + low + close) / 3.0
    typical_ma = typical.rolling(window=10, min_periods=3).mean()
    typical_std = typical.rolling(window=10, min_periods=3).std().clip(lower=eps)
    feats["ms_typical_z"] = ((typical - typical_ma) / typical_std).clip(-4.0, 4.0)

    # 4. Amihud illiquidity z-score: |return| / volume (only when volume is meaningful)
    # Higher = less liquid; z-score normalizes across different volume scales
    if has_usable_volume:
        ret = close.pct_change(fill_method=None).fillna(0.0)
        vol_safe = volume.where(volume > 0, np.nan).ffill().bfill().clip(lower=eps)
        amihud = ret.abs() / vol_safe
        am_mean = amihud.rolling(window=60, min_periods=10).mean()
        am_std = amihud.rolling(window=60, min_periods=10).std().clip(lower=eps)
        feats["ms_illiquidity_z"] = ((amihud - am_mean) / am_std).clip(-4.0, 4.0)

    return feats
