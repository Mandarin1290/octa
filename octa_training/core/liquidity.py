from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class LiquiditySnapshot:
    adv_usd: Optional[float]
    adv_shares: Optional[float]
    history_days: int
    lookback_days: int


def _daily_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Liquidity check requires DatetimeIndex")
    if df.index.tz is not None:
        idx = df.index.tz_convert("UTC")
    else:
        idx = df.index
    tmp = df.copy()
    tmp.index = idx

    out = pd.DataFrame(index=tmp.resample("1D").sum().index)
    if "close" in tmp.columns:
        out["close"] = tmp["close"].resample("1D").last()
    if "volume" in tmp.columns:
        out["volume"] = tmp["volume"].resample("1D").sum()
    return out.dropna(how="all")


def compute_liquidity_snapshot(df: pd.DataFrame, lookback_days: int = 20) -> LiquiditySnapshot:
    daily = _daily_ohlcv(df)
    history_days = int(daily.shape[0])

    adv_usd = None
    adv_shares = None

    if history_days == 0:
        return LiquiditySnapshot(adv_usd=None, adv_shares=None, history_days=0, lookback_days=lookback_days)

    lb = max(1, int(lookback_days))
    tail = daily.tail(lb)

    if "volume" in tail.columns:
        adv_shares = float(np.nanmean(tail["volume"].astype(float).values))

    if "close" in tail.columns and "volume" in tail.columns:
        dollar_vol = tail["close"].astype(float).values * tail["volume"].astype(float).values
        adv_usd = float(np.nanmean(dollar_vol))

    return LiquiditySnapshot(adv_usd=adv_usd, adv_shares=adv_shares, history_days=history_days, lookback_days=lb)


def passes_liquidity_filter(df: pd.DataFrame, cfg: Any) -> Tuple[bool, str, Dict[str, Any]]:
    """Return (passed, reason, details).

    Conservative/simple: daily ADV based on close*volume (USD proxy) and/or shares.
    The filter is only enforced when cfg.liquidity.enabled is True.
    """

    liq = getattr(cfg, "liquidity", None)
    if not liq or not getattr(liq, "enabled", False):
        return True, "disabled", {}

    snap = compute_liquidity_snapshot(df, lookback_days=getattr(liq, "adv_lookback_days", 20))
    details: Dict[str, Any] = {
        "adv_usd": snap.adv_usd,
        "adv_shares": snap.adv_shares,
        "history_days": snap.history_days,
        "lookback_days": snap.lookback_days,
        "min_adv_usd": getattr(liq, "min_adv_usd", None),
        "min_adv_shares": getattr(liq, "min_adv_shares", None),
        "min_history_days": getattr(liq, "min_history_days", 0),
    }

    min_hist = int(getattr(liq, "min_history_days", 0) or 0)
    if snap.history_days < min_hist:
        return False, "insufficient_history_days", details

    min_adv_usd = getattr(liq, "min_adv_usd", None)
    if min_adv_usd is not None and not (isinstance(min_adv_usd, float) and np.isnan(min_adv_usd)):
        if snap.adv_usd is None or snap.adv_usd < float(min_adv_usd):
            return False, "adv_usd_below_threshold", details

    min_adv_shares = getattr(liq, "min_adv_shares", None)
    if min_adv_shares is not None and not (isinstance(min_adv_shares, float) and np.isnan(min_adv_shares)):
        if snap.adv_shares is None or snap.adv_shares < float(min_adv_shares):
            return False, "adv_shares_below_threshold", details

    return True, "pass", details
