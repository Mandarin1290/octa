"""
Dedicated 1H intraday feature builder for US equity hourly bars.

Feature philosophy:
- Short windows only (≤ 14 bars = 14H ≈ 2 trading sessions) — no daily-scale lookback
- Session structure (hour of day, day of week, cyclic encodings)
- All price features normalized — ratios, z-scores, no raw price levels
- Volume relative to short-term baseline — no accumulating totals (no OBV)
- Gap features computed from bar-to-bar open/close differences

Leakage safety: all features use shift(1) — only prior-bar data is visible at prediction time.
"""
from __future__ import annotations

import logging
import math
from typing import Dict

import numpy as np
import pandas as pd

from octa.core.features.features import FeatureBuildResult

_LOG = logging.getLogger(__name__)

# US equity regular session in UTC: 9:30 AM ET = 14:30 UTC, 4:00 PM ET = 21:00 UTC.
# If parquet is tz-naive (stored in ET), open=9, close=15.
# We handle both by converting tz-aware UTC to ET when available.
_US_OPEN_HOUR_ET = 9
_US_CLOSE_HOUR_ET = 15
_US_OPEN_HOUR_UTC = 14
_US_CLOSE_HOUR_UTC = 21


def _rsi(series: pd.Series, window: int) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(com=window - 1, adjust=True, min_periods=1).mean()
    ma_down = down.ewm(com=window - 1, adjust=True, min_periods=1).mean()
    rs = ma_up / ma_down.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50)


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int) -> pd.Series:
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(com=window - 1, adjust=True, min_periods=1).mean()


def _stoch(high: pd.Series, low: pd.Series, close: pd.Series, k: int = 8, d: int = 3):
    lo_k = low.rolling(k, min_periods=1).min()
    hi_k = high.rolling(k, min_periods=1).max()
    denom = (hi_k - lo_k).replace(0, np.nan)
    stoch_k = 100 * (close - lo_k) / denom
    stoch_k = stoch_k.fillna(50)
    stoch_d = stoch_k.rolling(d, min_periods=1).mean()
    return stoch_k, stoch_d


def _add_session_features(
    X: pd.DataFrame,
    df: pd.DataFrame,
    close_prev: pd.Series,
    open_prev: pd.Series,
    high_prev: pd.Series,
    low_prev: pd.Series,
) -> pd.DataFrame:
    """Add intraday session-structure features to an existing feature matrix.

    Called from build_features() for sub-daily timeframes to augment the standard
    indicator set with time-of-day context and daily-open reference.

    All inputs are already shift(1)-adjusted (leakage-safe).
    """
    X = X.copy()

    # --- Timezone normalisation ---
    idx_for_session = df.index
    try:
        if df.index.tz is not None:
            idx_for_session = df.index.tz_convert("America/New_York")
    except Exception:
        pass

    hour_arr = np.array(idx_for_session.hour, dtype=np.int8)
    dow_arr = np.array(idx_for_session.dayofweek, dtype=np.int8)

    # Cyclic hour/dow encoding (already in X as "hour"/"minute" from build_features,
    # but those are raw integers; add the normalised cyclic variants).
    X["1h_hour_sin"] = np.sin(2 * math.pi * hour_arr / 24)
    X["1h_hour_cos"] = np.cos(2 * math.pi * hour_arr / 24)
    X["1h_dow_sin"] = np.sin(2 * math.pi * dow_arr / 5)
    X["1h_dow_cos"] = np.cos(2 * math.pi * dow_arr / 5)

    # Session position flags (ET session: open=9, close=15)
    X["1h_is_open_bar"] = pd.Series((hour_arr == _US_OPEN_HOUR_ET).astype(float), index=df.index)
    X["1h_is_close_bar"] = pd.Series((hour_arr == _US_CLOSE_HOUR_ET).astype(float), index=df.index)
    session_h = np.clip(hour_arr - _US_OPEN_HOUR_ET, 0, 7).astype(float)
    X["1h_session_hour"] = pd.Series(session_h, index=df.index)

    # Overnight gap: prior bar's open vs the bar before that's close
    # (both already in shift(1) space: open_prev=t-1 open, close_prev=t-1 close)
    prior_close2 = close_prev.shift(1)  # t-2 close
    gap_pct = ((open_prev - prior_close2) / prior_close2.replace(0, np.nan)).fillna(0).clip(-0.1, 0.1)
    X["1h_gap_pct"] = gap_pct

    # ATR(14) of prior bars for normalisation
    try:
        range_hl = (high_prev - low_prev).clip(lower=1e-10)
        tr = pd.concat([
            range_hl,
            (high_prev - close_prev.shift(1)).abs(),
            (low_prev - close_prev.shift(1)).abs(),
        ], axis=1).max(axis=1)
        atr14 = tr.ewm(com=13, adjust=True, min_periods=5).mean().replace(0, np.nan)
    except Exception:
        atr14 = None

    # Position relative to daily open (how far has current close moved from session open)
    try:
        _open_raw = df["open"].astype(float) if "open" in df.columns else close_prev * 0 + np.nan
        _idx_et = idx_for_session
        _date_s = pd.Series(np.array(_idx_et.date), index=df.index)
        _open_by_date = _open_raw.groupby(_date_s).transform("first")
        _daily_open_shifted = _open_by_date.shift(1).replace(0, np.nan)
        if atr14 is not None:
            X["1h_dist_from_daily_open"] = ((close_prev - _daily_open_shifted) / atr14).fillna(0).clip(-5, 5)
        X["1h_intraday_dir"] = np.sign(close_prev - _daily_open_shifted).fillna(0)
    except Exception:
        X["1h_dist_from_daily_open"] = 0.0
        X["1h_intraday_dir"] = 0.0

    return X


def build_features_1h(
    raw: pd.DataFrame,
    settings,
    asset_class: str,
    build_targets: bool = True,
    symbol: str = "",
) -> FeatureBuildResult:
    """Build leakage-safe intraday features from 1H OHLCV data.

    All features use prior-bar values (shift(1)) so no information from bar t
    is used to predict bar t+k.
    """
    df = raw.copy()

    # Ensure DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        for col in ("timestamp", "datetime", "date", "Date", "Timestamp"):
            if col in df.columns:
                df = df.set_index(col)
                break
        df.index = pd.to_datetime(df.index, errors="coerce")

    df = df.sort_index()
    if df.index.duplicated().any():
        df = df[~df.index.duplicated(keep="first")]

    n = len(df)
    close = df["close"].astype(float)
    open_ = df["open"].astype(float) if "open" in df.columns else close
    high = df["high"].astype(float) if "high" in df.columns else close
    low = df["low"].astype(float) if "low" in df.columns else close
    vol = df["volume"].astype(float) if "volume" in df.columns else pd.Series(np.nan, index=df.index)

    # Leakage-safe: shift all OHLCV by 1 so at bar t we only see data up to t-1
    c = close.shift(1)
    o = open_.shift(1)
    h = high.shift(1)
    lo = low.shift(1)
    v = vol.shift(1)

    has_volume = v.dropna().abs().sum() > 0

    X = pd.DataFrame(index=df.index)

    # -------------------------------------------------------------------------
    # 1. Session structure — cyclic encoding avoids artificial discontinuities
    # -------------------------------------------------------------------------
    # Normalise timezone: convert tz-aware UTC index to US/Eastern for session hours.
    idx_for_session = df.index
    try:
        if df.index.tz is not None:
            idx_for_session = df.index.tz_convert("America/New_York")
    except Exception:
        pass  # Keep original index if conversion fails

    hour_arr = np.array(idx_for_session.hour, dtype=np.int8)
    dow_arr = np.array(idx_for_session.dayofweek, dtype=np.int8)

    X["hour"] = pd.Series(hour_arr.astype(float), index=df.index)
    X["dow"] = pd.Series(dow_arr.astype(float), index=df.index)
    X["hour_sin"] = np.sin(2 * math.pi * hour_arr / 24)
    X["hour_cos"] = np.cos(2 * math.pi * hour_arr / 24)
    X["dow_sin"] = np.sin(2 * math.pi * dow_arr / 5)
    X["dow_cos"] = np.cos(2 * math.pi * dow_arr / 5)

    # Session position flags for US equity hours (9:30–16:00 ET)
    open_hour = _US_OPEN_HOUR_ET
    close_hour = _US_CLOSE_HOUR_ET
    X["is_open_bar"] = pd.Series((hour_arr == open_hour).astype(float), index=df.index)
    X["is_close_bar"] = pd.Series((hour_arr == close_hour).astype(float), index=df.index)
    # Hours into session (first bar = 0): clamp to 0–7 range
    session_h = np.clip(hour_arr - open_hour, 0, 7).astype(float)
    X["session_hour"] = pd.Series(session_h, index=df.index)

    # -------------------------------------------------------------------------
    # 2. Bar-level price action (prior bar)
    # -------------------------------------------------------------------------
    range_hl = (h - lo).clip(lower=1e-10)

    bar_body = c - o
    body_top = np.maximum(o.values, c.values)
    body_bot = np.minimum(o.values, c.values)
    body_top_s = pd.Series(body_top, index=df.index)
    body_bot_s = pd.Series(body_bot, index=df.index)

    X["bar_ret"] = (bar_body / o.replace(0, np.nan)).fillna(0).clip(-0.1, 0.1)
    X["bar_body_ratio"] = (bar_body.abs() / range_hl).clip(0, 1).fillna(0.5)
    X["upper_shadow"] = ((h - body_top_s) / range_hl).clip(0, 1).fillna(0)
    X["lower_shadow"] = ((body_bot_s - lo) / range_hl).clip(0, 1).fillna(0)
    X["close_in_range"] = ((c - lo) / range_hl).clip(0, 1).fillna(0.5)
    X["bar_direction"] = np.sign(bar_body).fillna(0)

    # Garman-Klass single-bar volatility estimate
    try:
        ratio_hl = (h / lo).replace([np.inf, -np.inf], np.nan)
        ratio_co = (c / o).replace([np.inf, -np.inf], np.nan)
        gk = np.sqrt(
            0.5 * np.log(ratio_hl) ** 2 - (2 * np.log(2) - 1) * np.log(ratio_co) ** 2
        )
        X["gk_vol"] = gk.fillna(0).clip(0, 0.1)
    except Exception:
        X["gk_vol"] = 0.0

    # -------------------------------------------------------------------------
    # 3. Overnight / session gap (detect large open-to-prior-close moves)
    # -------------------------------------------------------------------------
    # gap = prior bar's open vs bar before that's close  (both already shifted)
    prior_close = c.shift(1)  # close of bar t-2 (from bar t's perspective)
    gap_raw = o - prior_close  # how much prior bar gapped on open
    gap_pct = (gap_raw / prior_close.replace(0, np.nan)).fillna(0).clip(-0.1, 0.1)
    X["gap_pct"] = gap_pct

    # Gap fill: what fraction of the gap did the prior bar's close fill?
    gap_denom = gap_raw.replace(0, np.nan)
    gap_fill = ((c - prior_close) / gap_denom).fillna(0).clip(-3, 3)
    X["gap_fill_ratio"] = gap_fill

    # -------------------------------------------------------------------------
    # 4. ATR (two windows: fast=4 and standard=14)
    # -------------------------------------------------------------------------
    atr_4 = _atr(h, lo, c, window=4)
    atr_14 = _atr(h, lo, c, window=14)
    X["atr_4"] = atr_4.clip(lower=0)
    X["atr_14"] = atr_14.clip(lower=0)

    # Volatility regime: fast ATR vs slow ATR — expanding > 1.0, contracting < 1.0
    X["vol_ratio_atr"] = (atr_4 / atr_14.replace(0, np.nan)).fillna(1.0).clip(0.2, 5.0)

    # Range of prior bar normalized by ATR(14)
    X["range_vs_atr14"] = (range_hl / atr_14.replace(0, np.nan)).fillna(1.0).clip(0, 5)

    # -------------------------------------------------------------------------
    # 5. Momentum across multiple horizons
    #    Short (intraday): 1, 2, 4, 6, 12 bars (1H–12H)
    #    Multi-session:    32 bars ≈ 5 days, 65 bars ≈ 10 days, 130 bars ≈ 20 days
    # -------------------------------------------------------------------------
    for nbars in (1, 2, 4, 6, 12, 32, 65, 130):
        ret = c.pct_change(nbars, fill_method=None).fillna(0).clip(-0.30, 0.30)
        X[f"ret_{nbars}"] = ret

    # Multi-session trend (normalized — not price level)
    # sma_ratio: close vs N-bar SMA, normalised by ATR(14).  Captures position within trend.
    atr_denom_early = atr_14.replace(0, np.nan)
    for win in (20, 50):
        sma_w = c.rolling(win, min_periods=win // 4).mean()
        X[f"sma_ratio_{win}"] = ((c - sma_w) / atr_denom_early).fillna(0).clip(-5, 5)

    # Trend slope (10-bar log-return slope) — captures directional drift
    try:
        _log_c = np.log(c.replace(0, np.nan).where(c > 0, np.nan).ffill().bfill())
        _idx10 = np.arange(10)
        _A = np.vstack([_idx10, np.ones(10)]).T

        def _slope10(arr: np.ndarray) -> float:
            try:
                return float(np.linalg.lstsq(_A, arr, rcond=None)[0][0])
            except Exception:
                return 0.0

        X["trend_slope_10"] = _log_c.rolling(10, min_periods=5).apply(_slope10, raw=True).fillna(0).clip(-0.02, 0.02)
        X["trend_dir_10"] = np.sign(X["trend_slope_10"]).fillna(0)
    except Exception:
        X["trend_slope_10"] = 0.0
        X["trend_dir_10"] = 0.0

    # -------------------------------------------------------------------------
    # 6. Oscillators — windows calibrated for 1H bars
    # -------------------------------------------------------------------------
    # RSI (14H window — matches typical daily RSI, stable)
    # RSI-8 removed: too short, predicts mean-reversion (bad for 6-12H continuation targets)
    X["rsi_14"] = _rsi(c, 14)

    # Bollinger Bands (20H window — medium session, ~3 trading days)
    # BB-10 removed: too short, mean-reversion signal
    bb_ma_20 = c.rolling(20, min_periods=4).mean()
    bb_std_20 = c.rolling(20, min_periods=4).std()
    bb_upper_20 = bb_ma_20 + 2 * bb_std_20
    bb_lower_20 = bb_ma_20 - 2 * bb_std_20
    X["bb_width_20"] = ((bb_upper_20 - bb_lower_20) / bb_ma_20.replace(0, np.nan)).fillna(0).clip(0, 0.5)
    X["bb_pct_b_20"] = (
        (c - bb_lower_20) / (bb_upper_20 - bb_lower_20).replace(0, np.nan)
    ).fillna(0.5).clip(-0.5, 1.5)

    # CCI and stochastic removed: both mean-reversion oriented for short windows

    # MACD (12/26/9 EMA) — normalized by ATR(14) to avoid price-level memorization
    # At 1H: 12-bar EMA = ~2 sessions, 26-bar EMA = ~4 days → captures multi-day trend
    ema_fast_macd = c.ewm(span=12, adjust=False, min_periods=6).mean()
    ema_slow_macd = c.ewm(span=26, adjust=False, min_periods=13).mean()
    macd_line = ema_fast_macd - ema_slow_macd
    macd_sig = macd_line.ewm(span=9, adjust=False, min_periods=5).mean()
    macd_hist_s = macd_line - macd_sig
    X["macd_norm"] = (macd_line / atr_denom_early).fillna(0).clip(-5, 5)
    X["macd_sig_norm"] = (macd_sig / atr_denom_early).fillna(0).clip(-5, 5)
    X["macd_hist_norm"] = (macd_hist_s / atr_denom_early).fillna(0).clip(-5, 5)

    # ADX — trend strength indicator (14-bar)
    dm_plus = (h - h.shift(1)).clip(lower=0)
    dm_minus = (lo.shift(1) - lo).clip(lower=0)
    tr_adx = pd.concat([h - lo, (h - c.shift(1)).abs(), (lo - c.shift(1)).abs()], axis=1).max(axis=1)
    tr_adx_sm = tr_adx.ewm(com=13, adjust=True, min_periods=5).mean().replace(0, np.nan)
    di_plus = 100 * (dm_plus.ewm(com=13, adjust=True, min_periods=5).mean() / tr_adx_sm)
    di_minus = 100 * (dm_minus.ewm(com=13, adjust=True, min_periods=5).mean() / tr_adx_sm)
    dx = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan)
    X["adx_14"] = dx.ewm(com=13, adjust=True, min_periods=5).mean().fillna(25).clip(0, 100)

    # Williams %R (14-bar)
    hi_14 = h.rolling(14, min_periods=5).max()
    lo_14 = lo.rolling(14, min_periods=5).min()
    X["williams_r_14"] = (-100 * (hi_14 - c) / (hi_14 - lo_14).replace(0, np.nan)).fillna(-50).clip(-100, 0)

    # -------------------------------------------------------------------------
    # 7. Mean-reversion distance (close vs short SMAs, ATR-normalized)
    # -------------------------------------------------------------------------
    sma_6 = c.rolling(6, min_periods=2).mean()
    sma_12 = c.rolling(12, min_periods=2).mean()

    X["dist_sma_6"] = ((c - sma_6) / atr_denom_early).fillna(0).clip(-5, 5)
    X["dist_sma_12"] = ((c - sma_12) / atr_denom_early).fillna(0).clip(-5, 5)

    # EMA(6) / EMA(12) ratio — trend direction (1.0 = neutral, >1.0 = uptrend)
    ema_6 = c.ewm(span=6, adjust=False, min_periods=2).mean()
    ema_12 = c.ewm(span=12, adjust=False, min_periods=2).mean()
    X["ema_fast_slow_ratio"] = (ema_6 / ema_12.replace(0, np.nan)).fillna(1.0).clip(0.9, 1.1)

    # -------------------------------------------------------------------------
    # 8a. Daily open reference — where is the current bar relative to today's open?
    #     This captures the intraday trend from session start.
    #     Daily open = first bar's open for each date (ET timezone-aware).
    # -------------------------------------------------------------------------
    try:
        # Use the ET-converted index for date grouping
        _idx_et = idx_for_session
        _date_s = pd.Series(_idx_et.date, index=df.index)
        # Daily open = prior bar's open for the first bar of each ET date
        _open_by_date = open_.groupby(_date_s).transform("first")  # raw open (not shifted)
        # Shift by 1: we see today's open only from the 2nd bar onwards
        _daily_open_shifted = _open_by_date.shift(1).replace(0, np.nan)
        # dist_from_daily_open: how far is prior close from the day's open, in ATR units
        X["dist_from_daily_open"] = ((c - _daily_open_shifted) / atr_denom_early).fillna(0).clip(-5, 5)
        # Intraday direction: sign of cumulative move from open so far
        X["intraday_dir"] = np.sign(c - _daily_open_shifted).fillna(0)
    except Exception:
        X["dist_from_daily_open"] = 0.0
        X["intraday_dir"] = 0.0

    # -------------------------------------------------------------------------
    # 8b. Realized volatility (short windows)
    # -------------------------------------------------------------------------
    ret_raw = c.pct_change(1, fill_method=None).fillna(0)
    X["realized_vol_6"] = ret_raw.rolling(6, min_periods=2).std().fillna(0).clip(0, 0.1)
    X["realized_vol_12"] = ret_raw.rolling(12, min_periods=2).std().fillna(0).clip(0, 0.1)
    # Vol-of-vol regime (expanding vs contracting vol)
    rv6 = X["realized_vol_6"]
    rv12 = X["realized_vol_12"]
    X["vol_ratio_rv"] = (rv6 / rv12.replace(0, np.nan)).fillna(1.0).clip(0.2, 5.0)

    # -------------------------------------------------------------------------
    # 8c. Long-horizon regime context (multi-session, 65-130H ≈ 10-20 trading days)
    #
    # SHORT features (atr_4/14, realized_vol_6/12) detect spikes within a session.
    # LONG features below detect STRUCTURAL regime shifts across multiple WF folds.
    # These are the features most likely to improve WF fold consistency by teaching
    # the model when its own signal quality is structurally different.
    # -------------------------------------------------------------------------

    # 1. Volatility regime z-score: ATR(14) vs its 65-bar EMA and std-dev.
    #    z > +1.5 → high-vol regime (signal quality degrades; model should reduce conviction)
    #    z < -1.0 → low-vol compression (breakout potential; trend features more reliable)
    #    65H ≈ 10 trading days: sufficient to capture multi-day vol regimes.
    atr14_ma65 = atr_14.rolling(65, min_periods=20).mean().replace(0, np.nan)
    atr14_std65 = atr_14.rolling(65, min_periods=20).std().replace(0, 1e-8)
    X["regime_vol_z"] = ((atr_14 - atr14_ma65) / atr14_std65).fillna(0).clip(-3, 3)

    # 2. ADX percentile over 65 bars: where is current trend strength vs recent history?
    #    Low percentile → trend weaker than usual → mean-reversion more likely
    #    High percentile → unusually strong trend → momentum continuation more reliable
    adx_ser = X["adx_14"]
    adx_min65 = adx_ser.rolling(65, min_periods=20).min()
    adx_max65 = adx_ser.rolling(65, min_periods=20).max()
    adx_range65 = (adx_max65 - adx_min65).replace(0, np.nan)
    X["adx_pct_65"] = ((adx_ser - adx_min65) / adx_range65).fillna(0.5).clip(0, 1)

    # 3. Momentum acceleration: short-term (6H) minus scaled medium-term (32H) momentum.
    #    Positive → momentum building (intraday trend strengthening vs multi-day)
    #    Negative → momentum fading (intraday reverting vs multi-day direction)
    _ret6 = c.pct_change(6, fill_method=None).fillna(0)
    _ret32 = c.pct_change(32, fill_method=None).fillna(0)
    X["mom_accel"] = (_ret6 - _ret32 * (6.0 / 32.0)).clip(-0.10, 0.10)

    # 4. Realized vol trend: is realized vol increasing or decreasing?
    #    Sign of the 12-bar slope of realized_vol_6.
    #    Positive → vol expanding (regime shift in progress)
    #    Negative → vol compressing (regime stabilizing)
    rv6_ser = X["realized_vol_6"]
    rv6_change = rv6_ser.diff(12).fillna(0)
    X["vol_trend_dir"] = np.sign(rv6_change).fillna(0)

    # 5. Range compression ratio: current bar range vs 65-bar baseline.
    #    < 0.5 → narrow bars (consolidation / regime compression before breakout)
    #    > 2.0 → wide bars (high event vol; reversal risk)
    range_ma65 = range_hl.rolling(65, min_periods=20).mean().replace(0, np.nan)
    X["range_regime_65"] = (range_hl / range_ma65).fillna(1.0).clip(0, 5)

    # -------------------------------------------------------------------------
    # 9. Volume features (only if volume is meaningful)
    # -------------------------------------------------------------------------
    if has_volume:
        vol_clean = v.replace(0, np.nan)
        vol_ma_5 = vol_clean.rolling(5, min_periods=2).mean().replace(0, np.nan)
        vol_ma_20 = vol_clean.rolling(20, min_periods=5).mean().replace(0, np.nan)
        X["vol_ratio_5"] = (vol_clean / vol_ma_5).fillna(1.0).clip(0, 10)
        X["vol_ratio_20"] = (vol_clean / vol_ma_20).fillna(1.0).clip(0, 10)
        X["vol_trend"] = (X["vol_ratio_5"] / X["vol_ratio_20"].replace(0, np.nan)).fillna(1.0).clip(0.1, 10)

        # Volume-price alignment: positive = volume supports price direction
        X["vol_price_align"] = (X["vol_ratio_5"] * X["bar_direction"]).clip(-10, 10)

        # VWAP deviation (ATR-normalized)
        try:
            vwap_num = (((h + lo + c) / 3) * v.replace(0, np.nan)).rolling(12, min_periods=2).sum()
            vwap_den = v.replace(0, np.nan).rolling(12, min_periods=2).sum().replace(0, np.nan)
            vwap_12 = vwap_num / vwap_den
            X["vwap_dev_12"] = ((c - vwap_12) / atr_denom_early).fillna(0).clip(-5, 5)
        except Exception:
            X["vwap_dev_12"] = 0.0
    else:
        for col in ("vol_ratio_5", "vol_ratio_20", "vol_trend", "vol_price_align", "vwap_dev_12"):
            X[col] = 0.0

    # -------------------------------------------------------------------------
    # 10. AltData (FRED macro and other offline sources) — required by pipeline
    # -------------------------------------------------------------------------
    # The pipeline checks altdata_enabled from features_res.meta["altdat"].
    # We must call the sidecar here to populate this meta correctly.
    # FRED daily features (forwarded to 1H bars) provide macro regime context.
    altdat_meta = None
    try:
        from octa.core.data.sources.altdata.sidecar import try_run as _altdat_try_run
        altdat_df, altdat_meta = _altdat_try_run(
            bars_df=df, settings=settings, asset_class=str(asset_class or "stock")
        )
        if isinstance(altdat_df, pd.DataFrame) and not altdat_df.empty:
            altdat_df = altdat_df.reindex(X.index)
            X = X.join(altdat_df, how="left")
    except Exception as _e:
        altdat_meta = {"enabled": True, "status": "ERROR", "error": str(_e)}

    # Deterministic source-presence features (keep feature dimension stable)
    try:
        _src_meta = (altdat_meta or {}).get("sources") if isinstance(altdat_meta, dict) else None
        if isinstance(_src_meta, dict):
            for _src_name in sorted(str(k) for k in _src_meta.keys()):
                _sraw = _src_meta.get(_src_name) or {}
                _sstatus = str(_sraw.get("status", "")).strip().upper()
                _present = 1.0 if _sstatus in {"OK", "SUCCESS"} else 0.0
                X[f"altdat_source_{_src_name}_present"] = _present
    except Exception:
        pass

    # -------------------------------------------------------------------------
    # 11. Drop rows where all features are NaN
    # -------------------------------------------------------------------------
    X = X.reindex(sorted(X.columns), axis=1)
    feature_cols = [c_ for c_ in X.columns if not X[c_].isna().all()]
    X = X[feature_cols]

    # -------------------------------------------------------------------------
    # 12. Targets (multi-horizon binary classification)
    # -------------------------------------------------------------------------
    feat_cfg = None
    try:
        feat_cfg = getattr(settings, "features", None)
    except Exception:
        pass
    if not isinstance(feat_cfg, dict):
        feat_cfg = {}

    if not build_targets:
        X_clean = X.loc[X.notna().any(axis=1)]
        _meta_base: Dict = {"timeframe": "1H", "n_features": len(X_clean.columns)}
        if altdat_meta is not None:
            _meta_base["altdat"] = altdat_meta
            _meta_base["altdata_enabled"] = bool(altdat_meta.get("enabled", False))
        return FeatureBuildResult(X=X_clean, y_dict={}, meta=_meta_base)

    horizons = feat_cfg.get("horizons", getattr(settings, "horizons", [6, 12]))
    try:
        horizons = [int(h_) for h_ in horizons]
    except Exception:
        horizons = [6, 12]

    y_dict: Dict[str, pd.Series] = {}
    for h_ in horizons:
        fwd = (df["close"].shift(-h_) / df["close"]) - 1.0
        _safe_close = df["close"].where(df["close"] > 0, np.nan).ffill().bfill()
        y_reg = np.log(_safe_close.shift(-h_)) - np.log(_safe_close)
        y_cls = (fwd > 0).astype(int)
        y_dict[f"y_reg_{h_}"] = y_reg
        y_dict[f"y_cls_{h_}"] = y_cls

    target_df = pd.concat([s.rename(k) for k, s in y_dict.items()], axis=1)
    valid_mask = ~target_df.isna().any(axis=1)
    feature_nonnull = X.notna().any(axis=1)
    keep_idx = valid_mask & feature_nonnull

    X_clean = X.loc[keep_idx]
    y_clean = {k: s.loc[keep_idx] for k, s in y_dict.items()}

    meta: Dict = {
        "timeframe": "1H",
        "n_features": len(X_clean.columns),
        "n_samples": len(X_clean),
        "horizons": horizons,
        "has_volume": has_volume,
    }
    if altdat_meta is not None:
        meta["altdat"] = altdat_meta
        meta["altdata_enabled"] = bool(altdat_meta.get("enabled", False))

    _LOG.debug(
        "build_features_1h: %d bars → %d samples × %d features (horizons=%s)",
        n, len(X_clean), len(X_clean.columns), horizons,
    )

    return FeatureBuildResult(X=X_clean, y_dict=y_clean, meta=meta)
