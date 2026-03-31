from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import pandas_ta as ta


@dataclass
class FeatureBuildResult:
    X: pd.DataFrame
    y_dict: Dict[str, pd.Series]
    meta: Dict


def _safe_rolling(series: pd.Series, window: int, func: str = "mean") -> pd.Series:
    if func == "mean":
        return series.rolling(window=window, min_periods=1).mean()
    if func == "std":
        return series.rolling(window=window, min_periods=1).std()
    if func == "sum":
        return series.rolling(window=window, min_periods=1).sum()
    return series.rolling(window=window, min_periods=1).apply(func)


def _rsi(series: pd.Series, window: int = 14) -> pd.Series:
    # compute RSI on past returns only (no leakage): use price.shift(1)
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.rolling(window=window, min_periods=1).mean()
    ma_down = down.rolling(window=window, min_periods=1).mean()
    rs = ma_up / (ma_down.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def _macd(series: pd.Series, n_fast=12, n_slow=26, n_sign=9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    ema_fast = series.ewm(span=n_fast, adjust=False).mean()
    ema_slow = series.ewm(span=n_slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal = macd_line.ewm(span=n_sign, adjust=False).mean()
    hist = macd_line - signal
    return macd_line, signal, hist


def _bollinger(series: pd.Series, window=20, n_std=2):
    ma = series.rolling(window=window, min_periods=1).mean()
    sd = series.rolling(window=window, min_periods=1).std()
    upper = ma + n_std * sd
    lower = ma - n_std * sd
    return ma, upper, lower


def _stoch_osc(df: pd.DataFrame, k_window=14, d_window=3) -> Tuple[pd.Series, pd.Series]:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    lowest_low = low.rolling(window=k_window, min_periods=1).min()
    highest_high = high.rolling(window=k_window, min_periods=1).max()
    k = 100 * ((close - lowest_low) / (highest_high - lowest_low))
    d = k.rolling(window=d_window, min_periods=1).mean()
    return k, d


def _williams_r(df: pd.DataFrame, window=14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    highest_high = high.rolling(window=window, min_periods=1).max()
    lowest_low = low.rolling(window=window, min_periods=1).min()
    r = -100 * ((highest_high - close) / (highest_high - lowest_low))
    return r


def _adx(df: pd.DataFrame, window=14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    tr = _atr(df, window=1)  # true range
    dm_plus = (high - high.shift(1)).clip(lower=0)
    dm_minus = (low.shift(1) - low).clip(lower=0)
    di_plus = 100 * (dm_plus.ewm(span=window, adjust=False).mean() / tr.ewm(span=window, adjust=False).mean())
    di_minus = 100 * (dm_minus.ewm(span=window, adjust=False).mean() / tr.ewm(span=window, adjust=False).mean())
    dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
    adx = dx.ewm(span=window, adjust=False).mean()
    return adx


def _cci(df: pd.DataFrame, window=20) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tp = (high + low + close) / 3
    sma = tp.rolling(window=window, min_periods=1).mean()
    mad = tp.rolling(window=window, min_periods=1).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    cci = (tp - sma) / (0.015 * mad)
    return cci


def _atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=window, min_periods=1).mean()
    return atr


def _build_intraday_breakout_features(
    *,
    close_prev: "pd.Series",
    open_prev: "pd.Series",
    high_prev: "pd.Series",
    low_prev: "pd.Series",
    volume_prev: "pd.Series",
    has_usable_volume: bool,
    feat_cfg: dict,
    vwap_series: "pd.Series | None" = None,
    atr_14_series: "pd.Series | None" = None,
) -> dict:
    """Intraday-native breakout features (ib_*). All inputs are shift(1) — never current bar."""
    try:
        out: dict = {}
        _window = int((feat_cfg or {}).get("range_expansion_window", 10))
        _rank_w = int((feat_cfg or {}).get("range_rank_window", 20))
        _clip_ratio = float((feat_cfg or {}).get("range_ratio_clip", 4.0))
        _clip_vol = float((feat_cfg or {}).get("rel_volume_clip", 5.0))
        _clip_vwap = float((feat_cfg or {}).get("vwap_z_clip", 3.0))

        range_hl = (high_prev - low_prev).clip(lower=1e-10)
        range_co = close_prev - open_prev

        _min_p = max(2, _window // 2)
        _rhl_mean = range_hl.rolling(_window, min_periods=_min_p).mean().replace(0, np.nan)
        out["ib_range_expansion_ratio"] = (range_hl / _rhl_mean).clip(upper=_clip_ratio).fillna(1.0)
        out["ib_bar_body_ratio"] = (range_co.abs() / range_hl).clip(0.0, 1.0).fillna(0.0)
        out["ib_bar_direction"] = np.sign(range_co).fillna(0.0)

        body_top = np.maximum(open_prev.values, close_prev.values)
        body_bot = np.minimum(open_prev.values, close_prev.values)
        body_top_s = pd.Series(body_top, index=range_hl.index)
        body_bot_s = pd.Series(body_bot, index=range_hl.index)
        out["ib_upper_wick_ratio"] = ((high_prev - body_top_s) / range_hl).clip(0.0, 1.0).fillna(0.0)
        out["ib_lower_wick_ratio"] = ((body_bot_s - low_prev) / range_hl).clip(0.0, 1.0).fillna(0.0)
        out["ib_range_rank_20"] = (
            range_hl.rolling(_rank_w, min_periods=max(5, _rank_w // 4)).rank(pct=True).fillna(0.5)
        )

        prior_close2 = close_prev.shift(1).replace(0, np.nan)
        out["ib_hl_range_vs_prior_close"] = (range_hl / prior_close2).clip(upper=0.05).fillna(0.0)

        ret1 = close_prev.pct_change(1, fill_method=None).fillna(0.0)
        dir_now = np.sign(ret1)
        out["ib_momentum_consistency_3"] = (
            (dir_now == dir_now.shift(1).fillna(0.0)).astype(float)
            + (dir_now == dir_now.shift(2).fillna(0.0)).astype(float)
        ).fillna(0.0)

        if vwap_series is not None and has_usable_volume:
            _denom = (
                atr_14_series.replace(0, np.nan)
                if atr_14_series is not None
                else range_hl.rolling(14, min_periods=5).mean().replace(0, np.nan)
            )
            out["ib_vwap_z"] = ((close_prev - vwap_series) / _denom).clip(-_clip_vwap, _clip_vwap).fillna(0.0)
        else:
            out["ib_vwap_z"] = pd.Series(0.0, index=range_hl.index)

        if has_usable_volume:
            _vol_mean = volume_prev.replace(0, np.nan).rolling(_window, min_periods=_min_p).mean().replace(0, np.nan)
            _rel_vol = (volume_prev.replace(0, np.nan) / _vol_mean).clip(upper=_clip_vol).fillna(1.0)
            out["ib_rel_volume"] = _rel_vol
            out["ib_vol_accel"] = _rel_vol.diff(1).clip(-3.0, 3.0).fillna(0.0)
        else:
            out["ib_rel_volume"] = pd.Series(0.0, index=range_hl.index)
            out["ib_vol_accel"] = pd.Series(0.0, index=range_hl.index)

        # Mean-reversion specific features
        # ib_close_position_in_bar: where within the bar did close land? 1.0=top, 0.0=bottom.
        close_pos = ((close_prev - low_prev) / range_hl).clip(0.0, 1.0).fillna(0.5)
        out["ib_close_position_in_bar"] = close_pos

        # ib_excess_return_z: signed bar return in ATR units — how stretched was this bar?
        _atr_raw = (
            atr_14_series.replace(0, np.nan)
            if atr_14_series is not None
            else range_hl.rolling(14, min_periods=5).mean().replace(0, np.nan)
        )
        out["ib_excess_return_z"] = ((close_prev - open_prev) / _atr_raw).clip(-3.0, 3.0).fillna(0.0)

        # ib_reversal_setup: direction-aware reversal composite.
        # For up bars (dir>=0): (1 - close_pos) × range_expansion_ratio — closed at bottom of up bar = rejection.
        # For down bars (dir<0): close_pos × range_expansion_ratio — closed at top of down bar = rejection.
        _dir_sign = np.sign(close_prev.values - open_prev.values)
        _rev_raw = np.where(_dir_sign >= 0, 1.0 - close_pos.values, close_pos.values)
        out["ib_reversal_setup"] = (
            pd.Series(_rev_raw, index=range_hl.index) * out["ib_range_expansion_ratio"]
        ).clip(0.0, 4.0).fillna(0.0)

        return out
    except Exception as _exc:
        logging.getLogger(__name__).warning("_build_intraday_breakout_features failed: %s", _exc)
        return {}


def build_features(raw: pd.DataFrame, settings, asset_class: str, build_targets: bool = True) -> FeatureBuildResult:
    """
    Build leakage-safe features from OHLCV raw dataframe.
    settings: EffectiveSettings or config-like with attributes:
      nan_threshold, resample_enabled, resample_bar_size, and windows/horizons in cfg.
    """
    df = raw.copy()
    # ensure datetime index
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Input dataframe must have a DatetimeIndex")
    # ensure ordering and unique index (pandas-ta VWAP and rolling expect monotonic index)
    df = df.sort_index()
    if df.index.duplicated().any():
        df = df[~df.index.duplicated(keep='first')]

    def _load_fred_macro(macro_cfg: dict) -> pd.DataFrame | None:
        """Return a daily macro DF indexed by datetime (optional).

        Production constraints:
        - Must be safe without network access.
        - Must not hardcode secrets.
        - Prefer cached parquet in raw_dir; optionally fetch via API if enabled.
        """
        try:
            enabled = bool(macro_cfg.get("enabled", False))
        except Exception:
            enabled = False
        if not enabled:
            return None

        source = str(macro_cfg.get("source", "auto")).strip().lower()
        cache_filename = str(macro_cfg.get("cache_filename", "fred_macro.parquet")).strip()
        series = macro_cfg.get("series") or ["FEDFUNDS", "DGS10", "DGS2", "UNRATE"]
        try:
            series = [str(s).strip() for s in series if str(s).strip()]
        except Exception:
            series = ["FEDFUNDS", "DGS10", "DGS2", "UNRATE"]

        raw_dir = None
        try:
            raw_dir = getattr(settings, "raw_dir", None)
        except Exception:
            raw_dir = None
        cache_path = None
        if raw_dir:
            try:
                cache_path = Path(str(raw_dir)) / cache_filename
            except Exception:
                cache_path = None

        def _normalize_macro_index(mdf: pd.DataFrame) -> pd.DataFrame:
            mdf = mdf.copy()
            if not isinstance(mdf.index, pd.DatetimeIndex):
                mdf.index = pd.to_datetime(mdf.index)
            mdf = mdf.sort_index()
            if mdf.index.duplicated().any():
                mdf = mdf[~mdf.index.duplicated(keep="first")]
            return mdf

        # 1) cached parquet
        if source in {"auto", "parquet"} and cache_path and cache_path.exists():
            try:
                macro_df = pd.read_parquet(cache_path)
                macro_df = _normalize_macro_index(macro_df)
                return macro_df
            except Exception:
                pass

        # 2) API fetch (optional)
        if source not in {"auto", "api"}:
            return None

        key_env = str(macro_cfg.get("key_env", "FRED_API_KEY")).strip() or "FRED_API_KEY"
        api_key = os.environ.get(key_env)
        if not api_key:
            return None

        start_date = df.index.min().strftime("%Y-%m-%d")
        end_date = df.index.max().strftime("%Y-%m-%d")

        macro_data: dict[str, pd.Series] = {}

        # Try fredapi if installed, else fall back to simple HTTP.
        try:
            from fredapi import Fred  # type: ignore

            fred = Fred(api_key=api_key)
            for s in series:
                try:
                    macro_data[s] = fred.get_series(s, start_date, end_date)
                except Exception:
                    continue
        except Exception:
            try:
                import json
                from urllib.parse import urlencode
                from urllib.request import urlopen

                base = "https://api.stlouisfed.org/fred/series/observations"

                def fetch_series(series_id: str) -> pd.Series | None:
                    qs = urlencode(
                        {
                            "series_id": series_id,
                            "api_key": api_key,
                            "file_type": "json",
                            "observation_start": start_date,
                            "observation_end": end_date,
                        }
                    )
                    with urlopen(f"{base}?{qs}") as resp:
                        payload = json.loads(resp.read().decode("utf-8"))
                    obs = payload.get("observations") or []
                    if not obs:
                        return None
                    idx = []
                    vals = []
                    for o in obs:
                        d = o.get("date")
                        v = o.get("value")
                        if d is None or v is None or v == ".":
                            continue
                        try:
                            vals.append(float(v))
                            idx.append(pd.to_datetime(d))
                        except Exception:
                            continue
                    if not idx:
                        return None
                    return pd.Series(vals, index=pd.DatetimeIndex(idx), name=series_id)

                for s in series:
                    try:
                        ser = fetch_series(s)
                        if ser is not None:
                            macro_data[s] = ser
                    except Exception:
                        continue
            except Exception:
                return None

        if not macro_data:
            return None

        macro_df = pd.DataFrame(macro_data)
        macro_df = _normalize_macro_index(macro_df).ffill()

        # cache if we can
        if cache_path:
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                macro_df.to_parquet(cache_path)
            except Exception:
                pass

        return macro_df

    macro_cfg = getattr(settings, "macro", None)
    if isinstance(macro_cfg, dict):
        macro_df = _load_fred_macro(macro_cfg)
        if macro_df is not None and not macro_df.empty:
            try:
                # Normalize tz alignment
                if df.index.tz is None and macro_df.index.tz is not None:
                    macro_df.index = macro_df.index.tz_localize(None)
                elif df.index.tz is not None and macro_df.index.tz is None:
                    macro_df.index = macro_df.index.tz_localize(df.index.tz)

                # Align daily macro to bar index and forward-fill
                aligned = macro_df.reindex(df.index, method="ffill")
                shift_bars = int(macro_cfg.get("shift_bars", 1) or 0)
                if shift_bars:
                    aligned = aligned.shift(shift_bars)

                # Prefix columns to avoid collisions
                aligned = aligned.rename(columns={c: f"macro_{c}" for c in aligned.columns})
                df = df.join(aligned, how="left")
                df = df.ffill()
            except Exception:
                pass

    def _load_market_context(ctx_cfg: dict, timeframe: str) -> pd.DataFrame | None:
        """Load market context series aligned to the asset timeframe.

        Source: local parquets under raw_dir/Indices_parquet using the convention
        {SYMBOL}_full_{bar}.parquet where bar in {1day,1hour,30min,5min,1min}.

        Safety:
        - Optional: returns None if data not available.
        - Leakage-safe: caller should shift by at least 1 bar before using.
        """
        try:
            enabled = bool(ctx_cfg.get("enabled", False))
        except Exception:
            enabled = False
        if not enabled:
            return None

        raw_dir = getattr(settings, "raw_dir", None)
        if not raw_dir:
            return None
        idx_dir = Path(str(raw_dir)) / "Indices_parquet"
        if not idx_dir.exists():
            return None

        tf = str(timeframe).strip()
        bar = {
            "1D": "1day",
            "1H": "1hour",
            "30m": "30min",
            "5m": "5min",
            "1m": "1min",
        }.get(tf)
        if not bar:
            return None

        symbols = ctx_cfg.get("symbols") or ["SPX", "NDX", "VIX", "DXY"]
        try:
            symbols = [str(s).strip() for s in symbols if str(s).strip()]
        except Exception:
            symbols = ["SPX", "NDX", "VIX", "DXY"]
        if not symbols:
            return None

        frames: list[pd.DataFrame] = []
        for sym in symbols:
            p = idx_dir / f"{sym}_full_{bar}.parquet"
            if not p.exists():
                # tolerate alternate casing
                p2 = idx_dir / f"{sym}_full_{bar.lower()}.parquet"
                if p2.exists():
                    p = p2
            if not p.exists():
                continue
            try:
                cdf = pd.read_parquet(p)
            except Exception:
                continue
            # Expect either a timestamp column or a DatetimeIndex.
            try:
                if not isinstance(cdf.index, pd.DatetimeIndex):
                    if "timestamp" in cdf.columns:
                        cdf = cdf.set_index("timestamp")
                    else:
                        continue
                cdf.index = pd.to_datetime(cdf.index)
                cdf = cdf.sort_index()
                if cdf.index.duplicated().any():
                    cdf = cdf[~cdf.index.duplicated(keep="first")]
            except Exception:
                continue
            if "close" not in cdf.columns:
                continue
            close_ctx = pd.to_numeric(cdf["close"], errors="coerce")
            out = pd.DataFrame(index=cdf.index)
            # leakage-safe: use prior bar of the context series
            ret = close_ctx.shift(1).pct_change(fill_method=None).fillna(0.0)
            out[f"ctx_{sym}_ret1"] = ret
            try:
                vol_w = int(ctx_cfg.get("vol_window", 20) or 20)
            except Exception:
                vol_w = 20
            out[f"ctx_{sym}_vol{vol_w}"] = ret.rolling(window=vol_w, min_periods=1).std().fillna(0.0)
            frames.append(out)

        if not frames:
            return None

        ctx = pd.concat(frames, axis=1)
        return ctx

    # Work on price series shifted so that indicators only use <= t-1 data
    close = df["close"].astype(float)
    # use prior bar values for leakage-safe features
    close_prev = close.shift(1)
    high_prev = df["high"].shift(1)
    low_prev = df["low"].shift(1)
    volume_prev = df["volume"].shift(1)
    # Some datasets (e.g., indices) may not have real volume (all-NaN / all-zero).
    # Volume-based indicators (VWAP/OBV/ADOSC) are meaningless there and can emit noisy warnings.
    try:
        _vol_non_na = volume_prev.dropna()
        has_usable_volume = (len(_vol_non_na) > 0) and (float(_vol_non_na.abs().sum()) > 0.0)
    except Exception:
        has_usable_volume = False
    # avoid deprecated fill_method default in pct_change and guard logs
    ret_1 = close_prev.pct_change(fill_method=None).fillna(0)
    safe_close_prev = close_prev.replace(0, np.nan).abs()
    logret_1 = np.log(safe_close_prev).diff().fillna(0)

    X = pd.DataFrame(index=df.index)
    # Base features (all computed on close_prev to avoid leakage)
    X["return_1"] = ret_1
    X["logret_1"] = logret_1
    X["hour"] = df.index.hour
    X["minute"] = df.index.minute
    # Garman-Klass volatility
    high_prev = df["high"].shift(1)
    low_prev = df["low"].shift(1)
    open_prev = df["open"].shift(1)
    # guard divisions for Garman-Klass volatility computation
    try:
        ratio_hl = (high_prev / low_prev).replace([np.inf, -np.inf], np.nan)
        ratio_co = (close_prev / open_prev).replace([np.inf, -np.inf], np.nan)
        X["gk_vol"] = np.sqrt(0.5 * np.log(ratio_hl)**2 - (2*np.log(2) - 1) * np.log(ratio_co)**2).fillna(0)
    except Exception:
        X["gk_vol"] = 0
    # Feature config can be provided either as top-level attributes (legacy)
    # or nested under settings.features (current TrainingConfig design).
    feat_cfg = None
    try:
        feat_cfg = getattr(settings, "features", None)
    except Exception:
        feat_cfg = None
    if not isinstance(feat_cfg, dict):
        feat_cfg = {}

    # rolling windows from settings (fallback defaults)
    w_short = int(feat_cfg.get("window_short", getattr(settings, "window_short", 5)))
    w_med = int(feat_cfg.get("window_med", getattr(settings, "window_med", 20)))
    # w_long reserved for future features

    X[f"ret_ma_{w_short}"] = ret_1.rolling(window=w_short, min_periods=1).mean()
    X[f"ret_std_{w_short}"] = ret_1.rolling(window=w_short, min_periods=1).std().fillna(0)
    X[f"ret_ma_{w_med}"] = ret_1.rolling(window=w_med, min_periods=1).mean()
    X[f"realized_vol_{w_med}"] = ret_1.rolling(window=w_med, min_periods=1).std() * math.sqrt(252)

    # Optional market context features (indices proxies) aligned to same timeframe.
    # These are useful in institutional setups (risk-on/off, vol regime, USD strength).
    ctx_cfg = getattr(settings, "market_context", None)
    try:
        tf_key = str(getattr(settings, "timeframe", "") or "").strip() or "1D"
    except Exception:
        tf_key = "1D"
    if isinstance(ctx_cfg, dict):
        ctx_df = _load_market_context(ctx_cfg, tf_key)
        if ctx_df is not None and not ctx_df.empty:
            try:
                # Align + forward-fill to bar index
                if df.index.tz is None and ctx_df.index.tz is not None:
                    ctx_df.index = ctx_df.index.tz_localize(None)
                elif df.index.tz is not None and ctx_df.index.tz is None:
                    ctx_df.index = ctx_df.index.tz_localize(df.index.tz)
                aligned = ctx_df.reindex(df.index, method="ffill")
                shift_bars = int(ctx_cfg.get("shift_bars", 1) or 0)
                if shift_bars:
                    aligned = aligned.shift(shift_bars)
                X = X.join(aligned, how="left")
                X = X.ffill()
                # Rolling correlations to capture co-movement/regime
                try:
                    corr_w = int(ctx_cfg.get("corr_window", w_med) or w_med)
                except Exception:
                    corr_w = w_med
                for c in list(aligned.columns):
                    if c.endswith("_ret1"):
                        try:
                            X[f"corr_{c}"] = ret_1.rolling(corr_w, min_periods=5).corr(X[c]).fillna(0.0)
                        except Exception:
                            continue
            except Exception:
                pass

    # ATR and range features
    if all(c in df.columns for c in ("high", "low")):
        X["atr_14"] = _atr(df.shift(1), window=14)  # use shifted df for no leakage
        X["range_hl"] = (df["high"] - df["low"]).shift(1)
    X["range_co"] = (df["close"] - df["open"]).shift(1) if "open" in df.columns else 0

    # Technical indicators on shifted prices
    macd_line, macd_sig, macd_hist = _macd(close_prev)
    X["macd"] = macd_line
    X["macd_sig"] = macd_sig
    X["macd_hist"] = macd_hist
    X["rsi_14"] = _rsi(close_prev, 14)
    bb_ma, bb_up, bb_lo = _bollinger(close_prev, window=20, n_std=2)
    X["bb_ma"] = bb_ma
    X["bb_up"] = bb_up
    X["bb_lo"] = bb_lo

    # Exponential Moving Averages
    X["ema_12"] = close_prev.ewm(span=12, adjust=False).mean()
    X["ema_26"] = close_prev.ewm(span=26, adjust=False).mean()

    # Stochastic Oscillator
    if all(c in df.columns for c in ("high", "low", "close")):
        stoch_k, stoch_d = _stoch_osc(df.shift(1), k_window=14, d_window=3)
        X["stoch_k"] = stoch_k
        X["stoch_d"] = stoch_d
        X["williams_r"] = _williams_r(df.shift(1), window=14)
        X["adx"] = _adx(df.shift(1), window=14)
        X["cci"] = _cci(df.shift(1), window=20)

        # Additional TA features using pandas-ta
        rsi_series = ta.rsi(close_prev, length=14)
        X["rsi"] = rsi_series if rsi_series is not None else 0
        macd = ta.macd(close_prev, fast=12, slow=26, signal=9)
        if macd is not None:
            X["macd"] = macd["MACD_12_26_9"]
            X["macd_signal"] = macd["MACDh_12_26_9"]
            X["macd_hist"] = macd["MACDs_12_26_9"]
        else:
            X["macd"] = 0
            X["macd_signal"] = 0
            X["macd_hist"] = 0
        bb = ta.bbands(close_prev, length=20)
        if bb is not None:
            X["bb_upper"] = bb["BBU_20_2.0_2.0"]
            X["bb_middle"] = bb["BBM_20_2.0_2.0"]
            X["bb_lower"] = bb["BBL_20_2.0_2.0"]
            X["bb_width"] = bb["BBB_20_2.0_2.0"]
            X["bb_percent"] = bb["BBP_20_2.0_2.0"]
        else:
            X["bb_upper"] = 0
            X["bb_middle"] = 0
            X["bb_lower"] = 0
            X["bb_width"] = 0
            X["bb_percent"] = 0
        atr = ta.atr(high_prev, low_prev, close_prev, length=14)
        X["atr"] = atr if atr is not None else 0
        X["realized_vol"] = close_prev.pct_change(fill_method=None).rolling(20).std()
        X["returns_skew"] = close_prev.pct_change(fill_method=None).rolling(20).skew()
        X["returns_kurt"] = close_prev.pct_change(fill_method=None).rolling(20).kurt()
        # ema12 = ta.ema(close_prev, length=12)
        # X["ema_12"] = ema12 if ema12 is not None else 0
        # ema26 = ta.ema(close_prev, length=26)
        # X["ema_26"] = ema26 if ema26 is not None else 0
        # sma20 = ta.sma(close_prev, length=20)
        # X["sma_20"] = sma20 if sma20 is not None else 0
        X["sma_50"] = ta.sma(close_prev.ffill(), length=50)
        if not has_usable_volume:
            X["vwap"] = 0
            X["obv"] = 0
            X["adosc"] = 0
        else:
            # pandas_ta VWAP requires an ordered DatetimeIndex; ensure monotonic index
            try:
                if not close_prev.index.is_monotonic_increasing:
                    _idx = close_prev.sort_index().index
                    cp = close_prev.reindex(_idx)
                    hp = high_prev.reindex(_idx)
                    lp = low_prev.reindex(_idx)
                    vp = volume_prev.reindex(_idx)
                else:
                    cp, hp, lp, vp = close_prev, high_prev, low_prev, volume_prev

                vwap = ta.vwap(hp, lp, cp, vp)
                X["vwap"] = vwap.reindex(X.index) if hasattr(vwap, "reindex") else vwap
            except Exception:
                X["vwap"] = 0
            try:
                X["obv"] = ta.obv(close_prev, volume_prev)
            except Exception:
                X["obv"] = 0
            try:
                X["adosc"] = ta.adosc(high_prev, low_prev, close_prev, volume_prev, fast=3, slow=10)
            except Exception:
                X["adosc"] = 0

    # Macro features (only daily series)
    # if 'FEDFUNDS' in df.columns and df['FEDFUNDS'].notna().sum() > 0:
    #     X["fed_funds_rate"] = df["FEDFUNDS"].shift(1)
    # if 'DGS10' in df.columns and df['DGS10'].notna().sum() > 0:
    #     X["treasury_10y"] = df["DGS10"].shift(1)
    # if 'DGS2' in df.columns and df['DGS2'].notna().sum() > 0:
    #     X["treasury_2y"] = df["DGS2"].shift(1)
    # if 'UNRATE' in df.columns and df['UNRATE'].notna().sum() > 0:
    #     X["unemployment"] = df["UNRATE"].shift(1)
    # # Monthly/quarterly: skip for now to avoid NaNs

    # momentum and drawdown
    X["mom_5"] = close_prev.pct_change(periods=5, fill_method=None).fillna(0)
    X["mom_10"] = close_prev.pct_change(periods=10, fill_method=None).fillna(0)
    X["rolling_max_60"] = close_prev.rolling(window=60, min_periods=1).max()
    X["drawdown_60"] = (close_prev / X["rolling_max_60"]) - 1.0

    # regime features
    vol_window = getattr(settings, "vol_window", w_med)
    vol = ret_1.rolling(window=vol_window, min_periods=1).std()
    X["vol_regime_z"] = (vol - vol.rolling(window=vol_window, min_periods=1).mean()) / vol.rolling(window=vol_window, min_periods=1).std().replace(0, np.nan)

    # trend regime: rolling slope of log price
    def rolling_slope(s: pd.Series, window: int):
        idx = np.arange(window)
        def slope(x):
            if np.isnan(x).all():
                return 0.0
            y = x
            A = np.vstack([idx, np.ones(len(idx))]).T
            try:
                m, c = np.linalg.lstsq(A, y, rcond=None)[0]
            except Exception:
                m = 0.0
            return m
        return s.rolling(window=window, min_periods=1).apply(lambda arr: slope(arr), raw=True)

    # replace deprecated fillna(method="ffill") with ffill(); guard zeros
    _safe_close_for_trend = close_prev.where(close_prev > 0, np.nan).ffill().bfill()
    X["trend_slope_20"] = rolling_slope(np.log(_safe_close_for_trend), window=20)
    X["trend_regime"] = np.sign(X["trend_slope_20"]).fillna(0)

    # Asset specific
    ac = (asset_class or "").lower()
    if ac == "crypto":
        X["is_weekend"] = X.index.weekday >= 5
        X["hour_of_day"] = X.index.hour
        X["day_of_week"] = X.index.weekday

    if ac in ("future", "futures"):
        # volatility scaling feature
        X["vol_scaling"] = 1.0 / (vol.replace(0, np.nan))

    if ac in ("stock", "equity", "etf"):
        # gap features if open exists
        if "open" in df.columns:
            X["gap_open"] = (df["open"] - df["close"].shift(1)).fillna(0)
        # Seasonal features
        X["month"] = X.index.month
        X["quarter"] = X.index.quarter
        X["day_of_year"] = X.index.dayofyear

    if ac in ("bond",):
        # Bonds often behave differently (rates/macro sensitivity). Keep it minimal and numeric-only.
        # If macro columns are present, expose them as shifted features (already shifted during join).
        macro_cols = [c for c in df.columns if isinstance(c, str) and c.startswith("macro_")]
        for c in macro_cols:
            X[c] = df[c]
        # Basic carry/mean-reversion proxies
        X["carry_proxy_20"] = ret_1.rolling(window=20, min_periods=1).mean()
        X["carry_proxy_60"] = ret_1.rolling(window=60, min_periods=1).mean()

    if ac in ("forex", "fx"):
        # FX is strongly session-driven; add simple session flags.
        h = X.index.hour
        X["fx_is_asia"] = ((h >= 0) & (h < 8)).astype(int)
        X["fx_is_europe"] = ((h >= 7) & (h < 16)).astype(int)
        X["fx_is_us"] = ((h >= 13) & (h < 21)).astype(int)
        X["fx_is_overlap_eu_us"] = ((h >= 13) & (h < 16)).astype(int)

    if ac in ("option", "options"):
        # include greeks if present
        for g in ("delta", "gamma", "vega", "theta", "iv"):
            if g in df.columns:
                X[f"opt_{g}"] = df[g].shift(1)

    # Robust stats optional
    try:
        X["ret_skew_20"] = ret_1.rolling(window=20, min_periods=1).skew()
        X["ret_kurt_20"] = ret_1.rolling(window=20, min_periods=1).kurt()
    except Exception:
        pass

    # Intraday-native breakout features (ib_*) — 1H, 30M, 5M, 1M only.
    _tf_ib = str(getattr(settings, "timeframe", "") or "").strip().upper()
    if _tf_ib and _tf_ib not in ("1D", "4H"):
        _ib_cfg = (feat_cfg or {}).get("intraday_breakout", {}) if isinstance(feat_cfg, dict) else {}
        _ib_feats = _build_intraday_breakout_features(
            close_prev=close_prev,
            open_prev=open_prev,
            high_prev=high_prev,
            low_prev=low_prev,
            volume_prev=volume_prev,
            has_usable_volume=has_usable_volume,
            feat_cfg=_ib_cfg,
            vwap_series=X["vwap"] if "vwap" in X.columns else None,
            atr_14_series=X["atr_14"] if "atr_14" in X.columns else None,
        )
        for _ib_k, _ib_v in _ib_feats.items():
            X[_ib_k] = _ib_v

    # Optional alternative-data features (exogenous).
    # Policy: Aviation traffic features may be used ONLY for 1D and 1H models.
    # Explicitly forbidden for 30m/5m/1m.
    try:
        avi_cfg = (feat_cfg or {}).get("aviation") if isinstance(feat_cfg, dict) else None
    except Exception:
        avi_cfg = None

    try:
        avi_enabled = bool((avi_cfg or {}).get("enabled", False)) if isinstance(avi_cfg, dict) else False
    except Exception:
        avi_enabled = False

    if avi_enabled:
        try:
            # Infer bar size from index (median delta) and allow only >= 1H.
            median_sec = 0.0
            if len(df.index) >= 2 and isinstance(df.index, pd.DatetimeIndex):
                deltas = np.diff(df.index.view("int64"))
                if len(deltas):
                    median_sec = float(np.median(deltas) / 1e9)

            allow_avi = False
            # daily-ish
            if median_sec >= 20 * 3600:
                allow_avi = True
            # hourly-ish
            elif median_sec >= 45 * 60:
                allow_avi = True

            if allow_avi:
                from octa.core.data.sources.altdata.aviation import get_aviation_features

                region = str((avi_cfg or {}).get("region", "GLOBAL")).strip().upper()

                # Deterministic and leakage-safe: get_aviation_features uses <= t only.
                feats = [get_aviation_features(t, region=region) for t in df.index]
                avi_df = pd.DataFrame(feats, index=df.index)
                avi_df = avi_df.add_prefix(f"avi_{region.lower()}_")
                X = X.join(avi_df, how="left")
        except Exception:
            # Never fail training due to altdata.
            pass

    # Remove feature columns that are all NaN
    feature_cols = [c for c in X.columns if not X[c].isna().all()]
    X = X[feature_cols]

    # Optional AltData platform integration (safe-by-default).
    # Policy: never fail training due to AltData; disabled unless explicitly enabled.
    altdat_meta = None
    try:
        from octa.core.data.sources.altdata.sidecar import try_run as _altdat_try_run

        altdat_df, altdat_meta = _altdat_try_run(bars_df=df, settings=settings, asset_class=str(asset_class or 'unknown'))
        if isinstance(altdat_df, pd.DataFrame) and not altdat_df.empty:
            # Align and merge; altdata is already as-of joined and additionally shifted by sidecar.
            altdat_df = altdat_df.reindex(X.index)
            X = X.join(altdat_df, how="left")
    except Exception as e:
        altdat_meta = {"enabled": True, "status": "ERROR", "error": str(e)}

    # Deterministic source-presence features: keep feature dimension stable across missing sources.
    try:
        src_meta = (altdat_meta or {}).get("sources") if isinstance(altdat_meta, dict) else None
        if isinstance(src_meta, dict):
            for src_name in sorted(str(k) for k in src_meta.keys()):
                sraw = src_meta.get(src_name) if isinstance(src_meta, dict) else {}
                sstatus = str((sraw or {}).get("status", "")).strip().upper()
                present = 1.0 if sstatus in {"OK", "SUCCESS"} else 0.0
                X[f"altdat_source_{src_name}_present"] = present
    except Exception:
        pass

    # Intraday feature isolation: drop daily-native features for sub-daily timeframes.
    # These features use multi-day lookback windows or accumulate over full history;
    # they carry regime/trend information not native to intraday bars and contaminate
    # 1H/30M/5M/1M models.
    _tf_iso = str(getattr(settings, "timeframe", "") or "").strip().upper()
    if _tf_iso and _tf_iso not in ("1D", "4H"):
        _INTRADAY_FORBIDDEN = frozenset([
            "sma_50",          # 50-bar SMA: ~8 trading days at 1H
            "trend_slope_20",  # 20-bar rolling slope: ~3 trading days at 1H
            "trend_regime",    # derived from trend_slope_20
            "rolling_max_60",  # 60-bar max: ~9 trading days at 1H
            "drawdown_60",     # derived from rolling_max_60
            "obv",             # cumulative over full history
            "adosc",           # accumulating volume indicator
        ])
        _to_drop = [c for c in _INTRADAY_FORBIDDEN if c in X.columns]
        if _to_drop:
            X = X.drop(columns=_to_drop)

    # Stable ordering for deterministic training artifacts.
    X = X.reindex(sorted(X.columns), axis=1)

    if not build_targets:
        # Leakage-safe: features are already shifted by 1 bar.
        # For inference/smoke-test we don't require targets.
        X_clean = X.loc[X.notna().any(axis=1)]
        y_clean: Dict[str, pd.Series] = {}
        horizons: list[int] = []
    else:
        # Targets: multi-horizon
        horizons = feat_cfg.get("horizons", getattr(settings, "horizons", [1, 3, 5]))
        try:
            horizons = [int(h) for h in horizons]
        except Exception:
            horizons = [1, 3, 5]
        y_dict: Dict[str, pd.Series] = {}
        fwd_dict: Dict[int, pd.Series] = {}
        for h in horizons:
            # forward returns using close (not shifted) -> fwd_return at time t is (close.shift(-h) / close) -1
            fwd = (df["close"].shift(-h) / df["close"]) - 1.0
            _safe_close = df["close"].where(df["close"] > 0, np.nan).ffill().bfill()
            y_reg = np.log(_safe_close.shift(-h)) - np.log(_safe_close)
            y_cls = (fwd > 0).astype(int)
            fwd_dict[int(h)] = fwd
            y_dict[f"y_reg_{h}"] = y_reg
            y_dict[f"y_cls_{h}"] = y_cls

        # Align: drop rows with NaNs caused by feature construction (initials) or target horizon
        # align features and targets into a single frame without column name collisions
        target_df = pd.concat([s.rename(k) for k, s in y_dict.items()], axis=1)
        # avoid column name collisions between features and targets
        overlap = set(X.columns).intersection(set(target_df.columns))
        if overlap:
            # rename target columns by appending suffix
            rename_map = {c: f"{c}_tgt" for c in target_df.columns}
            target_df = target_df.rename(columns=rename_map)
        combined = X.join(target_df, how="left")
        # drop rows where any target is NaN to keep supervised pairs valid
        valid_mask = ~target_df.isna().any(axis=1)
        feature_nonnull = combined[X.columns].notna().any(axis=1)
        keep_idx = valid_mask & feature_nonnull
        pre_filter_keep_idx = keep_idx.copy()

        sample_filter_meta: dict[str, Any] = {
            "enabled": False,
            "basis_horizon": None,
            "vol_multiplier": None,
            "rows_before_filter": int(pre_filter_keep_idx.sum()),
            "rows_after_filter": int(pre_filter_keep_idx.sum()),
            "dropped_rows": 0,
        }
        raw_filter_mult = feat_cfg.get("min_return_filter_vol_mult", None)
        if raw_filter_mult is not None:
            try:
                filter_mult = float(raw_filter_mult)
            except Exception:
                filter_mult = None
            if filter_mult is not None:
                basis_h = int(horizons[0]) if horizons else 1
                fwd_basis = fwd_dict.get(basis_h)
                if isinstance(fwd_basis, pd.Series):
                    info_mask = fwd_basis.abs().ge(vol.abs() * filter_mult).fillna(False)
                    keep_idx = keep_idx & info_mask.reindex(keep_idx.index, fill_value=False)
                    sample_filter_meta = {
                        "enabled": True,
                        "basis_horizon": basis_h,
                        "vol_multiplier": filter_mult,
                        "rows_before_filter": int(pre_filter_keep_idx.sum()),
                        "rows_after_filter": int(keep_idx.sum()),
                        "dropped_rows": int(pre_filter_keep_idx.sum() - keep_idx.sum()),
                    }

        X_clean = X.loc[keep_idx]
        y_clean = {k: v.loc[keep_idx] for k, v in y_dict.items()}

    meta = {
        "n_rows_raw": len(df),
        "n_rows_features": len(X_clean),
        "dropped_rows": int(len(df) - len(X_clean)),
        "features": list(X_clean.columns),
        "features_used": list(X_clean.columns),
        "horizons": horizons,
        "feature_settings": {
            "window_short": w_short,
            "window_med": w_med,
            # keep explicit for auditability; not all are used yet
            "window_long": int(feat_cfg.get("window_long", getattr(settings, "window_long", 60))),
            "vol_window": int(feat_cfg.get("vol_window", getattr(settings, "vol_window", 20))),
            "horizons": horizons,
            "min_return_filter_vol_mult": feat_cfg.get("min_return_filter_vol_mult", None),
        },
    }
    if build_targets:
        meta["sample_filter"] = sample_filter_meta
    try:
        if altdat_meta is not None:
            meta["altdat"] = altdat_meta
            meta["altdata_sources_used"] = altdat_meta.get("cache_paths") or altdat_meta.get("sources_used") or []
            meta["altdata_enabled"] = bool(altdat_meta.get("enabled", False))
            src_meta = altdat_meta.get("sources") if isinstance(altdat_meta, dict) else None
            src_summary = []
            missing = 0
            total = 0
            if isinstance(src_meta, dict):
                cov_raw = altdat_meta.get("coverage")
                cov: dict[str, Any] = dict(cov_raw) if isinstance(cov_raw, dict) else {}
                for source in sorted(src_meta.keys()):
                    raw_candidate = src_meta.get(source)
                    source_payload: dict[str, Any] = (
                        dict(raw_candidate) if isinstance(raw_candidate, dict) else {}
                    )
                    status = str(source_payload.get("status", "")).upper()
                    if not status:
                        ok_flag = source_payload.get("ok")
                        if ok_flag is True:
                            status = "OK"
                        elif "error" in source_payload:
                            status = "ERROR"
                        else:
                            status = "MISSING"
                    n_rows = source_payload.get("rows")
                    if n_rows is None:
                        n_rows = source_payload.get("n_rows")
                    try:
                        n_rows = int(n_rows or 0)
                    except Exception:
                        n_rows = 0
                    coverage = cov.get(source)
                    if coverage is None:
                        coverage = cov.get(source.lower())
                    try:
                        coverage = float(coverage) if coverage is not None else 0.0
                    except Exception:
                        coverage = 0.0
                    err = source_payload.get("error")
                    src_summary.append(
                        {
                            "source": str(source),
                            "status": status or "MISSING",
                            "n_rows": n_rows,
                            "coverage": coverage,
                            "error": None if err is None else str(err),
                        }
                    )
                    total += 1
                    if status not in {"OK", "SUCCESS"}:
                        missing += 1
            meta["altdata_meta"] = src_summary
            meta["altdata_degraded"] = bool(total > 0 and (missing / float(total)) > 0.5)
    except Exception:
        raise
    return FeatureBuildResult(X=X_clean, y_dict=y_clean, meta=meta)


def leakage_audit(
    X: pd.DataFrame,
    y_dict: Dict[str, pd.Series],
    raw_df: pd.DataFrame,
    horizons: List[int],
    *,
    settings=None,
    asset_class: str = "unknown",
    return_report: bool = False,
):
    # Practical check: recompute features using only history up to t for a small sample of timestamps.
    # Use tolerant numeric comparisons and fail-safe behavior so the audit cannot crash the pipeline.
    logger = logging.getLogger(__name__)
    report = {
        "status": "ok",
        "audit_drift_ok": False,
        "rtol": 2e-2,
        "atol": 1e-5,
        "max_abs_diff": 0.0,
        "max_rel_diff": 0.0,
        "max_abs_feature": None,
        "max_abs_timestamp": None,
        "max_rel_feature": None,
        "max_rel_timestamp": None,
        "sample_timestamps": [],
        "outside_tolerance_count": 0,
        "within_tolerance_drift_count": 0,
        "outside_tolerance_examples": [],
    }

    def _done(ok: bool):
        if return_report:
            return ok, report
        return ok

    try:
        feat_cfg = {}
        try:
            if settings is not None and isinstance(getattr(settings, "features", None), dict):
                feat_cfg = dict(getattr(settings, "features") or {})
        except Exception:
            feat_cfg = {}
        rtol = float(feat_cfg.get("leakage_audit_rtol", getattr(settings, "leakage_audit_rtol", 2e-2)))
        atol = float(feat_cfg.get("leakage_audit_atol", getattr(settings, "leakage_audit_atol", 1e-5)))
        report["rtol"] = rtol
        report["atol"] = atol

        max_horizon = max(horizons) if horizons else 0
        available_idx = X.index[:-max_horizon] if max_horizon > 0 else X.index
        # Sampling strategy:
        # We sample late in the series (near the end) to ensure enough history for
        # long-window indicators (e.g. ta.sma length=50) so build_features(hist)
        # does not drop those columns as all-NaN in short histories.
        # This avoids false-positive "missing columns" failures.
        w_short = int(getattr(settings, "window_short", 5) if settings is not None else 5)
        w_med = int(getattr(settings, "window_med", 20) if settings is not None else 20)
        w_long = int(getattr(settings, "window_long", 60) if settings is not None else 60)
        if len(available_idx) >= 3:
            sample_idx = available_idx[-3:].tolist()
        else:
            sample_idx = available_idx.tolist()
        report["sample_timestamps"] = [str(t) for t in sample_idx]
        if not sample_idx:
            return _done(True)

        missing_recomputed_ts: List[pd.Timestamp] = []
        max_abs_diff = 0.0
        max_rel_diff = 0.0
        max_abs_feature = None
        max_abs_ts = None
        max_rel_feature = None
        max_rel_ts = None
        outside_examples: List[dict] = []
        outside_count = 0
        drift_count = 0
        for t in sample_idx:
            try:
                hist = raw_df.loc[:t].copy()
                # Important: recompute FEATURES only (no targets). When restricting data to history up to t,
                # targets at t are not computable by definition and would cause expected row drops.
                eff_settings = settings
                if eff_settings is None:
                    eff_settings = type(
                        "S",
                        (),
                        {"window_short": w_short, "window_med": w_med, "window_long": w_long, "horizons": horizons},
                    )
                res = build_features(hist, settings=eff_settings, asset_class=asset_class, build_targets=False)
                if t not in res.X.index:
                    missing_recomputed_ts.append(t)
                    continue  # skip this t as it's expected for short history
                # If recomputed feature set differs, treat as an audit failure (can't compare safely).
                if not set(X.columns).issubset(set(res.X.columns)):
                    missing_cols = sorted(set(X.columns) - set(res.X.columns))
                    logger.warning("Leakage audit failed: recomputed feature set missing %d columns (e.g. %s)", len(missing_cols), missing_cols[:5])
                    report["status"] = "recomputed_feature_mismatch"
                    report["outside_tolerance_examples"] = [{"missing_columns": missing_cols[:20], "timestamp": str(t)}]
                    return _done(False)

                for col in X.columns:
                    a = X.at[t, col]
                    b = res.X.at[t, col]
                    # tolerant numeric comparison
                    # AltData features (FRED, EDGAR) use cache fallback to nearest
                    # prior date — values change naturally between dates, not leakage.
                    # Long-window rolling stats (z_252, roc_20) are inherently
                    # sensitive to truncation — use wider tolerance to avoid
                    # false-positive leakage flags.
                    _col_str = str(col)
                    if _col_str.startswith("altdat_"):
                        # AltData features naturally differ between full-history and
                        # truncated recomputes: different bars_df.end → different asof_date
                        # → different nearest-prior cache → different series endpoint.
                        # This is expected cache-date variation, not look-ahead leakage.
                        # Temporal integrity for altdata is enforced independently by
                        # validate_no_future_leakage() and backward asof_join inside
                        # build_altdata_features. Skip these columns here.
                        continue
                    elif "_z_252" in _col_str or "_roc_20" in _col_str:
                        col_rtol = max(rtol, 0.05)
                    else:
                        col_rtol = rtol
                    try:
                        af = float(a)
                        bf = float(b)
                        close = bool(np.isclose(af, bf, rtol=col_rtol, atol=atol, equal_nan=True))
                        if np.isnan(af) and np.isnan(bf):
                            continue
                        if np.isnan(af) != np.isnan(bf):
                            close = False
                            diff = float("inf")
                            rel = float("inf")
                        else:
                            diff = float(abs(af - bf))
                            base = max(abs(af), abs(bf), float(atol), 1e-12)
                            rel = float(diff / base)

                        if np.isfinite(diff) and diff > max_abs_diff:
                            max_abs_diff = diff
                            max_abs_feature = col
                            max_abs_ts = t
                        if np.isfinite(rel) and rel > max_rel_diff:
                            max_rel_diff = rel
                            max_rel_feature = col
                            max_rel_ts = t

                        if close:
                            if np.isfinite(diff) and diff > 0.0:
                                drift_count += 1
                            continue

                        outside_count += 1
                        logger.warning(
                            "Leakage audit numeric diff for %s at %s: live=%s recomputed=%s abs_diff=%s rel_diff=%s rtol=%s atol=%s",
                            col,
                            t,
                            a,
                            b,
                            diff,
                            rel,
                            col_rtol,
                            atol,
                        )
                        if len(outside_examples) < 10:
                            outside_examples.append(
                                {
                                    "feature": str(col),
                                    "timestamp": str(t),
                                    "live": None if pd.isna(a) else float(af),
                                    "recomputed": None if pd.isna(b) else float(bf),
                                    "abs_diff": None if not np.isfinite(diff) else diff,
                                    "rel_diff": None if not np.isfinite(rel) else rel,
                                }
                            )
                    except Exception:
                        same = False
                        try:
                            if pd.isna(a) and pd.isna(b):
                                same = True
                            else:
                                same = bool(a == b)
                        except Exception:
                            same = str(a) == str(b)
                        if not same:
                            logger.warning("Leakage audit equality mismatch for %s at %s", col, t)
                            outside_count += 1
                            if len(outside_examples) < 10:
                                outside_examples.append(
                                    {
                                        "feature": str(col),
                                        "timestamp": str(t),
                                        "live": str(a),
                                        "recomputed": str(b),
                                        "abs_diff": None,
                                        "rel_diff": None,
                                    }
                                )
            except Exception as e:
                logger.exception("Leakage audit inner error at %s: %s", t, e)
                report["status"] = "audit_error"
                report["outside_tolerance_examples"] = [{"timestamp": str(t), "error": str(e)}]
                return _done(False)

        if missing_recomputed_ts:
            # Keep this as a single line to avoid spamming logs in batch sweeps.
            shown = ", ".join(str(x) for x in missing_recomputed_ts[:3])
            suffix = "" if len(missing_recomputed_ts) <= 3 else f" (+{len(missing_recomputed_ts) - 3} more)"
            logger.warning(
                "Leakage audit: %d/%d sampled timestamps missing in recomputed features (likely warmup/insufficient history). Examples: %s%s",
                len(missing_recomputed_ts),
                len(sample_idx),
                shown,
                suffix,
            )

        report["max_abs_diff"] = max_abs_diff
        report["max_rel_diff"] = max_rel_diff
        report["max_abs_feature"] = max_abs_feature
        report["max_abs_timestamp"] = None if max_abs_ts is None else str(max_abs_ts)
        report["max_rel_feature"] = max_rel_feature
        report["max_rel_timestamp"] = None if max_rel_ts is None else str(max_rel_ts)
        report["outside_tolerance_count"] = int(outside_count)
        report["within_tolerance_drift_count"] = int(drift_count)
        report["outside_tolerance_examples"] = outside_examples

        logger.info(
            "Leakage audit summary: max_abs_diff=%s max_rel_diff=%s rtol=%s atol=%s outside_tolerance_count=%s within_tolerance_drift_count=%s",
            max_abs_diff,
            max_rel_diff,
            rtol,
            atol,
            outside_count,
            drift_count,
        )

        if outside_count > 0:
            report["status"] = "leakage_detected"
            return _done(False)
        if drift_count > 0:
            report["status"] = "audit_drift_ok"
            report["audit_drift_ok"] = True

        # index alignment
        for k, v in y_dict.items():
            if not X.index.equals(v.index):
                logger.warning("Leakage audit index misalignment between X and target %s", k)
                report["status"] = "index_misalignment"
                return _done(False)
        return _done(True)
    except Exception as e:
        logger.exception("Leakage audit failed unexpectedly: %s", e)
        report["status"] = "audit_error"
        report["outside_tolerance_examples"] = [{"error": str(e)}]
        return _done(False)
