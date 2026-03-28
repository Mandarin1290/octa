from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from octa_training.core.metrics_contract import (
    MetricsMetadata,
    MetricsSummary,
)


@dataclass
class EvalSettings:
    mode: str = "cls"  # 'cls' or 'reg'
    upper_q: float = 0.9
    lower_q: float = 0.1
    causal_quantiles: bool = False
    quantile_window: Optional[int] = 252
    adaptive_density_quantiles: bool = False
    density_target: float = 0.10
    density_window: Optional[int] = 63
    density_relax_max: float = 0.0
    leverage_cap: float = 3.0
    vol_target: float = 0.1  # annual target vol (10% by default)
    realized_vol_window: int = 20
    cost_bps: float = 1.0  # bps per turnover
    spread_bps: float = 0.5  # bps per trade
    freq: Optional[str] = None  # infer from index
    stress_cost_multiplier: float = 3.0
    align_tolerance: str = "1min"
    # Optional session filter (intraday): if enabled, disable trading outside session.
    session_enabled: bool = False
    session_timezone: str = "UTC"
    session_open: str = "00:00"  # HH:MM
    session_close: str = "23:59"  # HH:MM
    session_weekdays: Optional[list[int]] = None  # 0=Mon .. 6=Sun
    timeframe: Optional[str] = None
    regime_policy: Optional[Dict[str, Any]] = None


def _safe_num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not np.isfinite(out):
        return float(default)
    return float(out)


def _rolling_percentile_rank(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    def _pct_last(values: pd.Series) -> float:
        try:
            return float(values.rank(pct=True).iloc[-1])
        except Exception:
            return float("nan")

    return series.rolling(window=window, min_periods=min_periods).apply(_pct_last, raw=False)


def _blend(low: float, high: float, weight: pd.Series | float) -> pd.Series:
    weight_s = pd.to_numeric(weight, errors='coerce')
    if not isinstance(weight_s, pd.Series):
        weight_s = pd.Series([weight_s], dtype=float)
    weight_s = weight_s.fillna(0.0).clip(0.0, 1.0)
    return low + (high - low) * weight_s


def _resolve_regime_policy(settings: EvalSettings) -> Dict[str, Any]:
    raw = settings.regime_policy if isinstance(settings.regime_policy, dict) else {}
    if not raw or not bool(raw.get("enabled", False)):
        return {}
    per_tf = raw.get("per_timeframe", {}) if isinstance(raw.get("per_timeframe"), dict) else {}
    tf = str(getattr(settings, "timeframe", "") or "").strip()
    tf_spec = {}
    if tf:
        tf_spec = per_tf.get(tf) or per_tf.get(tf.upper()) or per_tf.get(tf.lower()) or {}
    merged = dict(raw)
    merged.pop("per_timeframe", None)
    if isinstance(tf_spec, dict):
        merged.update(tf_spec)
    return merged


def _normalize_market_frame(market_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if not isinstance(market_df, pd.DataFrame) or market_df.empty:
        return None
    out = market_df.copy()
    if not isinstance(out.index, pd.DatetimeIndex):
        for cand in ("timestamp", "datetime", "date", "time"):
            if cand in out.columns:
                try:
                    out[cand] = pd.to_datetime(out[cand], utc=True, errors="coerce")
                    out = out.set_index(cand)
                    break
                except Exception:
                    continue
    if not isinstance(out.index, pd.DatetimeIndex):
        return None
    return out.sort_index()


def _rolling_quantile_thresholds(
    pred: pd.Series,
    upper_q: float,
    lower_q: float,
    window: Optional[int],
) -> tuple[pd.Series, pd.Series]:
    """Leakage-safe quantile thresholds using only past predictions.

    - If window is None: expanding quantiles.
    - Else: rolling window quantiles.
    Always shifts by 1 to ensure thresholds at t are computed from < t.
    """
    pred = pd.to_numeric(pred, errors="coerce").astype(float)
    if window is None:
        u = pred.expanding(min_periods=50).quantile(upper_q).shift(1)
        lo = pred.expanding(min_periods=50).quantile(lower_q).shift(1)
        return u, lo
    win = int(window)
    if win <= 1:
        # degenerate: effectively expanding
        u = pred.expanding(min_periods=50).quantile(upper_q).shift(1)
        lo = pred.expanding(min_periods=50).quantile(lower_q).shift(1)
        return u, lo
    u = pred.rolling(win, min_periods=min(50, win)).quantile(upper_q).shift(1)
    lo = pred.rolling(win, min_periods=min(50, win)).quantile(lower_q).shift(1)
    return u, lo


def _adapt_thresholds_for_density(
    pred: pd.Series,
    upper: pd.Series,
    lower: pd.Series,
    *,
    density_window: Optional[int],
    density_target: float,
    density_relax_max: float,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    target = max(float(density_target), 1e-6)
    relax_max = min(max(float(density_relax_max), 0.0), 1.0)
    if relax_max <= 0.0:
        zero = pd.Series(0.0, index=pred.index, dtype=float)
        return upper, lower, zero

    active = ((pred > upper) | (pred < lower)).astype(float)
    try:
        dwin = int(density_window) if density_window is not None else 63
    except Exception:
        dwin = 63
    dwin = max(dwin, 10)
    density = active.rolling(window=dwin, min_periods=min(20, dwin)).mean().shift(1)
    density = density.fillna(active.expanding(min_periods=10).mean().shift(1)).fillna(0.0)
    pressure = ((target - density) / target).clip(lower=0.0, upper=1.0)

    mid = ((upper + lower) / 2.0).astype(float)
    upper_adj = upper - (upper - mid).clip(lower=0.0) * pressure * relax_max
    lower_adj = lower + (mid - lower).clip(lower=0.0) * pressure * relax_max
    return upper_adj, lower_adj, pressure


def infer_frequency(index: pd.Index) -> float:
    if len(index) < 2:
        return 252.0
    deltas = np.diff(index.astype('int64'))
    median_ns = np.median(deltas)
    # seconds
    sec = median_ns / 1e9
    # approximate annualization factor
    if sec <= 0:
        return 252.0
    # if daily-ish
    if sec >= 20 * 3600:
        return 252.0
    # if hourly-ish
    if sec >= 30 * 60:
        return 252.0 * 24.0
    # minute-ish
    return 252.0 * 6.5 * 60.0


def realized_vol(returns: pd.Series, window: int, annualization: float) -> pd.Series:
    rv = returns.rolling(window).std(ddof=0) * math.sqrt(annualization)
    return rv


def compute_equity_and_metrics(
    prices: pd.Series,
    preds: pd.Series,
    settings: EvalSettings,
    market_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    # coerce preds to Series and align indices with prices where possible
    if not isinstance(preds, pd.Series):
        try:
            preds = pd.Series(preds)
        except Exception:
            preds = pd.Series(list(preds))

    # try to convert preds index to datetimes (handles ISO strings)
    try:
        idx = pd.to_datetime(preds.index, utc=True, errors='coerce')
        if not idx.isna().all():
            preds.index = idx
        else:
            # if conversion produced all NaT and lengths match, align by position
            if len(preds) == len(prices):
                preds.index = prices.index
    except Exception:
        if len(preds) == len(prices):
            preds.index = prices.index

    # if both indices are tz-aware attempt to align timezones
    try:
        if getattr(prices.index, 'tz', None) is not None and getattr(preds.index, 'tz', None) is not None:
            preds.index = preds.index.tz_convert(prices.index.tz)
    except Exception:
        pass

    # join on index; if no overlap, try positional alignment as a fallback
    df = pd.DataFrame({'price': prices}).join(pd.Series(preds, name='pred'), how='inner')
    df = df.dropna(subset=['price', 'pred'])
    if df.empty:
        # fallback: if lengths match, align predictions by position
        if len(preds) == len(prices):
            try:
                preds_pos = pd.Series(preds.values, index=prices.index)
                df = pd.DataFrame({'price': prices}).join(pd.Series(preds_pos, name='pred'), how='inner')
                df = df.dropna(subset=['price', 'pred'])
            except Exception:
                pass
    if df.empty:
        # try numeric-index-as-positions fallback
        try:
            idx_vals = list(preds.index)
            # detect integer-like indices
            int_idx = [int(x) for x in idx_vals]
            if all(isinstance(i, int) for i in int_idx) and max(int_idx) < len(prices):
                try:
                    mapped_idx = prices.index.take(int_idx)
                    preds_pos = pd.Series(preds.values, index=mapped_idx)
                    df = pd.DataFrame({'price': prices}).join(pd.Series(preds_pos, name='pred'), how='inner')
                    df = df.dropna(subset=['price', 'pred'])
                except Exception:
                    pass
        except Exception:
            pass

        # final fallback: nearest timestamp merge within tolerance
        try:
            # prepare dataframes for merge_asof
            pd_price = prices.reset_index()
            pd_price.columns = ['time', 'price']
            pd_pred = pd.Series(preds, name='pred').reset_index()
            pd_pred.columns = ['time', 'pred']
            pd_price = pd_price.sort_values('time')
            pd_pred = pd_pred.sort_values('time')
            tol = pd.Timedelta(settings.align_tolerance)
            merged = pd.merge_asof(pd_price, pd_pred, on='time', direction='nearest', tolerance=tol)
            merged = merged.dropna(subset=['price', 'pred'])
            if not merged.empty:
                merged = merged.set_index('time')
                df = merged[['price', 'pred']]
        except Exception:
            pass
    if df.empty:
        # include diagnostics to help debug alignment issues
        pd_sample_pred_idx = list(map(str, list(preds.index[:10]))) if len(preds) > 0 else []
        pd_sample_price_idx = list(map(str, list(prices.index[:10]))) if len(prices) > 0 else []
        raise ValueError(f'No overlapping price/prediction data; preds_len={len(preds)}, prices_len={len(prices)}, sample_pred_idx={pd_sample_pred_idx}, sample_price_idx={pd_sample_price_idx}')

    market = _normalize_market_frame(market_df)
    if isinstance(market, pd.DataFrame):
        extra_cols = [c for c in ("open", "high", "low", "close", "volume") if c in market.columns]
        if extra_cols:
            extra = market[extra_cols].copy()
            extra = extra[~extra.index.duplicated(keep="last")]
            df = df.join(extra, how="left")
    if "close" not in df.columns:
        df["close"] = df["price"]

    # compute returns
    # use close-to-close log returns; guard against non-positive prices
    price = pd.to_numeric(df['price'], errors='coerce').astype(float)
    price = price.where(price > 0, np.nan).ffill().bfill()
    df['ret'] = np.log(price).diff().fillna(0.0)

    # infer annualization
    ann = infer_frequency(df.index)
    # bars/day inferred from annualization convention (ann ~= 252 * bars_per_day)
    bars_per_day = float(ann) / 252.0 if ann and ann > 0 else 1.0

    # raw signals
    if settings.mode == 'cls':
        if bool(getattr(settings, 'causal_quantiles', False)):
            u_s, l_s = _rolling_quantile_thresholds(
                df['pred'],
                upper_q=float(settings.upper_q),
                lower_q=float(settings.lower_q),
                window=getattr(settings, 'quantile_window', None),
            )
            density_pressure = pd.Series(0.0, index=df.index, dtype=float)
            if bool(getattr(settings, 'adaptive_density_quantiles', False)):
                u_s, l_s, density_pressure = _adapt_thresholds_for_density(
                    df['pred'],
                    u_s,
                    l_s,
                    density_window=getattr(settings, 'density_window', None),
                    density_target=float(getattr(settings, 'density_target', 0.10) or 0.10),
                    density_relax_max=float(getattr(settings, 'density_relax_max', 0.0) or 0.0),
                )
        else:
            u = df['pred'].quantile(settings.upper_q)
            lo = df['pred'].quantile(settings.lower_q)
            density_pressure = pd.Series(0.0, index=df.index, dtype=float)
        raw = pd.Series(0.0, index=df.index)
        # Classification target y_cls_* is 1 for positive forward return.
        # Therefore higher predicted probability should map to LONG, lower to SHORT.
        if bool(getattr(settings, 'causal_quantiles', False)):
            raw[df['pred'] > u_s] = 1.0
            raw[df['pred'] < l_s] = -1.0
        else:
            raw[df['pred'] > u] = 1.0
            raw[df['pred'] < lo] = -1.0
    else:
        # regression: use quantile bands around median
        median = df['pred'].median()
        if bool(getattr(settings, 'causal_quantiles', False)):
            up_s, low_s = _rolling_quantile_thresholds(
                df['pred'],
                upper_q=float(settings.upper_q),
                lower_q=float(settings.lower_q),
                window=getattr(settings, 'quantile_window', None),
            )
            density_pressure = pd.Series(0.0, index=df.index, dtype=float)
            if bool(getattr(settings, 'adaptive_density_quantiles', False)):
                up_s, low_s, density_pressure = _adapt_thresholds_for_density(
                    df['pred'],
                    up_s,
                    low_s,
                    density_window=getattr(settings, 'density_window', None),
                    density_target=float(getattr(settings, 'density_target', 0.10) or 0.10),
                    density_relax_max=float(getattr(settings, 'density_relax_max', 0.0) or 0.0),
                )
        else:
            up = df['pred'].quantile(settings.upper_q)
            low = df['pred'].quantile(settings.lower_q)
            density_pressure = pd.Series(0.0, index=df.index, dtype=float)
        raw = np.sign(df['pred'] - median)
        raw = pd.Series(raw, index=df.index)
        if bool(getattr(settings, 'causal_quantiles', False)):
            raw[df['pred'] > up_s] = 1.0
            raw[df['pred'] < low_s] = -1.0
        else:
            raw[df['pred'] > up] = 1.0
            raw[df['pred'] < low] = -1.0

    df['raw_signal'] = raw
    df['adaptive_density_pressure'] = density_pressure

    # Session filter (if enabled): prevent signals/positions outside trading hours.
    # IMPORTANT: only apply to intraday bars. Applying to daily bars can zero all
    # signals because daily timestamps (often 00:00 UTC) do not map to session times.
    try:
        if bool(getattr(settings, 'session_enabled', False)) and float(bars_per_day) > 1.5:
            tz = str(getattr(settings, 'session_timezone', 'UTC') or 'UTC')
            open_s = str(getattr(settings, 'session_open', '00:00') or '00:00')
            close_s = str(getattr(settings, 'session_close', '23:59') or '23:59')
            weekdays = getattr(settings, 'session_weekdays', None)

            idx = df.index
            # Only apply to datetime-like indices.
            if isinstance(idx, pd.DatetimeIndex):
                try:
                    idx_local = idx.tz_convert(tz) if getattr(idx, 'tz', None) is not None else idx.tz_localize('UTC').tz_convert(tz)
                except Exception:
                    idx_local = idx

                try:
                    oh, om = [int(x) for x in open_s.split(':')[:2]]
                    ch, cm = [int(x) for x in close_s.split(':')[:2]]
                except Exception:
                    oh, om, ch, cm = 0, 0, 23, 59

                tmins = idx_local.hour * 60 + idx_local.minute
                open_m = oh * 60 + om
                close_m = ch * 60 + cm

                if close_m >= open_m:
                    in_time = (tmins >= open_m) & (tmins <= close_m)
                else:
                    # overnight session
                    in_time = (tmins >= open_m) | (tmins <= close_m)

                if isinstance(weekdays, list) and weekdays:
                    in_day = np.isin(idx_local.dayofweek, np.array([int(x) for x in weekdays]))
                else:
                    in_day = np.ones(len(idx_local), dtype=bool)

                in_session = in_time & in_day
                df.loc[~in_session, 'raw_signal'] = 0.0
    except Exception:
        pass

    # realized vol
    rv = realized_vol(df['ret'], settings.realized_vol_window, ann)
    df['rv'] = rv.replace(0, np.nan).bfill().ffill()

    policy = _resolve_regime_policy(settings)
    if bool(policy):
        if settings.mode == 'cls':
            if bool(getattr(settings, 'causal_quantiles', False)):
                band = (u_s - l_s).abs().replace(0, np.nan)
                long_strength = ((df['pred'] - u_s) / band).clip(lower=0.0)
                short_strength = ((l_s - df['pred']) / band).clip(lower=0.0)
            else:
                band = max(float(u - lo), 1e-9)
                long_strength = ((df['pred'] - float(u)) / band).clip(lower=0.0)
                short_strength = ((float(lo) - df['pred']) / band).clip(lower=0.0)
        else:
            if bool(getattr(settings, 'causal_quantiles', False)):
                band = (up_s - low_s).abs().replace(0, np.nan)
                long_strength = ((df['pred'] - up_s) / band).clip(lower=0.0)
                short_strength = ((low_s - df['pred']) / band).clip(lower=0.0)
            else:
                band = max(float(up - low), 1e-9)
                long_strength = ((df['pred'] - float(up)) / band).clip(lower=0.0)
                short_strength = ((float(low) - df['pred']) / band).clip(lower=0.0)

        df['signal_strength_raw'] = np.where(
            df['raw_signal'] > 0,
            long_strength,
            np.where(df['raw_signal'] < 0, short_strength, 0.0),
        )
        df['signal_strength_raw'] = pd.to_numeric(df['signal_strength_raw'], errors='coerce').fillna(0.0)
        df['signal_strength'] = (df['signal_strength_raw'] / (1.0 + df['signal_strength_raw'])).clip(0.0, 1.0)

        regime_window = max(int(policy.get('regime_window', 120) or 120), 20)
        atr_window = max(int(policy.get('atr_window', 14) or 14), 5)
        trend_window = max(int(policy.get('trend_window', 20) or 20), 5)
        quality_window = max(int(policy.get('quality_window', 240) or 240), 20)
        quality_keep_quantile = min(max(_safe_num(policy.get('quality_keep_quantile', 0.70), 0.70), 0.0), 0.99)

        vol_bar = df['ret'].rolling(window=max(int(policy.get('vol_window', 24) or 24), 5), min_periods=5).std(ddof=0)
        vol_mean = vol_bar.rolling(window=regime_window, min_periods=min(30, regime_window)).mean()
        vol_std = vol_bar.rolling(window=regime_window, min_periods=min(30, regime_window)).std(ddof=0)
        df['vol_regime_z_eval'] = ((vol_bar - vol_mean) / vol_std.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)

        prev_close = df['close'].shift(1)
        if 'high' in df.columns and 'low' in df.columns:
            high = pd.to_numeric(df['high'], errors='coerce').astype(float)
            low_col = pd.to_numeric(df['low'], errors='coerce').astype(float)
            tr = pd.concat(
                [
                    (high - low_col).abs(),
                    (high - prev_close).abs(),
                    (low_col - prev_close).abs(),
                ],
                axis=1,
            ).max(axis=1)
        else:
            tr = df['ret'].abs() * df['close']
        atr_ratio = (tr.rolling(window=atr_window, min_periods=max(5, atr_window // 2)).mean() / df['close']).replace([np.inf, -np.inf], np.nan)
        df['atr_ratio_eval'] = atr_ratio.fillna(0.0)
        df['atr_percentile_eval'] = _rolling_percentile_rank(
            df['atr_ratio_eval'].fillna(0.0),
            window=regime_window,
            min_periods=min(30, regime_window),
        ).fillna(0.5)

        log_price = np.log(df['close'].replace(0, np.nan)).ffill().bfill()
        trend_move = log_price.diff(trend_window)
        trend_noise = df['ret'].rolling(window=trend_window, min_periods=max(5, trend_window // 2)).std(ddof=0) * math.sqrt(float(trend_window))
        df['trend_strength_eval'] = (trend_move.abs() / trend_noise.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        df['trend_strength_norm'] = (df['trend_strength_eval'] / (1.0 + df['trend_strength_eval'])).clip(0.0, 1.0)
        df['trend_align_eval'] = np.where(
            df['raw_signal'] == 0.0,
            0.0,
            (np.sign(trend_move.fillna(0.0)) == np.sign(df['raw_signal'])).astype(float),
        )
        df['signal_persistence_eval'] = (
            df['raw_signal'].replace(0.0, np.nan).rolling(window=3, min_periods=1).mean().abs().fillna(0.0).clip(0.0, 1.0)
        )
        df['feature_consensus_proxy'] = (0.6 * df['trend_align_eval'] + 0.4 * df['signal_persistence_eval']).clip(0.0, 1.0)

        if 'volume' in df.columns:
            volume = pd.to_numeric(df['volume'], errors='coerce').astype(float).fillna(0.0)
            rel_vol = volume / volume.rolling(window=max(int(policy.get('liquidity_window', 120) or 120), 20), min_periods=20).median().replace(0, np.nan)
            df['liquidity_score_eval'] = rel_vol.clip(lower=0.5, upper=1.5).fillna(1.0)
        else:
            df['liquidity_score_eval'] = 1.0

        low_vol = (
            (df['vol_regime_z_eval'] <= _safe_num(policy.get('low_vol_z_max', -0.20), -0.20))
            & (df['atr_percentile_eval'] <= _safe_num(policy.get('low_vol_atr_pct_max', 0.45), 0.45))
        )
        high_vol = (
            (df['vol_regime_z_eval'] >= _safe_num(policy.get('high_vol_z_min', 0.35), 0.35))
            | (df['atr_percentile_eval'] >= _safe_num(policy.get('high_vol_atr_pct_min', 0.70), 0.70))
        )
        df['vol_regime_label'] = np.select([low_vol, high_vol], ['LOW_VOL', 'HIGH_VOL'], default='MID_VOL')
        z_span = max(
            _safe_num(policy.get('high_vol_z_min', 0.35), 0.35) - _safe_num(policy.get('low_vol_z_max', -0.20), -0.20),
            1e-6,
        )
        atr_span = max(
            _safe_num(policy.get('high_vol_atr_pct_min', 0.70), 0.70) - _safe_num(policy.get('low_vol_atr_pct_max', 0.45), 0.45),
            1e-6,
        )
        regime_heat_z = (
            (df['vol_regime_z_eval'] - _safe_num(policy.get('low_vol_z_max', -0.20), -0.20)) / z_span
        ).clip(0.0, 1.0)
        regime_heat_atr = (
            (df['atr_percentile_eval'] - _safe_num(policy.get('low_vol_atr_pct_max', 0.45), 0.45)) / atr_span
        ).clip(0.0, 1.0)
        df['regime_heat_eval'] = (0.5 * regime_heat_z + 0.5 * regime_heat_atr).clip(0.0, 1.0)

        regime_score = np.select(
            [df['vol_regime_label'] == 'LOW_VOL', df['vol_regime_label'] == 'HIGH_VOL'],
            [
                _safe_num(policy.get('low_vol_quality_score', 0.25), 0.25),
                _safe_num(policy.get('high_vol_quality_score', 1.0), 1.0),
            ],
            default=_safe_num(policy.get('mid_vol_quality_score', 0.75), 0.75),
        )
        df['quality_score'] = (
            0.40 * df['signal_strength']
            + 0.20 * regime_score
            + 0.20 * df['feature_consensus_proxy']
            + 0.20 * df['trend_strength_norm']
        ).clip(0.0, 1.0)

        ensemble_cfg = policy.get('ensemble', {}) if isinstance(policy.get('ensemble'), dict) else {}
        if bool(ensemble_cfg.get('enabled', False)):
            sleeves_cfg = ensemble_cfg.get('sleeves', {}) if isinstance(ensemble_cfg.get('sleeves'), dict) else {}
            active_floor = _safe_num(ensemble_cfg.get('active_score_min', 0.10), 0.10)
            diversity_min = max(int(ensemble_cfg.get('diversity_min', 2) or 2), 1)
            model_weight = _safe_num(ensemble_cfg.get('model_weight', 0.60), 0.60)
            ensemble_weight = _safe_num(ensemble_cfg.get('ensemble_weight', 0.40), 0.40)
            ensemble_only_min_score = _safe_num(ensemble_cfg.get('ensemble_only_min_score', 0.45), 0.45)
            low_vol_ensemble_only = bool(ensemble_cfg.get('low_vol_allow_ensemble_only', False))

            def _sleeve_weight(name: str, default: float) -> float:
                cfg = sleeves_cfg.get(name, {}) if isinstance(sleeves_cfg.get(name), dict) else {}
                return _safe_num(cfg.get('weight', default), default)

            def _sleeve_enabled(name: str, default: bool = True) -> bool:
                cfg = sleeves_cfg.get(name, {}) if isinstance(sleeves_cfg.get(name), dict) else {}
                return bool(cfg.get('enabled', default))

            sleeve_scores: Dict[str, pd.Series] = {}

            if _sleeve_enabled('momentum'):
                sleeve_scores['momentum'] = (
                    np.sign(trend_move.fillna(0.0)) * (0.65 * df['trend_strength_norm'] + 0.35 * df['signal_strength'])
                ).clip(-1.0, 1.0)

            if _sleeve_enabled('breakout'):
                breakout_window = max(int((sleeves_cfg.get('breakout', {}) or {}).get('range_window', 24) or 24), 8)
                rolling_high = pd.to_numeric(df['close'], errors='coerce').rolling(window=breakout_window, min_periods=max(5, breakout_window // 2)).max()
                rolling_low = pd.to_numeric(df['close'], errors='coerce').rolling(window=breakout_window, min_periods=max(5, breakout_window // 2)).min()
                breakout_pos = ((df['close'] - rolling_low) / (rolling_high - rolling_low).replace(0, np.nan)).fillna(0.5)
                breakout_score = ((breakout_pos - 0.5) * 2.0).clip(-1.0, 1.0)
                breakout_gate = (df['atr_percentile_eval'] >= _safe_num((sleeves_cfg.get('breakout', {}) or {}).get('atr_pct_min', 0.55), 0.55)).astype(float)
                sleeve_scores['breakout'] = (breakout_score * breakout_gate).clip(-1.0, 1.0)

            if _sleeve_enabled('mean_reversion'):
                mr_window = max(int((sleeves_cfg.get('mean_reversion', {}) or {}).get('z_window', 24) or 24), 8)
                close_mean = df['close'].rolling(window=mr_window, min_periods=max(5, mr_window // 2)).mean()
                close_std = df['close'].rolling(window=mr_window, min_periods=max(5, mr_window // 2)).std(ddof=0).replace(0, np.nan)
                close_z = ((df['close'] - close_mean) / close_std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
                mr_clip = _safe_num((sleeves_cfg.get('mean_reversion', {}) or {}).get('z_clip', 2.5), 2.5)
                mr_score = (-close_z / max(mr_clip, 1e-6)).clip(-1.0, 1.0)
                mr_gate = (df['atr_percentile_eval'] <= _safe_num((sleeves_cfg.get('mean_reversion', {}) or {}).get('atr_pct_max', 0.65), 0.65)).astype(float)
                sleeve_scores['mean_reversion'] = (mr_score * mr_gate).clip(-1.0, 1.0)

            if _sleeve_enabled('volume_flow'):
                if 'volume' in df.columns:
                    volume_s = pd.to_numeric(df['volume'], errors='coerce').fillna(0.0)
                else:
                    volume_s = pd.Series(0.0, index=df.index, dtype=float)
                signed_volume = np.sign(df['ret'].fillna(0.0)) * volume_s
                obv = signed_volume.cumsum()
                obv_window = max(int((sleeves_cfg.get('volume_flow', {}) or {}).get('obv_window', 12) or 12), 4)
                obv_delta = obv.diff(obv_window)
                obv_scale = obv.abs().rolling(window=max(12, obv_window * 2), min_periods=max(6, obv_window)).median().replace(0, np.nan)
                obv_score = (obv_delta / obv_scale).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(-1.0, 1.0)
                rel_vol_gate = (df['liquidity_score_eval'] >= _safe_num((sleeves_cfg.get('volume_flow', {}) or {}).get('rel_vol_min', 0.9), 0.9)).astype(float)
                sleeve_scores['volume_flow'] = (obv_score * rel_vol_gate).clip(-1.0, 1.0)

            if _sleeve_enabled('intraday_structure') and isinstance(df.index, pd.DatetimeIndex):
                hour_bucket = pd.Series(df.index.hour, index=df.index)
                hour_ret = df['ret'].fillna(0.0).groupby(hour_bucket)
                hour_cum = hour_ret.cumsum().shift(1)
                hour_cnt = hour_ret.cumcount()
                hour_mean = (hour_cum / hour_cnt.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
                hour_scale = df['ret'].rolling(window=48, min_periods=12).std(ddof=0).replace(0, np.nan)
                hour_score = (hour_mean / hour_scale).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(-1.0, 1.0)
                sleeve_scores['intraday_structure'] = hour_score

            if _sleeve_enabled('range_release'):
                comp_window = max(int((sleeves_cfg.get('range_release', {}) or {}).get('compression_window', 20) or 20), 8)
                tr_mean = tr.rolling(window=comp_window, min_periods=max(5, comp_window // 2)).mean()
                range_pct = _rolling_percentile_rank((tr_mean / df['close']).fillna(0.0), window=regime_window, min_periods=min(30, regime_window)).fillna(0.5)
                compression_gate = (range_pct <= _safe_num((sleeves_cfg.get('range_release', {}) or {}).get('compression_pct_max', 0.40), 0.40)).astype(float)
                release_score = ((df['atr_percentile_eval'] - range_pct) * 2.0).clip(-1.0, 1.0)
                sleeve_scores['range_release'] = (np.sign(trend_move.fillna(0.0)) * release_score * compression_gate).clip(-1.0, 1.0)

            weight_map = {
                name: _sleeve_weight(
                    name,
                    {
                        'momentum': 1.0,
                        'breakout': 0.9,
                        'mean_reversion': 0.8,
                        'volume_flow': 0.7,
                        'intraday_structure': 0.5,
                        'range_release': 0.8,
                    }.get(name, 1.0),
                )
                for name in sleeve_scores
            }
            abs_weight_sum = pd.Series(0.0, index=df.index, dtype=float)
            weighted_score_sum = pd.Series(0.0, index=df.index, dtype=float)
            pos_support = pd.Series(0.0, index=df.index, dtype=float)
            neg_support = pd.Series(0.0, index=df.index, dtype=float)
            active_support = pd.Series(0.0, index=df.index, dtype=float)
            diversity_count = pd.Series(0.0, index=df.index, dtype=float)

            for name, score in sleeve_scores.items():
                s = pd.to_numeric(score, errors='coerce').fillna(0.0).clip(-1.0, 1.0)
                w = float(weight_map[name])
                active = (s.abs() >= active_floor).astype(float)
                df[f'ensemble_{name}_score'] = s
                df[f'ensemble_{name}_active'] = active.astype(bool)
                abs_weight_sum = abs_weight_sum + active * w
                weighted_score_sum = weighted_score_sum + s * w
                pos_support = pos_support + np.where(s > 0, s * w, 0.0)
                neg_support = neg_support + np.where(s < 0, (-s) * w, 0.0)
                active_support = active_support + active * w
                diversity_count = diversity_count + np.where(s.abs() >= active_floor, 1.0, 0.0)

            ensemble_score = (weighted_score_sum / abs_weight_sum.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(-1.0, 1.0)
            ensemble_conf = (np.maximum(pos_support, neg_support) / active_support.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)
            diversity_norm = (diversity_count / max(len(sleeve_scores), 1)).clip(0.0, 1.0)
            model_signal_score = np.sign(df['raw_signal']).astype(float) * df['signal_strength']
            blended_score = (
                model_weight * model_signal_score
                + ensemble_weight * ensemble_score * np.maximum(ensemble_conf, diversity_norm)
            ).clip(-1.0, 1.0)
            blended_sign = np.sign(blended_score).astype(float)
            ensemble_only_allowed = (
                (df['raw_signal'] == 0.0)
                & (ensemble_conf >= ensemble_only_min_score)
                & (diversity_count >= float(diversity_min))
                & np.where(df['vol_regime_label'] == 'LOW_VOL', low_vol_ensemble_only, True)
            )
            df['ensemble_score'] = ensemble_score
            df['ensemble_confidence'] = ensemble_conf
            df['ensemble_diversity_count'] = diversity_count
            df['ensemble_diversity_norm'] = diversity_norm
            df['ensemble_only_allowed'] = ensemble_only_allowed.astype(bool)
            df['raw_signal'] = np.where(
                ensemble_only_allowed & (blended_sign != 0.0),
                blended_sign,
                df['raw_signal'],
            )
            df['signal_strength'] = np.where(
                ensemble_only_allowed,
                np.maximum(df['signal_strength'], np.abs(blended_score)),
                df['signal_strength'],
            )
            df['quality_score'] = (
                0.30 * df['signal_strength']
                + 0.15 * regime_score
                + 0.15 * df['feature_consensus_proxy']
                + 0.15 * df['trend_strength_norm']
                + 0.15 * df['ensemble_confidence']
                + 0.10 * df['ensemble_diversity_norm']
            ).clip(0.0, 1.0)
        else:
            df['ensemble_score'] = 0.0
            df['ensemble_confidence'] = 0.0
            df['ensemble_diversity_count'] = 0.0
            df['ensemble_diversity_norm'] = 0.0
            df['ensemble_only_allowed'] = False

        hist_quality = df['quality_score'].where(df['raw_signal'] != 0.0)
        quality_threshold = hist_quality.rolling(window=quality_window, min_periods=min(30, quality_window)).quantile(quality_keep_quantile).shift(1)
        quality_threshold = quality_threshold.fillna(hist_quality.expanding(min_periods=20).quantile(quality_keep_quantile).shift(1))
        quality_threshold = quality_threshold.fillna(_safe_num(policy.get('base_quality_floor', 0.55), 0.55))
        low_floor = _safe_num(policy.get('low_vol_quality_floor', 0.80), 0.80)
        mid_floor = _safe_num(policy.get('mid_vol_quality_floor', 0.65), 0.65)
        high_floor = _safe_num(policy.get('high_vol_quality_floor', 0.55), 0.55)
        regime_floor = np.select(
            [df['vol_regime_label'] == 'LOW_VOL', df['vol_regime_label'] == 'HIGH_VOL'],
            [low_floor, high_floor],
            default=mid_floor,
        )
        low_keep = _safe_num(policy.get('low_vol_quality_keep_quantile', quality_keep_quantile), quality_keep_quantile)
        mid_keep = _safe_num(policy.get('mid_vol_quality_keep_quantile', min(quality_keep_quantile, 0.60)), min(quality_keep_quantile, 0.60))
        high_keep = _safe_num(policy.get('high_vol_quality_keep_quantile', min(mid_keep, 0.50)), min(mid_keep, 0.50))
        regime_quantile_relax = np.select(
            [df['vol_regime_label'] == 'LOW_VOL', df['vol_regime_label'] == 'HIGH_VOL'],
            [max(quality_keep_quantile - low_keep, 0.0), max(quality_keep_quantile - high_keep, 0.0)],
            default=max(quality_keep_quantile - mid_keep, 0.0),
        )
        quality_threshold = (
            quality_threshold - regime_quantile_relax * _safe_num(policy.get('quality_quantile_relax_scale', 0.30), 0.30)
        ).clip(lower=0.0)

        density_window = max(int(policy.get('density_window', quality_window) or quality_window), 20)
        raw_density = (df['raw_signal'] != 0.0).astype(float).rolling(window=density_window, min_periods=min(20, density_window)).mean().shift(1)
        raw_density = raw_density.fillna((df['raw_signal'] != 0.0).astype(float).expanding(min_periods=10).mean().shift(1)).fillna(0.0)

        base_cost_edge = float((_safe_num(settings.cost_bps) + _safe_num(settings.spread_bps)) / 10000.0)
        prelim_signal = (df['raw_signal'] != 0.0)
        prelim_signal &= (df['quality_score'] >= np.maximum(quality_threshold, regime_floor))
        prelim_signal &= (df['signal_strength'] * (df['rv'] / math.sqrt(float(ann))).replace([np.inf, -np.inf], np.nan).fillna(0.0) * df['liquidity_score_eval'] >= base_cost_edge)
        prelim_signal &= np.where(
            df['vol_regime_label'] == 'LOW_VOL',
            df['signal_strength'] >= _safe_num(policy.get('low_vol_signal_strength_min', 0.55), 0.55),
            True,
        )
        if bool(policy.get('low_vol_require_trend_alignment', True)):
            prelim_signal &= np.where(
                df['vol_regime_label'] == 'LOW_VOL',
                df['trend_align_eval'] >= 1.0,
                True,
            )
        accepted_density = prelim_signal.astype(float).rolling(window=density_window, min_periods=min(20, density_window)).mean().shift(1)
        accepted_density = accepted_density.fillna(prelim_signal.astype(float).expanding(min_periods=10).mean().shift(1)).fillna(0.0)
        density_target = _safe_num(policy.get('min_signal_density', 0.015), 0.015)
        density_pressure = ((density_target - accepted_density) / max(density_target, 1e-6)).clip(lower=0.0, upper=1.0)
        density_relax = density_pressure * _safe_num(policy.get('density_floor_relax', 0.12), 0.12)
        df['density_pressure_eval'] = density_pressure
        df['signal_rejection_ratio_eval'] = np.where(raw_density > 0.0, 1.0 - (accepted_density / raw_density.clip(lower=1e-6)), 0.0)

        if isinstance(df.index, pd.DatetimeIndex):
            hour_bucket = pd.Series(df.index.hour, index=df.index)
            raw_opps = (df['raw_signal'] != 0.0).astype(float)
            raw_hour_obs = raw_opps.groupby(hour_bucket).cumsum().shift(1).fillna(0.0)
            accepted_hour_obs = prelim_signal.astype(float).groupby(hour_bucket).cumsum().shift(1).fillna(0.0)
            hour_accept_ratio = ((accepted_hour_obs + 1.0) / (raw_hour_obs + 2.0)).clip(0.0, 1.0)
            global_accept_ratio = ((accepted_density + 1e-6) / raw_density.clip(lower=1e-6)).clip(0.0, 1.0)
            hour_density_pressure = ((global_accept_ratio - hour_accept_ratio) / global_accept_ratio.clip(lower=0.1)).clip(lower=0.0, upper=1.0)
            hour_density_pressure = np.where(raw_hour_obs >= _safe_num(policy.get('hour_min_observations', 8), 8), hour_density_pressure, 0.0)
            df['hour_density_pressure_eval'] = pd.Series(hour_density_pressure, index=df.index, dtype=float).fillna(0.0)
        else:
            df['hour_density_pressure_eval'] = 0.0

        df['quality_threshold'] = (
            np.maximum(quality_threshold, regime_floor)
            - density_relax
            - df['hour_density_pressure_eval'] * _safe_num(policy.get('hour_coverage_relax', 0.03), 0.03)
        ).clip(lower=_safe_num(policy.get('base_quality_floor', 0.55), 0.55))

        per_bar_vol = (df['rv'] / math.sqrt(float(ann))).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        df['expected_edge'] = (df['signal_strength'] * per_bar_vol * df['liquidity_score_eval']).fillna(0.0)
        base_buffer = _safe_num(policy.get('cost_edge_buffer', 1.10), 1.10)
        min_buffer = max(_safe_num(policy.get('density_floor_cost_buffer_min', 1.0), 1.0), 1.0)
        dynamic_cost_buffer = base_buffer - density_pressure * max(base_buffer - min_buffer, 0.0)
        df['estimated_cost_edge'] = base_cost_edge * dynamic_cost_buffer

        allow_signal = (df['raw_signal'] != 0.0)
        allow_signal &= (df['quality_score'] >= df['quality_threshold'])
        allow_signal &= (df['expected_edge'] >= df['estimated_cost_edge'])
        allow_signal &= np.where(
            df['vol_regime_label'] == 'LOW_VOL',
            df['signal_strength'] >= _safe_num(policy.get('low_vol_signal_strength_min', 0.55), 0.55),
            True,
        )
        if bool(policy.get('low_vol_require_trend_alignment', True)):
            allow_signal &= np.where(
                df['vol_regime_label'] == 'LOW_VOL',
                df['trend_align_eval'] >= 1.0,
                True,
            )
        df['policy_allowed'] = allow_signal.astype(bool)
        df['raw_signal'] = np.where(df['policy_allowed'], df['raw_signal'], 0.0)
    else:
        df['signal_strength_raw'] = 0.0
        df['signal_strength'] = 0.0
        df['vol_regime_z_eval'] = 0.0
        df['atr_ratio_eval'] = 0.0
        df['atr_percentile_eval'] = 0.5
        df['trend_strength_eval'] = 0.0
        df['trend_strength_norm'] = 0.0
        df['trend_align_eval'] = 0.0
        df['signal_persistence_eval'] = 0.0
        df['feature_consensus_proxy'] = 0.0
        df['liquidity_score_eval'] = 1.0
        df['vol_regime_label'] = 'MID_VOL'
        df['quality_score'] = 0.0
        df['quality_threshold'] = 0.0
        df['expected_edge'] = 0.0
        df['estimated_cost_edge'] = 0.0
        df['policy_allowed'] = df['raw_signal'] != 0.0
        df['regime_heat_eval'] = 0.5
        df['density_pressure_eval'] = 0.0
        df['signal_rejection_ratio_eval'] = 0.0
        df['hour_density_pressure_eval'] = 0.0

    # position sizing: vol scaling
    vol_scale = settings.vol_target / df['rv']
    vol_scale = vol_scale.clip(upper=settings.leverage_cap)
    if bool(policy):
        heat = df['regime_heat_eval'].clip(0.0, 1.0)
        low_anchor = _safe_num(policy.get('low_vol_size_mult', 0.35), 0.35)
        mid_low = _safe_num(policy.get('mid_vol_size_mult_low', 0.90), 0.90)
        mid_high = _safe_num(policy.get('mid_vol_size_mult_high', 1.10), 1.10)
        high_low = _safe_num(policy.get('high_vol_size_mult_low', 1.10), 1.10)
        size_mult = np.where(
            heat <= 0.5,
            _blend(low_anchor, mid_high, heat / 0.5),
            _blend(mid_low, high_low, (heat - 0.5) / 0.5),
        )
        size_mult = pd.Series(size_mult, index=df.index, dtype=float).fillna(_safe_num(policy.get('mid_vol_size_mult', 1.0), 1.0))
        quality_size_floor = _safe_num(policy.get('quality_size_floor', 0.50), 0.50)
        df['size_multiplier_eval'] = pd.to_numeric(
            size_mult * (quality_size_floor + (1.0 - quality_size_floor) * df['quality_score']),
            errors='coerce',
        ).fillna(0.0)
    else:
        df['size_multiplier_eval'] = 1.0
    df['pos'] = (df['raw_signal'] * vol_scale * df['size_multiplier_eval']).clip(lower=-settings.leverage_cap, upper=settings.leverage_cap)

    # compute turnover and costs
    df['pos_prev'] = df['pos'].shift(1).fillna(0.0)
    df['turnover'] = (df['pos'] - df['pos_prev']).abs()
    # transaction cost bps -> convert to log return adjustment: bps/10000
    per_turn_cost = settings.cost_bps / 10000.0
    df['tcost'] = df['turnover'] * per_turn_cost
    # spread cost when sign changes (trade direction)
    sign_change = (np.sign(df['pos']) != np.sign(df['pos_prev'])) & (df['pos'] != 0)
    df['scost'] = 0.0
    df.loc[sign_change, 'scost'] = settings.spread_bps / 10000.0
    df['costs'] = df['tcost'] + df['scost']

    # strategy return approx: pos_prev * ret - costs * sign (costs taken absolute)
    df['strat_ret'] = df['pos_prev'] * df['ret'] - df['costs']

    # equity (log) and simple
    df['equity_log'] = df['strat_ret'].cumsum()
    df['equity'] = np.exp(df['equity_log'])

    def _safe_float(x, *, allow_none: bool = True, clamp: tuple[float, float] | None = None) -> float | None:
        if x is None:
            return None if allow_none else 0.0
        try:
            v = float(x)
        except Exception:
            return None if allow_none else 0.0
        if not np.isfinite(v):
            return None if allow_none else 0.0
        if clamp is not None:
            lo, hi = clamp
            if v < lo:
                v = lo
            if v > hi:
                v = hi
        return v

    # metrics
    perf = df['strat_ret']
    mean_ann = perf.mean() * ann
    vol_ann = perf.std(ddof=0) * math.sqrt(ann)
    sharpe = float(mean_ann / vol_ann) if vol_ann > 0 else 0.0

    # downside volatility for Sortino
    neg = perf[perf < 0]
    down_vol = neg.std(ddof=0) * math.sqrt(ann) if len(neg) > 0 else 0.0
    sortino = float(mean_ann / down_vol) if down_vol > 0 else 0.0

    # max drawdown on equity series
    roll_max = df['equity'].cummax()
    drawdown = df['equity'] / roll_max - 1.0
    raw_max_dd = float(drawdown.min())
    # expose max_drawdown as positive fraction (0..1+) to satisfy schema expectations
    max_dd = abs(raw_max_dd)
    # clamp to reasonable range expected by MetricsSummary validators
    if max_dd < 0:
        max_dd = 0.0
    if max_dd > 10.0:
        max_dd = 10.0

    # CAGR
    total_periods = len(df)
    years = total_periods / ann
    cagr = float(df['equity'].iloc[-1] ** (1.0 / years) - 1.0) if years > 0 else 0.0

    # Calmar = CAGR / abs(MaxDD)
    calmar = float(cagr / max_dd) if max_dd > 0 else float('inf')

    # Profit factor (PF) = gross profit / gross loss
    gains = df.loc[df['strat_ret'] > 0, 'strat_ret'].sum()
    losses = -df.loc[df['strat_ret'] < 0, 'strat_ret'].sum()
    pf = float(gains / losses) if losses > 0 else float('inf')

    # Cost coverage: Net PnL / Gross PnL
    # gross_ret stream excludes costs; net is strat_ret.
    gross_stream = (df['pos_prev'] * df['ret']) if 'pos_prev' in df.columns else (0.0 * df['ret'])
    net_pnl = float(df['strat_ret'].sum()) if len(df) else 0.0
    gross_pnl = float(gross_stream.sum()) if len(df) else 0.0
    if abs(gross_pnl) > 1e-12:
        net_to_gross = float(net_pnl / gross_pnl)
    else:
        net_to_gross = None

    # CVaR 95/99 (expected shortfall)
    # Calibrate to avoid pathological inflation when the strategy is mostly flat.
    # If we include long flat stretches (zero returns), per-bar sigma collapses,
    # while tails remain driven by occasional trading costs -> huge ratios.
    # For gating purposes we therefore compute tail risk on "active" bars.
    try:
        eps = 1e-12
        active = (df.get('pos_prev', 0.0).astype(float).abs() > eps) | (df.get('turnover', 0.0).astype(float) > eps)
        perf_active = perf[active] if isinstance(active, (pd.Series, np.ndarray, list)) else perf
        if perf_active is None or len(perf_active) < 20:
            perf_active = perf
    except Exception:
        perf_active = perf

    q99 = perf_active.quantile(0.01) if len(perf_active) else 0.0
    tail99 = perf_active[perf_active <= q99] if len(perf_active) else perf_active
    cvar99 = float(tail99.mean()) if tail99 is not None and len(tail99) else 0.0

    q95 = perf_active.quantile(0.05) if len(perf_active) else 0.0
    tail95 = perf_active[perf_active <= q95] if len(perf_active) else perf_active
    cvar95 = float(tail95.mean()) if tail95 is not None and len(tail95) else 0.0

    # Tail-risk proxy in sigma units (frequency-agnostic)
    vol_bar_active = float(perf_active.std(ddof=0)) if perf_active is not None and len(perf_active) else 0.0
    cvar99_sigma = float(abs(cvar99) / (vol_bar_active + 1e-12)) if vol_bar_active > 0 else 0.0

    # daily vol (from annualized vol)
    daily_vol = float(vol_ann / math.sqrt(252.0)) if vol_ann > 0 else 0.0
    cvar95_over_daily_vol = float(abs(cvar95) / (daily_vol + 1e-12)) if daily_vol > 0 else 0.0

    # turnover and hit rate
    turnover = float(df['turnover'].sum())
    turnover_per_day = float(df['turnover'].astype(float).mean() * bars_per_day) if 'turnover' in df.columns and len(df) else 0.0
    avg_gross = float(df['pos'].astype(float).abs().mean()) if 'pos' in df.columns and len(df) else 0.0
    trades = int((df['pos'] != df['pos_prev']).sum())
    hit_rate = float((np.sign(df['pos_prev']).shift(-1).fillna(0) * np.sign(df['ret']) > 0).sum()) / trades if trades > 0 else 0.0

    avg_net_trade_return = float(net_pnl / trades) if trades > 0 else 0.0

    # stress metrics with inflated costs
    stress_settings = EvalSettings(**{**settings.__dict__, 'cost_bps': settings.cost_bps * settings.stress_cost_multiplier, 'spread_bps': settings.spread_bps * settings.stress_cost_multiplier})
    df_s = df.copy()
    df_s['scost'] = 0.0
    sign_change = (np.sign(df_s['pos']) != np.sign(df_s['pos_prev'])) & (df_s['pos'] != 0)
    df_s.loc[sign_change, 'scost'] = stress_settings.spread_bps / 10000.0
    df_s['tcost'] = df_s['turnover'] * (stress_settings.cost_bps / 10000.0)
    df_s['costs'] = df_s['tcost'] + df_s['scost']
    df_s['strat_ret'] = df_s['pos_prev'] * df_s['ret'] - df_s['costs']
    perf_s = df_s['strat_ret']
    mean_ann_s = perf_s.mean() * ann
    vol_ann_s = perf_s.std(ddof=0) * math.sqrt(ann)
    sharpe_s = float(mean_ann_s / vol_ann_s) if vol_ann_s > 0 else 0.0

    # assemble MetricsSummary
    meta = MetricsMetadata(
        cost_bps=_safe_float(settings.cost_bps),
        spread_bps=_safe_float(settings.spread_bps),
        sample_start=df.index.min().to_pydatetime() if len(df.index) else None,
        sample_end=df.index.max().to_pydatetime() if len(df.index) else None,
    )
    ms = MetricsSummary(
        n_trades=trades,
        sharpe=_safe_float(sharpe),
        sortino=_safe_float(sortino),
        max_drawdown=_safe_float(max_dd, clamp=(0.0, 10.0)),
        calmar=_safe_float(calmar),
        profit_factor=_safe_float(pf),
        cagr=_safe_float(cagr),
        net_to_gross=_safe_float(net_to_gross),
        cvar_99=_safe_float(cvar99),
        cvar_99_sigma=_safe_float(cvar99_sigma),
        cvar_95=_safe_float(cvar95),
        daily_vol=_safe_float(daily_vol),
        cvar_95_over_daily_vol=_safe_float(cvar95_over_daily_vol),
        turnover=_safe_float(turnover),
        turnover_per_day=_safe_float(turnover_per_day),
        avg_gross_exposure=_safe_float(avg_gross),
        hit_rate=_safe_float(hit_rate),
        avg_net_trade_return=_safe_float(avg_net_trade_return),
        metadata=meta,
    )

    stress = MetricsSummary(
        n_trades=trades,
        sharpe=_safe_float(sharpe_s),
        sortino=_safe_float(0.0),
        max_drawdown=_safe_float(abs((df_s['equity'] / df_s['equity'].cummax() - 1.0).min()), clamp=(0.0, 10.0)),
        calmar=_safe_float(0.0),
        profit_factor=_safe_float(0.0),
        cagr=_safe_float(0.0),
        cvar_99=_safe_float(perf_s.quantile(0.01)) if len(perf_s) else 0.0,
        cvar_99_sigma=None,
        turnover=_safe_float(float(df_s['turnover'].sum())),
        turnover_per_day=_safe_float(float(df_s['turnover'].astype(float).mean() * bars_per_day)) if 'turnover' in df_s.columns and len(df_s) else 0.0,
        avg_gross_exposure=_safe_float(float(df_s['pos'].astype(float).abs().mean())) if 'pos' in df_s.columns and len(df_s) else 0.0,
        hit_rate=_safe_float(hit_rate),
        metadata=meta,
    )

    return {
        'df': df,
        'metrics': ms,
        'stress_metrics': stress,
    }


def save_metrics_report(symbol: str, metrics: MetricsSummary, stress_metrics: MetricsSummary, out_path: str) -> None:
    payload = {
        'symbol': symbol,
        'metrics': metrics.dict(),
        'stress_metrics': stress_metrics.dict(),
    }
    with open(out_path, 'w') as fh:
        json.dump(payload, fh, indent=2, default=lambda o: o if not hasattr(o, '__dict__') else o.__dict__)
