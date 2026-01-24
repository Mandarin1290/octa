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
        l = pred.expanding(min_periods=50).quantile(lower_q).shift(1)
        return u, l
    win = int(window)
    if win <= 1:
        # degenerate: effectively expanding
        u = pred.expanding(min_periods=50).quantile(upper_q).shift(1)
        l = pred.expanding(min_periods=50).quantile(lower_q).shift(1)
        return u, l
    u = pred.rolling(win, min_periods=min(50, win)).quantile(upper_q).shift(1)
    l = pred.rolling(win, min_periods=min(50, win)).quantile(lower_q).shift(1)
    return u, l


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


def compute_equity_and_metrics(prices: pd.Series, preds: pd.Series, settings: EvalSettings) -> Dict[str, Any]:
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
        else:
            u = df['pred'].quantile(settings.upper_q)
            l = df['pred'].quantile(settings.lower_q)
        raw = pd.Series(0.0, index=df.index)
        # Classification target y_cls_* is 1 for positive forward return.
        # Therefore higher predicted probability should map to LONG, lower to SHORT.
        if bool(getattr(settings, 'causal_quantiles', False)):
            raw[df['pred'] > u_s] = 1.0
            raw[df['pred'] < l_s] = -1.0
        else:
            raw[df['pred'] > u] = 1.0
            raw[df['pred'] < l] = -1.0
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
        else:
            up = df['pred'].quantile(settings.upper_q)
            low = df['pred'].quantile(settings.lower_q)
        raw = np.sign(df['pred'] - median)
        raw = pd.Series(raw, index=df.index)
        if bool(getattr(settings, 'causal_quantiles', False)):
            raw[df['pred'] > up_s] = 1.0
            raw[df['pred'] < low_s] = -1.0
        else:
            raw[df['pred'] > up] = 1.0
            raw[df['pred'] < low] = -1.0

    df['raw_signal'] = raw

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

    # position sizing: vol scaling
    vol_scale = settings.vol_target / df['rv']
    vol_scale = vol_scale.clip(upper=settings.leverage_cap)
    df['pos'] = df['raw_signal'] * vol_scale

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
