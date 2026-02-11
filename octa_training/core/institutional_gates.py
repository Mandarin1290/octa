from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from octa_training.core.evaluation import compute_equity_and_metrics, infer_frequency


@dataclass(frozen=True)
class TimeWindowPolicy:
    timeframe: str
    train_bars: int
    oos_bars: int
    min_bars_regime: int
    liquidity_window: int
    vol_window: int


_TIMEFRAME_POLICY: Dict[str, TimeWindowPolicy] = {
    "1D": TimeWindowPolicy("1D", train_bars=252, oos_bars=63, min_bars_regime=150, liquidity_window=252, vol_window=20),
    "1H": TimeWindowPolicy("1H", train_bars=24 * 120, oos_bars=24 * 20, min_bars_regime=24 * 40, liquidity_window=24 * 20, vol_window=24),
    "30M": TimeWindowPolicy("30M", train_bars=13 * 120, oos_bars=13 * 20, min_bars_regime=13 * 40, liquidity_window=13 * 20, vol_window=26),
    "5M": TimeWindowPolicy("5M", train_bars=78 * 80, oos_bars=78 * 15, min_bars_regime=78 * 25, liquidity_window=78 * 20, vol_window=78),
    "1M": TimeWindowPolicy("1M", train_bars=390 * 60, oos_bars=390 * 10, min_bars_regime=390 * 20, liquidity_window=390 * 20, vol_window=120),
}


def _infer_timeframe_key(index: pd.Index) -> str:
    try:
        if not isinstance(index, pd.DatetimeIndex) or len(index) < 2:
            return "1D"
        deltas = np.diff(index.astype("int64"))
        sec = float(np.median(deltas) / 1e9)
        if sec >= 20 * 3600:
            return "1D"
        if sec >= 50 * 60:
            return "1H"
        if sec >= 20 * 60:
            return "30M"
        if sec >= 4 * 60:
            return "5M"
        return "1M"
    except Exception:
        return "1D"


def _policy_for(timeframe: Optional[str], index: pd.Index) -> TimeWindowPolicy:
    tf = str(timeframe or "").upper().strip()
    if not tf:
        tf = _infer_timeframe_key(index)
    return _TIMEFRAME_POLICY.get(tf, _TIMEFRAME_POLICY["1D"])


def _max_drawdown_from_returns(r: np.ndarray) -> float:
    if r.size == 0:
        return float("nan")
    eq = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(eq)
    dd = 1.0 - (eq / peak)
    return float(np.max(dd))


def _profit_factor_from_returns(r: np.ndarray) -> float:
    if r.size == 0:
        return float("nan")
    gains = float(np.sum(r[r > 0.0]))
    losses = float(-np.sum(r[r < 0.0]))
    if losses <= 0.0:
        return 10.0
    return float(gains / losses)


def _segment_metrics(
    df_backtest: pd.DataFrame,
    idx: np.ndarray,
) -> Dict[str, float]:
    seg = df_backtest.iloc[idx]
    ret = pd.to_numeric(seg.get("strat_ret"), errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).dropna()
    if ret.empty:
        return {
            "pf": float("nan"),
            "sharpe": float("nan"),
            "max_drawdown": float("nan"),
            "win_rate": float("nan"),
            "avg_trade": float("nan"),
            "turnover": float("nan"),
            "n_bars": int(len(seg)),
            "n_trades": 0,
        }
    ann = float(infer_frequency(seg.index)) if isinstance(seg.index, pd.DatetimeIndex) else 252.0
    mu = float(np.mean(ret.values))
    sd = float(np.std(ret.values, ddof=0))
    sharpe = float((mu / sd) * np.sqrt(ann)) if sd > 0 else 0.0
    turnover = pd.to_numeric(seg.get("turnover"), errors="coerce").astype(float) if "turnover" in seg.columns else pd.Series(np.zeros(len(seg), dtype=float), index=seg.index)
    trade_mask = (turnover.abs() > 0.0).to_numpy()
    trade_rets = ret.values[trade_mask[: len(ret.values)]]
    if trade_rets.size == 0:
        trade_rets = ret.values[ret.values != 0.0]
    avg_trade = float(np.mean(trade_rets)) if trade_rets.size else 0.0
    win_rate = float(np.mean(ret.values > 0.0))
    return {
        "pf": _profit_factor_from_returns(ret.values),
        "sharpe": float(sharpe),
        "max_drawdown": _max_drawdown_from_returns(ret.values),
        "win_rate": float(win_rate),
        "avg_trade": float(avg_trade),
        "turnover": float(np.nanmean(turnover.abs().values)) if len(turnover) else 0.0,
        "n_bars": int(len(seg)),
        "n_trades": int(np.sum(trade_mask)),
    }


def evaluate_walk_forward_oos(
    df_backtest: pd.DataFrame,
    gate: Any,
    *,
    timeframe: Optional[str] = None,
) -> Dict[str, Any]:
    if not isinstance(df_backtest, pd.DataFrame) or "strat_ret" not in df_backtest.columns:
        return {
            "enabled": True,
            "passed": False,
            "reason": "walkforward_missing_strat_ret",
            "walkforward_pass": False,
            "walkforward_meta": {"folds": 0, "fold_metrics": []},
        }
    if not isinstance(df_backtest.index, pd.DatetimeIndex) or not df_backtest.index.is_monotonic_increasing:
        return {
            "enabled": True,
            "passed": False,
            "reason": "walkforward_invalid_time_order",
            "walkforward_pass": False,
            "walkforward_meta": {"folds": 0, "fold_metrics": []},
        }

    pol = _policy_for(timeframe, df_backtest.index)
    n = int(len(df_backtest))
    min_3 = int(pol.train_bars + (3 * pol.oos_bars))
    min_2 = int(pol.train_bars + (2 * pol.oos_bars))
    if n >= min_3:
        fold_count = 3
    elif n >= min_2:
        fold_count = 2
    else:
        return {
            "enabled": True,
            "passed": False,
            "reason": "insufficient_history_for_walkforward",
            "walkforward_pass": False,
            "walkforward_meta": {
                "folds": 0,
                "history_bars": n,
                "required_bars_for_2": min_2,
                "required_bars_for_3": min_3,
                "timeframe": pol.timeframe,
                "fold_metrics": [],
            },
        }

    folds: List[Dict[str, Any]] = []
    fold_passes = 0
    oos_pf_min = float(getattr(gate, "walkforward_oos_pf_min", None) or (float(getattr(gate, "profit_factor_min", 1.0)) * float(getattr(gate, "walkforward_oos_pf_scale", 0.95))))
    oos_sharpe_min = float(getattr(gate, "walkforward_oos_sharpe_min", None) or (float(getattr(gate, "sharpe_min", 0.0)) * float(getattr(gate, "walkforward_oos_sharpe_scale", 0.90))))
    oos_dd_limit = float(getattr(gate, "walkforward_oos_maxdd_max", None) or (float(getattr(gate, "max_drawdown_max", 1.0)) * float(getattr(gate, "walkforward_oos_dd_scale", 1.0))))

    global_end = n - 1
    for i in range(fold_count):
        oos_end = int(global_end - ((fold_count - 1 - i) * pol.oos_bars))
        oos_start = int(oos_end - pol.oos_bars + 1)
        train_end = int(oos_start - 1)
        train_start = int(max(0, train_end - pol.train_bars + 1))
        if train_end <= train_start or oos_start < 0:
            return {
                "enabled": True,
                "passed": False,
                "reason": "walkforward_window_construction_failed",
                "walkforward_pass": False,
                "walkforward_meta": {"folds": len(folds), "fold_metrics": folds, "timeframe": pol.timeframe},
            }

        train_idx = np.arange(train_start, train_end + 1)
        oos_idx = np.arange(oos_start, oos_end + 1)
        is_metrics = _segment_metrics(df_backtest, train_idx)
        oos_metrics = _segment_metrics(df_backtest, oos_idx)
        fold_ok = bool(
            np.isfinite(oos_metrics["pf"])
            and np.isfinite(oos_metrics["sharpe"])
            and np.isfinite(oos_metrics["max_drawdown"])
            and (oos_metrics["pf"] >= oos_pf_min)
            and (oos_metrics["sharpe"] >= oos_sharpe_min)
            and (oos_metrics["max_drawdown"] <= oos_dd_limit)
        )
        if fold_ok:
            fold_passes += 1
        folds.append(
            {
                "fold": int(i),
                "train_start": str(df_backtest.index[train_start]),
                "train_end": str(df_backtest.index[train_end]),
                "oos_start": str(df_backtest.index[oos_start]),
                "oos_end": str(df_backtest.index[oos_end]),
                "train_size": int(train_idx.size),
                "oos_size": int(oos_idx.size),
                "is_metrics": {k: (round(v, 10) if isinstance(v, float) and np.isfinite(v) else v) for k, v in is_metrics.items()},
                "oos_metrics": {k: (round(v, 10) if isinstance(v, float) and np.isfinite(v) else v) for k, v in oos_metrics.items()},
                "passed": bool(fold_ok),
            }
        )

    min_ratio = float(getattr(gate, "walkforward_min_fold_pass_ratio", 1.0) or 1.0)
    pass_ratio = float(fold_passes / float(fold_count))
    passed = bool(pass_ratio >= min_ratio)
    return {
        "enabled": True,
        "passed": passed,
        "reason": None if passed else "walkforward_oos_threshold_failed",
        "walkforward_pass": passed,
        "walkforward_meta": {
            "timeframe": pol.timeframe,
            "global_end": str(df_backtest.index[global_end]),
            "folds": int(fold_count),
            "folds_passed": int(fold_passes),
            "fold_pass_ratio": round(pass_ratio, 10),
            "thresholds": {
                "oos_pf_min": round(oos_pf_min, 10),
                "oos_sharpe_min": round(oos_sharpe_min, 10),
                "oos_maxdd_max": round(oos_dd_limit, 10),
                "min_fold_pass_ratio": round(min_ratio, 10),
            },
            "window_policy": {
                "train_bars": int(pol.train_bars),
                "oos_bars": int(pol.oos_bars),
            },
            "fold_metrics": folds,
        },
    }


def evaluate_regime_stability(
    df_backtest: pd.DataFrame,
    gate: Any,
    *,
    timeframe: Optional[str] = None,
    walkforward_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not isinstance(df_backtest, pd.DataFrame) or "ret" not in df_backtest.columns or "strat_ret" not in df_backtest.columns:
        return {"enabled": True, "passed": False, "reason": "regime_missing_required_columns", "regime_meta": {}}
    pol = _policy_for(timeframe, df_backtest.index)

    use_idx: np.ndarray
    try:
        fold_rows = (walkforward_meta or {}).get("fold_metrics") if isinstance(walkforward_meta, dict) else None
        rows: List[int] = []
        if isinstance(fold_rows, list):
            for f in fold_rows:
                if not isinstance(f, dict):
                    continue
                oos_start = pd.Timestamp(str(f.get("oos_start")))
                oos_end = pd.Timestamp(str(f.get("oos_end")))
                mask = (df_backtest.index >= oos_start) & (df_backtest.index <= oos_end)
                rows.extend(np.flatnonzero(mask).tolist())
        if rows:
            use_idx = np.array(sorted(set(rows)), dtype=int)
        else:
            use_idx = np.arange(len(df_backtest), dtype=int)
    except Exception:
        use_idx = np.arange(len(df_backtest), dtype=int)

    if int(use_idx.size) < int(pol.min_bars_regime):
        return {
            "enabled": True,
            "passed": False,
            "reason": "regime_split_insufficient_bars",
            "regime_meta": {"timeframe": pol.timeframe, "bars": int(use_idx.size), "min_bars": int(pol.min_bars_regime)},
        }

    oos_df = df_backtest.iloc[use_idx].copy()
    ret = pd.to_numeric(oos_df["ret"], errors="coerce").astype(float)
    rv = ret.rolling(window=int(pol.vol_window), min_periods=max(5, pol.vol_window // 2)).std(ddof=0)
    if rv.dropna().empty:
        return {"enabled": True, "passed": False, "reason": "regime_split_insufficient_bars", "regime_meta": {"timeframe": pol.timeframe, "bars": int(use_idx.size)}}
    q1 = float(rv.quantile(0.33))
    q2 = float(rv.quantile(0.66))
    if not np.isfinite(q1) or not np.isfinite(q2):
        return {"enabled": True, "passed": False, "reason": "regime_quantiles_invalid", "regime_meta": {"timeframe": pol.timeframe}}

    labels = pd.Series(index=oos_df.index, dtype="object")
    labels.loc[rv <= q1] = "low"
    labels.loc[(rv > q1) & (rv <= q2)] = "mid"
    labels.loc[rv > q2] = "high"
    oos_df["regime"] = labels.fillna("mid")

    regime_metrics: Dict[str, Dict[str, Any]] = {}
    reasons: List[str] = []
    sharpe_vals: Dict[str, float] = {}
    dd_limit = float(getattr(gate, "regime_dd_limit", None) or float(getattr(gate, "max_drawdown_max", 1.0)))
    worst_pf_min = float(getattr(gate, "regime_pf_min_worst", 1.0) or 1.0)
    base_pf_min = float(getattr(gate, "regime_pf_min", 1.1) or 1.1)
    collapse_ratio = float(getattr(gate, "regime_sharpe_collapse_ratio", 0.35) or 0.35)

    for regime in ("low", "mid", "high"):
        seg = oos_df.loc[oos_df["regime"] == regime]
        if seg.empty:
            continue
        m = _segment_metrics(seg, np.arange(len(seg), dtype=int))
        sharpe_vals[regime] = float(m.get("sharpe", 0.0) or 0.0)
        pf_min = worst_pf_min if regime == "high" else base_pf_min
        regime_pass = bool(
            np.isfinite(m.get("max_drawdown", float("nan")))
            and np.isfinite(m.get("pf", float("nan")))
            and (float(m["max_drawdown"]) <= dd_limit)
            and (float(m["pf"]) >= pf_min)
        )
        regime_metrics[regime] = {
            **{k: (round(v, 10) if isinstance(v, float) and np.isfinite(v) else v) for k, v in m.items()},
            "pf_min": round(pf_min, 10),
            "regime_pass": bool(regime_pass),
        }
        if not regime_pass:
            reasons.append(f"regime_{regime}_failed")

    if not regime_metrics:
        return {"enabled": True, "passed": False, "reason": "regime_split_insufficient_bars", "regime_meta": {"timeframe": pol.timeframe}}

    sharpes = [v for v in sharpe_vals.values() if np.isfinite(v)]
    if sharpes:
        hi = max(sharpes)
        lo = min(sharpes)
        if hi > 0 and lo < (hi * collapse_ratio):
            reasons.append("regime_sharpe_collapse")

    passed = len(reasons) == 0
    return {
        "enabled": True,
        "passed": passed,
        "reason": None if passed else reasons[0],
        "reasons": reasons,
        "regime_meta": {
            "timeframe": pol.timeframe,
            "vol_window": int(pol.vol_window),
            "quantiles": {"q33": round(q1, 10), "q66": round(q2, 10)},
            "regime_dd_limit": round(dd_limit, 10),
            "regime_pf_min": round(base_pf_min, 10),
            "regime_pf_min_worst": round(worst_pf_min, 10),
            "regime_sharpe_collapse_ratio": round(collapse_ratio, 10),
            "regime_metrics": regime_metrics,
        },
    }


def evaluate_cost_stress(
    prices: pd.Series,
    preds: pd.Series,
    settings: Any,
    gate: Any,
) -> Dict[str, Any]:
    try:
        from copy import deepcopy

        s2 = deepcopy(settings)
    except Exception:
        s2 = settings
    base_cost = float(getattr(settings, "cost_bps", 0.0))
    base_spread = float(getattr(settings, "spread_bps", 0.0))
    s2.cost_bps = base_cost * 2.0
    s2.spread_bps = base_spread * 2.0

    out = compute_equity_and_metrics(prices, preds, s2)
    m = out["metrics"]
    ann = float(infer_frequency(out["df"].index)) if isinstance(out.get("df"), pd.DataFrame) else 252.0
    monthly_net = float(out["df"]["strat_ret"].mean() * ann / 12.0) if isinstance(out.get("df"), pd.DataFrame) and len(out["df"]) else float("nan")
    stress_pf = float(getattr(m, "profit_factor", 0.0) or 0.0)
    stress_dd = float(getattr(m, "max_drawdown", 10.0) or 10.0)
    stress_pf_min = float(getattr(gate, "stress_pf_min", 1.05) or 1.05)
    stress_dd_limit = float(getattr(gate, "stress_dd_limit", None) or (float(getattr(gate, "max_drawdown_max", 1.0)) * float(getattr(gate, "stress_dd_mult", 1.25))))
    passed = bool(
        (stress_pf >= stress_pf_min)
        and (stress_dd <= stress_dd_limit)
        and (np.isfinite(monthly_net) and monthly_net > 0.0)
    )
    reasons: List[str] = []
    if stress_pf < stress_pf_min:
        reasons.append("stress_pf_below_min")
    if stress_dd > stress_dd_limit:
        reasons.append("stress_dd_above_limit")
    if not np.isfinite(monthly_net) or monthly_net <= 0.0:
        reasons.append("stress_monthly_net_non_positive")
    return {
        "enabled": True,
        "passed": passed,
        "reason": None if passed else reasons[0],
        "reasons": reasons,
        "metrics": {
            "pf_stress": round(stress_pf, 10),
            "sharpe_stress": round(float(getattr(m, "sharpe", 0.0) or 0.0), 10),
            "dd_stress": round(stress_dd, 10),
            "avg_trade_stress": round(float(getattr(m, "avg_trade", 0.0) or 0.0), 10),
            "monthly_net_return_stress": round(float(monthly_net), 10) if np.isfinite(monthly_net) else None,
        },
        "config": {
            "spread_multiplier": 2.0,
            "slippage_multiplier": 2.0,
            "base_cost_bps": round(base_cost, 10),
            "base_spread_bps": round(base_spread, 10),
            "stress_pf_min": round(stress_pf_min, 10),
            "stress_dd_limit": round(stress_dd_limit, 10),
        },
    }


def evaluate_liquidity_gate(
    source_df: pd.DataFrame,
    *,
    timeframe: Optional[str],
    gate: Any,
    asset_class: Optional[str],
) -> Dict[str, Any]:
    if not isinstance(source_df, pd.DataFrame):
        return {"enabled": True, "passed": False, "reason": "liquidity_missing_source_df", "liquidity_meta": {}}
    pol = _policy_for(timeframe, source_df.index)
    vol = pd.to_numeric(source_df.get("volume"), errors="coerce").astype(float) if "volume" in source_df.columns else None
    ac = str(asset_class or "").lower()
    if vol is None or vol.dropna().empty:
        if ac in {"fx", "forex"}:
            return {
                "enabled": True,
                "passed": True,
                "reason": None,
                "liquidity_unknown": True,
                "liquidity_method": "na",
                "liquidity_meta": {"method": "na", "timeframe": pol.timeframe, "window": int(pol.liquidity_window), "threshold_percentile": float(getattr(gate, "liquidity_percentile_min", 40.0) or 40.0)},
            }
        return {"enabled": True, "passed": False, "reason": "liquidity_volume_missing_non_fx", "liquidity_meta": {"method": "missing_volume"}}

    vol = vol.replace([np.inf, -np.inf], np.nan).dropna()
    if len(vol) < max(30, pol.liquidity_window // 4):
        return {"enabled": True, "passed": False, "reason": "liquidity_insufficient_history", "liquidity_meta": {"bars": int(len(vol)), "required": int(max(30, pol.liquidity_window // 4))}}

    w = int(pol.liquidity_window)
    values = vol.to_numpy(dtype=float, copy=False)
    pranks = np.empty(values.shape[0], dtype=float)
    for i in range(values.shape[0]):
        s = max(0, i - w + 1)
        sample = values[s : i + 1]
        pranks[i] = 100.0 * float(np.sum(sample <= values[i])) / float(sample.size)
    median_pct = float(np.median(pranks[-w:])) if pranks.size >= w else float(np.median(pranks))
    threshold = float(getattr(gate, "liquidity_percentile_min", 40.0) or 40.0)
    passed = bool(median_pct >= threshold)
    return {
        "enabled": True,
        "passed": passed,
        "reason": None if passed else "liquidity_percentile_below_threshold",
        "liquidity_unknown": False,
        "liquidity_method": "volume_percentile",
        "liquidity_meta": {
            "method": "volume_percentile",
            "timeframe": pol.timeframe,
            "window": w,
            "median_volume_percentile": round(median_pct, 10),
            "threshold_percentile": round(threshold, 10),
        },
    }


def evaluate_cross_timeframe_consistency(stages: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    stage_by_tf = {str(s.get("timeframe", "")).upper(): s for s in stages if isinstance(s, dict)}
    checks: List[Dict[str, Any]] = []
    required = ("1D", "1H", "30M", "5M", "1M")
    if not all(tf in stage_by_tf for tf in required):
        return {
            "executed": True,
            "passed": False,
            "reason": "cross_tf_missing_stages",
            "checks": [],
            "inputs_used": sorted(stage_by_tf.keys()),
        }

    def _dir(stage: Dict[str, Any]) -> float:
        m = stage.get("metrics_summary") or {}
        cagr = m.get("cagr")
        if isinstance(cagr, (int, float)) and np.isfinite(float(cagr)):
            return float(cagr)
        avg_trade = m.get("avg_trade")
        if isinstance(avg_trade, (int, float)) and np.isfinite(float(avg_trade)):
            return float(avg_trade)
        sharpe = m.get("sharpe")
        if isinstance(sharpe, (int, float)) and np.isfinite(float(sharpe)):
            return float(sharpe)
        return 0.0

    d_1d = _dir(stage_by_tf["1D"])
    d_1h = _dir(stage_by_tf["1H"])
    directional_ok = not (d_1d * d_1h < 0.0)
    checks.append({"name": "directional_alignment_1D_1H", "passed": bool(directional_ok), "value_1d": round(d_1d, 10), "value_1h": round(d_1h, 10)})

    dd_1h = float((stage_by_tf["1H"].get("metrics_summary") or {}).get("max_drawdown") or 0.0)
    intraday_ok = True
    risk_rows: List[Dict[str, Any]] = []
    if dd_1h <= 0:
        intraday_ok = False
    else:
        for tf in ("30M", "5M", "1M"):
            dd = float((stage_by_tf[tf].get("metrics_summary") or {}).get("max_drawdown") or 0.0)
            ok = dd <= (1.2 * dd_1h)
            intraday_ok = intraday_ok and ok
            risk_rows.append({"timeframe": tf, "dd": round(dd, 10), "limit": round(1.2 * dd_1h, 10), "passed": bool(ok)})
    checks.append({"name": "risk_alignment_intraday_vs_1H", "passed": bool(intraday_ok), "rows": risk_rows, "dd_1h": round(dd_1h, 10)})

    contradictions = int(sum(1 for c in checks if not bool(c.get("passed"))))
    passed = contradictions == 0
    return {
        "executed": True,
        "passed": passed,
        "reason": None if passed else "cross_tf_inconsistent",
        "contradictions": contradictions,
        "checks": checks,
        "inputs_used": {
            tf: {
                "status": stage_by_tf[tf].get("status"),
                "metrics_summary": stage_by_tf[tf].get("metrics_summary"),
                "monte_carlo": stage_by_tf[tf].get("monte_carlo"),
                "walk_forward": stage_by_tf[tf].get("walk_forward"),
                "regime_stability": stage_by_tf[tf].get("regime_stability"),
                "cost_stress": stage_by_tf[tf].get("cost_stress"),
                "liquidity": stage_by_tf[tf].get("liquidity"),
            }
            for tf in required
        },
    }
