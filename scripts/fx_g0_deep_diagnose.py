#!/usr/bin/env python3
"""FX-G0 deep-dive diagnostics (audit-grade transparency).

Goal: recompute and expose all intermediate components used to compute:
- cvar_95_over_daily_vol (tail kill switch)
- cost_stress_sharpe (as used by FX-G0 robustness: 2x costs)
- turnover_per_day
- net_to_gross

Constraints:
- No threshold changes
- No trading logic changes
- Diagnostics-only

This script runs the existing FX-G0 evaluation pipeline for selected symbols and
then reconstructs the metrics using the exact definitions in
octa_training.core.evaluation.compute_equity_and_metrics.

Outputs:
- Writes JSON per symbol: reports/fx_g0_deep_<symbol>_<timestamp>.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

# Keep stdout clean (no tqdm).
os.environ.setdefault("OCTA_DISABLE_TQDM", "1")

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _series_stats(x: pd.Series) -> Dict[str, Any]:
    try:
        v = pd.to_numeric(x, errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).dropna()
        if v.empty:
            return {"n": 0}
        return {
            "n": int(v.shape[0]),
            "min": float(v.min()),
            "median": float(v.median()),
            "max": float(v.max()),
            "p01": float(v.quantile(0.01)),
            "p05": float(v.quantile(0.05)),
            "p95": float(v.quantile(0.95)),
            "p99": float(v.quantile(0.99)),
            "mean": float(v.mean()),
            "std": float(v.std(ddof=0)),
        }
    except Exception as e:
        return {"error": str(e)}


def _extract_diag_map(gate: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    diags = gate.get("diagnostics")
    if not isinstance(diags, list):
        return out
    for d in diags:
        if not isinstance(d, dict):
            continue
        name = d.get("name")
        if isinstance(name, str) and name:
            out[name] = d
    return out


def _apply_sweep_fast(cfg: Any) -> None:
    """Mirror the sweep-fast overrides used in scripts/global_gate_diagnose.py."""
    try:
        if hasattr(cfg, "tuning"):
            cfg.tuning.enabled = False
            cfg.tuning.optuna_trials = min(int(getattr(cfg.tuning, "optuna_trials", 50)), 5)
    except Exception:
        pass

    try:
        cfg.models_order = [m for m in list(getattr(cfg, "models_order", [])) if str(m).lower() != "catboost"]
    except Exception:
        pass

    try:
        if hasattr(cfg, "tuning") and hasattr(cfg.tuning, "models_order"):
            cfg.tuning.models_order = [m for m in list(cfg.tuning.models_order) if str(m).lower() != "catboost"]
    except Exception:
        pass

    try:
        cfg.num_boost_round = min(int(getattr(cfg, "num_boost_round", 1000)), 300)
        cfg.early_stopping_rounds = min(int(getattr(cfg, "early_stopping_rounds", 50)), 30)
    except Exception:
        pass

    try:
        if not hasattr(cfg, "features") or not isinstance(cfg.features, dict):
            cfg.features = {}
        cfg.features["horizons"] = [1]
    except Exception:
        pass

    try:
        cfg.prefer_gpu = False
    except Exception:
        pass


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    def _default(o: Any):
        if isinstance(o, (np.generic,)):
            return o.item()
        if isinstance(o, (pd.Timestamp,)):
            return o.isoformat()
        return str(o)

    path.write_text(json.dumps(payload, indent=2, default=_default), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/dev.yaml")
    ap.add_argument("--fx-dir", default="raw/FX_parquet")
    ap.add_argument(
        "--symbols",
        default="AUDBRL,EURZAR,NZDMXN,USDTRY,EURCNH",
        help="Comma-separated FX symbols (base symbol, no timeframe suffix)",
    )
    ap.add_argument(
        "--sweep-fast",
        action="store_true",
        help="Apply the same sweep-fast runtime overrides as the diagnose sweep runner",
    )
    args = ap.parse_args()

    from octa_training.core.config import load_config
    from octa_training.core.evaluation import (
        EvalSettings,
        infer_frequency,
    )
    from octa_training.core.gates import GateSpec
    from octa_training.core.pipeline import train_evaluate_package
    from octa_training.core.robustness import cost_stress_test
    from octa_training.core.state import StateRegistry

    cfg = load_config(args.config)
    if bool(args.sweep_fast):
        _apply_sweep_fast(cfg)

    # Enable diagnose mode (do not mutate committed config files)
    try:
        if not hasattr(cfg, "gates") or not isinstance(cfg.gates, dict):
            cfg.gates = {}
        cfg.gates["diagnose_mode"] = True
    except Exception:
        pass

    state = StateRegistry(str(cfg.paths.state_dir))
    ts_tag = _now_tag()
    run_id = f"fx_g0_deep_{ts_tag}"

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    fx_dir = Path(args.fx_dir)
    out_dir = Path("reports")
    out_dir.mkdir(parents=True, exist_ok=True)

    # FX-G0 gate overrides (alpha not required at 1D)
    gate_overrides = {
        "sharpe_min": -1e9,
        "sortino_min": -1e9,
        "profit_factor_min": -1e9,
        "avg_net_trade_return_min": -1e9,
        "sharpe_oos_over_is_min": -1e9,
    }

    for sym in symbols:
        p = fx_dir / f"{sym}_1D.parquet"
        out_path = out_dir / f"fx_g0_deep_{sym}_{ts_tag}.json"

        payload: Dict[str, Any] = {
            "symbol": sym,
            "parquet": str(p),
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "config": str(args.config),
            "notes": {
                "g0_mode": "1D risk/regime overlay; alpha metrics non-binding",
                "metrics_source": "Recomputed from compute_equity_and_metrics definitions",
            },
        }

        if not p.exists():
            payload["error"] = "missing_1d_data"
            _write_json(out_path, payload)
            print(f"{sym}: missing 1D parquet -> {out_path}")
            continue

        # Run pipeline once to obtain the exact predictions, backtest df, and settings.
        res = train_evaluate_package(
            symbol=sym,
            cfg=cfg,
            state=state,
            run_id=run_id,
            safe_mode=True,
            parquet_path=str(p),
            gate_overrides=gate_overrides,
            robustness_profile="risk_overlay",
            debug=True,
        )

        gate = None
        try:
            gate_obj = getattr(res, "gate_result", None)
            if gate_obj is not None:
                if hasattr(gate_obj, "model_dump"):
                    gate = gate_obj.model_dump()
                elif hasattr(gate_obj, "dict"):
                    gate = gate_obj.dict()
        except Exception:
            gate = None

        payload["pipeline"] = {
            "passed": bool(getattr(res, "passed", False)),
            "error": getattr(res, "error", None),
            "gate": gate,
        }

        debug_bundle = None
        try:
            pr = getattr(res, "pack_result", None)
            if isinstance(pr, dict):
                debug_bundle = pr.get("debug")
        except Exception:
            debug_bundle = None

        if not isinstance(debug_bundle, dict):
            payload["error"] = "missing_debug_bundle"
            _write_json(out_path, payload)
            print(f"{sym}: missing debug bundle -> {out_path}")
            continue

        df_bt = debug_bundle.get("df_backtest")
        preds = debug_bundle.get("preds")
        es = debug_bundle.get("eval_settings")

        if not isinstance(df_bt, pd.DataFrame) or df_bt.empty:
            payload["error"] = "missing_backtest_df"
            _write_json(out_path, payload)
            print(f"{sym}: missing backtest df -> {out_path}")
            continue

        if not isinstance(preds, pd.Series):
            try:
                preds = pd.Series(preds)
            except Exception:
                preds = None

        # Snapshot EvalSettings for the report (avoid serializing arbitrary objects)
        eval_settings_dump: Dict[str, Any] = {}
        try:
            if isinstance(es, EvalSettings):
                eval_settings_dump = {
                    "mode": getattr(es, "mode", None),
                    "upper_q": _as_float(getattr(es, "upper_q", None)),
                    "lower_q": _as_float(getattr(es, "lower_q", None)),
                    "causal_quantiles": bool(getattr(es, "causal_quantiles", False)),
                    "quantile_window": getattr(es, "quantile_window", None),
                    "leverage_cap": _as_float(getattr(es, "leverage_cap", None)),
                    "vol_target": _as_float(getattr(es, "vol_target", None)),
                    "realized_vol_window": getattr(es, "realized_vol_window", None),
                    "cost_bps": _as_float(getattr(es, "cost_bps", None)),
                    "spread_bps": _as_float(getattr(es, "spread_bps", None)),
                    "stress_cost_multiplier": _as_float(getattr(es, "stress_cost_multiplier", None)),
                    "align_tolerance": getattr(es, "align_tolerance", None),
                    "session_enabled": bool(getattr(es, "session_enabled", False)),
                    "session_timezone": getattr(es, "session_timezone", None),
                    "session_open": getattr(es, "session_open", None),
                    "session_close": getattr(es, "session_close", None),
                    "session_weekdays": getattr(es, "session_weekdays", None),
                }
        except Exception:
            eval_settings_dump = {}

        # Reconstruct key series and scalars
        price = pd.to_numeric(df_bt["price"], errors="coerce").astype(float)
        pct_ret = price.pct_change().fillna(0.0)
        log_ret = np.log(price.where(price > 0, np.nan).ffill().bfill()).diff().fillna(0.0)

        pos = pd.to_numeric(df_bt.get("pos"), errors="coerce").astype(float) if "pos" in df_bt.columns else pd.Series(0.0, index=df_bt.index)
        pos_prev = pd.to_numeric(df_bt.get("pos_prev"), errors="coerce").astype(float) if "pos_prev" in df_bt.columns else pos.shift(1).fillna(0.0)
        turnover = pd.to_numeric(df_bt.get("turnover"), errors="coerce").astype(float) if "turnover" in df_bt.columns else (pos - pos_prev).abs()
        tcost = pd.to_numeric(df_bt.get("tcost"), errors="coerce").astype(float) if "tcost" in df_bt.columns else pd.Series(np.nan, index=df_bt.index)
        scost = pd.to_numeric(df_bt.get("scost"), errors="coerce").astype(float) if "scost" in df_bt.columns else pd.Series(np.nan, index=df_bt.index)
        costs = pd.to_numeric(df_bt.get("costs"), errors="coerce").astype(float) if "costs" in df_bt.columns else pd.Series(np.nan, index=df_bt.index)

        gross_stream = pos_prev * log_ret
        strat_ret = pd.to_numeric(df_bt.get("strat_ret"), errors="coerce").astype(float) if "strat_ret" in df_bt.columns else (gross_stream - costs.fillna(0.0))

        # Active-bar filtering for tail risk (as in compute_equity_and_metrics)
        eps = 1e-12
        active_mask = (pos_prev.abs() > eps) | (turnover > eps)
        perf_active = strat_ret.loc[active_mask]
        if perf_active.shape[0] < 20:
            perf_active = strat_ret

        # Also compute "full" tail stats (for unit/series mismatch audits)
        perf_full = strat_ret

        # NEW (FX-G0 binding): market log returns tail ratio (no positions, no costs)
        r_mkt = pd.to_numeric(log_ret, errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).dropna()
        if r_mkt.shape[0] > 1 and float(r_mkt.iloc[0]) == 0.0:
            r_mkt = r_mkt.iloc[1:]
        var95_mkt = float(r_mkt.quantile(0.05)) if r_mkt.shape[0] else 0.0
        tail95_mkt = r_mkt.loc[r_mkt <= var95_mkt] if r_mkt.shape[0] else r_mkt
        cvar95_mkt = float(tail95_mkt.mean()) if tail95_mkt.shape[0] else 0.0
        vol_mkt = float(r_mkt.std(ddof=0)) if r_mkt.shape[0] else 0.0
        ratio_mkt = float(abs(cvar95_mkt) / (vol_mkt + 1e-12)) if vol_mkt > 0 else 0.0

        var95 = float(perf_active.quantile(0.05)) if perf_active.shape[0] else 0.0
        tail95 = perf_active.loc[perf_active <= var95]
        cvar95 = float(tail95.mean()) if tail95.shape[0] else 0.0

        var95_full = float(perf_full.quantile(0.05)) if perf_full.shape[0] else 0.0
        tail95_full = perf_full.loc[perf_full <= var95_full]
        cvar95_full = float(tail95_full.mean()) if tail95_full.shape[0] else 0.0

        ann = float(infer_frequency(df_bt.index))
        bars_per_day = float(ann) / 252.0 if ann and ann > 0 else 1.0

        vol_ann = float(strat_ret.std(ddof=0) * np.sqrt(ann)) if strat_ret.shape[0] else 0.0
        daily_vol = float(vol_ann / np.sqrt(252.0)) if vol_ann > 0 else 0.0
        # For transparency: daily vol computed on full series, while CVaR uses perf_active.
        vol_ann_active = float(perf_active.std(ddof=0) * np.sqrt(ann)) if perf_active.shape[0] else 0.0
        daily_vol_active = float(vol_ann_active / np.sqrt(252.0)) if vol_ann_active > 0 else 0.0

        ratio = float(abs(cvar95) / (daily_vol + 1e-12)) if daily_vol > 0 else 0.0
        ratio_full_full = float(abs(cvar95_full) / (daily_vol + 1e-12)) if daily_vol > 0 else 0.0
        ratio_active_active = float(abs(cvar95) / (daily_vol_active + 1e-12)) if daily_vol_active > 0 else 0.0

        turnover_per_day = float(turnover.mean() * bars_per_day) if turnover.shape[0] else 0.0
        trades = int((pos != pos_prev).sum())
        days = float(len(df_bt) / bars_per_day) if bars_per_day > 0 else float(len(df_bt))
        trades_per_day = float(trades / days) if days > 0 else 0.0

        net_pnl = float(strat_ret.sum()) if strat_ret.shape[0] else 0.0
        gross_pnl = float(gross_stream.sum()) if gross_stream.shape[0] else 0.0
        net_to_gross = float(net_pnl / gross_pnl) if abs(gross_pnl) > 1e-12 else None

        # Cost stress sharpe used in robustness: fixed 2x cost_bps & spread_bps (see robustness.cost_stress_test)
        cs_sharpe = None
        cs_passed = None
        cs_settings: Dict[str, Any] = {"multiplier": 2.0}
        try:
            if isinstance(es, EvalSettings) and preds is not None:
                stress_min_sharpe = None
                try:
                    stress_min_sharpe = _as_float(getattr(getattr(cfg, "robustness", None), "stress_min_sharpe", None))
                except Exception:
                    stress_min_sharpe = None
                gate_for_cs = GateSpec(robustness_stress_min_sharpe=float(stress_min_sharpe) if stress_min_sharpe is not None else 0.5)
                cs_settings.update(
                    {
                        "base_cost_bps": _as_float(getattr(es, "cost_bps", None)),
                        "base_spread_bps": _as_float(getattr(es, "spread_bps", None)),
                        "stressed_cost_bps": _as_float(getattr(es, "cost_bps", None)) * 2.0
                        if _as_float(getattr(es, "cost_bps", None)) is not None
                        else None,
                        "stressed_spread_bps": _as_float(getattr(es, "spread_bps", None)) * 2.0
                        if _as_float(getattr(es, "spread_bps", None)) is not None
                        else None,
                        "gate_threshold_robustness_stress_min_sharpe": _as_float(getattr(gate_for_cs, "robustness_stress_min_sharpe", None)),
                    }
                )
                cs = cost_stress_test(price, preds, es, gate_for_cs)
                cs_sharpe = _as_float(cs.get("sharpe"))
                cs_passed = bool(cs.get("passed"))
                cs_settings["impl"] = "octa_training.core.robustness.cost_stress_test"
        except Exception as e:
            cs_settings = {"error": str(e), **cs_settings}

        # Gate diagnostics values/thresholds for cross-checks
        gate_diag = _extract_diag_map(gate or {}) if isinstance(gate, dict) else {}

        def _diag(name: str, _gate_diag: Dict[str, Any] = gate_diag) -> Dict[str, Any]:
            d = _gate_diag.get(name, {})
            return {
                "value": _as_float(d.get("value")),
                "threshold": _as_float(d.get("threshold")),
                "op": d.get("op"),
                "passed": d.get("passed"),
                "reason": d.get("reason"),
                "evaluable": d.get("evaluable"),
            }

        # Assemble requested diagnostics
        payload["eval_settings"] = eval_settings_dump
        payload["return_series"] = {
            "definitions": {
                "raw_returns": "pct_change(close) (requested; NOT used by engine)",
                "raw_returns_pct": "pct_change(close) (alias of raw_returns)",
                "raw_returns_log": "log(close).diff() (USED by engine: df['ret'])",
                "gross_returns": "pos_prev * raw_returns_log",
                "net_returns": "strat_ret = pos_prev * raw_returns_log - costs",
                "active_mask": "abs(pos_prev)>1e-12 OR turnover>1e-12 (used for CVaR tail selection)",
                "series_used_for_cvar_95": "net_returns on active bars (fallback to full if <20 active)",
                "series_used_for_daily_vol": "net_returns full series (std annualized -> daily)",
            },
            "stats": {
                "raw_returns": _series_stats(pct_ret),
                "raw_returns_pct": _series_stats(pct_ret),
                "raw_returns_log": _series_stats(log_ret),
                "gross_returns": _series_stats(gross_stream),
                "net_returns": _series_stats(strat_ret),
                "net_returns_active": _series_stats(perf_active),
            },
            "series": {
                "index": [t.isoformat() if hasattr(t, "isoformat") else str(t) for t in df_bt.index],
                "close": price.replace([np.inf, -np.inf], np.nan).ffill().bfill().tolist(),
                "raw_returns": pct_ret.tolist(),
                "raw_returns_pct": pct_ret.tolist(),
                "raw_returns_log": log_ret.tolist(),
                "pos": pos.tolist(),
                "pos_prev": pos_prev.tolist(),
                "turnover": turnover.tolist(),
                "tcost": tcost.tolist(),
                "scost": scost.tolist(),
                "costs": costs.tolist(),
                "gross_returns": gross_stream.tolist(),
                "net_returns": strat_ret.tolist(),
                "active_mask": active_mask.astype(bool).tolist(),
            },
        }

        payload["daily_vol"] = {
            "formula": "daily_vol = (std(net_returns_full)*sqrt(ann)) / sqrt(252)",
            "ann_factor": ann,
            "bars_per_day": bars_per_day,
            "realized_vol_window": getattr(es, "realized_vol_window", None),
            "rv_series_stats": _series_stats(pd.to_numeric(df_bt.get("rv"), errors="coerce") if "rv" in df_bt.columns else pd.Series([], dtype=float)),
            "vol_ann": vol_ann,
            "daily_vol": daily_vol,
            "vol_ann_active": vol_ann_active,
            "daily_vol_active": daily_vol_active,
            "series_used": {
                "daily_vol": "net_returns (strat_ret) full series",
                "note": "Engine uses full-series vol; CVaR uses active-bar filter.",
            },
        }

        payload["cvar_95"] = {
            "tail_definition": "left tail: returns <= quantile(0.05)",
            "series_used": "net_returns (strat_ret) filtered to active bars if enough samples",
            "active_filter": "abs(pos_prev)>1e-12 OR turnover>1e-12; fallback to full series if <20 active bars",
            "n_total": int(strat_ret.shape[0]),
            "n_active": int(active_mask.sum()),
            "n_used": int(perf_active.shape[0]),
            "var_95": var95,
            "cvar_95": cvar95,
            "tail_count": int(tail95.shape[0]),
            "full_series_reference": {
                "var_95_full": var95_full,
                "cvar_95_full": cvar95_full,
                "tail_count_full": int(tail95_full.shape[0]),
            },
            "market_reference": {
                "series": "market_log_returns",
                "var_95_mkt": var95_mkt,
                "cvar_95_mkt": cvar95_mkt,
                "vol_mkt": vol_mkt,
                "tail_ratio_mkt": ratio_mkt,
            },
        }

        payload["ratio"] = {
            "cvar_95_over_daily_vol": ratio,
            "variants": {
                "active_cvar_over_full_daily_vol": ratio,
                "full_cvar_over_full_daily_vol": ratio_full_full,
                "active_cvar_over_active_daily_vol": ratio_active_active,
                "market_cvar_over_market_vol": ratio_mkt,
            },
            "components": {
                "abs_cvar_95": float(abs(cvar95)),
                "daily_vol": daily_vol,
                "daily_vol_active": daily_vol_active,
                "abs_cvar_95_mkt": float(abs(cvar95_mkt)),
                "vol_mkt": vol_mkt,
            },
            "definition_used_by_engine": "abs(CVaR95(active)) / daily_vol(full)",
            "gate_diagnostic": _diag("cvar_95_over_daily_vol"),
            "fx_g0_binding_tail_metric": {
                "series": "market_log_returns",
                "fx_g0_cvar95_mkt": cvar95_mkt,
                "fx_g0_vol_mkt": vol_mkt,
                "fx_g0_tail_ratio_mkt": ratio_mkt,
                "note": "After pipeline change: FX-G0 tail_kill_switch should use market_log_returns (not strategy returns).",
            },
        }

        payload["cost_stress"] = {
            "base_costs": {
                "cost_bps": _as_float(getattr(es, "cost_bps", None)),
                "spread_bps": _as_float(getattr(es, "spread_bps", None)),
                "stress_cost_multiplier_config": _as_float(getattr(es, "stress_cost_multiplier", None)),
                "model": {
                    "turnover_cost": "tcost = turnover * (cost_bps/10000)",
                    "spread_cost": "scost = spread_bps/10000 applied when sign(pos) changes and pos!=0",
                    "note": "No separate commission/slippage fields beyond cost_bps + spread_bps in this engine.",
                },
            },
            "g0_cost_stress_rule": "FX-G0 robustness cost stress uses fixed 2x cost_bps & 2x spread_bps (not config stress_cost_multiplier)",
            "stress": {
                "settings": cs_settings,
                "cost_stress_sharpe": cs_sharpe,
                "passed": cs_passed,
                "series_used": "net_returns (strat_ret) with stressed costs",
            },
            "robustness_detail": ((gate or {}).get("robustness") or {}).get("details", {}).get("cost_stress") if isinstance(gate, dict) else None,
        }

        payload["turnover"] = {
            "definition": "turnover = abs(pos - pos_prev); turnover_per_day = mean(turnover)*bars_per_day",
            "bars_per_day": bars_per_day,
            "turnover_stats": _series_stats(turnover),
            "turnover_per_day": turnover_per_day,
            "gate_diagnostic": _diag("turnover_per_day"),
            "trades": {
                "n_trades": trades,
                "days_est": days,
                "trades_per_day": trades_per_day,
                "avg_abs_position_change_per_day": turnover_per_day,
            },
        }

        gross_exposure = pos.abs()
        payload["exposure"] = {
            "gross_exposure_series_stats": _series_stats(gross_exposure),
            "net_exposure_series_stats": _series_stats(pos),
            "avg_gross_exposure": float(gross_exposure.mean()) if gross_exposure.shape[0] else 0.0,
            "gate_diagnostic": _diag("avg_gross_exposure"),
            "series": {
                "gross_exposure": gross_exposure.tolist(),
                "net_exposure": pos.tolist(),
            },
            "net_to_gross": {
                "definition": "net_to_gross = sum(net_returns) / sum(gross_returns) where gross_returns = pos_prev*raw_returns_log",
                "net_pnl": net_pnl,
                "gross_pnl": gross_pnl,
                "gross_pnl_abs": float(abs(gross_pnl)),
                "gross_pnl_sign": float(np.sign(gross_pnl)) if gross_pnl is not None else None,
                "net_to_gross": net_to_gross,
                "gate_diagnostic": _diag("net_to_gross"),
                "notes": "Can be negative when gross_pnl is negative; can be extreme when gross_pnl is close to 0. No clipping is applied; returns None if |gross_pnl|<=1e-12.",
            },
        }

        # Internal cross-checks (non-fatal): compare recomputed values to MetricsSummary if present
        try:
            ms = debug_bundle.get("metrics")
            if ms is not None:
                m_cvar = _as_float(getattr(ms, "cvar_95_over_daily_vol", None))
                m_tpd = _as_float(getattr(ms, "turnover_per_day", None))
                m_ntg = _as_float(getattr(ms, "net_to_gross", None))
                m_dv = _as_float(getattr(ms, "daily_vol", None))
                m_cvar95 = _as_float(getattr(ms, "cvar_95", None))
                payload["cross_check"] = {
                    "metrics_summary": {
                        "cvar_95_over_daily_vol": m_cvar,
                        "turnover_per_day": m_tpd,
                        "net_to_gross": m_ntg,
                        "daily_vol": m_dv,
                        "cvar_95": m_cvar95,
                    },
                    "recomputed": {
                        "cvar_95_over_daily_vol": ratio,
                        "turnover_per_day": turnover_per_day,
                        "net_to_gross": net_to_gross,
                        "daily_vol": daily_vol,
                        "cvar_95": cvar95,
                    },
                    "abs_diff": {
                        "cvar_95_over_daily_vol": None if m_cvar is None else float(abs(m_cvar - ratio)),
                        "turnover_per_day": None if m_tpd is None else float(abs(m_tpd - turnover_per_day)),
                        "net_to_gross": None if m_ntg is None or net_to_gross is None else float(abs(m_ntg - net_to_gross)),
                        "daily_vol": None if m_dv is None else float(abs(m_dv - daily_vol)),
                        "cvar_95": None if m_cvar95 is None else float(abs(m_cvar95 - cvar95)),
                    },
                }

                # Soft-assertions: highlight definition drift without aborting multi-symbol runs.
                tol = 1e-9
                checks = {
                    "cvar_95_over_daily_vol_match": (m_cvar is None) or (abs(m_cvar - ratio) <= 1e-6),
                    "turnover_per_day_match": (m_tpd is None) or (abs(m_tpd - turnover_per_day) <= 1e-6),
                    "daily_vol_match": (m_dv is None) or (abs(m_dv - daily_vol) <= 1e-6),
                    "cvar_95_match": (m_cvar95 is None) or (abs(m_cvar95 - cvar95) <= 1e-6),
                    "net_to_gross_match": (m_ntg is None) or (net_to_gross is None) or (abs(m_ntg - net_to_gross) <= 1e-6),
                    "gross_pnl_not_near_zero": abs(gross_pnl) > 1e-12 + tol,
                }
                payload["assertions"] = {
                    "tolerances": {"abs": 1e-6},
                    "checks": checks,
                }
        except Exception:
            pass

        # Minimal stdout summary per symbol (keeps series in JSON only)
        try:
            ntg_s = f"{float(net_to_gross):.6g}" if net_to_gross is not None else "None"
            cs_s = f"{float(cs_sharpe):.6g}" if cs_sharpe is not None else "None"
            print(
                (
                    f"{sym}: CVaR95(active)={cvar95:.6g} VaR95(active)={var95:.6g} "
                    f"daily_vol(full)={daily_vol:.6g} ratio={ratio:.6g} "
                    f"turnover/day={turnover_per_day:.6g} net_to_gross={ntg_s} "
                    f"cost_stress_sharpe={cs_s}"
                ),
                flush=True,
            )
        except Exception:
            pass

        _write_json(out_path, payload)
        print(f"{sym}: wrote {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
