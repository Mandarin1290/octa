#!/usr/bin/env python3
"""FX-G1 1H scaling debug runner.

Purpose
- Diagnose suspiciously extreme FX-G1 Sharpe/Sortino/cost-stress values by printing
  the exact annualization factor, bars/day, mean/std of strategy returns, and realized costs/day.

Constraints
- Diagnostics only; no gate/threshold changes.
- Intended for FX 1H only.

Example
PYTHONPATH=. python scripts/fx_g1_scale_debug.py --config configs/dev.yaml --fx-dir raw/FX_parquet --symbol AUDBRL
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/dev.yaml")
    ap.add_argument("--fx-dir", default="raw/FX_parquet")
    ap.add_argument("--symbol", default="AUDBRL")
    ap.add_argument("--sweep-fast", action="store_true")
    args = ap.parse_args()

    from octa_training.core.config import load_config
    from octa_training.core.evaluation import infer_frequency
    from octa_training.core.pipeline import train_evaluate_package
    from octa_training.core.state import StateRegistry

    cfg = load_config(args.config)

    # Mirror the common sweep-fast overrides used elsewhere.
    if args.sweep_fast:
        try:
            if hasattr(cfg, "tuning"):
                cfg.tuning.enabled = False
        except Exception:
            pass
        try:
            cfg.models_order = [m for m in list(getattr(cfg, "models_order", [])) if str(m).lower() != "catboost"]
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

    sym = str(args.symbol).strip().upper()
    p = Path(args.fx_dir) / f"{sym}_1H.parquet"
    if not p.exists():
        raise SystemExit(f"missing: {p}")

    # Enable diagnose mode to ensure run returns diagnostics even if failing.
    try:
        if not hasattr(cfg, "gates") or not isinstance(cfg.gates, dict):
            cfg.gates = {}
        cfg.gates["diagnose_mode"] = True
    except Exception:
        pass

    state = StateRegistry(str(cfg.paths.state_dir))
    run_id = f"fx_g1_scale_debug_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    res = train_evaluate_package(
        symbol=sym,
        cfg=cfg,
        state=state,
        run_id=run_id,
        safe_mode=True,
        parquet_path=str(p),
        debug=True,
    )

    gate = getattr(res, "gate_result", None)
    if gate is not None:
        try:
            reasons = list(getattr(gate, "reasons", None) or [])
            if reasons:
                print("gate.reasons:")
                for r in reasons:
                    print(f"- {r}")
        except Exception:
            pass
        try:
            diags = list(getattr(gate, "diagnostics", None) or [])
            fx_diags = [d for d in diags if isinstance(d, dict) and str(d.get("name", "")).startswith("fx_g1_")]
            if fx_diags:
                print("fx_g1 diagnostics:")
                for d in fx_diags:
                    print(f"- {d.get('name')}: value={d.get('value')} reason={d.get('reason')}")
        except Exception:
            pass

    pr = getattr(res, "pack_result", None) or {}
    dbg = pr.get("debug") if isinstance(pr, dict) else None
    df = (dbg or {}).get("df_backtest")
    es = (dbg or {}).get("eval_settings")
    metrics = (dbg or {}).get("metrics")

    if not isinstance(df, pd.DataFrame) or df.empty:
        print("No df_backtest in debug bundle")
        return 0

    ann = float(infer_frequency(df.index))
    bpd = float(ann) / 252.0 if ann and ann > 0 else float("nan")

    try:
        deltas = df.index.to_series().diff().dropna()
        med_delta = deltas.median() if len(deltas) else None
    except Exception:
        med_delta = None

    strat = pd.to_numeric(df.get("strat_ret"), errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).dropna()
    costs = pd.to_numeric(df.get("costs"), errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).dropna()
    turnover = pd.to_numeric(df.get("turnover"), errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).dropna()

    mean_bar = float(strat.mean()) if len(strat) else float("nan")
    std_bar = float(strat.std(ddof=0)) if len(strat) else float("nan")
    sharpe_calc = (mean_bar / std_bar) * float(np.sqrt(ann)) if np.isfinite(mean_bar) and np.isfinite(std_bar) and std_bar > 0 and ann > 0 else float("nan")

    costs_per_day = float(costs.mean() * bpd) if len(costs) and np.isfinite(bpd) else float("nan")
    turnover_per_day = float(turnover.mean() * bpd) if len(turnover) and np.isfinite(bpd) else float("nan")

    print(f"symbol={sym} parquet={p}")
    if med_delta is not None:
        print(f"median_bar_spacing={med_delta}")
    expected_hint = "~24 for true FX 1H" if med_delta is None or (hasattr(med_delta, 'total_seconds') and med_delta.total_seconds() < 20 * 3600) else "~1 for daily-like data"
    print(f"ann_factor={ann:.6g} bars_per_day={bpd:.6g} (expected {expected_hint})")
    print(f"session_enabled={getattr(es, 'session_enabled', None)}")
    print(f"cost_bps={_as_float(getattr(es, 'cost_bps', None))} spread_bps={_as_float(getattr(es, 'spread_bps', None))}")
    print(f"mean_ret_per_bar={mean_bar:.6g} std_ret_per_bar={std_bar:.6g} sharpe_calc={sharpe_calc:.6g}")
    print(f"costs_per_day(mean*bars/day)={costs_per_day:.6g} turnover_per_day(mean*bars/day)={turnover_per_day:.6g}")
    if metrics is not None:
        print(f"metrics.sharpe={_as_float(getattr(metrics, 'sharpe', None))} metrics.sortino={_as_float(getattr(metrics, 'sortino', None))}")
        print(f"metrics.turnover_per_day={_as_float(getattr(metrics, 'turnover_per_day', None))} metrics.net_to_gross={_as_float(getattr(metrics, 'net_to_gross', None))}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
