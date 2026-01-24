#!/usr/bin/env python3
"""Baseline distribution of FX tail ratio (no strategy).

Goal
- Compute a baseline distribution for the FX-G0 tail-kill metric using ONLY raw FX daily returns.
- No gating changes. Pure calibration sanity check.

Method (per FX symbol, 1D)
- Load close prices from raw/FX_parquet/<SYMBOL>_1D.parquet
- Compute daily pct returns: r_t = close_t / close_{t-1} - 1
- Compute daily_vol as:
  - full-sample scalar: std(r) on all available returns
  - fixed-window scalar (default 252): std(r_tail) on last N returns
- Compute left-tail VaR_95 and CVaR_95 on the SAME sample used for daily_vol.
- Ratio (comparable to gate threshold): abs(CVaR_95) / daily_vol

Output
- Prints count, min/median/p90/max for ratios, and how many <= 2.5
- Writes reports/fx_tail_ratio_baseline_<timestamp>.json

Notes
- Returns are in decimal units (e.g. 1% = 0.01).
- ddof=0 for std for determinism (matches much of this repo’s metrics style).
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_default(o: Any):
    try:
        if isinstance(o, (np.generic,)):
            return o.item()
    except Exception:
        pass
    try:
        if isinstance(o, (pd.Timestamp,)):
            return o.isoformat()
    except Exception:
        pass
    return str(o)


def _series_stats(x: pd.Series) -> Dict[str, Any]:
    v = pd.to_numeric(x, errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).dropna()
    if v.empty:
        return {"n": 0}
    return {
        "n": int(v.shape[0]),
        "min": float(v.min()),
        "median": float(v.median()),
        "p90": float(v.quantile(0.90)),
        "p95": float(v.quantile(0.95)),
        "max": float(v.max()),
        "mean": float(v.mean()),
        "std": float(v.std(ddof=0)),
    }


def _list_stats(vals: List[float]) -> Dict[str, Any]:
    v = np.asarray([x for x in vals if x is not None and np.isfinite(x)], dtype=float)
    if v.size == 0:
        return {"n": 0}
    return {
        "n": int(v.size),
        "min": float(np.min(v)),
        "median": float(np.median(v)),
        "p90": float(np.quantile(v, 0.90)),
        "max": float(np.max(v)),
    }


def _compute_tail_ratio(r: pd.Series, eps: float = 1e-12) -> Dict[str, Any]:
    """Compute VaR_95, CVaR_95, vol, and ratios on the provided sample r."""

    rr = pd.to_numeric(r, errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).dropna()
    out: Dict[str, Any] = {
        "n": int(rr.shape[0]),
        "var_95": None,
        "cvar_95": None,
        "daily_vol": None,
        "ratio_signed": None,
        "ratio_abs": None,
        "tail_count": 0,
    }
    if rr.shape[0] < 20:
        return out

    var_95 = float(rr.quantile(0.05))
    tail = rr.loc[rr <= var_95]
    cvar_95 = float(tail.mean()) if tail.shape[0] else float("nan")
    daily_vol = float(rr.std(ddof=0))

    out.update(
        {
            "var_95": var_95,
            "cvar_95": cvar_95,
            "daily_vol": daily_vol,
            "tail_count": int(tail.shape[0]),
            "ratio_signed": float(cvar_95 / (daily_vol + eps)) if np.isfinite(cvar_95) else None,
            "ratio_abs": float(abs(cvar_95) / (daily_vol + eps)) if np.isfinite(cvar_95) else None,
        }
    )
    return out


def _load_close(p: Path) -> Tuple[pd.Series, Dict[str, Any]]:
    df = pd.read_parquet(p)

    meta: Dict[str, Any] = {"file": str(p), "columns": list(df.columns)}

    # Most FX parquets here have explicit timestamp column.
    if "timestamp" in df.columns:
        idx = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.copy()
        df.index = idx

    # Prefer 'close', fall back to 'price'.
    if "close" in df.columns:
        close = pd.to_numeric(df["close"], errors="coerce").astype(float)
        meta["close_column"] = "close"
    elif "price" in df.columns:
        close = pd.to_numeric(df["price"], errors="coerce").astype(float)
        meta["close_column"] = "price"
    else:
        # last resort: try common variants
        for c in ("Close", "CLOSE", "adj_close", "Adj Close"):
            if c in df.columns:
                close = pd.to_numeric(df[c], errors="coerce").astype(float)
                meta["close_column"] = c
                break
        else:
            raise KeyError(f"No close/price column found in {p}")

    close = close.replace([np.inf, -np.inf], np.nan).dropna()
    close = close[close > 0]
    close = close.sort_index()

    meta["n_rows_after_clean"] = int(close.shape[0])
    meta["start"] = close.index.min().isoformat() if len(close.index) else None
    meta["end"] = close.index.max().isoformat() if len(close.index) else None

    return close, meta


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fx-dir", default="raw/FX_parquet")
    ap.add_argument("--window", type=int, default=252)
    ap.add_argument("--threshold", type=float, default=2.5, help="Reference threshold for counting <= (no gating changes)")
    ap.add_argument("--out", default=None, help="Optional output path (default: reports/fx_tail_ratio_baseline_<timestamp>.json)")
    args = ap.parse_args()

    fx_dir = Path(args.fx_dir)
    window = int(args.window)
    thr = float(args.threshold)

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else (reports_dir / f"fx_tail_ratio_baseline_{_now_tag()}.json")

    paths = sorted([p for p in fx_dir.glob("*_1D.parquet") if p.is_file()])

    per_symbol: Dict[str, Any] = {}
    ratios_full: List[float] = []
    ratios_win: List[float] = []

    errors: Dict[str, str] = {}

    for p in paths:
        sym = p.name.split("_")[0].upper()
        try:
            close, meta = _load_close(p)
            r = close.pct_change().dropna()

            full = _compute_tail_ratio(r)
            tail_sample = r.tail(window) if window and window > 0 else r
            win = _compute_tail_ratio(tail_sample)

            per_symbol[sym] = {
                "meta": meta,
                "returns": {
                    "definition": "pct_change(close)",
                    "stats": _series_stats(r),
                },
                "full_sample": {
                    **full,
                    "definition": "VaR/CVaR and daily_vol computed on full returns sample",
                },
                "fixed_window": {
                    **win,
                    "window": window,
                    "definition": "VaR/CVaR and daily_vol computed on last N returns (N=window)",
                },
            }

            if full.get("ratio_abs") is not None:
                ratios_full.append(float(full["ratio_abs"]))
            if win.get("ratio_abs") is not None:
                ratios_win.append(float(win["ratio_abs"]))

        except Exception as e:
            errors[sym] = str(e)

    stats_full = _list_stats(ratios_full)
    stats_win = _list_stats(ratios_win)

    n_full_le = int(sum(1 for x in ratios_full if np.isfinite(x) and x <= thr))
    n_win_le = int(sum(1 for x in ratios_win if np.isfinite(x) and x <= thr))

    payload: Dict[str, Any] = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "fx_dir": str(fx_dir),
        "n_files": int(len(paths)),
        "window": window,
        "threshold_reference": thr,
        "definition": {
            "returns": "daily pct returns of close",
            "daily_vol": "std(returns) (scalar, ddof=0)",
            "cvar_95": "mean(returns <= VaR_95) with VaR_95=quantile(0.05)",
            "ratio": "abs(CVaR_95) / daily_vol",
        },
        "summary": {
            "full_sample": {
                "stats": stats_full,
                "n_le_threshold": n_full_le,
            },
            "fixed_window": {
                "stats": stats_win,
                "n_le_threshold": n_win_le,
            },
        },
        "per_symbol": per_symbol,
        "errors": errors,
    }

    out_path.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")

    # Print the requested summary (both variants; full-sample first)
    def _fmt_stats(s: Dict[str, Any]) -> str:
        if not s or int(s.get("n", 0)) == 0:
            return "n=0"
        return f"n={s['n']} min={s['min']:.6g} med={s['median']:.6g} p90={s['p90']:.6g} max={s['max']:.6g}"

    print("FX baseline tail ratio (abs(CVaR95)/vol) using daily pct returns")
    print(f"Files: {len(paths)} (errors: {len(errors)})")
    print(f"Full-sample ratios:   {_fmt_stats(stats_full)}; <= {thr}: {n_full_le}")
    print(f"Fixed-window ratios:  {_fmt_stats(stats_win)}; <= {thr}: {n_win_le} (window={window})")
    print(f"Wrote: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
