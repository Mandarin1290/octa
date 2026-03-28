#!/usr/bin/env python3
"""Diagnostic inference for gate-failed symbols. NEVER placed as real orders.

Loads PKLs directly from paper_ready/ — bypasses registry entirely.
Uses the same feature/inference pipeline as paper_runner.
All output explicitly labeled DIAGNOSTIC / NOT TRADEABLE.

Usage:
    python3 scripts/run_diagnostic_inference.py [ALB AWR ...]
    OCTA_MAX_STALE_SECONDS=86400 python3 scripts/run_diagnostic_inference.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# Belt-and-suspenders: this script must never produce real orders
os.environ["OCTA_BROKER_MODE"] = "sandbox"

sys.path.insert(0, str(Path(__file__).parent.parent))

PAPER_READY_ROOT = Path("octa/var/models/paper_ready")
DIAG_LOG = Path("artifacts/diagnostic_inference.ndjson")
_SHADOW_CFG = "configs/shadow_trading_adc_tws.yaml"

# Documented gate failure reasons per symbol (for audit trail)
GATE_FAILURES: Dict[str, List[str]] = {
    "ALB": ["walkforward_oos_threshold_failed", "regime_sharpe_collapse"],
    "AWR": ["is_oos_degradation: sharpe_oos_over_is=0.203 < 0.35"],
}


def _run_one(symbol: str) -> Dict[str, Any]:
    import pandas as pd
    from octa_training.core.artifact_io import load_tradeable_artifact
    from octa_training.core.config import load_config
    from octa_ops.autopilot.paper_runner import _load_recent_features

    rec: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "mode": "DIAGNOSTIC",
        "gate_failure_reasons": GATE_FAILURES.get(symbol, ["unclassified"]),
        "warning": "NOT_TRADEABLE — gate-failed symbol, observation only, never promoted",
    }

    pkl = PAPER_READY_ROOT / symbol / "1H" / f"{symbol}.pkl"
    sha = pkl.with_suffix(".sha256")
    if not pkl.exists():
        rec["error"] = "pkl_not_found"
        return rec

    try:
        artifact = load_tradeable_artifact(str(pkl), str(sha) if sha.exists() else None)
    except Exception as e:
        rec["error"] = f"artifact_load_failed: {e}"
        return rec

    if "safe_inference" not in artifact:
        rec["error"] = "missing_safe_inference"
        return rec

    asset = artifact.get("asset", {}) or {}
    bar_size = str(asset.get("bar_size") or "1H")
    asset_class = str(asset.get("asset_class") or "stock")
    rec["bar_size"] = bar_size
    rec["asset_class"] = asset_class

    # Resolve parquet (same logic as paper_runner)
    try:
        cfg = load_config(_SHADOW_CFG)
    except Exception as e:
        rec["error"] = f"config_load_failed: {e}"
        return rec

    raw_dir = Path(cfg.paths.raw_dir)
    candidates = [
        raw_dir / "Stock_parquet" / f"{symbol}_{bar_size}.parquet",
        raw_dir / "Stock_parquet" / f"{symbol}_1D.parquet",
    ]
    pq = next((str(c) for c in candidates if c.exists()), None)
    if not pq:
        rec["error"] = "parquet_not_found"
        return rec

    # Stale check
    try:
        from octa_training.core.io_parquet import load_parquet
        df_check = load_parquet(Path(pq))
        if isinstance(df_check.index, pd.DatetimeIndex) and len(df_check.index):
            last_ts = df_check.index.max()
            now_utc = pd.Timestamp.utcnow()
            if now_utc.tzinfo is None:
                now_utc = now_utc.tz_localize("UTC")
            age_s = float((now_utc - pd.Timestamp(last_ts).tz_convert("UTC")).total_seconds())
            max_stale = int(os.getenv("OCTA_MAX_STALE_SECONDS") or str(3 * 3600))
            if age_s > max_stale:
                rec["error"] = f"stale_data: age={age_s:.0f}s max={max_stale}s"
                return rec
            rec["data_age_s"] = int(age_s)
            rec["last_bar_ts"] = str(last_ts)
    except Exception as e:
        rec["error"] = f"stale_check_failed: {e}"
        return rec

    # Build features using same helper as paper_runner
    try:
        X = _load_recent_features(cfg, asset_class, pq, last_n=300, timeframe=bar_size)
    except Exception as e:
        rec["error"] = f"feature_build_failed: {e}"
        return rec

    # Inference
    try:
        infer = artifact["safe_inference"]
        pred = infer.predict(X)
        rec["pred"] = {k: (float(v) if isinstance(v, (int, float)) else v) for k, v in pred.items()}
        rec["signal"] = int(pred.get("signal", 0) or 0)
        rec["confidence"] = float(pred.get("confidence", 0.0) or 0.0)
        rec["position"] = float(pred.get("position", 0.0) or 0.0)
    except Exception as e:
        rec["error"] = f"inference_failed: {e}"
        return rec

    rec["status"] = "OK"
    return rec


def main() -> int:
    symbols = sys.argv[1:] if len(sys.argv) > 1 else list(GATE_FAILURES.keys())
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"\n{'='*62}")
    print(f"  Diagnostic Inference — {ts}")
    print(f"  ⚠️  DIAGNOSTIC ONLY — gate-failed symbols, NOT TRADEABLE")
    print(f"  Symbols: {symbols}")
    print(f"{'='*62}\n")

    DIAG_LOG.parent.mkdir(parents=True, exist_ok=True)

    all_ok = True
    with open(DIAG_LOG, "a") as fout:
        for sym in symbols:
            print(f"[{sym}] running...")
            r = _run_one(sym)
            fout.write(json.dumps(r, default=str) + "\n")
            fout.flush()

            if r.get("status") == "OK":
                sig = r.get("signal", 0)
                conf = r.get("confidence", 0.0)
                pos = r.get("position", 0.0)
                bar = r.get("last_bar_ts", "?")[:16]
                age_h = r.get("data_age_s", 0) / 3600
                gate_str = ", ".join(r["gate_failure_reasons"])
                marker = "🎯 SIGNAL" if sig != 0 else "◯ no signal"
                print(f"  {marker}: signal={sig} conf={conf:.3f} pos={pos:.3f}")
                print(f"  last_bar={bar} age={age_h:.1f}h")
                print(f"  gates_failed: {gate_str}")
                print(f"  → logged to {DIAG_LOG} (OBSERVATION ONLY)")
            else:
                print(f"  ❌ error={r.get('error', '?')}")
                all_ok = False
            print()

    print(f"{'='*62}")
    print(f"  Log: {DIAG_LOG}")
    print(f"  Signals above are DIAGNOSTIC — cannot be promoted to paper")
    print(f"{'='*62}\n")

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
