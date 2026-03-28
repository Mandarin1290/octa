#!/usr/bin/env python3
"""Shadow execution entry point for Phase 2 validation.

Data source: TWS (ib_insync) — NOT yfinance.
Flow:
  1. Refresh parquets from TWS for all paper-ready symbols
  2. Run ML inference via paper_runner (OCTA_BROKER_MODE=sandbox)
  3. Log decisions to artifacts/shadow_orders.ndjson
  4. Does NOT place real orders

Usage:
  python scripts/run_shadow_execution.py
  OCTA_BROKER_MODE=sandbox python scripts/run_shadow_execution.py

TWS must be running on port 7497 before calling this script.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent))

from octa_ops.autopilot.paper_runner import run_paper
from octa_ops.autopilot.types import now_utc_iso


_REGISTRY_PATH = "artifacts/registry.sqlite3"
_TWS_HOST = "127.0.0.1"
_TWS_PORT = 7497
_TWS_CLIENT_ID = 202  # dedicated client ID for shadow refresh


def _get_paper_ready_symbols(registry_path: str) -> List[Dict]:
    """Return list of {symbol, timeframe, asset_class} for PAPER-status artifacts."""
    if not Path(registry_path).exists():
        return []
    try:
        db = sqlite3.connect(registry_path)
        rows = db.execute(
            "SELECT symbol, timeframe, meta_json FROM artifacts "
            "WHERE lifecycle_status = 'PAPER' ORDER BY id"
        ).fetchall()
        db.close()
    except Exception:
        return []

    result = []
    for sym, tf, meta_json in rows:
        asset_class = "stock"
        if meta_json:
            try:
                meta = json.loads(meta_json)
                asset_class = meta.get("asset_class") or "stock"
            except Exception:
                pass
        result.append({"symbol": sym, "timeframe": tf, "asset_class": asset_class})
    return result


def _refresh_from_tws(symbols_info: List[Dict]) -> Dict:
    """Fetch fresh bars from TWS and update parquets. CRITICAL: TWS only, NOT yfinance."""
    from scripts.shadow_refresh_parquets_tws import refresh_symbols

    refresh_results = {}
    # Group by timeframe+asset_class for batched fetch
    from collections import defaultdict
    groups: Dict = defaultdict(list)
    for info in symbols_info:
        key = (info["timeframe"], info["asset_class"])
        groups[key].append(info["symbol"])

    for (tf, ac), syms in groups.items():
        print(f"  [TWS refresh] {syms} {tf} ({ac})...")
        try:
            results = refresh_symbols(
                symbols=syms,
                timeframe=tf,
                duration="5 D",  # last 5 trading days
                asset_class=ac,
                host=_TWS_HOST,
                port=_TWS_PORT,
                client_id=_TWS_CLIENT_ID,
            )
            for r in results:
                sym = r.get("symbol", "?")
                refresh_results[f"{sym}/{tf}"] = r
                status = r.get("status")
                if status == "ok":
                    print(f"    ✅ {sym}/{tf}: last={r.get('last_ts','?')} +{r.get('new_rows',0)} rows")
                else:
                    print(f"    ⚠️  {sym}/{tf}: {status}")
        except Exception as e:
            print(f"    ❌ TWS refresh failed for {syms}/{tf}: {e}", file=sys.stderr)
            for sym in syms:
                refresh_results[f"{sym}/{tf}"] = {"symbol": sym, "timeframe": tf, "status": "error", "error": str(e)}

    return refresh_results


def main() -> int:
    # Enforce sandbox mode — this script must never place real orders
    if os.environ.get("OCTA_BROKER_MODE", "sandbox") not in ("sandbox", ""):
        print(
            f"ERROR: OCTA_BROKER_MODE={os.environ.get('OCTA_BROKER_MODE')} is not sandbox. "
            "This script is for shadow execution only.",
            file=sys.stderr,
        )
        return 1
    os.environ["OCTA_BROKER_MODE"] = "sandbox"

    # Allow yesterday's last bar to pass the stale check before market open.
    # US markets open at 14:30 UTC; the stale window is extended to 24h for shadow.
    if not os.environ.get("OCTA_MAX_STALE_SECONDS"):
        os.environ["OCTA_MAX_STALE_SECONDS"] = "86400"  # 24h

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"shadow_tws_{ts}"

    print(f"=== Shadow Execution (TWS data) ===")
    print(f"run_id: {run_id}")
    print(f"ts: {ts}")
    print()

    # Step 1: Discover paper-ready symbols from registry
    symbols_info = _get_paper_ready_symbols(_REGISTRY_PATH)
    if not symbols_info:
        print("⚠️  No PAPER-status artifacts found in registry — nothing to shadow trade.")
        print(f"   Registry: {_REGISTRY_PATH}")
    else:
        print(f"Paper-ready symbols: {[(i['symbol'], i['timeframe']) for i in symbols_info]}")
        print()

        # Step 2: Refresh parquets from TWS (CRITICAL: TWS only, NOT yfinance)
        print("Step 1: Refreshing parquets from TWS...")
        refresh_results = _refresh_from_tws(symbols_info)
        print()

    # Step 3: Run paper/shadow inference
    print("Step 2: Running ML inference (sandbox mode)...")
    result = run_paper(
        run_id=run_id,
        config_path="configs/shadow_trading_adc_tws.yaml",
        level="paper",
        live_enable=False,
        registry_root="artifacts",
        ledger_dir="artifacts/ledger_shadow",
        paper_log_path="artifacts/shadow_orders.ndjson",
        max_runtime_s=300,
        broker_cfg_path="configs/execution_ibkr.yaml",
    )

    summary = {
        "run_id": run_id,
        "ts": ts,
        "data_source": "tws",
        "tws_host": _TWS_HOST,
        "tws_port": _TWS_PORT,
        "promoted_count": result.get("promoted_count", 0),
        "placed": len(result.get("placed", [])),
        "skipped": len(result.get("skipped", [])),
        "placed_details": result.get("placed", []),
        "skipped_details": result.get("skipped", []),
        "refresh_results": refresh_results if symbols_info else {},
    }

    print()
    print("=== Shadow Run Summary ===")
    print(json.dumps(summary, indent=2, default=str))

    placed = len(result.get("placed", []))
    skipped = len(result.get("skipped", []))
    print()
    if placed > 0:
        print(f"✅ Shadow signals generated: {placed} placed, {skipped} skipped")
    else:
        print(f"⚠️  No signals placed ({skipped} skipped) — check skipped_details above")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
