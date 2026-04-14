#!/usr/bin/env python3
"""Shadow execution entry point for Phase 2 validation (v0.0.0 regime-ensemble).

Data source: TWS (ib_insync) — NOT yfinance.
Flow:
  1. Refresh parquets from TWS for all paper-ready symbols
  2. Load ensemble_manifest.json (v0.0.0 schema) — version-check schema_version
  3. Run RegimeDetector.predict() to select active submodel
  4. Emit EVENT_REGIME_ACTIVATED to governance audit chain
  5. Log [shadow] decisions to artifacts/shadow_orders.ndjson
  6. Does NOT place real orders

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
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from octa_ops.autopilot.paper_runner import run_paper
from octa_ops.autopilot.autopilot_types import now_utc_iso
from scripts.shadow_refresh_parquets_tws import ShadowDataIncompleteError


_REGISTRY_PATH = "artifacts/registry.sqlite3"
_PAPER_READY_ROOT = Path("octa/var/models/paper_ready")
_TWS_HOST = "127.0.0.1"
_TWS_PORT = 7497
_TWS_CLIENT_ID = 202  # dedicated client ID for shadow refresh
_ENSEMBLE_MANIFEST_NAME = "ensemble_manifest.json"
_EXPECTED_SCHEMA_VERSION = "v0.0.0"
_REGIME_WINDOW_BARS = 20  # trailing bars for regime distribution


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


def _load_ensemble_manifest(symbol: str) -> Optional[Dict]:
    """Load ensemble_manifest.json for a symbol from paper_ready dir.

    Returns None if not found or schema_version mismatch.
    Logs a warning to stderr on version mismatch.
    """
    manifest_path = _PAPER_READY_ROOT / symbol / _ENSEMBLE_MANIFEST_NAME
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"  [shadow] WARN: failed to load manifest for {symbol}: {exc}", file=sys.stderr)
        return None

    schema_ver = manifest.get("schema_version", "")
    if schema_ver != _EXPECTED_SCHEMA_VERSION:
        print(
            f"  [shadow] WARN: {symbol} manifest schema_version={schema_ver!r} "
            f"(expected {_EXPECTED_SCHEMA_VERSION!r}) — skipping regime routing",
            file=sys.stderr,
        )
        return None
    return manifest


def _detect_active_regime(
    symbol: str,
    timeframe: str,
    parquet_path: Optional[str],
    gov_audit: Optional[object],
    run_id: str,
) -> str:
    """Detect active regime for symbol/TF using RegimeDetector.

    Falls back to 'neutral' on any error (non-fatal).
    Emits EVENT_REGIME_ACTIVATED to gov_audit if successful.

    Parameters
    ----------
    symbol : ticker symbol
    timeframe : e.g. '1D', '1H'
    parquet_path : path to recent price parquet; if None, regime detection skipped
    gov_audit : GovernanceAudit instance or None
    run_id : for audit chain event

    Returns
    -------
    str: detected regime name ('crisis' | 'bear' | 'bull' | 'neutral')
    """
    from octa_training.core.regime_labels import REGIME_NEUTRAL

    detector_path = (
        Path("octa/var/models/regime_detectors")
        / symbol
        / timeframe
        / f"{symbol}_{timeframe}_regime.pkl"
    )

    if not detector_path.exists():
        print(f"  [shadow] {symbol}/{timeframe}: no regime detector found — using neutral")
        return REGIME_NEUTRAL

    if parquet_path is None or not Path(parquet_path).exists():
        print(f"  [shadow] {symbol}/{timeframe}: no parquet for regime detection — using neutral")
        return REGIME_NEUTRAL

    try:
        from octa_training.core.regime_detector import RegimeDetector
        from octa_training.core.io_parquet import load_parquet

        detector = RegimeDetector.load(detector_path)
        df = load_parquet(parquet_path)
        # Use only the last N bars for distribution (rolling window)
        df_recent = df.iloc[-max(_REGIME_WINDOW_BARS * 3, 252):] if len(df) > 252 else df
        active_regime = detector.current_regime(df_recent, window=_REGIME_WINDOW_BARS)

        dist = detector.predict_distribution(df_recent, window=_REGIME_WINDOW_BARS)
        print(
            f"  [shadow] {symbol}/{timeframe}: regime={active_regime}  "
            f"dist={{{', '.join(f'{r}={v:.2f}' for r, v in dist.items() if v > 0)}}}"
        )

        # Emit governance event
        if gov_audit is not None:
            try:
                from octa.core.governance.governance_audit import EVENT_REGIME_ACTIVATED
                gov_audit.emit(
                    EVENT_REGIME_ACTIVATED,
                    {
                        "run_id": run_id,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "active_regime": active_regime,
                        "regime_distribution": dist,
                    },
                )
            except Exception:
                pass

        return active_regime

    except Exception as exc:
        print(
            f"  [shadow] {symbol}/{timeframe}: regime detection error — {exc} — using neutral",
            file=sys.stderr,
        )
        from octa_training.core.regime_labels import REGIME_NEUTRAL
        return REGIME_NEUTRAL


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
        except ShadowDataIncompleteError:
            raise  # propagate — main() handles this with GovernanceAudit + exit(1)
        except Exception as e:
            print(f"    ❌ TWS refresh failed for {syms}/{tf}: {e}", file=sys.stderr)
            for sym in syms:
                refresh_results[f"{sym}/{tf}"] = {"symbol": sym, "timeframe": tf, "status": "error", "error": str(e)}

    return refresh_results


def _run_regime_routing(
    symbols_info: List[Dict],
    refresh_results: Dict,
    run_id: str,
    gov_audit: Optional[object],
) -> Dict[str, str]:
    """For each paper-ready symbol, detect active regime and log routing decision.

    Returns dict mapping '{symbol}/{tf}' → active_regime_name
    """
    regime_routing: Dict[str, str] = {}

    for info in symbols_info:
        sym = info["symbol"]
        tf = info["timeframe"]
        key = f"{sym}/{tf}"

        # Load ensemble_manifest to check architecture version
        manifest = _load_ensemble_manifest(sym)
        if manifest is None:
            print(f"  [shadow] {sym}/{tf}: no v0.0.0 manifest — skipping regime routing")
            regime_routing[key] = "neutral"
            continue

        # Resolve parquet path from refresh results
        refresh = refresh_results.get(key, {})
        parquet_path: Optional[str] = refresh.get("parquet_path") or refresh.get("path")

        # Detect active regime
        active_regime = _detect_active_regime(
            symbol=sym,
            timeframe=tf,
            parquet_path=parquet_path,
            gov_audit=gov_audit,
            run_id=run_id,
        )
        regime_routing[key] = active_regime

    return regime_routing


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

    print(f"=== Shadow Execution v0.0.0 (TWS data, regime-ensemble) ===")
    print(f"run_id: {run_id}")
    print(f"ts: {ts}")
    print()

    # Init governance audit (best-effort — non-fatal if unavailable)
    gov_audit: Optional[object] = None
    try:
        from octa.core.governance.governance_audit import GovernanceAudit
        gov_audit = GovernanceAudit(run_id=run_id)
    except Exception:
        pass

    # Step 1: Discover paper-ready symbols from registry
    symbols_info = _get_paper_ready_symbols(_REGISTRY_PATH)
    refresh_results: Dict = {}

    if not symbols_info:
        print("⚠️  No PAPER-status artifacts found in registry — nothing to shadow trade.")
        print(f"   Registry: {_REGISTRY_PATH}")
    else:
        print(f"Paper-ready symbols: {[(i['symbol'], i['timeframe']) for i in symbols_info]}")
        print()

        # Step 2: Refresh parquets from TWS (CRITICAL: TWS only, NOT yfinance)
        print("Step 1: Refreshing parquets from TWS...")
        try:
            refresh_results = _refresh_from_tws(symbols_info)
        except ShadowDataIncompleteError as exc:
            print(
                f"❌ SHADOW_PARTIAL_FETCH_ABORT: {len(exc.symbols_failed)} symbol(s) failed: "
                f"{exc.symbols_failed}",
                file=sys.stderr,
            )
            if gov_audit is not None:
                try:
                    from octa.core.governance.governance_audit import EVENT_SHADOW_PARTIAL_FETCH_ABORT
                    gov_audit.emit(
                        EVENT_SHADOW_PARTIAL_FETCH_ABORT,
                        {
                            "run_id": run_id,
                            "symbols_failed": exc.symbols_failed,
                            "total_symbols": len(symbols_info),
                            "reason_code": "SHADOW_PARTIAL_FETCH_ABORT",
                        },
                    )
                except Exception:
                    pass
            return 1
        print()

        # Step 3: Regime routing — detect active regime per symbol/TF
        print("Step 2: Regime routing (v0.0.0 ensemble)...")
        regime_routing = _run_regime_routing(
            symbols_info=symbols_info,
            refresh_results=refresh_results,
            run_id=run_id,
            gov_audit=gov_audit,
        )
        print()

    # Step 4: Run paper/shadow inference
    print("Step 3: Running ML inference (sandbox mode)...")
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
        "schema_version": "v0.0.0",
        "data_source": "tws",
        "tws_host": _TWS_HOST,
        "tws_port": _TWS_PORT,
        "promoted_count": result.get("promoted_count", 0),
        "placed": len(result.get("placed", [])),
        "skipped": len(result.get("skipped", [])),
        "placed_details": result.get("placed", []),
        "skipped_details": result.get("skipped", []),
        "refresh_results": refresh_results,
        "regime_routing": regime_routing if symbols_info else {},
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
