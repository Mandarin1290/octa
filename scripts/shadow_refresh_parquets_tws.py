#!/usr/bin/env python3
"""Refresh parquet data for paper-ready symbols from TWS (ib_insync).

Fetches recent 1H bars from TWS and appends them to the local parquet files,
ensuring the paper runner's stale-data circuit breaker won't block execution.

Usage:
    python scripts/shadow_refresh_parquets_tws.py
    python scripts/shadow_refresh_parquets_tws.py --symbols ADC --duration "5 D"

Requires:
    - TWS running and accessible on port 7497
    - OCTA_IBKR_USERNAME / OCTA_IBKR_PASSWORD credentials
    - ib_insync installed
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd


# ---------------------------------------------------------------------------
# Parquet path resolution (mirrors paper_runner.py logic)
# ---------------------------------------------------------------------------

_STOCK_PARQUET_DIR = Path("raw/Stock_parquet")
_ETF_PARQUET_DIR = Path("raw/ETF_Parquet")
_FX_PARQUET_DIR = Path("raw/FX_parquet")

_TIMEFRAME_TO_BAR_SIZE = {
    "1H": "1 hour",
    "1D": "1 day",
    "30M": "30 mins",
    "5M": "5 mins",
    "1M": "1 min",
    "4H": "4 hours",
}

_TIMEFRAME_DURATION_DEFAULT = {
    "1H": "5 D",
    "1D": "30 D",
    "30M": "3 D",
    "5M": "1 D",
    "1M": "1 D",
    "4H": "10 D",
}


def _parquet_path(symbol: str, timeframe: str, asset_class: str = "stock") -> Path:
    """Return the expected parquet path for a symbol+timeframe."""
    tf = timeframe.upper()
    ac = (asset_class or "stock").lower()
    if ac in ("fx", "forex"):
        return _FX_PARQUET_DIR / f"{symbol}_{tf}.parquet"
    elif ac in ("etf", "etfs"):
        return _ETF_PARQUET_DIR / f"{symbol}_{tf}.parquet"
    else:
        return _STOCK_PARQUET_DIR / f"{symbol}_{tf}.parquet"


# ---------------------------------------------------------------------------
# TWS data fetch
# ---------------------------------------------------------------------------

def _fetch_bars_from_tws(
    ib,
    symbol: str,
    timeframe: str,
    duration: str,
    asset_class: str = "stock",
    exchange: str = "SMART",
    currency: str = "USD",
) -> Optional[pd.DataFrame]:
    """Fetch historical bars from TWS for one symbol+timeframe."""
    from ib_insync import Contract, Forex, Future, Stock

    tf = timeframe.upper()
    bar_size = _TIMEFRAME_TO_BAR_SIZE.get(tf, "1 hour")
    ac = (asset_class or "stock").lower()

    try:
        if ac in ("fx", "forex"):
            # Forex: symbol like "EURUSD"
            pair = symbol[:3], symbol[3:]
            contract = Forex(symbol)
        elif ac in ("future", "futures"):
            contract = Future(symbol, exchange=exchange, currency=currency)
        else:
            contract = Stock(symbol, exchange, currency)

        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",  # now
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=True,    # regular trading hours only
            formatDate=1,   # date string
            keepUpToDate=False,
        )
    except Exception as e:
        print(f"  [{symbol}/{tf}] reqHistoricalData failed: {e}", file=sys.stderr)
        return None

    if not bars:
        print(f"  [{symbol}/{tf}] no bars returned from TWS", file=sys.stderr)
        return None

    from ib_insync import util
    df = util.df(bars)

    if df is None or df.empty:
        return None

    # Normalise columns to match parquet schema:
    # timestamp, open, high, low, close, volume
    df = df.rename(columns={"date": "timestamp"})
    keep = ["timestamp", "open", "high", "low", "close", "volume"]
    df = df[[c for c in keep if c in df.columns]]

    # Ensure timestamp is tz-aware UTC
    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"])
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("US/Eastern").dt.tz_convert("UTC")
        else:
            ts = ts.dt.tz_convert("UTC")
        df["timestamp"] = ts

    df = df.dropna(subset=["timestamp"]).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Merge + write parquet
# ---------------------------------------------------------------------------

def _merge_and_save(pq_path: Path, new_df: pd.DataFrame) -> Dict:
    """Append new bars to existing parquet, dedup by timestamp."""
    pq_path.parent.mkdir(parents=True, exist_ok=True)

    if pq_path.exists():
        existing = pd.read_parquet(pq_path)
        existing_ts = pd.to_datetime(existing["timestamp"])
        if existing_ts.dt.tz is None:
            existing_ts = existing_ts.dt.tz_localize("UTC")
        existing["timestamp"] = existing_ts

        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df.copy()

    # Dedup by timestamp, keep last (new data wins)
    combined["timestamp"] = pd.to_datetime(combined["timestamp"])
    combined = (
        combined
        .drop_duplicates(subset=["timestamp"], keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    combined.to_parquet(pq_path, index=False)
    return {
        "path": str(pq_path),
        "total_rows": len(combined),
        "new_rows": len(new_df),
        "last_ts": str(combined["timestamp"].iloc[-1]),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def refresh_symbols(
    symbols: List[str],
    timeframe: str,
    duration: str,
    asset_class: str = "stock",
    host: str = "127.0.0.1",
    port: int = 7497,
    client_id: int = 201,
) -> List[Dict]:
    """Connect to TWS, fetch bars, update parquets. Returns result list."""
    from ib_insync import IB, util

    ib = IB()
    try:
        ib.connect(host, port, clientId=client_id, timeout=10, readonly=True)
        print(f"✅ Connected to TWS: accounts={ib.managedAccounts()}")
    except Exception as e:
        print(f"❌ TWS connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    results = []
    try:
        for sym in symbols:
            print(f"\n  Fetching {sym}/{timeframe} (duration={duration})...")
            df = _fetch_bars_from_tws(
                ib, sym, timeframe, duration,
                asset_class=asset_class,
            )
            if df is None or df.empty:
                results.append({"symbol": sym, "timeframe": timeframe, "status": "no_data"})
                continue

            pq_path = _parquet_path(sym, timeframe, asset_class)
            info = _merge_and_save(pq_path, df)
            info.update({"symbol": sym, "timeframe": timeframe, "status": "ok"})
            results.append(info)
            print(f"  ✅ {sym}/{timeframe}: +{info['new_rows']} rows → {info['total_rows']} total, last={info['last_ts']}")
    finally:
        ib.disconnect()

    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh parquets from TWS")
    parser.add_argument("--symbols", default="ADC", help="Comma-separated symbols (default: ADC)")
    parser.add_argument("--timeframe", default="1H", help="Timeframe (default: 1H)")
    parser.add_argument("--duration", default="", help="TWS duration string (default: auto)")
    parser.add_argument("--asset-class", default="stock", help="Asset class (default: stock)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497)
    parser.add_argument("--client-id", type=int, default=201)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    tf = args.timeframe.upper()
    duration = args.duration or _TIMEFRAME_DURATION_DEFAULT.get(tf, "5 D")

    print(f"=== TWS Parquet Refresh ===")
    print(f"Symbols: {symbols}, TF={tf}, Duration={duration}")
    print(f"TWS: {args.host}:{args.port} clientId={args.client_id}")
    print()

    results = refresh_symbols(
        symbols=symbols,
        timeframe=tf,
        duration=duration,
        asset_class=args.asset_class,
        host=args.host,
        port=args.port,
        client_id=args.client_id,
    )

    print()
    print("=== Summary ===")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary = {"ts": ts, "results": results}
    print(json.dumps(summary, indent=2, default=str))

    failed = [r for r in results if r.get("status") != "ok"]
    if failed:
        print(f"\n⚠️  {len(failed)} symbol(s) failed: {[r['symbol'] for r in failed]}")
        return 1

    print(f"\n✅ All {len(results)} symbol(s) refreshed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
