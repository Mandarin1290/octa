#!/usr/bin/env python3
"""Refresh OHLCV parquets for shadow/paper execution using yfinance.

Refreshes both 1D and 1H parquets for all paper-promoted symbols.
Called before shadow_runner execution.
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import pandas as pd
import yfinance as yf
from octa_ops.autopilot.registry import ArtifactRegistry


def refresh_symbol_1h(symbol: str, parquet_dir: Path) -> dict:
    pq_path = parquet_dir / f"{symbol}_1H.parquet"
    if not pq_path.exists():
        return {"symbol": symbol, "tf": "1H", "status": "not_found"}
    
    df_existing = pd.read_parquet(pq_path)
    last_ts = pd.Timestamp(df_existing["timestamp"].max())
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize("UTC")
    
    ticker = yf.Ticker(symbol)
    df_new = ticker.history(start=(last_ts - pd.Timedelta("1D")).strftime("%Y-%m-%d"), interval="1h", 
                            auto_adjust=True, prepost=False)
    if df_new.empty:
        return {"symbol": symbol, "tf": "1H", "status": "no_new_data"}
    
    df_new = df_new.reset_index()
    df_new.columns = [c.lower() for c in df_new.columns]
    df_new = df_new.rename(columns={"datetime": "timestamp"})
    if df_new["timestamp"].dt.tz is None:
        df_new["timestamp"] = df_new["timestamp"].dt.tz_localize("UTC")
    else:
        df_new["timestamp"] = df_new["timestamp"].dt.tz_convert("UTC")
    
    keep_cols = [c for c in ["timestamp", "open", "high", "low", "close", "volume"] if c in df_new.columns]
    df_new = df_new[keep_cols]
    
    existing_ts = set(df_existing["timestamp"].astype(str).tolist())
    mask = [str(ts) not in existing_ts for ts in df_new["timestamp"].tolist()]
    df_append = df_new[mask]
    
    if len(df_append) == 0:
        return {"symbol": symbol, "tf": "1H", "status": "up_to_date", "last": str(last_ts)}
    
    df_combined = pd.concat([df_existing, df_append], ignore_index=True)
    df_combined.to_parquet(pq_path, index=False)
    new_last = pd.Timestamp(df_append["timestamp"].max())
    return {"symbol": symbol, "tf": "1H", "status": "updated", "new_rows": len(df_append), "last": str(new_last)}


def main():
    reg = ArtifactRegistry(root="artifacts", ctx={"execution_active": False})
    promoted = reg.get_promoted_artifacts(level="paper")
    symbols = list({r["symbol"] for r in promoted if r.get("symbol")})
    print(f"Refreshing {len(symbols)} promoted symbols: {symbols}")
    
    parquet_dir = Path("raw/Stock_parquet")
    results = []
    for sym in symbols:
        r = refresh_symbol_1h(sym, parquet_dir)
        results.append(r)
        print(f"  {r}")
    
    print(f"\nDone: {len(results)} symbols refreshed")


if __name__ == "__main__":
    main()
