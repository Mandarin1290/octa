#!/usr/bin/env python3
"""Refresh 1D Stock parquets for specified symbols using yfinance.

Appends rows after the existing last date. Preserves the raw parquet format
(RangeIndex + timestamp column in UTC).

Usage:
  python3 scripts/refresh_parquets_1d.py --symbols "ASA,AMAT,AWR,AAPL" --parquet-dir raw/Stock_parquet
  python3 scripts/refresh_parquets_1d.py --symbols-file /tmp/symbols.txt --parquet-dir raw/Stock_parquet
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
import yfinance as yf


def _load_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df


def _last_timestamp(df: pd.DataFrame) -> Optional[pd.Timestamp]:
    if "timestamp" in df.columns and len(df):
        return pd.Timestamp(df["timestamp"].max()).tz_localize("UTC") if df["timestamp"].max().tzinfo is None else pd.Timestamp(df["timestamp"].max()).tz_convert("UTC")
    if isinstance(df.index, pd.DatetimeIndex) and len(df.index):
        ts = df.index.max()
        return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return None


def _fetch_new_data(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> Optional[pd.DataFrame]:
    try:
        ticker = yf.Ticker(symbol)
        raw = ticker.history(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=True,
        )
        if raw is None or len(raw) == 0:
            return None
        # Convert index to UTC
        if raw.index.tzinfo is None:
            raw.index = raw.index.tz_localize("UTC")
        else:
            raw.index = raw.index.tz_convert("UTC")
        # Rename columns to lowercase
        raw = raw.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
        # Keep only OHLCV
        cols = [c for c in ["open", "high", "low", "close", "volume"] if c in raw.columns]
        raw = raw[cols].copy()
        # Add timestamp column (UTC)
        raw.insert(0, "timestamp", raw.index.tz_convert("UTC"))
        raw = raw.reset_index(drop=True)
        return raw
    except Exception as e:
        print(f"  [warn] yfinance fetch failed for {symbol}: {e}", file=sys.stderr)
        return None


def refresh_symbol(symbol: str, parquet_dir: Path, end_date: pd.Timestamp) -> dict:
    path = parquet_dir / f"{symbol}_1D.parquet"
    if not path.exists():
        return {"symbol": symbol, "status": "no_parquet"}

    existing = _load_parquet(path)
    last_ts = _last_timestamp(existing)
    if last_ts is None:
        return {"symbol": symbol, "status": "no_timestamp"}

    # Download rows strictly after last_ts
    start_fetch = last_ts + pd.Timedelta(days=1)
    if start_fetch >= end_date:
        return {"symbol": symbol, "status": "up_to_date", "last_date": str(last_ts.date())}

    new_data = _fetch_new_data(symbol, start_fetch, end_date)
    if new_data is None or len(new_data) == 0:
        return {"symbol": symbol, "status": "no_new_data", "last_date": str(last_ts.date())}

    # Filter strictly after last_ts
    new_data = new_data[pd.to_datetime(new_data["timestamp"]).dt.tz_convert("UTC") > last_ts].copy()
    if len(new_data) == 0:
        return {"symbol": symbol, "status": "no_new_rows_after_filter", "last_date": str(last_ts.date())}

    # Check existing columns match
    existing_cols = list(existing.columns)
    new_cols = list(new_data.columns)
    if set(new_cols) != set(existing_cols):
        # Align to existing columns
        for c in existing_cols:
            if c not in new_data.columns:
                new_data[c] = None
        new_data = new_data[existing_cols]

    # Concatenate and reset index
    combined = pd.concat([existing, new_data], ignore_index=True)
    combined.to_parquet(path, index=False)

    return {
        "symbol": symbol,
        "status": "updated",
        "rows_added": len(new_data),
        "old_last_date": str(last_ts.date()),
        "new_last_date": str(pd.to_datetime(new_data["timestamp"].max()).date()),
        "total_rows": len(combined),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Refresh 1D parquets from yfinance")
    p.add_argument("--symbols", default=None, help="Comma-separated symbols")
    p.add_argument("--symbols-file", default=None, help="File with one symbol per line")
    p.add_argument("--parquet-dir", default="raw/Stock_parquet")
    p.add_argument("--end-date", default=None, help="End date YYYY-MM-DD (default: today)")
    args = p.parse_args()

    symbols: List[str] = []
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if args.symbols_file:
        lines = Path(args.symbols_file).read_text().splitlines()
        symbols += [s.strip().upper() for s in lines if s.strip()]
    if not symbols:
        p.error("Provide --symbols or --symbols-file")

    parquet_dir = Path(args.parquet_dir)
    end_date = pd.Timestamp(args.end_date, tz="UTC") if args.end_date else pd.Timestamp.now("UTC").normalize()

    print(f"Refreshing {len(symbols)} symbols from {parquet_dir} up to {end_date.date()}")
    results = []
    for sym in symbols:
        result = refresh_symbol(sym, parquet_dir, end_date)
        results.append(result)
        status = result["status"]
        if status == "updated":
            print(f"  {sym}: +{result['rows_added']} rows  ({result['old_last_date']} → {result['new_last_date']})")
        elif status == "up_to_date":
            print(f"  {sym}: up_to_date ({result['last_date']})")
        else:
            print(f"  {sym}: {status}")

    updated = sum(1 for r in results if r["status"] == "updated")
    skipped = sum(1 for r in results if r["status"] not in {"updated"})
    print(f"\nDone: {updated} updated, {skipped} skipped/no-change")


if __name__ == "__main__":
    main()
