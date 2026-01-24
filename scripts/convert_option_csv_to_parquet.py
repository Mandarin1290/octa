#!/usr/bin/env python3
from pathlib import Path

import pandas as pd

SRC = Path("raw/frd_complete_plus_options_sample/option_chain_AAPL.csv")
DST = Path("raw/AAPL_OPT.parquet")

if not SRC.exists():
    raise SystemExit(f"Source file not found: {SRC}")

# Read CSV
df = pd.read_csv(SRC)
# Normalize column names
df.columns = [c.strip() for c in df.columns]
# Parse dates
if 'Trade Date' in df.columns:
    df['trade_date'] = pd.to_datetime(df['Trade Date'])
elif 'trade_date' in df.columns:
    df['trade_date'] = pd.to_datetime(df['trade_date'])
else:
    raise SystemExit('No trade date column found')
# Lowercase columns and map to internal names
col_map = {
    'Last Trade Price': 'close',
    'Bid Price': 'bid',
    'Ask Price': 'ask',
    'Open Interest': 'open_interest',
    'Volume': 'volume',
    'Delta': 'delta',
    'Gamma': 'gamma',
    'Vega': 'vega',
    'Theta': 'theta',
    'Rho': 'rho',
    'Bid Implied Volatility': 'iv_bid',
    'Ask Implied Volatility': 'iv_ask',
    'STrike': 'strike',
    'Expiry Date': 'expiry',
    'Call/Put': 'option_type',
}
for k, v in list(col_map.items()):
    if k in df.columns:
        df = df.rename(columns={k: v})
# Compute single iv if available
if 'iv_bid' in df.columns and 'iv_ask' in df.columns:
    try:
        df['iv'] = df[['iv_bid', 'iv_ask']].replace(0.0, pd.NA).mean(axis=1)
    except Exception:
        df['iv'] = pd.NA
# Ensure required columns exist
required = ['trade_date', 'close', 'strike', 'expiry', 'option_type']
for c in required:
    if c not in df.columns:
        df[c] = pd.NA
# Standardize option_type to lower 'c'/'p' => 'call'/'put'
df['option_type'] = df['option_type'].astype(str).str.lower().str.strip().map({'c': 'call', 'p': 'put'}).fillna(df['option_type'])
# Set index to trade_date
df = df.set_index(pd.to_datetime(df['trade_date'])).sort_index()
# Add a symbol column
df['symbol'] = 'AAPL_OPT'
# Keep relevant columns and write parquet
keep_cols = ['symbol', 'strike', 'expiry', 'option_type', 'close', 'bid', 'ask', 'iv', 'iv_bid', 'iv_ask', 'open_interest', 'volume', 'delta', 'gamma', 'vega', 'theta', 'rho']
keep = [c for c in keep_cols if c in df.columns]
df_out = df[keep]
# Ensure raw directory exists
DST.parent.mkdir(parents=True, exist_ok=True)
print(f"Writing {DST} rows={len(df_out)} start={df_out.index.min()} end={df_out.index.max()}")
df_out.to_parquet(DST)
print("Done.")
