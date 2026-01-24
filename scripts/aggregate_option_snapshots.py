#!/usr/bin/env python3
"""Aggregate option chain CSV snapshots into a time-indexed Parquet for option training.
Supports multiple CSV files in `raw/frd_complete_plus_options_sample/`.
Computes `tte_days` and `moneyness` (if underlying AAPL parquet available).
"""
from pathlib import Path

import pandas as pd

SRC_DIR = Path("raw/frd_complete_plus_options_sample")
DST = Path("raw/AAPL_OPT.parquet")
UNDERLYING_PARQUET_CANDIDATES = [Path("raw/AAPL.parquet"), Path("raw/AAPL_1D.parquet"), Path("raw/AAPL_1day.parquet"), Path("raw/AAPL_daily.parquet")]

files = sorted(SRC_DIR.glob("*.csv"))
if not files:
    print("No CSVs found in", SRC_DIR)
    raise SystemExit(1)

frames = []
for f in files:
    # use python engine and warn on bad lines to tolerate inconsistent CSVs
    # read CSV and skip malformed lines to avoid noisy ParserWarnings
    try:
        df = pd.read_csv(f, engine='python', on_bad_lines='skip')
    except TypeError:
        # older pandas versions may not have on_bad_lines
        df = pd.read_csv(f, engine='python', error_bad_lines=False)
    df.columns = [c.strip() for c in df.columns]
    # parse trade date
    # parse trade date (case-insensitive match)
    cols_lower = {c.lower(): c for c in df.columns}
    if 'trade date' in cols_lower:
        df['trade_date'] = pd.to_datetime(df[cols_lower['trade date']], utc=True, errors='coerce')
    elif 'trade_date' in cols_lower:
        df['trade_date'] = pd.to_datetime(df[cols_lower['trade_date']], utc=True, errors='coerce')
    else:
        # try first column
        df['trade_date'] = pd.to_datetime(df.iloc[:,0], errors='coerce')
    # expiry
    if 'expiry date' in cols_lower:
        df['expiry'] = pd.to_datetime(df[cols_lower['expiry date']], utc=True, errors='coerce')
    elif 'expiry' in cols_lower:
        df['expiry'] = pd.to_datetime(df[cols_lower['expiry']], utc=True, errors='coerce')
    # normalize columns (case-insensitive matching)
    col_map = {
        'last trade price': 'close',
        'bid price': 'bid',
        'ask price': 'ask',
        'open interest': 'open_interest',
        'volume': 'volume',
        'delta': 'delta',
        'gamma': 'gamma',
        'vega': 'vega',
        'theta': 'theta',
        'rho': 'rho',
        'bid implied volatility': 'iv_bid',
        'ask implied volatility': 'iv_ask',
        'strike': 'strike',
        'call/put': 'option_type',
        'callput': 'option_type'
    }
    # rename columns by lowercasing the source names for robust matching
    rename_map = {}
    for col in df.columns:
        key = col.strip().lower()
        if key in col_map:
            rename_map[col] = col_map[key]
    if rename_map:
        df = df.rename(columns=rename_map)
    # compute iv mean
    if 'iv_bid' in df.columns and 'iv_ask' in df.columns:
        df['iv'] = df[['iv_bid','iv_ask']].replace(0.0, pd.NA).mean(axis=1)
    # ensure key columns
    for c in ['trade_date','close','strike','expiry','option_type']:
        if c not in df.columns:
            df[c] = pd.NA
    df['symbol'] = 'AAPL_OPT'
    # coerce numeric columns
    for ncol in ('close','bid','ask','strike','delta','iv','iv_bid','iv_ask'):
        if ncol in df.columns:
            df[ncol] = pd.to_numeric(df[ncol], errors='coerce')
    # compute mid if missing
    if 'mid' not in df.columns:
        if 'bid' in df.columns and 'ask' in df.columns:
            df['mid'] = (df['bid'] + df['ask']) / 2.0
        else:
            df['mid'] = df.get('close')
    frames.append(df)

all_df = pd.concat(frames, ignore_index=True, sort=False)
# set timestamp index
all_df['timestamp'] = pd.to_datetime(all_df['trade_date'], utc=True, errors='coerce')
all_df = all_df.sort_values('timestamp')
# Attempt to annotate underlying close by looking for AAPL parquet
underlying_close = None
for cand in UNDERLYING_PARQUET_CANDIDATES:
    if cand.exists():
        try:
            ut = pd.read_parquet(cand)
            # normalize column names
            ut.columns = [c.lower() for c in ut.columns]
            # prefer an explicit timestamp column
            if 'timestamp' in ut.columns:
                ut['timestamp'] = pd.to_datetime(ut['timestamp'], utc=True, errors='coerce')
                ut = ut.set_index('timestamp')
            else:
                # if index is already datetime-like, coerce
                try:
                    ut.index = pd.to_datetime(ut.index)
                except Exception:
                    pass
            if 'close' in ut.columns or ('close' in ut.index.names):
                underlying_close = ut
                break
        except Exception:
            continue

if underlying_close is not None:
    # reindex underlying to daily by date if needed
    uc = underlying_close.copy()
    # ensure index is date-only
    uc.index = pd.to_datetime(uc.index, utc=True)
    # Map each option row to nearest underlying close on same date
    def lookup_underlying(ts):
        if pd.isna(ts):
            return pd.NA
        try:
            d = pd.to_datetime(ts).normalize()
            # index should be datetime; match by normalized date
            idx = uc.index.normalize()
            match = (idx == d)
            if match.any():
                val = uc.loc[match]
                # prefer column 'close'
                if 'close' in val.columns:
                    return float(val['close'].iloc[0])
                # otherwise try first numeric column
                for c in val.columns:
                    try:
                        return float(val[c].iloc[0])
                    except Exception:
                        continue
        except Exception:
            return pd.NA
        return pd.NA
    all_df['underlying_close'] = all_df['timestamp'].apply(lookup_underlying)
else:
    print('Underlying AAPL parquet not found; `moneyness` will be NaN if no underlying_close provided')

# compute tte_days and moneyness
all_df['expiry'] = pd.to_datetime(all_df['expiry'], errors='coerce')
all_df['tte_days'] = (all_df['expiry'] - all_df['timestamp']).dt.days
all_df['moneyness'] = pd.NA
mask = all_df['underlying_close'].notna() & all_df['strike'].notna()
all_df.loc[mask, 'moneyness'] = all_df.loc[mask, 'underlying_close'].astype(float) / all_df.loc[mask, 'strike'].astype(float)

# index by timestamp
out = all_df.set_index('timestamp')
# Ensure timestamp exists as a column for downstream loaders
out_to_write = out.reset_index()
DST.parent.mkdir(parents=True, exist_ok=True)
print(f'Writing {DST} rows={len(out_to_write)} start={out_to_write["timestamp"].min()} end={out_to_write["timestamp"].max()}')
out_to_write.to_parquet(DST)
print('Done')
