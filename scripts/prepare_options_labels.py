#!/usr/bin/env python3
"""Prepare option labels (delta-hedged P&L) from aggregated option parquet.

Usage: scripts/prepare_options_labels.py --symbol AAPL_OPT --horizon_days 1
"""
import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def detect_columns(df: pd.DataFrame):
    # heuristics for commonly named cols
    colmap = {}
    if 'mid' in df.columns:
        colmap['mid'] = 'mid'
    elif 'mid_price' in df.columns:
        colmap['mid'] = 'mid_price'
    elif 'ask' in df.columns and 'bid' in df.columns:
        colmap['mid'] = None  # compute from bid/ask
    if 'delta' in df.columns:
        colmap['delta'] = 'delta'
    else:
        for c in df.columns:
            if c.lower().startswith('d') and 'elta' in c.lower():
                colmap['delta'] = c
                break
    # contract identifiers
    for c in ('contract_symbol','contract','contract_id'):
        if c in df.columns:
            colmap['contract_id'] = c
            break
    # fallback to strike+expiry+type
    for c in ('strike','expiry','opt_type','type','cp'):
        if c in df.columns and 'contract_id' not in colmap:
            # we will build compound id later
            pass
    # underlying close
    for c in ('underlying_close','underlying','underlying_price','close'):
        if c in df.columns:
            colmap['underlying_close'] = c
            break
    return colmap


def build_contract_id(df: pd.DataFrame):
    for c in ('contract_symbol','contract','contract_id'):
        if c in df.columns:
            return df[c].astype(str)
    # fallback: build a per-row compound id
    parts = []
    # expiry -> normalized date string, fall back to timestamp date
    if 'expiry' in df.columns:
        expiry_dt = pd.to_datetime(df['expiry'], errors='coerce')
        if 'timestamp' in df.columns:
            ts_dt = pd.to_datetime(df['timestamp'], errors='coerce')
            expiry_str = expiry_dt.dt.strftime('%Y-%m-%d')
            expiry_str = expiry_str.fillna(ts_dt.dt.strftime('%Y-%m-%d'))
        else:
            expiry_str = expiry_dt.dt.strftime('%Y-%m-%d').fillna('noexp')
        parts.append(expiry_str.astype(str))
    # strike -> formatted numeric or placeholder
    if 'strike' in df.columns:
        try:
            strike_num = pd.to_numeric(df['strike'], errors='coerce')
            strike_str = strike_num.apply(lambda x: f"{x:.4g}" if not pd.isna(x) else 'strnan')
        except Exception:
            strike_str = df['strike'].astype(str).fillna('strnan')
        parts.append(strike_str.astype(str))
    # option type
    if 'option_type' in df.columns:
        parts.append(df['option_type'].astype(str).fillna('U'))
    elif 'type' in df.columns:
        parts.append(df['type'].astype(str).fillna('U'))
    elif 'opt_type' in df.columns:
        parts.append(df['opt_type'].astype(str).fillna('U'))

    if parts:
        cid = parts[0]
        for s in parts[1:]:
            cid = cid.str.cat(s, sep='_')
        # ensure no global NaT_nan grouping: if any cid entries are 'nan' or contain 'NaT', append row index
        bad = cid.isin(['nan', 'NaT', 'NaT_nan', 'strnan_U', 'strnan'])
        if bad.any():
            idx = pd.Series(df.index.astype(str), index=df.index)
            cid = cid.where(~bad, cid + '_' + idx)
        return cid
    # last resort: unique per row
    return pd.Series([f"unknown_{i}" for i in df.index], index=df.index)


def process_symbol(path: Path, horizon_days: float, out_path: Path):
    logging.info("Loading %s", path)
    df = pd.read_parquet(path)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    elif df.index.dtype.kind in 'M':
        df = df.reset_index()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    else:
        raise RuntimeError('No timestamp column or datetime index found in parquet')

    df = df.sort_values('timestamp').reset_index(drop=True)

    # compute mid
    if 'mid' not in df.columns and 'ask' in df.columns and 'bid' in df.columns:
        df['mid'] = (df['ask'] + df['bid']) / 2.0
    if 'mid' not in df.columns:
        logging.warning('No mid/bid/ask found; attempting to use close column')
        if 'close' in df.columns:
            df['mid'] = df['close']
        else:
            raise RuntimeError('No price column found to compute mid')

    # coerce mid to numeric and drop rows without price
    df['mid'] = pd.to_numeric(df['mid'], errors='coerce')

    # delta
    if 'delta' not in df.columns:
        for c in df.columns:
            if c.lower() == 'delta' or c.lower().endswith('_delta'):
                df['delta'] = df[c]
                break
    if 'delta' not in df.columns:
        logging.warning('No `delta` column found; filling zeros')
        df['delta'] = 0.0
    # ensure numeric delta and fill missing with 0.0
    df['delta'] = pd.to_numeric(df['delta'], errors='coerce').fillna(0.0)

    # underlying
    if 'underlying_close' not in df.columns:
        for c in ('underlying','underlying_price','underlying_close'):
            if c in df.columns:
                df['underlying_close'] = df[c]
                break
    if 'underlying_close' not in df.columns:
        logging.warning('No underlying close available; forward-filling if possible')
        df['underlying_close'] = df.get('close', np.nan)
    # coerce underlying_close to numeric
    df['underlying_close'] = pd.to_numeric(df['underlying_close'], errors='coerce')

    # report rows missing essential numeric fields; keep rows for global/underlying fallback
    missing_mask = df['mid'].isna() | df['underlying_close'].isna()
    num_missing = int(missing_mask.sum())
    logging.info('%d rows missing mid or underlying_close; keeping them for fallback processing', num_missing)

    # contract id
    df['contract_id'] = build_contract_id(df)

    # extra features
    if 'strike' in df.columns and 'underlying_close' in df.columns:
        df['moneyness'] = df['underlying_close'] / df['strike']
    if 'expiry' in df.columns:
        # try to parse expiry to datetime
        try:
            df['expiry_dt'] = pd.to_datetime(df['expiry'])
            df['tte_days'] = (df['expiry_dt'] - df['timestamp']).dt.total_seconds() / 86400.0
        except Exception:
            df['tte_days'] = np.nan

    # per-group merge_asof to find future mid and underlying after horizon_days
    horizon = pd.Timedelta(days=float(horizon_days))

    out_parts = []
    grouped = df.groupby('contract_id', sort=False)
    for _name, g in grouped:
        g = g.sort_values('timestamp').reset_index(drop=True)
        if len(g) < 2:
            continue
        left = g.copy()
        left['timestamp_target'] = left['timestamp'] + horizon
        right = g[['timestamp','mid','underlying_close']].rename(columns={'timestamp':'timestamp_right','mid':'mid_future','underlying_close':'underlying_future'})
        right = right.sort_values('timestamp_right')
        right = right[~right['timestamp_right'].isna()]
        if right.empty:
            continue
        # merge_asof requires column names aligned and sorted
        left_sorted = left.sort_values('timestamp_target')
        left_sorted = left_sorted[~left_sorted['timestamp_target'].isna()]
        if left_sorted.empty:
            continue
        merged = pd.merge_asof(left_sorted, right, left_on='timestamp_target', right_on='timestamp_right', direction='forward')
        # compute delta-hedged pnl: (mid_future - mid) - delta * (underlying_future - underlying_close)
        # ensure future columns exist (may be NaN if no match)
        if 'mid_future' not in merged.columns:
            merged['mid_future'] = np.nan
        if 'underlying_future' not in merged.columns:
            merged['underlying_future'] = np.nan

        # drop rows where future mid or underlying is missing (no forward match)
        merged = merged[~(merged['mid_future'].isna() | merged['underlying_future'].isna())]
        if merged.empty:
            continue
        merged['delta_hedged_pnl'] = (merged['mid_future'] - merged['mid']) - merged['delta'] * (merged['underlying_future'] - merged['underlying_close'])
        # relative return (guard divide-by-zero)
        merged['delta_hedged_return'] = merged['delta_hedged_pnl'] / merged['mid'].replace({0: np.nan})
        merged['label_delta_hedge_pos'] = (merged['delta_hedged_return'] > 0).astype(int)
        out_parts.append(merged)

    if out_parts:
        out = pd.concat(out_parts, ignore_index=True)
    else:
        logging.info('No groups with sufficient history found; attempting global merge_asof fallback')
        out = pd.DataFrame()
    # restore useful columns from original df if missing
    for c in df.columns:
        if c not in out.columns:
            out[c] = df[c]

    # Attempt global merge_asof by contract_id to find future mid and underlying after horizon
    left = df.copy()
    left['timestamp_target'] = left['timestamp'] + horizon
    # prepare right table
    right = df[['contract_id','timestamp','mid','underlying_close']].rename(columns={'timestamp':'timestamp_right','mid':'mid_future','underlying_close':'underlying_future'})
    # drop nulls in keys
    left = left[~left['timestamp_target'].isna()]
    right = right[~right['timestamp_right'].isna()]
    if left.empty or right.empty:
        logging.warning('No valid timestamps to perform contract merge_asof; falling back to underlying-only labels')
        do_underlying_fallback = True
    else:
        # ensure datetime and proper sorting required by merge_asof
        left['timestamp_target'] = pd.to_datetime(left['timestamp_target'], errors='coerce')
        right['timestamp_right'] = pd.to_datetime(right['timestamp_right'], errors='coerce')
        left_sorted = left.sort_values(['contract_id','timestamp_target']).reset_index(drop=True)
        right_sorted = right.sort_values(['contract_id','timestamp_right']).reset_index(drop=True)
        try:
            merged = pd.merge_asof(left_sorted, right_sorted, left_on='timestamp_target', right_on='timestamp_right', by='contract_id', direction='forward')
            # drop rows without forward match
            merged = merged[~(merged['mid_future'].isna() | merged['underlying_future'].isna())]
            if not merged.empty:
                # compute pnl and labels using option future + underlying future
                merged['delta_hedged_pnl'] = (merged['mid_future'] - merged['mid']) - merged['delta'] * (merged['underlying_future'] - merged['underlying_close'])
                merged['delta_hedged_return'] = merged['delta_hedged_pnl'] / merged['mid'].replace({0: np.nan})
                # also compute vanilla option return when available
                merged['option_return'] = (merged['mid_future'] - merged['mid']) / merged['mid'].replace({0: np.nan})
                # fallback: if delta_hedged_return is NaN, use option_return
                merged['delta_hedged_return'] = merged['delta_hedged_return'].fillna(merged['option_return'])
                merged['label_delta_hedge_pos'] = (merged['delta_hedged_return'] > 0).astype(int)
                merged['label_option_pos'] = (merged['option_return'] > 0).astype(int)
                out = merged.reset_index(drop=True)
                do_underlying_fallback = False
            else:
                do_underlying_fallback = True
        except Exception as e:
            logging.warning('Global merge_asof failed: %s; falling back to underlying-only labels', e)
            do_underlying_fallback = True

    if 'do_underlying_fallback' in locals() and do_underlying_fallback:
        logging.warning('No per-contract forward matches found — falling back to underlying-only label approximation')
        # load underlying series (assume symbol AAPL present at raw/AAPL.parquet)
        underlying_path = Path('raw') / 'AAPL.parquet'
        if not underlying_path.exists():
            raise RuntimeError('Underlying series raw/AAPL.parquet not found for fallback labeling')
        up = pd.read_parquet(underlying_path)
        if 'timestamp' in up.columns:
            up['timestamp'] = pd.to_datetime(up['timestamp'])
        elif up.index.dtype.kind in 'M':
            up = up.reset_index()
            up['timestamp'] = pd.to_datetime(up['timestamp'])
        else:
            raise RuntimeError('Underlying series has no timestamp')
        if 'close' not in up.columns:
            raise RuntimeError('Underlying series missing `close` column')
        up = up.sort_values('timestamp')
        # merge_asof to get underlying future for left
        left_for_uf = left.sort_values('timestamp_target').reset_index(drop=True)
        uf = pd.merge_asof(left_for_uf, up[['timestamp','close']].rename(columns={'timestamp':'timestamp_right','close':'underlying_future'}), left_on='timestamp_target', right_on='timestamp_right', direction='forward')
        uf = uf[~uf['underlying_future'].isna()]
        if uf.empty:
            raise RuntimeError('No underlying future matches found; cannot compute fallback labels')
        uf['delta_hedged_pnl'] = - uf['delta'] * (uf['underlying_future'] - uf['underlying_close'])
        uf['delta_hedged_return'] = uf['delta_hedged_pnl'] / uf['mid'].replace({0: np.nan})
        # when mid is missing or delta is zero, also provide a coarse option label from underlying direction
        uf['label_delta_hedge_pos'] = (uf['delta_hedged_return'] > 0).astype(int)
        uf['label_option_pos'] = (uf['underlying_future'] - uf['underlying_close'] > 0).astype(int)
        out = uf.reset_index(drop=True)

    return out


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Prepare option labels (delta-hedged P&L)')
    p.add_argument('--symbol', required=True, help='Symbol base name in raw/, e.g. AAPL_OPT')
    p.add_argument('--horizon_days', type=float, default=1.0)
    p.add_argument('--out', default=None, help='Output parquet path (optional)')
    args = p.parse_args()
    src = Path('raw') / f"{args.symbol}.parquet"
    if args.out:
        dst = Path(args.out)
    else:
        dst = Path('raw') / f"{args.symbol}_labeled.parquet"
    out_df = process_symbol(src, args.horizon_days, dst)
    logging.info('Writing labeled parquet to %s rows=%d', dst, len(out_df))
    dst.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(dst)
