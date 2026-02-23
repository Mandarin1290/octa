#!/usr/bin/env python3
"""Aggregate option chain CSV snapshots into a time-indexed Parquet for option training.

Supports multiple CSV files in a source directory.
Computes `tte_days`, `moneyness`, `contract_id` (if underlying parquet available).

Usage:
    python aggregate_option_snapshots.py \
        --symbol AAPL \
        --src-dir raw/frd_complete_plus_options_sample \
        --dst raw/AAPL_OPT.parquet
"""
import argparse
from pathlib import Path
from typing import List, Optional

import pandas as pd


def aggregate(
    *,
    src_dir: Path,
    dst: Path,
    underlying_symbol: str,
    underlying_parquet_candidates: Optional[List[Path]] = None,
) -> pd.DataFrame:
    """Aggregate option CSV snapshots into a single parquet.

    Returns the dataframe written.
    Raises ValueError on schema violations (missing required columns).
    No module-level execution — must be called explicitly.
    """
    src_dir = Path(src_dir)
    dst = Path(dst)

    files = sorted(src_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSVs found in {src_dir}")

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, engine='python', on_bad_lines='skip')
        except TypeError:
            df = pd.read_csv(f, engine='python', error_bad_lines=False)
        df.columns = [c.strip() for c in df.columns]

        # parse trade date (case-insensitive)
        cols_lower = {c.lower(): c for c in df.columns}
        if 'trade date' in cols_lower:
            df['trade_date'] = pd.to_datetime(df[cols_lower['trade date']], utc=True, errors='coerce')
        elif 'trade_date' in cols_lower:
            df['trade_date'] = pd.to_datetime(df[cols_lower['trade_date']], utc=True, errors='coerce')
        else:
            df['trade_date'] = pd.to_datetime(df.iloc[:, 0], errors='coerce')

        # expiry
        if 'expiry date' in cols_lower:
            df['expiry'] = pd.to_datetime(df[cols_lower['expiry date']], utc=True, errors='coerce')
        elif 'expiry' in cols_lower:
            df['expiry'] = pd.to_datetime(df[cols_lower['expiry']], utc=True, errors='coerce')

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
            'callput': 'option_type',
        }
        rename_map = {}
        for col in df.columns:
            key = col.strip().lower()
            if key in col_map:
                rename_map[col] = col_map[key]
        if rename_map:
            df = df.rename(columns=rename_map)

        # normalise option_type to "C" / "P"
        if 'option_type' in df.columns:
            df['option_type'] = df['option_type'].astype(str).str.strip().str.upper().str[:1]

        if 'iv_bid' in df.columns and 'iv_ask' in df.columns:
            df['iv'] = df[['iv_bid', 'iv_ask']].replace(0.0, pd.NA).mean(axis=1)

        for c in ['trade_date', 'close', 'strike', 'expiry', 'option_type']:
            if c not in df.columns:
                df[c] = pd.NA

        df['symbol'] = f"{underlying_symbol}_OPT"

        for ncol in ('close', 'bid', 'ask', 'strike', 'delta', 'iv', 'iv_bid', 'iv_ask'):
            if ncol in df.columns:
                df[ncol] = pd.to_numeric(df[ncol], errors='coerce')

        if 'mid' not in df.columns:
            if 'bid' in df.columns and 'ask' in df.columns:
                df['mid'] = (df['bid'] + df['ask']) / 2.0
            else:
                df['mid'] = df.get('close')

        frames.append(df)

    all_df = pd.concat(frames, ignore_index=True, sort=False)
    all_df['timestamp'] = pd.to_datetime(all_df['trade_date'], utc=True, errors='coerce')
    all_df = all_df.sort_values('timestamp')

    # Build contract_id if absent (spec requirement: never null)
    if 'contract_id' not in all_df.columns or all_df['contract_id'].isna().all():
        def _make_contract_id(row):
            try:
                exp_str = pd.to_datetime(row['expiry']).strftime('%Y%m%d') if pd.notna(row['expiry']) else 'NOEXP'
                strike_str = f"{float(row['strike']):.2f}" if pd.notna(row['strike']) else 'NOSTRIKE'
                otype = str(row.get('option_type', 'X')).strip()[:1].upper() or 'X'
                return f"{underlying_symbol}_{exp_str}_{strike_str}_{otype}"
            except Exception:
                return f"{underlying_symbol}_UNKNOWN"
        all_df['contract_id'] = all_df.apply(_make_contract_id, axis=1)

    # Resolve underlying close from candidate parquets
    underlying_close = None
    candidates = underlying_parquet_candidates or [
        Path(f"raw/{underlying_symbol}.parquet"),
        Path(f"raw/{underlying_symbol}_1D.parquet"),
        Path(f"raw/{underlying_symbol}_1day.parquet"),
        Path(f"raw/{underlying_symbol}_daily.parquet"),
    ]
    for cand in candidates:
        if cand.exists():
            try:
                ut = pd.read_parquet(cand)
                ut.columns = [c.lower() for c in ut.columns]
                if 'timestamp' in ut.columns:
                    ut['timestamp'] = pd.to_datetime(ut['timestamp'], utc=True, errors='coerce')
                    ut = ut.set_index('timestamp')
                else:
                    try:
                        ut.index = pd.to_datetime(ut.index)
                    except Exception:
                        pass
                if 'close' in ut.columns:
                    underlying_close = ut
                    break
            except Exception:
                continue

    if underlying_close is not None:
        uc = underlying_close.copy()
        uc.index = pd.to_datetime(uc.index, utc=True)

        def lookup_underlying(ts):
            if pd.isna(ts):
                return pd.NA
            try:
                d = pd.to_datetime(ts).normalize()
                idx = uc.index.normalize()
                match = (idx == d)
                if match.any():
                    val = uc.loc[match]
                    if 'close' in val.columns:
                        return float(val['close'].iloc[0])
            except Exception:
                return pd.NA
            return pd.NA

        all_df['underlying_close'] = all_df['timestamp'].apply(lookup_underlying)
        all_df['underlying_symbol'] = underlying_symbol
    else:
        print(f'Underlying {underlying_symbol} parquet not found; moneyness will be NaN')
        all_df['underlying_close'] = pd.NA
        all_df['underlying_symbol'] = underlying_symbol

    all_df['expiry'] = pd.to_datetime(all_df['expiry'], errors='coerce')
    all_df['tte_days'] = (all_df['expiry'] - all_df['timestamp']).dt.days
    all_df['moneyness'] = pd.NA
    mask = all_df['underlying_close'].notna() & all_df['strike'].notna()
    all_df.loc[mask, 'moneyness'] = (
        all_df.loc[mask, 'underlying_close'].astype(float)
        / all_df.loc[mask, 'strike'].astype(float)
    )

    # --- Schema validation ---
    required_cols = [
        'timestamp', 'symbol', 'underlying_symbol', 'contract_id',
        'option_type', 'strike', 'expiry', 'tte_days', 'mid',
        'underlying_close', 'moneyness', 'delta', 'iv',
    ]
    missing = [c for c in required_cols if c not in all_df.columns]
    if missing:
        raise ValueError(f"aggregate_option_snapshots: missing required output columns: {missing}")

    # timestamp must not be all-NaN
    if all_df['timestamp'].isna().all():
        raise ValueError("aggregate_option_snapshots: all timestamps are NaN")

    out_to_write = all_df.set_index('timestamp').reset_index()
    dst.parent.mkdir(parents=True, exist_ok=True)
    print(
        f"Writing {dst} rows={len(out_to_write)}"
        f" start={out_to_write['timestamp'].min()}"
        f" end={out_to_write['timestamp'].max()}"
    )
    out_to_write.to_parquet(dst)
    print('Done')
    return out_to_write


def main() -> None:
    p = argparse.ArgumentParser(description="Aggregate option CSV snapshots into parquet")
    p.add_argument('--symbol', required=True, help="Underlying symbol, e.g. AAPL")
    p.add_argument('--src-dir', default=None, help="Source CSV directory")
    p.add_argument('--dst', default=None, help="Output parquet path")
    args = p.parse_args()

    underlying = args.symbol.upper()
    src_dir = Path(args.src_dir) if args.src_dir else Path(f"raw/frd_complete_plus_options_sample")
    dst = Path(args.dst) if args.dst else Path(f"raw/{underlying}_OPT.parquet")

    aggregate(src_dir=src_dir, dst=dst, underlying_symbol=underlying)


if __name__ == '__main__':
    main()
