#!/usr/bin/env python3
"""Convert index TXT files inside ZIPs to Parquet files in OCTA schema.

Writes per-symbol parquet files with index timestamp (UTC) and columns
`open,high,low,close,volume` (volume may be NaN if not present).
"""
from __future__ import annotations

import argparse
import io
import sys
import zipfile
from pathlib import Path

import pandas as pd


def convert_txt_stream_to_parquet(fh, out_path: Path):
    # Read CSV without header: expected columns: timestamp, open, high, low, close [, volume]
    try:
        df = pd.read_csv(fh, header=None)
    except Exception:
        # try with explicit encoding
        fh.seek(0)
        df = pd.read_csv(fh, header=None, encoding='utf-8', engine='python')

    # normalize columns depending on number of cols
    if df.shape[1] >= 5:
        names = ['timestamp', 'open', 'high', 'low', 'close'] + ([f'v{i}' for i in range(6, df.shape[1]+1)])
        df.columns = names[: df.shape[1]]
    else:
        raise RuntimeError(f'unexpected column count: {df.shape[1]}')

    # parse timestamp, coerce errors
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    # ensure timezone-aware UTC; treat naive timestamps as UTC
    try:
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
    except Exception:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')

    # lower-case column names and map first five
    df.columns = [str(c).lower() for c in df.columns]

    # ensure required 'close' exists
    if 'close' not in df.columns:
        raise RuntimeError('close column missing')

    # keep only first five if extras present
    keep_cols = ['timestamp', 'open', 'high', 'low', 'close']
    for c in keep_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[keep_cols]

    # ensure volume column exists (may be NaN)
    if 'volume' not in df.columns:
        df['volume'] = pd.NA

    # sort and deduplicate by timestamp, keep timestamp as column for io_parquet compatibility
    df = df.sort_values('timestamp')
    df = df[~df['timestamp'].duplicated(keep='first')]

    # write parquet with timestamp column (index=False)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, compression='snappy')


def process_zip(zip_path: Path, out_dir: Path, pattern: str = None, overwrite: bool = False):
    with zipfile.ZipFile(zip_path, 'r') as z:
        for name in z.namelist():
            if not name.lower().endswith('.txt'):
                continue
            if pattern and pattern not in name:
                continue
            # derive output name from full TXT basename (preserve timeframe)
            # e.g. "DJI_full_1min.txt" -> "DJI_full_1min.parquet"
            base = Path(name).stem
            out_name = base
            out_path = out_dir / f"{out_name}.parquet"
            if out_path.exists() and not overwrite:
                print(f'skipping existing {out_path}')
                continue
            print(f'converting {name} -> {out_path}')
            with z.open(name) as fh:
                # wrap binary stream as text for pandas
                try:
                    b = fh.read()
                    s = io.BytesIO(b)
                    convert_txt_stream_to_parquet(s, out_path)
                except Exception as e:
                    print(f'failed to convert {name}: {e}', file=sys.stderr)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--src', default='raw/Indices', help='source folder containing ZIPs')
    p.add_argument('--out', default='raw/Indices_parquet', help='output parquet directory')
    p.add_argument('--zip', default=None, help='only process this zip filename')
    p.add_argument('--pattern', default=None, help='only convert txt filenames containing this substring')
    p.add_argument('--overwrite', action='store_true')
    args = p.parse_args()

    src = Path(args.src)
    out = Path(args.out)
    if args.zip:
        z = src / args.zip
        if not z.exists():
            print('zip not found', z)
            return 2
        process_zip(z, out, pattern=args.pattern, overwrite=args.overwrite)
        return 0

    for z in src.iterdir():
        if z.suffix.lower() != '.zip':
            continue
        process_zip(z, out, pattern=args.pattern, overwrite=args.overwrite)


if __name__ == '__main__':
    raise SystemExit(main())
