#!/usr/bin/env python3
"""Convert sample CSVs in raw/frd_complete_plus_options_sample/ to Parquet in raw/
and run multi-timeframe training for selected symbols.
"""
import glob
import json
import shutil
import subprocess
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

SRC = Path("raw/frd_complete_plus_options_sample")
DST_DIR = Path("raw")
DST_DIR.mkdir(parents=True, exist_ok=True)

SUFFIX_MAP = {
    '1day': '',
    '1hour': '_1H',
    '30min': '_30M',
    '5min': '_5M',
    '1min': '_1M',
    'adjusted': ''
}

# helper to detect time column
TIME_CANDIDATES = ['timestamp', 'datetime', 'date', 'time']

print('Scanning', SRC)
for csv in sorted(SRC.glob('*.csv')):
    name = csv.stem
    parts = name.split('_')
    if len(parts) < 2:
        print('Skipping unexpected file name', csv)
        continue
    base = parts[0]
    token = None
    for p in parts[1:]:
        pl = p.lower()
        if pl in SUFFIX_MAP:
            token = pl
            break
    if token is None:
        print('Unknown timeframe token for', csv, '- copying as', base + '.parquet')
        dest_name = f"{base}.parquet"
    else:
        suffix = SUFFIX_MAP[token]
        dest_symbol = f"{base}{suffix}" if suffix else base
        dest_name = f"{dest_symbol}.parquet"
    dest = DST_DIR / dest_name
    # read CSV, skip malformed lines to avoid spurious ParserWarnings
    try:
        df = pd.read_csv(csv, engine='python', on_bad_lines='skip')
    except TypeError:
        # older pandas compatibility
        df = pd.read_csv(csv, engine='python', error_bad_lines=False)
    # normalize column names
    df.columns = [c.strip() for c in df.columns]
    # find time column
    time_col = None
    cols_lower = [c.lower() for c in df.columns]
    for cand in TIME_CANDIDATES:
        if cand in cols_lower:
            time_col = df.columns[cols_lower.index(cand)]
            break
    if time_col is None:
        # fallback to first column
        time_col = df.columns[0]
    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors='coerce')
    # ensure 'close' exists
    if 'close' not in [c.lower() for c in df.columns]:
        # try to find a close-like column
        close_col = None
        for c in df.columns:
            if c.lower() in ('last','last_price','close_price'):
                close_col = c
                break
        if close_col:
            df = df.rename(columns={close_col: 'close'})
        else:
            print('File', csv, 'has no close column, skipping')
            continue
    # write parquet (preserve column names)
    # synthesize missing OHLC/volume if not present so sample data can exercise training
    # keep deterministic randomness for reproducibility
    rng = np.random.default_rng(42)
    col_l = [c.lower() for c in df.columns]
    if 'open' not in col_l or 'high' not in col_l or 'low' not in col_l:
        # normalize col lookup
        close_col = None
        for c in df.columns:
            if c.lower() == 'close':
                close_col = c
                break
        closes = pd.to_numeric(df[close_col], errors='coerce').fillna(method='ffill').fillna(method='bfill').astype(float)
        # create open as previous close
        opens = closes.shift(1).fillna(closes.iloc[0])
        # small random wiggle for high/low
        wiggle = (np.abs(closes - opens) + 1e-6) * 0.5
        rand = rng.standard_normal(len(closes))
        highs = np.maximum(opens, closes) + np.abs(rand) * wiggle + (closes * 1e-3)
        lows = np.minimum(opens, closes) - np.abs(rand) * wiggle - (closes * 1e-3)
        # assign back with reasonable column names
        df['open'] = opens.values
        df['high'] = highs
        df['low'] = lows
    if 'volume' not in [c.lower() for c in df.columns]:
        # synthetic volumes scaled to timeframe (if timeframe token in filename)
        # attempt to infer timeframe from dest_name
        vol_scale = 1
        if '_1M' in dest_name or '1min' in str(csv.name).lower():
            vol_scale = 100
        elif '_5M' in dest_name or '5min' in str(csv.name).lower():
            vol_scale = 50
        elif '_1H' in dest_name or '1hour' in str(csv.name).lower():
            vol_scale = 10
        vols = (np.abs(rng.integers(100, 1000, size=len(df))) * vol_scale)
        df['volume'] = vols
    try:
        # suppress pandas.ParserWarning noise for now
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            df.to_parquet(dest)
        print('Wrote', dest, 'rows=', len(df))
    except Exception as e:
        print('Failed writing', dest, e)

# run option aggregator to create *_OPT.parquet
agg = Path('scripts/aggregate_option_snapshots.py')
if agg.exists():
    print('Running option aggregator')
    subprocess.run(['python', str(agg)], check=False)

# run multi-TF training for all detected symbols in raw/
parquets = sorted(DST_DIR.glob('*.parquet'))
symbols = set()
for p in parquets:
    name = p.stem
    # include option aggregate files as their own symbol so they can be processed
    if name.endswith('_OPT'):
        symbols.add(name)
        continue
    base = name.split('_')[0]
    symbols.add(base)

symbols = sorted(symbols)

# prepare artifacts dir
ARTIFACT_ROOT = Path('artifacts')
ARTIFACT_ROOT.mkdir(exist_ok=True)
run_ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
run_dir = ARTIFACT_ROOT / run_ts
run_dir.mkdir(parents=True, exist_ok=True)

for s in symbols:
    print('\nRunning train_multiframe_symbol for', s)
    # invoke via same interpreter from environment
    subprocess.run(['python', 'scripts/train_multiframe_symbol.py', '--symbol', s], check=False)
    # collect artifacts (models, parquet, metrics) that mention the symbol
    dest = run_dir / s
    dest.mkdir(parents=True, exist_ok=True)
    # copy related raw parquets
    for f in DST_DIR.glob(f'{s}*.parquet'):
        try:
            shutil.copy2(f, dest)
        except Exception:
            pass
    # copy models/ files matching symbol
    for f in glob.glob('models/*'):
        if s in Path(f).name:
            try:
                shutil.copy2(f, dest)
            except Exception:
                pass
    # copy any metric jsons under models or raw
    for f in glob.glob('**/*metrics*.json', recursive=True):
        if s in Path(f).name:
            try:
                shutil.copy2(f, dest)
            except Exception:
                pass
    # create a short run manifest
    manifest = {
        'symbol': s,
        'collected': [str(p.name) for p in dest.iterdir()]
    }
    with open(dest / 'manifest.json', 'w') as fh:
        json.dump(manifest, fh)

print('\nDone')
