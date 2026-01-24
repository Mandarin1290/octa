from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import pandas as pd


def detect_from_parquet(parquet_path: str) -> Dict:
    """Inspect a parquet file and return a simple asset profile.

    Returns dict with keys: asset_type (one of 'timeseries','tabular','unknown'),
    freq (approx), columns, numeric_cols, categorical_cols
    """
    p = Path(parquet_path)
    if not p.exists():
        raise FileNotFoundError(parquet_path)
    df = pd.read_parquet(p)
    cols = list(df.columns)
    numeric = df.select_dtypes(include=['number']).columns.tolist()
    categorical = [c for c in cols if c not in numeric]

    # heuristics: if there's a timestamp column and numeric features -> timeseries
    ts_cols = [c for c in cols if 'time' in c.lower() or 'timestamp' in c.lower() or 'date' in c.lower()]
    asset_type = 'unknown'
    freq = None
    if ts_cols and numeric:
        asset_type = 'timeseries'
        # attempt to infer frequency from first timestamp column
        try:
            s = pd.to_datetime(df[ts_cols[0]]).sort_values().dropna()
            if len(s) >= 3:
                diffs = s.diff().dropna().astype('timedelta64[s]').values
                if len(diffs):
                    avg = int(diffs.mean())
                    freq = avg
        except Exception:
            freq = None
    elif numeric:
        asset_type = 'tabular'

    return {
        'asset_type': asset_type,
        'freq_seconds': freq,
        'columns': cols,
        'numeric_cols': numeric,
        'categorical_cols': categorical,
    }


def detect_from_path(path: str) -> Dict:
    p = Path(path)
    if p.is_file() and p.suffix in ('.parquet', '.pq'):
        return detect_from_parquet(str(p))
    # fallback: inspect first parquet under dir
    if p.is_dir():
        files = sorted(p.rglob('*.parquet'))
        if files:
            return detect_from_parquet(str(files[0]))
    raise FileNotFoundError(path)


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--parquet', required=True)
    args = p.parse_args()
    profile = detect_from_path(args.parquet)
    print(json.dumps(profile, indent=2))
