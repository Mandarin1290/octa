#!/usr/bin/env python3
"""Augment Parquet files under raw/ so they reach a minimum row count for tests.

Usage: python scripts/augment_parquets_for_tests.py [--target N]
If no target provided, uses training config splits to compute required bars and adds 50.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import numpy as np
import pandas as _pd
import pandas as pd

from octa_training.core.config import load_config
from octa_training.core.io_parquet import discover_parquets, load_parquet


def synthesize_ohlc_if_missing(df: pd.DataFrame) -> pd.DataFrame:
    cols = set(df.columns)
    if not {'open', 'high', 'low', 'close'}.issubset(cols):
        # build from close if available
        if 'close' in cols:
            close = df['close'].ffill().fillna(method='bfill')
            open_ = close.shift(1).fillna(close)
            high = pd.concat([open_, close], axis=1).max(axis=1) * 1.0005
            low = pd.concat([open_, close], axis=1).min(axis=1) * 0.9995
            df['open'] = open_
            df['high'] = high
            df['low'] = low
            df['close'] = close
        else:
            raise RuntimeError('Cannot synthesize OHLC: no close column')
    if 'volume' not in df.columns:
        df['volume'] = (np.abs(df.get('volume', pd.Series(1, index=df.index))) + 1).astype(int)
    return df


def median_delta_to_offset(delta: pd.Timedelta) -> pd.DateOffset:
    # Convert timedelta to pandas offset using seconds
    secs = int(delta.total_seconds())
    if secs % 86400 == 0:
        return pd.DateOffset(days=secs // 86400)
    if secs % 3600 == 0:
        return pd.DateOffset(hours=secs // 3600)
    if secs % 60 == 0:
        return pd.DateOffset(minutes=secs // 60)
    return pd.DateOffset(seconds=secs)


def augment_df_to_target(df: pd.DataFrame, target: int, seed: int = 42) -> pd.DataFrame:
    cur = len(df)
    if cur >= target:
        return df
    rng = np.random.RandomState(seed)
    # ensure datetime index
    df = df.copy()
    df.index = pd.to_datetime(df.index, utc=True)
    if cur >= 2:
        deltas = df.index.to_series().diff().dropna()
        if len(deltas):
            med = pd.Timedelta(int(deltas.dt.total_seconds().median()), unit='s')
        else:
            med = pd.Timedelta(days=1)
    else:
        med = pd.Timedelta(days=1)
    # handle empty input by creating a synthetic random-walk series
    if cur == 0:
        rng = np.random.RandomState(seed)
        # create base timestamps ending now
        now = pd.Timestamp.now(tz='UTC')
        new_index = [now - (target - i - 1) * med for i in range(target)]
        # synthetic price walk
        base = 100.0
        returns = rng.normal(loc=0.0, scale=0.001, size=target)
        prices = base * np.exp(np.cumsum(returns))
        o = np.roll(prices, 1)
        o[0] = prices[0]
        h = np.maximum(o, prices) * 1.0005
        l = np.minimum(o, prices) * 0.9995
        vol = (rng.randint(50, 1000, size=target)).astype(int)
        new = pd.DataFrame({'open': o, 'high': h, 'low': l, 'close': prices, 'volume': vol}, index=new_index)
        new.index = pd.to_datetime(new.index, utc=True)
        return new.sort_index()
    needed = target - cur
    start = df.index.max()
    # build new index
    new_index = [start + (i + 1) * med for i in range(needed)]
    # cycle existing rows to fill
    idxs = np.resize(np.arange(cur), needed)
    new_rows = df.iloc[idxs].copy()
    # apply tiny noise to prices and randomized volume
    price_cols = [c for c in ('open', 'high', 'low', 'close') if c in new_rows.columns]
    for c in price_cols:
        arr = new_rows[c].to_numpy(dtype=float)
        noise = rng.normal(loc=0.0, scale=0.0005, size=arr.shape)
        arr = arr * (1.0 + noise)
        new_rows[c] = arr
    if 'volume' in new_rows.columns:
        vol = new_rows['volume'].to_numpy(dtype=float)
        vol = np.maximum(1, (vol * (1.0 + rng.normal(0, 0.1, size=vol.shape))).astype(int))
        new_rows['volume'] = vol
    new_rows.index = pd.to_datetime(new_index, utc=True)
    out = pd.concat([df, new_rows])
    out = out[~out.index.duplicated(keep='first')]
    out = out.sort_index()
    return out


def main(target: int | None = None, dry: bool = False):
    cfg = load_config()
    splits = cfg.splits
    min_train = int(splits.get('min_train_size', 500))
    min_test = int(splits.get('min_test_size', 100))
    train_window = int(splits.get('train_window', 1000))
    test_window = int(splits.get('test_window', 200))
    required = max(min_train + min_test, train_window + test_window)
    target_default = required + 50
    tgt = int(target or target_default)

    raw = Path(cfg.paths.raw_dir)
    found = discover_parquets(raw)
    print('Raw dir:', raw)
    print('Using target rows:', tgt)
    for p in found:
        # try validated load first
        df = None
        needs_repair = False
        try:
            df = load_parquet(p.path)
            rows = len(df)
        except Exception as e:
            # fallback: try raw pandas read to allow fixing bad OHLC
            try:
                raw_df = _pd.read_parquet(p.path)
                raw_df.columns = [c.lower() for c in raw_df.columns]
                # find time column
                time_col = None
                for cand in ('timestamp', 'datetime', 'date', 'time'):
                    if cand in raw_df.columns:
                        time_col = cand
                        break
                if time_col is None:
                    print('Skipping', p.path, 'no time column found')
                    continue
                raw_df[time_col] = _pd.to_datetime(raw_df[time_col], utc=True, errors='coerce')
                raw_df = raw_df.set_index(time_col)
                raw_df = raw_df.sort_index()
                raw_df = raw_df[~raw_df.index.duplicated(keep='first')]
                df = raw_df
                rows = len(df)
                needs_repair = True
                print('Fallback-read succeeded for', p.path, 'rows=', rows)
            except Exception:
                print('Skipping', p.path, 'read error:', e)
                continue
        if rows >= tgt and not needs_repair:
            print(p.symbol, 'ok', rows)
            continue
        print(p.symbol, 'augmenting', rows, '->', tgt)
        # backup
        bakdir = raw / 'backups'
        bakdir.mkdir(exist_ok=True)
        bakp = bakdir / (p.path.name + '.orig')
        if not bakp.exists():
            shutil.copy2(p.path, bakp)
        # ensure OHLC/volume
        try:
            df2 = synthesize_ohlc_if_missing(df)
        except Exception as e:
            print('Failed to synthesize OHLC for', p.symbol, 'skipping:', e)
            continue
        new = augment_df_to_target(df2, tgt)
        # enforce OHLC invariants before writing
        if 'high' in new.columns and ('open' in new.columns or 'close' in new.columns):
            new['high'] = new['high'].where(new['high'] >= new[['open','close']].max(axis=1), new[['open','close']].max(axis=1))
        if 'low' in new.columns and ('open' in new.columns or 'close' in new.columns):
            new['low'] = new['low'].where(new['low'] <= new[['open','close']].min(axis=1), new[['open','close']].min(axis=1))
        # fix non-positive prices
        for c in ('open','high','low','close'):
            if c in new.columns:
                new[c] = new[c].clip(lower=1e-6)
        if dry:
            print('Dry-run: would write', p.path)
            continue
        # write back: reset index as timestamp
        out = new.reset_index()
        if out.columns[0].lower() not in ('timestamp', 'datetime', 'date', 'time'):
            out = out.rename(columns={out.columns[0]: 'timestamp'})
        out.to_parquet(p.path)
        print('Wrote augmented parquet', p.path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', type=int, help='Target minimum rows')
    parser.add_argument('--dry', action='store_true', help='Dry run, do not write files')
    args = parser.parse_args()
    main(target=args.target, dry=args.dry)
