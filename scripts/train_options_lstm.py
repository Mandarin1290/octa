#!/usr/bin/env python3
"""Train a per-contract LSTM on refined option labels.

This is a lightweight trainer for verification and prototyping.
"""
import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import tensorflow as tf

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def build_sequences(df: pd.DataFrame, seq_len=32, features=None, min_seq=4):
    feats = features or ['delta','moneyness','tte_days','mid','underlying_close']
    feats = [f for f in feats if f in df.columns]
    X, y = [], []
    for _contract, g in df.groupby('contract_id'):
        g = g.sort_values('timestamp')
        arr = g[feats].fillna(0).to_numpy(dtype=np.float32)
        labels = g['label_refined_pos'].to_numpy()
        if len(arr) < min_seq:
            continue
        for i in range(len(arr) - seq_len + 1):
            X.append(arr[i:i+seq_len])
            y.append(labels[i+seq_len-1])
    if not X:
        return None, None, feats
    X = np.stack(X)
    y = np.array(y, dtype=np.int32)
    return X, y, feats


def wf_date_split(df: pd.DataFrame, n_folds=3):
    dates = pd.to_datetime(df['timestamp']).dt.date.unique()
    n = len(dates)
    splits = []
    for i in range(n_folds):
        train_end = int((i + 1) * n / (n_folds + 1))
        val_end = int((i + 2) * n / (n_folds + 1))
        if train_end < 1 or val_end <= train_end:
            continue
        train_dates = dates[:train_end]
        val_dates = dates[train_end:val_end]
        splits.append((train_dates, val_dates))
    return splits


def build_model(input_shape):
    model = tf.keras.Sequential([
        tf.keras.layers.Input(shape=input_shape),
        tf.keras.layers.Masking(mask_value=0.0),
        tf.keras.layers.LSTM(64, return_sequences=False),
        tf.keras.layers.Dense(32, activation='relu'),
        tf.keras.layers.Dense(1, activation='sigmoid')
    ])
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['AUC','accuracy'])
    return model


def run(path: Path, seq_len=32, epochs=2, batch_size=256):
    logging.info('Loading %s', path)
    df = pd.read_parquet(path)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    else:
        raise RuntimeError('timestamp required')

    splits = wf_date_split(df, n_folds=3)
    if not splits:
        raise RuntimeError('Not enough dates for WF splits')

    # build sequences once (could be memory heavy)
    X, y, feats = build_sequences(df, seq_len=seq_len)
    if X is None:
        raise RuntimeError('No sequences built; check seq_len or data density')
    logging.info('Built sequences X=%s y=%s features=%s', X.shape, y.shape, feats)

    # for simplicity do a single WF split: use first split
    train_dates, val_dates = splits[0]
    # reconstruct indices mapping sequences back to last timestamp's date
    # We'll approximate by rebuilding sequence endpoints to dates
    endpoints = []
    for _contract, g in df.groupby('contract_id'):
        g = g.sort_values('timestamp')
        for i in range(len(g) - seq_len + 1):
            endpoints.append(g['timestamp'].iloc[i+seq_len-1].date())
    endpoints = np.array(endpoints)
    train_idx = np.isin(endpoints, train_dates)
    val_idx = np.isin(endpoints, val_dates)
    X_train, y_train = X[train_idx], y[train_idx]
    X_val, y_val = X[val_idx], y[val_idx]
    logging.info('Train sequences=%d Val sequences=%d', len(X_train), len(X_val))

    model = build_model(input_shape=(seq_len, X.shape[2]))
    history = model.fit(X_train, y_train, validation_data=(X_val, y_val), epochs=epochs, batch_size=batch_size)

    out_dir = Path('models')
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / f"{path.stem}_lstm.h5"
    model.save(model_path)
    metrics_path = Path('raw') / f"{path.stem}_lstm_metrics.json"
    import json
    json.dump({k: v[-1] for k, v in history.history.items()}, open(metrics_path, 'w'))
    logging.info('Saved LSTM model to %s and metrics to %s', model_path, metrics_path)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--symbol', required=True)
    p.add_argument('--seq_len', type=int, default=32)
    p.add_argument('--epochs', type=int, default=2)
    p.add_argument('--batch_size', type=int, default=256)
    args = p.parse_args()
    src = Path('raw') / f"{args.symbol}.parquet"
    run(src, seq_len=args.seq_len, epochs=args.epochs, batch_size=args.batch_size)


if __name__ == '__main__':
    main()
