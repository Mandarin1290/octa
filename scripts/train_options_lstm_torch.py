#!/usr/bin/env python3
"""PyTorch LSTM trainer for per-contract option sequences.

Lightweight; intended for quick verification on `raw/{symbol}.parquet`.
"""
import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def build_sequences(df: pd.DataFrame, seq_len=32, features=None, min_seq=4):
    feats = features or ['delta','moneyness','tte_days','mid','underlying_close']
    feats = [f for f in feats if f in df.columns]
    X, y, endpoints = [], [], []
    for _contract, g in df.groupby('contract_id'):
        g = g.sort_values('timestamp')
        # ensure numeric dtypes to avoid downcasting warnings
        sub = g[feats].apply(pd.to_numeric, errors='coerce')
        arr = sub.fillna(0).to_numpy(dtype=np.float32)
        # prefer refined option label if present, else fall back to option_pos or delta-hedge label
        if 'label_option_pos' in g.columns:
            labels = g['label_option_pos'].to_numpy()
        elif 'label_refined_pos' in g.columns:
            labels = g['label_refined_pos'].to_numpy()
        elif 'label_delta_hedge_pos' in g.columns:
            labels = g['label_delta_hedge_pos'].to_numpy()
        else:
            # unable to find label column; skip this contract
            continue
        if len(arr) < min_seq:
            continue
        for i in range(len(arr) - seq_len + 1):
            X.append(arr[i:i+seq_len])
            y.append(labels[i+seq_len-1])
            endpoints.append(g['timestamp'].iloc[i+seq_len-1].date())
    if not X:
        return None, None, None, feats
    X = np.stack(X)
    y = np.array(y, dtype=np.int64)
    endpoints = np.array(endpoints)
    return X, y, endpoints, feats


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


def train_torch(path: Path, seq_len=32, epochs=2, batch_size=256):
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, TensorDataset

    logging.info('Loading %s', path)
    df = pd.read_parquet(path)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    else:
        raise RuntimeError('timestamp required')

    splits = wf_date_split(df, n_folds=3)
    if not splits:
        raise RuntimeError('Not enough dates for WF splits')

    X, y, endpoints, feats = build_sequences(df, seq_len=seq_len)
    if X is None:
        raise RuntimeError('No sequences built; check seq_len or data density')
    logging.info('Built sequences X=%s y=%s features=%s', X.shape, y.shape, feats)

    train_dates, val_dates = splits[0]
    train_idx = np.isin(endpoints, train_dates)
    val_idx = np.isin(endpoints, val_dates)
    X_train, y_train = X[train_idx], y[train_idx]
    X_val, y_val = X[val_idx], y[val_idx]
    logging.info('Train sequences=%d Val sequences=%d', len(X_train), len(X_val))

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    class LSTMNet(nn.Module):
        def __init__(self, input_size):
            super().__init__()
            self.lstm = nn.LSTM(input_size, 64, batch_first=True)
            self.fc = nn.Sequential(nn.Linear(64, 32), nn.ReLU(), nn.Linear(32,1), nn.Sigmoid())

        def forward(self, x):
            out, _ = self.lstm(x)
            out = out[:, -1, :]
            return self.fc(out).squeeze(-1)

    model = LSTMNet(X.shape[2]).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=1e-3)
    loss_fn = nn.BCELoss()

    ds_train = TensorDataset(torch.from_numpy(X_train), torch.from_numpy(y_train.astype(np.float32)))
    ds_val = TensorDataset(torch.from_numpy(X_val), torch.from_numpy(y_val.astype(np.float32)))
    dl_train = DataLoader(ds_train, batch_size=batch_size, shuffle=True)
    dl_val = DataLoader(ds_val, batch_size=batch_size)

    for epoch in range(epochs):
        model.train()
        total_loss = 0.0
        for xb, yb in dl_train:
            xb = xb.to(device)
            yb = yb.to(device)
            opt.zero_grad()
            preds = model(xb)
            loss = loss_fn(preds, yb)
            loss.backward()
            opt.step()
            total_loss += loss.item() * xb.size(0)
        avg_loss = total_loss / len(ds_train)
        # val
        model.eval()
        ys, ps = [], []
        with torch.no_grad():
            for xb, yb in dl_val:
                xb = xb.to(device)
                p = model(xb).cpu().numpy()
                ps.append(p)
                ys.append(yb.numpy())
        ys = np.concatenate(ys)
        ps = np.concatenate(ps)
        from sklearn.metrics import accuracy_score, roc_auc_score
        auc = None
        try:
            auc = roc_auc_score(ys, ps)
        except Exception:
            pass
        acc = accuracy_score(ys, (ps > 0.5).astype(int))
        logging.info('Epoch %d loss=%.6f val_auc=%s val_acc=%.4f', epoch, avg_loss, auc, acc)

    out_dir = Path('models')
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / f"{path.stem}_lstm_torch.pt"
    torch.save(model.state_dict(), model_path)
    metrics_path = Path('raw') / f"{path.stem}_lstm_torch_metrics.json"
    import json
    json.dump({'epochs': epochs, 'last_loss': float(avg_loss), 'val_auc': auc, 'val_acc': float(acc)}, open(metrics_path, 'w'))
    logging.info('Saved PyTorch LSTM to %s and metrics to %s', model_path, metrics_path)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--symbol', required=True)
    p.add_argument('--seq_len', type=int, default=32)
    p.add_argument('--epochs', type=int, default=2)
    p.add_argument('--batch_size', type=int, default=256)
    args = p.parse_args()
    src = Path('raw') / f"{args.symbol}.parquet"
    train_torch(src, seq_len=args.seq_len, epochs=args.epochs, batch_size=args.batch_size)


if __name__ == '__main__':
    main()
