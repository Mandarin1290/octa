#!/usr/bin/env python3
"""Train options time-series classifier/regressor on labeled option parquet.

Usage: scripts/train_options_time_series.py --symbol AAPL_OPT_labeled --folds 5
"""
import argparse
import logging
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def choose_model(task='cls'):
    try:
        import lightgbm as lgb
        if task == 'cls':
            return lgb.LGBMClassifier(n_jobs=4)
        else:
            return lgb.LGBMRegressor(n_jobs=4)
    except Exception:
        logging.warning('LightGBM not available, falling back to RandomForest')
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
        if task == 'cls':
            return RandomForestClassifier(n_jobs=4, n_estimators=100)
        else:
            return RandomForestRegressor(n_jobs=4, n_estimators=100)


def time_series_wf_split(df: pd.DataFrame, n_splits=5):
    # simple expanding-window by unique dates
    df = df.sort_values('timestamp')
    dates = pd.to_datetime(df['timestamp']).dt.date.unique()
    n = len(dates)
    if n < 2:
        raise RuntimeError('Not enough dates for WF split')
    splits = []
    for i in range(n_splits):
        train_end = int((i + 1) * n / (n_splits + 1))
        val_end = int((i + 2) * n / (n_splits + 1))
        if train_end < 1 or val_end <= train_end:
            continue
        train_dates = dates[:train_end]
        val_dates = dates[train_end:val_end]
        train_idx = df['timestamp'].dt.date.isin(train_dates)
        val_idx = df['timestamp'].dt.date.isin(val_dates)
        splits.append((train_idx, val_idx))
    return splits


def run_training(path: Path, folds=5, task='cls', target='label_delta_hedge_pos'):
    logging.info('Loading %s', path)
    df = pd.read_parquet(path)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    else:
        raise RuntimeError('timestamp column required')

    df = df.sort_values('timestamp').reset_index(drop=True)
    if target not in df.columns:
        raise RuntimeError(f'target {target} not in dataframe')
    df = df.dropna(subset=[target])

    features = ['delta','gamma','vega','theta','rho','moneyness','tte_days','underlying_close','mid']
    features = [f for f in features if f in df.columns]
    if not features:
        raise RuntimeError('No features found in dataframe for training')

    X = df[features]
    y = df[target]

    splits = time_series_wf_split(df, n_splits=folds)
    if not splits:
        raise RuntimeError('No valid WF splits produced')

    model = choose_model(task=task)
    from sklearn.metrics import accuracy_score, roc_auc_score

    metrics = []
    last_model = None
    for i, (train_idx, val_idx) in enumerate(splits):
        if train_idx.sum() < 10 or val_idx.sum() < 10:
            logging.warning('Fold %d too small, skipping', i)
            continue
        X_train, y_train = X.loc[train_idx], y.loc[train_idx]
        X_val, y_val = X.loc[val_idx], y.loc[val_idx]
        # log class distribution
        unique_train, counts_train = np.unique(y_train, return_counts=True)
        unique_val, counts_val = np.unique(y_val, return_counts=True)
        logging.info('Fold %d: train=%d val=%d train_classes=%s val_classes=%s', i, len(X_train), len(X_val), dict(zip(unique_train, counts_train, strict=False)), dict(zip(unique_val, counts_val, strict=False)))
        # skip folds where train or val contains only one class
        if len(unique_train) < 2 or len(unique_val) < 2:
            logging.warning('Fold %d has single-class in train or val; skipping', i)
            continue
        # for sklearn RandomForest, set class_weight balanced if available
        try:
            if hasattr(model, 'class_weight') and model.__class__.__name__.startswith('RandomForest'):
                model.set_params(class_weight='balanced')
        except Exception:
            pass
        logging.info('Fold %d: train=%d val=%d', i, len(X_train), len(X_val))
        model.fit(X_train, y_train)
        # robust positive-class probability extraction
        def positive_proba(m, X_):
            if hasattr(m, 'predict_proba'):
                p = m.predict_proba(X_)
                if p.ndim == 1:
                    return p
                if p.shape[1] == 1:
                    return p.ravel()
                return p[:, 1]
            if hasattr(m, 'decision_function'):
                scores = m.decision_function(X_)
                if np.isscalar(scores) or scores.ndim == 0:
                    return np.array([scores])
                s = scores.astype(float)
                if s.max() == s.min():
                    return np.zeros_like(s)
                return (s - s.min()) / (s.max() - s.min())
            return m.predict(X_)

        preds = positive_proba(model, X_val)
        auc = roc_auc_score(y_val, preds) if task == 'cls' else None
        acc = accuracy_score(y_val, (preds > 0.5).astype(int)) if task == 'cls' else None
        metrics.append({'fold': i, 'auc': auc, 'acc': acc, 'train_samples': len(X_train), 'val_samples': len(X_val)})
        last_model = model

    # save model and metrics
    out_dir = Path('models')
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / f"{path.stem}_wf_model.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(last_model, f)
    metrics_path = Path('raw') / f"{path.stem}_wf_metrics.json"
    import json
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    logging.info('Saved model to %s and metrics to %s', model_path, metrics_path)
    return metrics


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--symbol', required=True)
    p.add_argument('--folds', type=int, default=5)
    p.add_argument('--task', choices=['cls','reg'], default='cls')
    p.add_argument('--target', default='label_option_pos')
    args = p.parse_args()
    src = Path('raw') / f"{args.symbol}.parquet"
    run_training(src, folds=args.folds, task=args.task, target=args.target)


if __name__ == '__main__':
    main()
