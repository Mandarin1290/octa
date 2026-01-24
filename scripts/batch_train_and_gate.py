#!/usr/bin/env python3
import json
import math
import os
import shutil
import subprocess
import sys
from pathlib import Path

RAW_DIR = Path('/home/n-b/Octa/raw')
PKL_DIR = RAW_DIR / 'PKL'
ARTIFACT_BASE = Path('artifacts/models/demo_model/regression')

PKL_DIR.mkdir(parents=True, exist_ok=True)

# Gate configuration: normalized MSE threshold and absolute MSE threshold
# Default normalized threshold tuned for stricter (hedge‑fund) gating
try:
    OCTA_GATE_NORM_MSE = float(os.getenv('OCTA_GATE_NORM_MSE', '0.02'))
except Exception:
    OCTA_GATE_NORM_MSE = 0.02
try:
    OCTA_GATE_ABS_MSE = float(os.getenv('OCTA_GATE_ABS_MSE', 'nan'))
except Exception:
    OCTA_GATE_ABS_MSE = float('nan')

# Hedge‑fund level additional criteria (all configurable):
# minimum training samples, max relative CV std, allowed CV/backtest MSE drift, min backtest samples
try:
    OCTA_MIN_TRAIN_N = int(os.getenv('OCTA_MIN_TRAIN_N', '1000'))
except Exception:
    OCTA_MIN_TRAIN_N = 1000
try:
    OCTA_MAX_CV_REL_STD = float(os.getenv('OCTA_MAX_CV_REL_STD', '0.2'))
except Exception:
    OCTA_MAX_CV_REL_STD = 0.2
try:
    OCTA_MAX_CV_BACKTEST_DELTA = float(os.getenv('OCTA_MAX_CV_BACKTEST_DELTA', '0.15'))
except Exception:
    OCTA_MAX_CV_BACKTEST_DELTA = 0.15
try:
    OCTA_MIN_BACKTEST_N = int(os.getenv('OCTA_MIN_BACKTEST_N', '2000'))
except Exception:
    OCTA_MIN_BACKTEST_N = 2000

try:
    OCTA_MIN_SHARPE = float(os.getenv('OCTA_MIN_SHARPE', '1.0'))
except Exception:
    OCTA_MIN_SHARPE = 1.0
try:
    OCTA_MAX_DRAWDOWN = float(os.getenv('OCTA_MAX_DRAWDOWN', '0.2'))
except Exception:
    OCTA_MAX_DRAWDOWN = 0.2

parquets = sorted(RAW_DIR.glob('*.parquet'))
if not parquets:
    print('No parquet files found under', RAW_DIR)
    sys.exit(0)

for pq in parquets:
    base = pq.stem
    print('--- Processing', base)
    # skip if PKL already exists
    existing = PKL_DIR / f'{base}.pkl'
    if existing.exists():
        print('SKIP', base, '(PKL exists)')
        continue
    # train
    env = os.environ.copy()
    env['OCTA_DISABLE_MLFLOW'] = '1'
    cmd = ['python3', '-m', 'scripts.train_and_save', '--parquet', str(pq), '--version', base, '--seed', '42', '--cv-folds', '5', '--hyperopt', '--backtest']
    log = Path('artifacts/logs') / f'train_{base}_gate.log'
    log.parent.mkdir(parents=True, exist_ok=True)
    print('Running trainer, logging to', log)
    with open(log, 'wb') as fh:
        proc = subprocess.run(cmd, env=env, stdout=fh, stderr=subprocess.STDOUT)
    if proc.returncode != 0:
        print('Trainer failed for', base, '-- see', log)
        continue

    # load meta.json if available
    meta = ARTIFACT_BASE / base / 'meta.json'
    metric_mse = None
    m = None
    if meta.exists():
        try:
            m = json.loads(meta.read_text())
            metric_mse = m.get('metrics', {}).get('mse')
        except Exception:
            metric_mse = None

    # if no mse in meta, compute by loading pkl and evaluating on test split from parquet
    if metric_mse is None:
        # attempt to compute using the model.pkl
        pklp = ARTIFACT_BASE / base / 'model.pkl'
        if pklp.exists():
            try:
                import pickle

                import pandas as pd
                obj = pickle.load(open(pklp, 'rb'))
                # reconstruct simple linear predict
                coef = obj.get('coef', [])
                intercept = obj.get('intercept', 0.0)
                df = pd.read_parquet(pq)
                # choose target as last numeric column
                numeric = df.select_dtypes(include=['number']).columns.tolist()
                if not numeric:
                    print('No numeric columns in', pq)
                    continue
                target = numeric[-1]
                y = df[target].astype(float).values
                # naive split consistent with trainer: 80/20 shuffle with seed 42
                import numpy as np
                idx = np.arange(len(y))
                rng = np.random.RandomState(42)
                rng.shuffle(idx)
                split = int(0.8 * len(idx))
                test_idx = idx[split:]
                y_test = y[test_idx]
                # need features to predict; simplest: if single-feature coefficient, use that column
                # assume features were numeric columns except target (trainer picks all numeric except target)
                feat_cols = [c for c in numeric if c != target]
                if not feat_cols:
                    # fallback: predict zeros
                    preds = [intercept for _ in y_test]
                else:
                    X = df[feat_cols].astype(float).values
                    X_test = X[test_idx]
                    # predict
                    if len(coef) == 1:
                        preds = (X_test[:, 0] * coef[0] + intercept).tolist()
                    else:
                        preds = (X_test.dot(coef) + intercept).tolist()
                import numpy as np
                mse = float(np.mean((np.array(preds) - y_test) ** 2))
                metric_mse = mse
            except Exception as e:
                print('Failed to compute mse for', base, e)
                metric_mse = None

    if metric_mse is None:
        print('No MSE metric available for', base, '; skipping gate')
        continue

    # compute variance of target for normalization
    try:
        import numpy as np
        import pandas as pd
        df = pd.read_parquet(pq)
        numeric = df.select_dtypes(include=['number']).columns.tolist()
        target = numeric[-1]
        y = df[target].astype(float).values
        var = float(np.var(y)) if len(y) > 0 else float('nan')
    except Exception as e:
        print('Failed to compute variance for', base, e)
        var = float('nan')

    normalized_mse = float(metric_mse) / var if var and not math.isnan(var) and var > 0 else float('inf')

    # Hedge‑fund level composite gate:
    #  - normalized_mse < OCTA_GATE_NORM_MSE
    #  - training samples >= OCTA_MIN_TRAIN_N
    #  - relative CV std (mse_std / mse_mean) <= OCTA_MAX_CV_REL_STD
    #  - backtest vs CV mean relative delta <= OCTA_MAX_CV_BACKTEST_DELTA
    #  - backtest samples >= OCTA_MIN_BACKTEST_N
    abs_ok = False
    try:
        if not math.isnan(OCTA_GATE_ABS_MSE):
            abs_ok = float(metric_mse) < float(OCTA_GATE_ABS_MSE)
    except Exception:
        abs_ok = False

    # gather CV/backtest stats from model card or meta if available
    cv_mean = None
    cv_std = None
    back_n = None
    bt_sharpe = None
    bt_max_dd = None
    try:
        mc = ARTIFACT_BASE / base / 'model_card.json'
        if mc.exists():
            mcj = json.loads(mc.read_text())
            cv = mcj.get('cv') or mcj.get('metrics', {}).get('cv')
            if cv:
                cv_mean = cv.get('mse_mean') or cv.get('mse') or None
                cv_std = cv.get('mse_std') or cv.get('std') or None
            bt = mcj.get('backtest') or mcj.get('metrics', {}).get('backtest')
            if bt:
                back_n = bt.get('n') or bt.get('samples') or None
                # also surface sharpe and max drawdown if available
                if isinstance(bt, dict):
                    bt_sharpe = bt.get('sharpe') or (bt.get('metrics') or {}).get('sharpe')
                    bt_max_dd = bt.get('max_drawdown') or (bt.get('metrics') or {}).get('max_drawdown')
    except Exception:
        cv_mean = cv_std = back_n = None
        bt_sharpe = None
        bt_max_dd = None

    # compute stability checks
    cv_rel_std = float(cv_std) / float(cv_mean) if cv_mean and cv_std else float('inf')
    cv_bt_delta = abs(float(metric_mse) - float(cv_mean)) / float(cv_mean) if cv_mean else float('inf')
    train_n = int(m.get('metrics', {}).get('n_train') if m else 0)

    # incorporate HF financial checks: sharpe and max drawdown (if available)
    sharpe_ok = (bt_sharpe is None) or (float(bt_sharpe) >= float(OCTA_MIN_SHARPE))
    dd_ok = (bt_max_dd is None) or (float(bt_max_dd) <= float(OCTA_MAX_DRAWDOWN))

    gate_pass = ( (normalized_mse < OCTA_GATE_NORM_MSE) or abs_ok ) and (train_n >= OCTA_MIN_TRAIN_N) and (cv_rel_std <= OCTA_MAX_CV_REL_STD) and (cv_bt_delta <= OCTA_MAX_CV_BACKTEST_DELTA) and ( (back_n or 0) >= OCTA_MIN_BACKTEST_N ) and sharpe_ok and dd_ok

    print(f'Base={base} mse={metric_mse:.6g} var={var:.6g} norm_mse={normalized_mse:.6g} train_n={train_n} cv_rel_std={cv_rel_std:.3g} cv_bt_delta={cv_bt_delta:.3g} back_n={back_n} sharpe={bt_sharpe} max_dd={bt_max_dd} gate_pass={gate_pass} (norm_thresh={OCTA_GATE_NORM_MSE} rel_std_max={OCTA_MAX_CV_REL_STD} cv_bt_delta_max={OCTA_MAX_CV_BACKTEST_DELTA} min_train={OCTA_MIN_TRAIN_N} min_backtest={OCTA_MIN_BACKTEST_N} min_sharpe={OCTA_MIN_SHARPE} max_dd_thr={OCTA_MAX_DRAWDOWN})')

    if gate_pass:
        src = ARTIFACT_BASE / base / 'model.pkl'
        dst = PKL_DIR / f'{base}.pkl'
        if src.exists():
            shutil.copy2(src, dst)
            print('COPIED', dst)
        else:
            print('No model.pkl to copy for', base)
    else:
        print('Gate failed for', base, '- not copying PKL')

print('Batch done. PKL dir contents:')
for p in sorted(PKL_DIR.glob('*.pkl')):
    print('-', p.name)
