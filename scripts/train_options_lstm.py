#!/usr/bin/env python3
"""Train a per-contract LSTM on refined option labels.

This is a lightweight trainer for verification and prototyping.

Public callable API (Phase E):
    train_options(*, symbol, timeframe, asset_class, run_id, stage_index,
                  parquet_path, model_root=None, ...) -> StageResult

The module-level tensorflow import is deferred to inside functions so the
module can be imported even when tensorflow is unavailable or broken.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
import traceback as _traceback
from pathlib import Path
from typing import Any, List, Optional, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_FEATURES = ['delta', 'moneyness', 'tte_days', 'mid', 'underlying_close']
_PERFORMANCE_THRESHOLD_AUC: float = 0.55


# ---------------------------------------------------------------------------
# Sequence building
# ---------------------------------------------------------------------------

def build_sequences(
    df: pd.DataFrame,
    seq_len: int = 32,
    features: Optional[List[str]] = None,
    min_seq: int = 4,
) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], List[str]]:
    feats = features or _DEFAULT_FEATURES
    feats = [f for f in feats if f in df.columns]
    X: list = []
    y: list = []
    for _contract, g in df.groupby('contract_id'):
        g = g.sort_values('timestamp')
        arr = g[feats].fillna(0).to_numpy(dtype=np.float32)
        labels = g['label_refined_pos'].to_numpy()
        if len(arr) < min_seq:
            continue
        for i in range(len(arr) - seq_len + 1):
            X.append(arr[i:i + seq_len])
            y.append(labels[i + seq_len - 1])
    if not X:
        return None, None, feats
    X_arr = np.stack(X)
    y_arr = np.array(y, dtype=np.int32)
    return X_arr, y_arr, feats


# ---------------------------------------------------------------------------
# Walk-forward splits
# ---------------------------------------------------------------------------

def wf_date_split(df: pd.DataFrame, n_folds: int = 3) -> list:
    """Original date-based WF split (no purge/embargo). Kept for backward compat."""
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


def wf_date_split_with_purge_embargo(
    df: pd.DataFrame,
    n_folds: int = 3,
    purge_bars: int = 5,
    embargo_bars: int = 2,
) -> list:
    """Walk-forward splits with purge + embargo gaps.

    purge_bars:  number of date-bars removed from the END of the training window
                 (prevents leakage from label look-ahead at fold boundary).
    embargo_bars: number of date-bars removed from the START of the validation
                  window (prevents leakage from close-to-boundary sequences).
    """
    dates = pd.to_datetime(df['timestamp']).dt.date.unique()
    n = len(dates)
    splits = []
    for i in range(n_folds):
        raw_train_end = int((i + 1) * n / (n_folds + 1))
        raw_val_end = int((i + 2) * n / (n_folds + 1))
        # Apply purge: trim last purge_bars bars from training set
        effective_train_end = max(1, raw_train_end - purge_bars)
        # Apply embargo: skip first embargo_bars bars of validation window
        effective_val_start = min(raw_train_end + embargo_bars, raw_val_end - 1)
        if effective_train_end < 1 or raw_val_end <= effective_val_start:
            continue
        train_dates = dates[:effective_train_end]
        val_dates = dates[effective_val_start:raw_val_end]
        if len(train_dates) < 1 or len(val_dates) < 1:
            continue
        splits.append((train_dates, val_dates))
    return splits


# ---------------------------------------------------------------------------
# Model building (deferred TF import)
# ---------------------------------------------------------------------------

def build_model(input_shape: tuple) -> Any:
    import tensorflow as tf  # deferred — module importable even if TF unavailable
    model = tf.keras.Sequential([
        tf.keras.layers.Input(shape=input_shape),
        tf.keras.layers.Masking(mask_value=0.0),
        tf.keras.layers.LSTM(64, return_sequences=False),
        tf.keras.layers.Dense(32, activation='relu'),
        tf.keras.layers.Dense(1, activation='sigmoid'),
    ])
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['AUC', 'accuracy'])
    return model


# ---------------------------------------------------------------------------
# Legacy run() — kept exactly for backward compat
# ---------------------------------------------------------------------------

def run(path: Path, seq_len: int = 32, epochs: int = 2, batch_size: int = 256) -> None:
    import tensorflow as tf  # deferred
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
    endpoints = []
    for _contract, g in df.groupby('contract_id'):
        g = g.sort_values('timestamp')
        for i in range(len(g) - seq_len + 1):
            endpoints.append(g['timestamp'].iloc[i + seq_len - 1].date())
    endpoints = np.array(endpoints)
    train_idx = np.isin(endpoints, train_dates)
    val_idx = np.isin(endpoints, val_dates)
    X_train, y_train = X[train_idx], y[train_idx]
    X_val, y_val = X[val_idx], y[val_idx]
    logging.info('Train sequences=%d Val sequences=%d', len(X_train), len(X_val))

    model = build_model(input_shape=(seq_len, X.shape[2]))
    history = model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=epochs,
        batch_size=batch_size,
    )

    out_dir = Path('models')
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / f"{path.stem}_lstm.h5"
    model.save(model_path)
    metrics_path = Path('raw') / f"{path.stem}_lstm_metrics.json"
    json.dump(
        {k: v[-1] for k, v in history.history.items()},
        open(metrics_path, 'w'),  # noqa: WPS515 (legacy script; not in production path)
    )
    logging.info('Saved LSTM model to %s and metrics to %s', model_path, metrics_path)


# ---------------------------------------------------------------------------
# Phase E: callable train_options() API
# ---------------------------------------------------------------------------

def train_options(
    *,
    symbol: str,
    timeframe: str = "30M",
    asset_class: str = "option",
    run_id: str,
    stage_index: int = 0,
    parquet_path: str,
    model_root: Optional[str] = None,
    seq_len: int = 32,
    epochs: int = 2,
    batch_size: int = 256,
    n_folds: int = 3,
    purge_bars: int = 5,
    embargo_bars: int = 2,
    min_train_sequences: int = 10,
    min_val_sequences: int = 5,
    performance_threshold_auc: float = _PERFORMANCE_THRESHOLD_AUC,
) -> Any:  # returns StageResult
    """Train options LSTM with WF purge/embargo and return a StageResult.

    Walk-forward uses wf_date_split_with_purge_embargo() to prevent leakage.
    Performance gate: oos_auc >= performance_threshold_auc (default 0.55).
    Model saved to <model_root>/option/<timeframe>/<symbol>_lstm.h5 with SHA-256 sidecar.
    Returns StageResult; does NOT raise on recoverable errors.
    """
    from octa_ops.autopilot.stage_result import StageMandatoryMetrics, StageResult

    elapsed_start = time.monotonic()

    def _fail(
        fail_status: str,
        fail_reason: str,
        *,
        bars_available: Optional[int] = None,
        error_tb: Optional[str] = None,
    ) -> Any:
        return StageResult(
            symbol=symbol,
            timeframe=timeframe,
            asset_class=asset_class,
            run_id=run_id,
            stage_index=stage_index,
            structural_pass=False,
            performance_pass=False,
            metrics=StageMandatoryMetrics(bars_available=bars_available),
            fail_status=fail_status,
            fail_reason=fail_reason,
            error_traceback=error_tb,
            elapsed_sec=float(time.monotonic() - elapsed_start),
        )

    # --- TF availability check ---
    try:
        import tensorflow as _tf  # noqa: F401 — just probe availability
    except Exception:
        return _fail("TRAIN_ERROR", "tensorflow_not_installed")

    try:
        # --- Load data ---
        df = pd.read_parquet(parquet_path)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        else:
            return _fail("DATA_INVALID", "missing_timestamp_column")

        bars_available = len(df)

        # --- WF splits with purge + embargo ---
        splits = wf_date_split_with_purge_embargo(
            df,
            n_folds=n_folds,
            purge_bars=purge_bars,
            embargo_bars=embargo_bars,
        )
        if not splits:
            return _fail(
                "DATA_INVALID",
                "insufficient_dates_for_wf_splits",
                bars_available=bars_available,
            )

        # --- Build all sequences once ---
        X_all, y_all, feats = build_sequences(df, seq_len=seq_len)
        if X_all is None:
            return _fail(
                "DATA_INVALID",
                "no_sequences_built",
                bars_available=bars_available,
            )

        # Sequence endpoints (date of last bar in each sequence)
        endpoints = []
        for _contract, g in df.groupby('contract_id'):
            g = g.sort_values('timestamp')
            for i in range(len(g) - seq_len + 1):
                endpoints.append(g['timestamp'].iloc[i + seq_len - 1].date())
        endpoints = np.array(endpoints)

        # --- Walk-forward training ---
        fold_aucs: list[float] = []
        fold_accs: list[float] = []
        total_train_seqs = 0
        total_val_seqs = 0
        best_model_weights = None
        best_auc = -1.0

        for fold_train_dates, fold_val_dates in splits:
            tr_idx = np.isin(endpoints, fold_train_dates)
            va_idx = np.isin(endpoints, fold_val_dates)
            X_tr, y_tr = X_all[tr_idx], y_all[tr_idx]
            X_va, y_va = X_all[va_idx], y_all[va_idx]
            if len(X_tr) < min_train_sequences or len(X_va) < min_val_sequences:
                continue
            total_train_seqs += len(X_tr)
            total_val_seqs += len(X_va)

            model = build_model(input_shape=(seq_len, X_all.shape[2]))
            model.fit(
                X_tr, y_tr,
                validation_data=(X_va, y_va),
                epochs=epochs,
                batch_size=batch_size,
                verbose=0,
            )
            eval_result = model.evaluate(X_va, y_va, verbose=0)
            fold_auc = float(eval_result[1]) if len(eval_result) > 1 else 0.0
            fold_acc = float(eval_result[2]) if len(eval_result) > 2 else 0.0
            fold_aucs.append(fold_auc)
            fold_accs.append(fold_acc)
            if fold_auc > best_auc:
                best_auc = fold_auc
                best_model_weights = model.get_weights()

        n_folds_completed = len(fold_aucs)
        if n_folds_completed == 0:
            return _fail(
                "DATA_INVALID",
                "insufficient_sequences_for_wf",
                bars_available=bars_available,
            )

        oos_auc = float(np.mean(fold_aucs))
        oos_accuracy = float(np.mean(fold_accs))
        performance_pass = bool(oos_auc >= performance_threshold_auc)
        walk_forward_passed = performance_pass and n_folds_completed >= 1

        # --- Save best model + SHA-256 sidecar ---
        model_path_str: Optional[str] = None
        model_hash: Optional[str] = None
        if best_model_weights is not None:
            root = Path(model_root) if model_root else Path("models")
            model_dir = root / "option" / str(timeframe)
            model_dir.mkdir(parents=True, exist_ok=True)
            mp = model_dir / f"{symbol}_lstm.h5"
            final_model = build_model(input_shape=(seq_len, X_all.shape[2]))
            final_model.set_weights(best_model_weights)
            final_model.save(str(mp))
            h = hashlib.sha256()
            with mp.open("rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    h.update(chunk)
            mp.with_suffix(".sha256").write_text(h.hexdigest() + "\n")
            model_path_str = str(mp)
            model_hash = h.hexdigest()

        metrics = StageMandatoryMetrics(
            oos_auc=oos_auc,
            oos_accuracy=oos_accuracy,
            walk_forward_passed=walk_forward_passed,
            n_folds_completed=n_folds_completed,
            bars_available=bars_available,
            bars_used_train=total_train_seqs,
            bars_used_test=total_val_seqs,
            leakage_detected=False,
        )
        return StageResult(
            symbol=symbol,
            timeframe=timeframe,
            asset_class=asset_class,
            run_id=run_id,
            stage_index=stage_index,
            structural_pass=True,
            performance_pass=performance_pass,
            metrics=metrics,
            model_path=model_path_str,
            model_hash=model_hash,
            fail_status="PASS" if performance_pass else "GATE_FAIL",
            fail_reason=(
                None if performance_pass
                else f"oos_auc_below_threshold:{oos_auc:.4f}<{performance_threshold_auc}"
            ),
            elapsed_sec=float(time.monotonic() - elapsed_start),
        )

    except Exception:
        return _fail(
            "TRAIN_ERROR",
            "train_exception",
            error_tb=_traceback.format_exc(),
        )


# ---------------------------------------------------------------------------
# CLI (unchanged)
# ---------------------------------------------------------------------------

def main() -> None:
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
