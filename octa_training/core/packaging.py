from __future__ import annotations

import hashlib
import json
import os
import pickle
import tempfile
from dataclasses import dataclass
from datetime import datetime as dt
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel

from octa_training.core.gates import GateResult
from octa_training.core.metrics_contract import MetricsSummary


def _json_sanitize(obj: Any) -> Any:
    """Convert common non-JSON-native scalar/container types to plain Python types.

    This is used for ArtifactMeta JSON serialization (pydantic v2 is strict about
    unknown types like numpy.bool_).
    """
    # numpy scalars (np.bool_, np.float64, etc.)
    try:
        if isinstance(obj, np.generic):
            return obj.item()
    except Exception:
        pass

    # pandas timestamp
    try:
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
    except Exception:
        pass

    # datetime
    if isinstance(obj, dt):
        return obj.isoformat()

    # pathlib
    try:
        from pathlib import Path as _Path

        if isinstance(obj, _Path):
            return str(obj)
    except Exception:
        pass

    if isinstance(obj, dict):
        return {str(k): _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_sanitize(v) for v in obj]

    return obj


class ArtifactMeta(BaseModel):
    schema_version: int = 1
    symbol: str
    asset_class: Optional[str]
    run_id: str
    created_at: dt
    artifact_kind: str = "tradeable"
    created_with: Optional[Dict[str, Any]] = None
    metrics: Dict[str, Any]
    gate: Dict[str, Any]
    feature_count: int
    horizons: list
    meta_sha256: Optional[str] = None


@dataclass
class SafeInference:
    model_name: str
    model_obj: Any
    feature_names: list
    scaler: Optional[Any]
    upper_q: float
    lower_q: float
    leverage_cap: float
    vol_target: float
    vol_window: int

    def predict(self, X: pd.DataFrame) -> Dict[str, Any]:
        # Expect X contains the precomputed features; pick columns
        Xf = X.copy()
        if set(self.feature_names).issubset(Xf.columns):
            Xf = Xf[self.feature_names]
        else:
            missing = set(self.feature_names) - set(Xf.columns)
            return {"signal": 0.0, "position": 0.0, "confidence": 0.0, "diagnostics": {"error": f"missing_features:{missing}"}}
        if self.scaler is not None:
            try:
                Xvals = self.scaler.transform(Xf)
            except Exception:
                Xvals = Xf.values
        else:
            Xvals = Xf.values

        # infer probabilities or scores
        try:
            if hasattr(self.model_obj, "predict_proba"):
                probs = self.model_obj.predict_proba(Xvals)
                if probs.ndim == 2:
                    conf = float(probs[:, 1].mean())
                    score_series = probs[:, 1]
                else:
                    conf = float(probs.mean())
                    score_series = probs
            else:
                scores = self.model_obj.predict(Xvals)
                conf = float(pd.Series(scores).abs().mean())
                score_series = scores
        except Exception as e:
            return {"signal": 0.0, "position": 0.0, "confidence": 0.0, "diagnostics": {"error": str(e)}}

        # threshold signals
        up = pd.Series(score_series).quantile(self.upper_q)
        low = pd.Series(score_series).quantile(self.lower_q)
        sig = 0
        latest = float(pd.Series(score_series).iloc[-1])
        # Classification target is 1 for up-move; higher score => LONG.
        if latest > up:
            sig = 1
        elif latest < low:
            sig = -1

        # position sizing: simple vol scale estimate using recent returns not available here; fallback to cap
        pos = float(sig * min(self.leverage_cap, self.vol_target))

        return {"signal": int(sig), "position": pos, "confidence": conf, "diagnostics": {}}


def _atomic_write_bytes(path: str, data: bytes) -> None:
    dirn = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=dirn)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


def _compute_sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _train_full_model(X: pd.DataFrame, y: pd.Series, model_name: str, task: str, settings: Any, seed: int = 42, device_profile=None):
    # Minimal retrain logic mirroring core.models choices
    if model_name == "logreg":
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        Xs = scaler.fit_transform(X.fillna(0))
        clf = LogisticRegression(random_state=seed, max_iter=200)
        clf.fit(Xs, y)
        return clf, list(X.columns), scaler
    elif model_name == "ridge":
        from sklearn.linear_model import Ridge
        scaler = None
        clf = Ridge()
        clf.fit(X.fillna(0).values, y)
        return clf, list(X.columns), None
    elif model_name == "lightgbm":
        import lightgbm as lgb
        params = getattr(settings, "lgbm_params", {}).copy()
        params.setdefault("random_state", seed)
        params.setdefault("objective", "regression" if task == "reg" else "binary")
        dtrain = lgb.Dataset(X, label=y)
        booster = lgb.train(params, dtrain, num_boost_round=getattr(settings, "num_boost_round", 1000))
        return booster, list(X.columns), None
    elif model_name == "xgboost":
        import xgboost as xgb
        params = getattr(settings, "xgb_params", {}).copy()
        params.setdefault("seed", seed)
        params.setdefault("objective", "reg:squarederror" if task == "reg" else "binary:logistic")
        dtrain = xgb.DMatrix(X, label=y)
        bst = xgb.train(params, dtrain, num_boost_round=int(getattr(settings, "num_boost_round", 1000)))
        return bst, list(X.columns), None
    elif model_name == "catboost":
        from catboost import CatBoostClassifier, CatBoostRegressor
        params = getattr(settings, "cat_params", {}).copy()
        params.setdefault("random_seed", seed)
        params.setdefault("verbose", False)
        if task == "cls":
            params.setdefault("loss_function", "Logloss")
            cb = CatBoostClassifier(**params)
        else:
            params.setdefault("loss_function", "RMSE")
            cb = CatBoostRegressor(**params)
        cb.fit(X.fillna(0), y)
        return cb, list(X.columns), None
    elif model_name == "random_forest":
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
        if task == "cls":
            clf = RandomForestClassifier(random_state=seed)
        else:
            clf = RandomForestRegressor(random_state=seed)
        clf.fit(X.fillna(0).values, y)
        return clf, list(X.columns), None
    elif model_name == "lstm":
        from sklearn.preprocessing import StandardScaler
        from tensorflow.keras.layers import LSTM, Dense, Dropout
        from tensorflow.keras.models import Sequential

        # Reshape for LSTM: (samples, timesteps, features)
        # Assume X is already time-ordered, use last 20 steps as sequence
        seq_len = 20
        if len(X) < seq_len:
            seq_len = len(X)
        X_seq = []
        y_seq = []
        for i in range(seq_len, len(X)):
            X_seq.append(X.iloc[i-seq_len:i].values)
            y_seq.append(y.iloc[i])
        X_seq = np.array(X_seq)
        y_seq = np.array(y_seq)

        if len(X_seq) == 0:
            # fallback
            return None, list(X.columns), None

        scaler = StandardScaler()
        X_seq_scaled = scaler.fit_transform(X_seq.reshape(-1, X.shape[1])).reshape(X_seq.shape)

        model = Sequential()
        model.add(LSTM(50, input_shape=(seq_len, X.shape[1]), return_sequences=True))
        model.add(Dropout(0.2))
        model.add(LSTM(50))
        model.add(Dropout(0.2))
        if task == "cls":
            model.add(Dense(1, activation='sigmoid'))
            model.compile(loss='binary_crossentropy', optimizer='adam')
        else:
            model.add(Dense(1))
            model.compile(loss='mse', optimizer='adam')
        model.fit(X_seq_scaled, y_seq, epochs=10, batch_size=32, verbose=0)
        return model, list(X.columns), scaler
    else:
        # fallback simple linear
        from sklearn.linear_model import LogisticRegression
        scaler = None
        clf = LogisticRegression(random_state=seed, max_iter=200)
        try:
            clf.fit(X.values, y)
        except Exception:
            clf.fit(X.fillna(0).values, y)
        return clf, list(X.columns), scaler


def save_tradeable_artifact(
    symbol: str,
    best_result: Any,
    features_res: Any,
    df_raw: pd.DataFrame,
    metrics: MetricsSummary,
    gate: GateResult,
    cfg: Any,
    state: Any,
    run_id: str,
    asset_class: Optional[str],
    parquet_path: str,
    pkl_dir_override: Optional[str] = None,
    update_state: bool = True,
    artifact_kind: str = "tradeable",
    enforce_improvement: bool = True,
) -> Dict[str, Any]:
    pkl_dir = os.path.join(str(pkl_dir_override) if pkl_dir_override else cfg.paths.pkl_dir)
    os.makedirs(pkl_dir, exist_ok=True)
    target_pkl = os.path.join(pkl_dir, f"{symbol}.pkl")
    meta_path = os.path.join(pkl_dir, f"{symbol}.meta.json")
    sha_path = os.path.join(pkl_dir, f"{symbol}.sha256")

    # Versioning check (tradeable artifacts only; debug artifacts should always be written)
    if enforce_improvement:
        existing_metric = None
        if os.path.exists(meta_path):
            try:
                with open(meta_path, 'r') as fh:
                    existing = json.load(fh)
                    existing_metric = existing.get('metrics', {}).get(cfg.packaging.compare_metric_name)
                    # check age
                    existing_created = existing.get('created_at') or existing.get('training_signature', {}).get('timestamp')
                    try:
                        if existing_created:
                            created_dt = dt.fromisoformat(existing_created)
                            max_age = getattr(cfg.packaging, 'max_age_days', None)
                            if max_age is not None and (dt.utcnow() - created_dt).total_seconds() > max_age * 86400:
                                pass
                    except Exception:
                        pass
            except Exception:
                existing_metric = None

        new_metric = getattr(metrics, cfg.packaging.compare_metric_name, None)
        if existing_metric is not None and new_metric is not None:
            thresh = cfg.packaging.min_improvement
            # allow replace if existing is older than max_age_days
            existing_too_old = False
            try:
                existing_created_dt = None
                if os.path.exists(meta_path):
                    raw = json.load(open(meta_path, 'r'))
                    existing_created = raw.get('created_at') or raw.get('training_signature', {}).get('timestamp')
                    if existing_created:
                        from datetime import datetime
                        existing_created_dt = datetime.fromisoformat(existing_created)
                max_age = getattr(cfg.packaging, 'max_age_days', None)
                if max_age is not None and existing_created_dt is not None:
                    from datetime import datetime
                    if (dt.utcnow() - existing_created_dt).total_seconds() > max_age * 86400:
                        existing_too_old = True
            except Exception:
                existing_too_old = False
            if not existing_too_old and not (new_metric >= existing_metric + thresh):
                return {"saved": False, "reason": "not_improved", "existing_metric": existing_metric, "new_metric": new_metric}

    # Determine horizon and y
    horizon = getattr(best_result, 'horizon', None)
    task = getattr(best_result, 'task', None)
    y_key = f"y_cls_{horizon}" if task == 'cls' else f"y_reg_{horizon}"
    y = features_res.y_dict.get(y_key)
    X = features_res.X.loc[y.index]

    # retrain full model
    model_name = best_result.model_name
    model_obj, feat_names, scaler = _train_full_model(X, y, model_name, task, cfg, seed=getattr(cfg, 'seed', 42))

    # build safe inference wrapper
    infer = SafeInference(model_name=model_name, model_obj=model_obj, feature_names=feat_names, scaler=scaler, upper_q=cfg.signal.upper_q, lower_q=cfg.signal.lower_q, leverage_cap=cfg.signal.leverage_cap, vol_target=cfg.signal.vol_target, vol_window=cfg.signal.realized_vol_window)

    # construct artifact
    metrics_dict = _json_sanitize(metrics.dict())
    gate_dict = _json_sanitize(gate.dict())

    artifact = {
        'artifact_kind': artifact_kind,
        'model_bundle': {
            'model_name': model_name,
            'task': task,
            'horizon': horizon,
            'device_used': getattr(best_result, 'device_used', 'cpu'),
        },
        'feature_spec': {
            'features': feat_names,
            'feature_config': features_res.meta if hasattr(features_res, 'meta') else {},
        },
        'inference': {
            'thresholds': {'upper_q': cfg.signal.upper_q, 'lower_q': cfg.signal.lower_q},
            'sizing': {'vol_target': cfg.signal.vol_target, 'leverage_cap': cfg.signal.leverage_cap, 'vol_window': cfg.signal.realized_vol_window},
            'costs': {'cost_bps': cfg.signal.cost_bps, 'spread_bps': cfg.signal.spread_bps},
        },
        'training_signature': {
            'run_id': run_id,
            'timestamp': dt.utcnow().isoformat(),
            'parquet_path': parquet_path,
            'code_version': None,
            'python': None,
            'libs': None,
        },
        'metrics': metrics_dict,
        'gate': gate_dict,
        'asset': {
            'symbol': symbol,
            'asset_class': asset_class,
            'bar_size': features_res.meta.get('bar_size') if hasattr(features_res, 'meta') and isinstance(features_res.meta, dict) else None,
            'sample_start': str(features_res.X.index.min()) if len(features_res.X.index) else None,
            'sample_end': str(features_res.X.index.max()) if len(features_res.X.index) else None,
        },
        'safe_inference': infer,
    }

    # fill training signature code_version and python/libs if possible
    try:
        import subprocess
        cv = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=os.getcwd(), stderr=subprocess.DEVNULL).decode().strip()
        artifact['training_signature']['code_version'] = cv
    except Exception:
        artifact['training_signature']['code_version'] = None
    try:
        import platform
        artifact['training_signature']['python'] = platform.python_version()
        # lightweight libs
        libs = {}
        for pkg in ('numpy','pandas','lightgbm','xgboost','catboost','sklearn'):
            try:
                m = __import__(pkg)
                libs[pkg] = getattr(m, '__version__', str(None))
            except Exception:
                libs[pkg] = None
        artifact['training_signature']['libs'] = libs
    except Exception:
        pass

    # include schema metadata for compatibility
    artifact['schema_version'] = 1
    artifact['created_with'] = {
        'code_version': artifact['training_signature'].get('code_version'),
        'python': artifact['training_signature'].get('python'),
        'libs': artifact['training_signature'].get('libs'),
    }

    # pickle artifact to bytes
    pkl_bytes = pickle.dumps(artifact, protocol=4)
    pkl_sha = _compute_sha256_bytes(pkl_bytes)

    # write pkl atomically
    _atomic_write_bytes(target_pkl, pkl_bytes)

    # write sha file
    _atomic_write_bytes(sha_path, pkl_sha.encode('utf-8'))

    # meta
    meta = ArtifactMeta(
        schema_version=1,
        symbol=symbol,
        asset_class=asset_class,
        run_id=run_id,
        created_at=dt.utcnow(),
        artifact_kind=artifact_kind,
        created_with=_json_sanitize(artifact.get('created_with')),
        metrics=metrics_dict,
        gate=gate_dict,
        feature_count=len(feat_names),
        horizons=[horizon],
    )
    meta_json = meta.model_dump_json(indent=2).encode('utf-8')
    meta_sha = _compute_sha256_bytes(meta_json)
    # add meta sha into meta and rewrite
    meta.meta_sha256 = meta_sha
    meta_json = meta.model_dump_json(indent=2).encode('utf-8')
    _atomic_write_bytes(meta_path, meta_json)

    # write sha for meta
    try:
        _atomic_write_bytes(sha_path, pkl_sha.encode('utf-8'))
    except Exception:
        pass

    # update state (only for tradeable/PASS path)
    if update_state:
        try:
            state.update_symbol_state(symbol, last_pass_time=dt.utcnow().isoformat(), artifact_path=target_pkl, artifact_hash=pkl_sha)
        except Exception:
            pass

    return {"saved": True, "pkl": target_pkl, "pkl_sha": pkl_sha, "meta": meta.dict(), "artifact_kind": artifact_kind}
