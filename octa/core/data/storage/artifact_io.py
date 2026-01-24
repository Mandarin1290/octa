from __future__ import annotations

import hashlib
import json
import os
import pickle
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from pydantic import BaseModel

SUPPORTED_SCHEMA_VERSION = 1


class ArtifactMeta(BaseModel):
    schema_version: int = 1
    symbol: str
    asset_class: Optional[str]
    run_id: str
    created_at: datetime
    artifact_kind: str = "tradeable"
    created_with: Optional[Dict[str, Any]] = None
    metrics: Dict[str, Any]
    gate: Dict[str, Any]
    feature_count: int
    horizons: list
    meta_sha256: Optional[str] = None


def quarantine_artifact(pkl_path: str, meta_path: str, sha_path: str, reason: str, quarantine_dir: Optional[str] = None) -> Dict[str, str]:
    p = Path(pkl_path)
    qdir = Path(quarantine_dir) if quarantine_dir else p.parent / "_quarantine" / p.stem
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    dest = qdir / ts
    dest.mkdir(parents=True, exist_ok=True)
    moved = {}
    for src in [pkl_path, meta_path, sha_path]:
        if src and Path(src).exists():
            dst = dest / Path(src).name
            shutil.move(src, dst)
            moved[src] = str(dst)
    # write reason file
    try:
        with open(dest / 'quarantine_reason.txt', 'w') as fh:
            fh.write(reason)
    except Exception:
        pass
    return {'quarantine_dir': str(dest), 'moved': moved}


def _compute_sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def verify_hash(pkl_path: str, sha_path: str) -> Tuple[bool, str]:
    if not os.path.exists(pkl_path):
        raise FileNotFoundError(pkl_path)
    if not os.path.exists(sha_path):
        raise FileNotFoundError(sha_path)
    with open(pkl_path, 'rb') as fh:
        data = fh.read()
    computed = _compute_sha256_bytes(data)
    with open(sha_path, 'r') as fh:
        expected = fh.read().strip()
    if computed != expected:
        raise ValueError(f"SHA256 mismatch for {pkl_path}: expected {expected} got {computed}")
    return True, computed


def load_tradeable_artifact(pkl_path: str, sha_path: Optional[str] = None) -> Dict[str, Any]:
    # validate meta first (best-effort)
    meta_path = pkl_path.replace('.pkl', '.meta.json')
    if sha_path:
        verify_hash(pkl_path, sha_path)
    if Path(meta_path).exists():
        try:
            raw = json.load(open(meta_path, 'r'))
            sv = raw.get('schema_version', 1)
            if sv > SUPPORTED_SCHEMA_VERSION:
                raise ValueError(f"Unsupported artifact schema_version: {sv}")
        except Exception:
            # caller should handle quarantine on error
            raise
    with open(pkl_path, 'rb') as fh:
        data = fh.read()
    obj = pickle.loads(data)
    # basic validation
    if not isinstance(obj, dict):
        raise ValueError("Artifact PKL must contain a dict")
    return obj


def read_meta(meta_path: str) -> ArtifactMeta:
    if not os.path.exists(meta_path):
        raise FileNotFoundError(meta_path)
    with open(meta_path, 'r') as fh:
        raw = json.load(fh)
    return ArtifactMeta(**raw)


def smoke_test_artifact(pkl_path: str, raw_dir: str, last_n: int = 50) -> Dict[str, Any]:
    # load artifact
    import os

    from octa.core.data.io.io_parquet import (
        discover_parquets,
        load_parquet,
        sanitize_symbol,
    )

    obj = load_tradeable_artifact(pkl_path, pkl_path.replace('.pkl', '.sha256'))
    # extract symbol
    symbol = obj.get('asset', {}).get('symbol')
    if not symbol:
        raise ValueError('Artifact missing symbol in asset')
    # find parquet
    discovered = discover_parquets(os.path.join(raw_dir), state=None)
    match = [d for d in discovered if d.symbol == symbol]

    # Fallback: many raw feeds store timeframe-suffixed parquets (e.g. STOCKS:
    # raw/Stock_parquet/AAPL_1H.parquet) while artifacts store base symbols.
    # Use the artifact's recorded bar_size to resolve the correct parquet.
    attempted_alt = None
    if not match:
        bar_size = (obj.get('asset', {}) or {}).get('bar_size')
        if bar_size:
            attempted_alt = sanitize_symbol(f"{symbol}_{str(bar_size).upper()}")
            match = [d for d in discovered if d.symbol == attempted_alt]

    if not match:
        if attempted_alt:
            raise FileNotFoundError(
                f'Parquet for symbol {symbol} (or {attempted_alt}) not found in {raw_dir}'
            )
        raise FileNotFoundError(f'Parquet for symbol {symbol} not found in {raw_dir}')
    df = load_parquet(match[0].path)

    feat_names = obj.get('feature_spec', {}).get('features', [])
    if not feat_names:
        raise ValueError('Artifact missing feature list')

    # Rebuild features from raw OHLCV rather than expecting engineered columns
    # to exist in the raw parquet (indices and many feeds won't store features).
    try:
        import re

        from octa.core.features.features import build_features

        asset_class = (obj.get('asset', {}) or {}).get('asset_class') or 'unknown'
        feature_cfg = (obj.get('feature_spec', {}) or {}).get('feature_config') or {}
        feature_settings = {}
        if isinstance(feature_cfg, dict):
            feature_settings = feature_cfg.get('feature_settings') or {}
        if not isinstance(feature_settings, dict):
            feature_settings = {}

        # Backward compatibility: older artifacts may not store feature_settings.
        # Infer key windows from feature names (e.g. ret_ma_3 / realized_vol_10).
        if not feature_settings:
            inferred = {}
            try:
                ma_ws = []
                vol_ws = []
                for name in feat_names:
                    m = re.match(r"^ret_ma_(\d+)$", str(name))
                    if m:
                        ma_ws.append(int(m.group(1)))
                    m = re.match(r"^realized_vol_(\d+)$", str(name))
                    if m:
                        vol_ws.append(int(m.group(1)))
                if ma_ws:
                    inferred['window_short'] = int(min(ma_ws))
                if vol_ws:
                    inferred['window_med'] = int(min(vol_ws))
            except Exception:
                inferred = {}
            feature_settings = inferred

        # Minimal config-like object compatible with build_features.
        Settings = type('S', (), {})
        s = Settings()
        s.features = feature_settings
        # keep legacy attribute fallbacks too
        for k in ('window_short', 'window_med', 'window_long', 'vol_window', 'horizons'):
            if k in feature_settings:
                try:
                    setattr(s, k, feature_settings[k])
                except Exception:
                    pass

        feats = build_features(df, settings=s, asset_class=str(asset_class), build_targets=False)
        X = feats.X
        missing = [c for c in feat_names if c not in X.columns]
        if missing:
            raise ValueError(f"Feature rebuild missing columns: {missing[:10]}" + ("..." if len(missing) > 10 else ""))
        X = X[feat_names].dropna()
    except Exception:
        # Fallback to legacy behavior (may fail on raw-only parquets).
        X = df[feat_names].dropna()

    if X.empty:
        raise ValueError('No feature rows available for smoke test')
    Xt = X.tail(last_n)
    infer = obj.get('safe_inference')
    if not infer:
        raise ValueError('Artifact missing safe_inference')
    # safe_inference is pickled object - in memory it's the callable dataclass
    if not hasattr(infer, 'predict'):
        raise ValueError('safe_inference does not expose predict()')
    # run predict() in a forked subprocess with timeout to avoid hangs
    try:
        from multiprocessing import get_context
        ctx = get_context('fork')
        q = ctx.Queue()

        def _worker(q, infer_obj, Xloc):
            try:
                res = infer_obj.predict(Xloc)
                q.put(('ok', res))
            except Exception as e:
                try:
                    q.put(('err', str(e)))
                except Exception:
                    pass

        proc = ctx.Process(target=_worker, args=(q, infer, Xt))
        proc.start()
        proc.join(15)  # 15s timeout for predict
        if proc.is_alive():
            proc.terminate()
            proc.join()
            return {'symbol': symbol, 'keys_ok': False, 'nan_free': False, 'output': None, 'error': 'predict_timeout'}
        # collect result
        out_status = None
        out_payload = None
        try:
            if not q.empty():
                out_status, out_payload = q.get_nowait()
        except Exception:
            pass
        if out_status != 'ok':
            return {'symbol': symbol, 'keys_ok': False, 'nan_free': False, 'output': None, 'error': out_payload}

        out = out_payload
        # basic checks
        keys_ok = {'signal', 'position', 'confidence', 'diagnostics'}.issubset(set(out.keys())) if isinstance(out, dict) else False
        nan_free = True
        if not isinstance(out, dict):
            nan_free = False
        else:
            for k in ('signal', 'position', 'confidence'):
                v = out.get(k)
                if v is None:
                    nan_free = False
                    break
                try:
                    # allow arrays or scalars; check for NaN in numeric scalars
                    if isinstance(v, float) and pd.isna(v):
                        nan_free = False
                        break
                except Exception:
                    nan_free = False
                    break
        return {'symbol': symbol, 'keys_ok': keys_ok, 'nan_free': nan_free, 'output': out}
    except Exception as e:
        return {'symbol': symbol, 'keys_ok': False, 'nan_free': False, 'output': None, 'error': str(e)}


def load_meta(meta_path: str) -> Dict[str, Any]:
    if not os.path.exists(meta_path):
        raise FileNotFoundError(meta_path)
    with open(meta_path, 'r') as fh:
        raw = json.load(fh)
    # best-effort compatibility: populate missing fields
    if 'schema_version' not in raw:
        raw['schema_version'] = 1
    return raw
