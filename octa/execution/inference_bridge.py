"""
Inference bridge for shadow/paper execution.

This module provides the ML inference pre-gate for the execution loop.
The signal produced here is a PRE-GATE: it decides whether a symbol
proceeds to the risk engine — NOT position size, NOT direction override.

Risk-First invariant:
  - approved=False → execution proceeds as before (no inference block)
  - approved=True, signal <= 0 → BLOCK entry (no conviction or short signal)
  - approved=True, signal == 1 → proceed to risk engine (unchanged)
  - risk_engine.decide_ml() signature is UNCHANGED

Never raises. All error paths return approved=False, signal=0.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Optional heavy dependencies — imported at module level so tests can patch them.
# These are always available in the production environment.
try:
    from octa.core.data.storage.artifact_io import load_tradeable_artifact as _load_tradeable_artifact
except ImportError:  # pragma: no cover
    _load_tradeable_artifact = None  # type: ignore[assignment]

try:
    from octa.core.data.io.io_parquet import (
        discover_parquets as _discover_parquets,
        load_parquet as _load_parquet,
        sanitize_symbol as _sanitize_symbol,
    )
except ImportError:  # pragma: no cover
    _discover_parquets = None  # type: ignore[assignment]
    _load_parquet = None  # type: ignore[assignment]
    _sanitize_symbol = None  # type: ignore[assignment]

try:
    from octa.core.features.features import build_features as _build_features
except ImportError:  # pragma: no cover
    _build_features = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# InferenceResult
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class InferenceResult:
    symbol: str
    timeframe: str
    signal: int           # -1=short, 0=no conviction, 1=long
    position: float       # position hint from model (not used for sizing)
    confidence: float     # probability or score
    approved: bool        # True = inference succeeded; False = unavailable/error
    reason: str           # "ok" | "artifact_not_found" | "predict_error:..." | …
    artifact_path: str    # path to .pkl used (empty if not found)
    artifact_hash: str    # 16-char sha256 prefix (for audit trail)
    feature_count_model: int    # features model expects
    feature_count_runtime: int  # of those, how many were available
    diagnostics: Dict[str, Any]


def _no_signal(
    symbol: str,
    timeframe: str,
    reason: str,
    artifact_path: str = "",
    artifact_hash: str = "",
    feature_count_model: int = 0,
    feature_count_runtime: int = 0,
    diagnostics: Optional[Dict[str, Any]] = None,
) -> InferenceResult:
    return InferenceResult(
        symbol=symbol,
        timeframe=timeframe,
        signal=0,
        position=0.0,
        confidence=0.0,
        approved=False,
        reason=reason,
        artifact_path=artifact_path,
        artifact_hash=artifact_hash,
        feature_count_model=feature_count_model,
        feature_count_runtime=feature_count_runtime,
        diagnostics=diagnostics or {},
    )


# ---------------------------------------------------------------------------
# Asset class directory name mapping
# (mirrors runner.py _ASSET_CLASS_META + actual raw/PKL directory structure)
# ---------------------------------------------------------------------------

_AC_DIR_MAP: Dict[str, str] = {
    "equities": "equity",
    "equity": "equity",
    "stock": "equity",
    "etf": "etf",
    "futures": "future",
    "future": "future",
    "forex": "fx",
    "fx": "fx",
    "fx_carry": "fx",
    "index": "index",
    "options": "option",
    "option": "option",
    "crypto": "crypto",
    "bond": "bond",
}

# ---------------------------------------------------------------------------
# P0-1: Direct raw parquet lookup (replaces full discover_parquets scan)
# Maps asset_class → raw/<SubDir>/ where <SYMBOL>_<TF>.parquet files live.
# Convention: raw/<SubDir>/<SYMBOL>_<TF>.parquet (uppercase TF).
# ---------------------------------------------------------------------------

_RAW_PARQUET_DIR_MAP: Dict[str, str] = {
    "equity":   "Stock_parquet",
    "equities": "Stock_parquet",
    "stock":    "Stock_parquet",
    "etf":      "ETF_Parquet",
    "forex":    "FX_parquet",
    "fx":       "FX_parquet",
    "fx_carry": "FX_parquet",
    "futures":  "Futures_Parquet",
    "future":   "Futures_Parquet",
    "index":    "Indices_parquet",
    "options":  "Option_parquet",
    "option":   "Option_parquet",
    "crypto":   "Crypto_parquet",
    "bond":     "Bond_parquet",
}


def _find_raw_parquet_direct(
    symbol: str,
    asset_class: str,
    timeframe: str,
    raw_data_dir: Path,
) -> Optional[Path]:
    """Direct O(1) parquet lookup: raw/<AssetClassDir>/<SYMBOL>_<TF>.parquet.

    Replaces the full discover_parquets() scan (was ~100s for 10,689 files).
    Returns None if asset_class unknown or file not found — caller treats as
    parquet_not_found → approved=False (fail-closed).
    """
    ac_raw_dir = _RAW_PARQUET_DIR_MAP.get(str(asset_class).lower().strip())
    if not ac_raw_dir:
        return None
    candidate = raw_data_dir / ac_raw_dir / f"{symbol.upper()}_{timeframe.upper()}.parquet"
    if candidate.exists():
        return candidate
    return None


def _find_artifact(
    symbol: str,
    asset_class: str,
    timeframe: str,
    artifact_dir: Path,
    paper_ready_dir: Optional[Path] = None,
) -> Optional[Path]:
    """Locate the tradeable .pkl for symbol+asset_class+timeframe.

    Search order:
    1. artifact_dir/<ac_dir>/<TF>/<SYMBOL>.pkl  (raw/PKL/ convention)
    2. paper_ready_dir/<SYMBOL>/<TF>/<SYMBOL>_<TF>.pkl  (training promotion output)

    Returns None if not found (caller treats as approved=False).
    """
    # Primary: raw/PKL/<ac>/<TF>/<SYMBOL>.pkl
    ac_dir_name = _AC_DIR_MAP.get(str(asset_class).lower().strip(), str(asset_class).lower())
    candidate = artifact_dir / ac_dir_name / timeframe.upper() / f"{symbol.upper()}.pkl"
    if candidate.exists():
        return candidate

    # Secondary (P0-3/P0-5): paper_ready/<SYMBOL>/<TF>/
    # Two naming conventions:
    #   - <SYMBOL>.pkl  (written by _promote_to_paper_ready via src.name)
    #   - <SYMBOL>_<TF>.pkl  (legacy convention, kept for forward compat)
    if paper_ready_dir is not None:
        sym_u = symbol.upper()
        tf_u = timeframe.upper()
        for _name in (f"{sym_u}.pkl", f"{sym_u}_{tf_u}.pkl"):
            paper_candidate = paper_ready_dir / sym_u / tf_u / _name
            if paper_candidate.exists():
                return paper_candidate

    return None


# ---------------------------------------------------------------------------
# Core inference function
# ---------------------------------------------------------------------------

def build_inference_proposal(
    symbol: str,
    asset_class: str,
    artifact_dir: Path,
    raw_data_dir: Path,
    timeframe: str = "1D",
    paper_ready_dir: Optional[Path] = None,
) -> InferenceResult:
    """Build an ML inference proposal for one symbol.

    Fail-closed: never raises, returns approved=False on any error.
    The caller must treat approved=False as "inference unavailable" —
    not as a block signal. Only approved=True with signal <= 0 blocks.

    Args:
        paper_ready_dir: Optional secondary artifact source (P0-3/P0-5).
            Checked as paper_ready_dir/<SYMBOL>/<TF>/<SYMBOL>_<TF>.pkl
            when artifact_dir lookup fails. Typically octa/var/models/paper_ready/.
    """
    sym_u = str(symbol).upper().strip()
    tf_u = str(timeframe).upper().strip()

    # 1. Find artifact (primary: raw/PKL/, secondary: paper_ready/)
    pkl_path = _find_artifact(sym_u, asset_class, tf_u, artifact_dir, paper_ready_dir)
    if pkl_path is None:
        # Try 1H fallback if 1D not found
        if tf_u == "1D":
            pkl_path = _find_artifact(sym_u, asset_class, "1H", artifact_dir, paper_ready_dir)
            if pkl_path is not None:
                tf_u = "1H"
    if pkl_path is None:
        return _no_signal(sym_u, tf_u, "artifact_not_found")

    # 2. Compute artifact hash before loading (for audit)
    try:
        artifact_bytes = pkl_path.read_bytes()
        artifact_hash = hashlib.sha256(artifact_bytes).hexdigest()[:16]
    except Exception as exc:
        return _no_signal(sym_u, tf_u, f"artifact_hash_error:{exc}",
                          artifact_path=str(pkl_path))

    # 3. Load artifact (with optional sha256 verify)
    try:
        sha_sidecar = str(pkl_path).replace(".pkl", ".sha256")
        if Path(sha_sidecar).exists():
            obj: Dict[str, Any] = _load_tradeable_artifact(str(pkl_path), sha_sidecar)
        else:
            obj = _load_tradeable_artifact(str(pkl_path))
    except Exception as exc:
        return _no_signal(sym_u, tf_u, f"artifact_load_error:{exc}",
                          artifact_path=str(pkl_path),
                          artifact_hash=artifact_hash)

    # 4. Extract SafeInference
    safe_inference = obj.get("safe_inference")
    if safe_inference is None or not hasattr(safe_inference, "predict"):
        return _no_signal(sym_u, tf_u, "no_safe_inference",
                          artifact_path=str(pkl_path),
                          artifact_hash=artifact_hash)

    feature_names: List[str] = list(getattr(safe_inference, "feature_names", None) or [])

    # 5. Build features from raw parquet (last bar)
    # P0-1: Direct O(1) lookup replaces discover_parquets() full scan (~100s → <1s).
    try:
        parquet_path = _find_raw_parquet_direct(sym_u, asset_class, tf_u, raw_data_dir)
        if parquet_path is None and tf_u != "1D":
            # No additional fallback for non-1D — the artifact TF must match
            pass
        if parquet_path is None:
            return _no_signal(sym_u, tf_u, "parquet_not_found",
                              artifact_path=str(pkl_path),
                              artifact_hash=artifact_hash,
                              feature_count_model=len(feature_names))

        df_raw = _load_parquet(parquet_path)
    except Exception as exc:
        return _no_signal(sym_u, tf_u, f"parquet_load_error:{exc}",
                          artifact_path=str(pkl_path),
                          artifact_hash=artifact_hash,
                          feature_count_model=len(feature_names))

    # 6. Compute features
    try:
        feat_cfg = (obj.get("feature_spec", {}) or {}).get("feature_config") or {}
        feature_settings: Dict[str, Any] = {}
        if isinstance(feat_cfg, dict):
            feature_settings = feat_cfg.get("feature_settings") or {}
        if not isinstance(feature_settings, dict):
            feature_settings = {}

        # Minimal settings-like object compatible with build_features
        Settings = type("_S", (), {})
        s = Settings()
        s.features = feature_settings
        for k in ("window_short", "window_med", "window_long", "vol_window", "horizons"):
            if k in feature_settings:
                try:
                    setattr(s, k, feature_settings[k])
                except Exception:
                    pass

        feats = _build_features(df_raw, settings=s, asset_class=str(asset_class), build_targets=False)
        X = feats.X
    except Exception as exc:
        return _no_signal(sym_u, tf_u, f"feature_build_error:{exc}",
                          artifact_path=str(pkl_path),
                          artifact_hash=artifact_hash,
                          feature_count_model=len(feature_names))

    # 7. Select last bar
    try:
        X_last = X.tail(1).copy()
        if X_last.empty:
            return _no_signal(sym_u, tf_u, "feature_row_empty",
                              artifact_path=str(pkl_path),
                              artifact_hash=artifact_hash,
                              feature_count_model=len(feature_names))
        feature_count_runtime = len([c for c in feature_names if c in X_last.columns])
    except Exception as exc:
        return _no_signal(sym_u, tf_u, f"feature_select_error:{exc}",
                          artifact_path=str(pkl_path),
                          artifact_hash=artifact_hash,
                          feature_count_model=len(feature_names))

    # 8. Run inference — SafeInference.predict() never raises
    try:
        result = safe_inference.predict(X_last)
    except Exception as exc:
        # Extra safety net (should never reach here per SafeInference contract)
        return _no_signal(sym_u, tf_u, f"predict_error:{exc}",
                          artifact_path=str(pkl_path),
                          artifact_hash=artifact_hash,
                          feature_count_model=len(feature_names),
                          feature_count_runtime=feature_count_runtime)

    if not isinstance(result, dict):
        return _no_signal(sym_u, tf_u, "predict_bad_return_type",
                          artifact_path=str(pkl_path),
                          artifact_hash=artifact_hash,
                          feature_count_model=len(feature_names),
                          feature_count_runtime=feature_count_runtime)

    # Diagnostics error → no conviction
    diag = result.get("diagnostics") or {}
    diag_error = diag.get("error")
    if diag_error:
        return InferenceResult(
            symbol=sym_u,
            timeframe=tf_u,
            signal=0,
            position=0.0,
            confidence=0.0,
            approved=False,
            reason=f"predict_diagnostics_error:{diag_error}",
            artifact_path=str(pkl_path),
            artifact_hash=artifact_hash,
            feature_count_model=len(feature_names),
            feature_count_runtime=feature_count_runtime,
            diagnostics=diag,
        )

    # 9. Extract signal — must be in {-1, 0, 1}
    try:
        sig = int(result.get("signal", 0))
    except Exception:
        sig = 0
    if sig not in (-1, 0, 1):
        sig = 0

    return InferenceResult(
        symbol=sym_u,
        timeframe=tf_u,
        signal=sig,
        position=float(result.get("position", 0.0)),
        confidence=float(result.get("confidence", 0.0)),
        approved=True,
        reason="ok",
        artifact_path=str(pkl_path),
        artifact_hash=artifact_hash,
        feature_count_model=len(feature_names),
        feature_count_runtime=feature_count_runtime,
        diagnostics=diag,
    )


# ---------------------------------------------------------------------------
# Cycle-level inference runner
# ---------------------------------------------------------------------------

def run_inference_cycle(
    eligible_rows: List[Dict[str, Any]],
    artifact_dir: Path,
    raw_data_dir: Path,
    timeframe: str,
    evidence_dir: Path,
    cycle_idx: int,
    paper_ready_dir: Optional[Path] = None,
) -> Dict[str, InferenceResult]:
    """Run inference for all eligible symbols. Returns {symbol: InferenceResult}.

    Writes inference_cycle_NNN.json to evidence_dir.
    Never raises.

    Args:
        paper_ready_dir: Optional secondary artifact source (P0-3/P0-5).
            When set, paper_ready/<SYMBOL>/<TF>/<SYMBOL>_<TF>.pkl is checked
            if the primary artifact_dir lookup fails.
    """
    results: Dict[str, InferenceResult] = {}

    for row in eligible_rows:
        symbol = str(row.get("symbol", "")).upper().strip()
        asset_class = str(row.get("asset_class", "unknown")).lower().strip()
        if not symbol:
            continue
        try:
            ir = build_inference_proposal(
                symbol=symbol,
                asset_class=asset_class,
                artifact_dir=artifact_dir,
                raw_data_dir=raw_data_dir,
                timeframe=timeframe,
                paper_ready_dir=paper_ready_dir,
            )
        except Exception as exc:
            # Should never happen — build_inference_proposal never raises
            ir = _no_signal(symbol, timeframe, f"unexpected_error:{exc}")
        results[symbol] = ir

    # Write evidence
    try:
        _write_inference_evidence(
            results=results,
            evidence_dir=evidence_dir,
            cycle_idx=cycle_idx,
        )
    except Exception:
        pass  # Evidence write failure must never block execution

    return results


def _write_inference_evidence(
    results: Dict[str, InferenceResult],
    evidence_dir: Path,
    cycle_idx: int,
) -> None:
    """Write inference_cycle_NNN.json evidence file."""
    rows = [
        {
            "symbol": ir.symbol,
            "timeframe": ir.timeframe,
            "signal": ir.signal,
            "confidence": ir.confidence,
            "position": ir.position,
            "approved": ir.approved,
            "reason": ir.reason,
            "artifact_path": ir.artifact_path,
            "artifact_hash": ir.artifact_hash,
            "feature_count_model": ir.feature_count_model,
            "feature_count_runtime": ir.feature_count_runtime,
            "diagnostics": ir.diagnostics,
        }
        for ir in sorted(results.values(), key=lambda x: x.symbol)
    ]

    n_approved = sum(1 for ir in results.values() if ir.approved)
    n_blocked_by_signal = sum(
        1 for ir in results.values() if ir.approved and ir.signal <= 0
    )
    n_errors = sum(1 for ir in results.values() if not ir.approved)

    payload = {
        "cycle": cycle_idx,
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "inference_enabled": True,
        "results": rows,
        "summary": {
            "total": len(results),
            "approved_inference": n_approved,
            "blocked_by_signal": n_blocked_by_signal,
            "inference_errors": n_errors,
        },
    }

    out_path = evidence_dir / f"inference_cycle_{cycle_idx:03d}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
