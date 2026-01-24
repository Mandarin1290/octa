#!/usr/bin/env python3
"""Train and package STOCKS tradeable PKLs for 1D and 1H only (HF-grade, fail-closed).

Hard constraints implemented:
- Stocks cascade: 1D -> 1H. Stop if 1D FAIL/ERROR.
- 30m/5m/1m are NOT trained here.
- PKLs are produced ONLY when the respective gate is PASS (PASS_FULL or PASS_LIMITED_STATISTICAL_CONFIDENCE).
- 1H is optional: if missing parquet or not "hourly-like" => SKIP_H1_NOT_ELIGIBLE.
- Symbol is ARMED only when 1D PASS and 1H is PASS or SKIP_H1_NOT_ELIGIBLE.

Outputs:
- out_root/1D/<SYMBOL>.pkl
- out_root/1H/<SYMBOL>.pkl (only if 1H PASS)
- out_root/meta/<SYMBOL>.meta.json
- out_root/meta/<SYMBOL>.audit.jsonl
- out_root/ARMED/<SYMBOL>.ok

Run logs + summary:
- reports/training_1d1h/<run_id>/

Usage:
  PYTHONPATH=. python scripts/train_stocks_pkl_1d_1h.py \
    --run-id stocks_pkl_1d1h_YYYYMMDDTHHMMSSZ \
        --symbols-file reports/pass_symbols_stock.txt \
    --parquet-root raw/Stock_parquet \
    --out-root /home/n-b/Octa/raw/PKL/stocks \
    --gate-version hf_gate_2026-01-03_v1 \
    --max-workers 1
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Allow running as a script from the repo root without installing the package
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    import yaml  # noqa: F401
except Exception:
    yaml = None

from octa_training.core.config import TrainingConfig, load_config
from octa_training.core.io_parquet import load_parquet, sanitize_symbol
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry

ALLOWED_PASS_STATUSES = {"PASS_FULL", "PASS_LIMITED_STATISTICAL_CONFIDENCE"}


def _json_sanitize(obj: Any) -> Any:
    """Convert common non-JSON-native scalars/containers to plain Python types."""
    # numpy scalars (np.bool_, np.float64, etc.)
    try:
        import numpy as np  # type: ignore

        if isinstance(obj, np.generic):
            return obj.item()
    except Exception:
        pass

    # pandas Timestamp
    try:
        import pandas as pd  # type: ignore

        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
    except Exception:
        pass

    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, Path):
        return str(obj)

    if isinstance(obj, dict):
        return {str(k): _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_sanitize(v) for v in obj]

    return obj


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_symbol(s: str) -> str:
    return str(s or "").strip().upper().replace("-", "_")


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _append_audit(path: Path, event: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(event, default=str)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()


def _append_ndjson(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")
        f.flush()


def _read_symbols(path: Path) -> List[str]:
    rows = []
    for ln in path.read_text(encoding="utf-8").splitlines():
        s = _norm_symbol(ln)
        if s:
            rows.append(s)
    # stable order, stable dedupe
    out = []
    seen = set()
    for s in rows:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _find_variant(discovered: list[dict], base: str, suffix: str) -> Optional[Path]:
    """Find parquet path for base+suffix using discover_parquets results.

    We accept common naming patterns and fall back to direct filesystem lookup.
    """
    b = _norm_symbol(base)
    suf = _norm_symbol(suffix)

    # Common exact form: SYMBOL_SUFFIX
    if suf:
        target = f"{b}_{suf}"
    else:
        target = b

    for d in discovered:
        if _norm_symbol(str(d.get("symbol") or "")) == target:
            return Path(str(d.get("path")))

    # Heuristic: match by startswith(base) and contains suffix token
    toks = []
    if suf in {"1D", "1DAY", "DAILY", ""}:
        toks = ["_1D", "_1DAY", "_DAILY", "_FULL_1DAY", "_FULL_DAILY"]
    elif suf in {"1H", "1HR", "1HOUR"}:
        toks = ["_1H", "_1HR", "_1HOUR", "_FULL_1HOUR", "_60MIN", "_60M"]

    best: Optional[Path] = None
    best_score = -1
    for d in discovered:
        name = _norm_symbol(str(d.get("symbol") or ""))
        if not name.startswith(b):
            continue
        score = 10
        if name == b:
            score += 10
        for t in toks:
            if t in name:
                score += 10
        if "FULL" in name:
            score += 1
        if score > best_score:
            best_score = score
            best = Path(str(d.get("path")))

    if best is not None:
        return best

    # Final fallback: direct path under parquet_root naming convention
    # Caller may use this by passing discovered built from that root.
    return None


def _is_hourly_like_index(idx) -> bool:
    """Strict-ish hourly-like check based on median spacing."""
    try:
        if idx is None or len(idx) < 50:
            return False
        # Use a bounded sample window for determinism.
        idx_s = idx[: min(len(idx), 2000)]
        deltas = idx_s.to_series().diff().dropna()
        if deltas.empty:
            return False
        med = deltas.median()
        # pandas returns Timedelta for timedelta64 series; convert robustly to seconds.
        try:
            import pandas as pd  # type: ignore

            med_sec = float(med / pd.Timedelta(seconds=1))
        except Exception:
            # Fallback for unexpected types
            med_sec = float(getattr(med, "total_seconds", lambda: 0.0)())
        # allow some vendor variation, but reject non-hourly
        return 50 * 60 <= med_sec <= 70 * 60
    except Exception:
        return False


def _gate_fields(gate_obj: Any) -> Dict[str, Any]:
    if gate_obj is None:
        return {"passed": False, "status": "ERROR", "gate_version": None, "reasons": ["missing_gate"], "passed_checks": [], "insufficient_evidence": []}
    try:
        if hasattr(gate_obj, "model_dump"):
            return gate_obj.model_dump()
        if hasattr(gate_obj, "dict"):
            return gate_obj.dict()
    except Exception:
        pass
    return {
        "passed": bool(getattr(gate_obj, "passed", False)),
        "status": getattr(gate_obj, "status", None),
        "gate_version": getattr(gate_obj, "gate_version", None),
        "reasons": getattr(gate_obj, "reasons", None),
        "passed_checks": getattr(gate_obj, "passed_checks", None),
        "insufficient_evidence": getattr(gate_obj, "insufficient_evidence", None),
    }


def _tf_passed(gate: Dict[str, Any]) -> Tuple[bool, bool]:
    """Return (passed, limited)."""
    status = str(gate.get("status") or "").upper()
    passed = bool(gate.get("passed"))
    if status in ALLOWED_PASS_STATUSES:
        return True, status == "PASS_LIMITED_STATISTICAL_CONFIDENCE"
    return passed and status.startswith("PASS"), False


def _stage_cfg(cfg: TrainingConfig, pkl_dir: Path) -> TrainingConfig:
    base = cfg.model_dump() if hasattr(cfg, "model_dump") else (cfg.dict() if hasattr(cfg, "dict") else {})
    # Force packaging invariants
    try:
        base.setdefault("packaging", {})
        base["packaging"]["save_debug_on_fail"] = False
    except Exception:
        pass
    # pkl output staging directory
    base.setdefault("paths", {})
    base["paths"]["pkl_dir"] = str(pkl_dir)

    # This script is an explicit orchestrator; disable recent-pass short-circuiting
    # so we always compute fresh gate results and (on PASS) produce artifacts.
    base.setdefault("retrain", {})
    try:
        base["retrain"]["skip_window_days"] = 0
    except Exception:
        pass
    return TrainingConfig(**base)


@dataclass
class TfOutcome:
    tf: str
    status: str  # PASS/FAIL/ERROR/SKIP_H1_NOT_ELIGIBLE
    limited: bool
    gate: Dict[str, Any]
    parquet_path: Optional[str]
    parquet_sha256: Optional[str]
    parquet_rows: Optional[int]
    parquet_start: Optional[str]
    parquet_end: Optional[str]
    pkl_path: Optional[str]
    pkl_sha256: Optional[str]


def _dataset_fingerprint(df) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    try:
        rows = int(len(df))
        start = str(df.index.min()) if rows else None
        end = str(df.index.max()) if rows else None
        return rows, start, end
    except Exception:
        return None, None, None


def train_one_symbol(
    *,
    symbol: str,
    cfg: TrainingConfig,
    state_dir: Path,
    run_id: str,
    parquet_root: Path,
    out_root: Path,
    gate_version_expected: str,
    reports_dir: Path,
    discovered: list[dict],
    train_1h_even_if_1d_fails: bool,
) -> Dict[str, Any]:
    sym = _norm_symbol(symbol)

    sym_report_dir = reports_dir / sym
    sym_report_dir.mkdir(parents=True, exist_ok=True)

    audit_path = out_root / "meta" / f"{sym}.audit.jsonl"
    meta_path = out_root / "meta" / f"{sym}.meta.json"
    armed_path = out_root / "ARMED" / f"{sym}.ok"

    staging_root = out_root / "_staging" / run_id / sym
    staging_1d = staging_root / "1D"
    staging_1h = staging_root / "1H"

    out_1d_dir = out_root / "1D"
    out_1h_dir = out_root / "1H"
    out_1d_dir.mkdir(parents=True, exist_ok=True)
    out_1h_dir.mkdir(parents=True, exist_ok=True)

    _append_audit(audit_path, {"type": "START", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "asset_profile": "stock"})

    state = StateRegistry(str(state_dir))

    # `discovered` is precomputed in main for determinism + less IO.

    # -------- 1D --------
    p1d = _find_variant(discovered, sym, "1D")
    if p1d is None or not p1d.exists():
        _append_audit(audit_path, {"type": "LOAD", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1D", "ok": False, "reason": "missing_parquet"})
        out = {
            "symbol": sym,
            "asset_profile": "stock",
            "run_id": run_id,
            "timeframe_status": {"1D": "ERROR", "1H": "SKIP_H1_NOT_ELIGIBLE"},
            "armed": False,
        }
        _atomic_write_text(meta_path, json.dumps(_json_sanitize(out), indent=2) + "\n")
        return out

    parquet_sha = _sha256_file(p1d)
    _append_audit(audit_path, {"type": "LOAD", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1D", "ok": True, "parquet_path": str(p1d), "parquet_sha256": parquet_sha})

    # Train/eval/gate (packaging only happens on PASS)
    cfg_1d = _stage_cfg(cfg, staging_1d)
    _append_audit(audit_path, {"type": "TRAIN_START", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1D"})
    res_1d = train_evaluate_package(sym, cfg_1d, state, run_id, safe_mode=False, smoke_test=False, parquet_path=str(p1d))
    _append_audit(audit_path, {"type": "TRAIN_END", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1D", "passed": bool(getattr(res_1d, "passed", False)), "error": getattr(res_1d, "error", None)})

    gate_1d = _gate_fields(getattr(res_1d, "gate_result", None))
    if gate_version_expected and gate_1d.get("gate_version") and str(gate_1d.get("gate_version")) != str(gate_version_expected):
        gate_1d = {**gate_1d, "passed": False, "status": "ERROR", "reasons": list(gate_1d.get("reasons") or []) + [f"gate_version_mismatch:{gate_1d.get('gate_version')}!={gate_version_expected}"]}

    passed_1d, limited_1d = _tf_passed(gate_1d)
    pack_1d = getattr(res_1d, "pack_result", None) or {}
    pkl_1d_src = pack_1d.get("pkl") if isinstance(pack_1d, dict) else None
    pkl_1d_sha = pack_1d.get("pkl_sha") if isinstance(pack_1d, dict) else None

    # Load once for range/rows
    try:
        df_1d = load_parquet(p1d)
        rows_1d, start_1d, end_1d = _dataset_fingerprint(df_1d)
    except Exception:
        rows_1d, start_1d, end_1d = None, None, None

    out_1d_path = out_1d_dir / f"{sym}.pkl"
    if passed_1d:
        if not pkl_1d_src or not Path(pkl_1d_src).exists():
            # Fail-closed: PASS but no PKL written is an ERROR.
            passed_1d = False
            gate_1d = {**gate_1d, "passed": False, "status": "ERROR", "reasons": list(gate_1d.get("reasons") or []) + ["packaging_missing_pkl"]}
        else:
            shutil.copy2(pkl_1d_src, out_1d_path)
            pkl_1d_sha = _sha256_file(out_1d_path)
            _append_audit(audit_path, {"type": "WRITE_PKL", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1D", "pkl": str(out_1d_path), "pkl_sha256": pkl_1d_sha})
    else:
        # Ensure no stray output
        if out_1d_path.exists():
            out_1d_path.unlink()

    out_1d = TfOutcome(
        tf="1D",
        status="PASS" if passed_1d else ("ERROR" if getattr(res_1d, "error", None) else "FAIL"),
        limited=bool(limited_1d),
        gate=gate_1d,
        parquet_path=str(p1d),
        parquet_sha256=parquet_sha,
        parquet_rows=rows_1d,
        parquet_start=start_1d,
        parquet_end=end_1d,
        pkl_path=str(out_1d_path) if passed_1d else None,
        pkl_sha256=str(pkl_1d_sha) if passed_1d else None,
    )

    if not passed_1d and not bool(train_1h_even_if_1d_fails):
        # Fail-closed: ensure stale ARMED markers from previous runs are removed.
        try:
            if armed_path.exists():
                armed_path.unlink()
                _append_audit(audit_path, {"type": "DISARM", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "ok": True, "armed": str(armed_path)})
        except Exception:
            pass

        meta = {
            "symbol": sym,
            "asset_profile": "stock",
            "run_id": run_id,
            "created_utc": _utc_now(),
            "gate_version": gate_version_expected,
            "timeframe_status": {"1D": out_1d.status, "1H": "SKIP_H1_NOT_ELIGIBLE"},
            "limited": {"1D": out_1d.limited, "1H": False},
            "datasets": {"1D": {"path": out_1d.parquet_path, "sha256": out_1d.parquet_sha256, "rows": out_1d.parquet_rows, "start": out_1d.parquet_start, "end": out_1d.parquet_end}},
            "models": {"1D": {"pkl": out_1d.pkl_path, "sha256": out_1d.pkl_sha256}, "1H": None},
            "gates": {"1D": out_1d.gate, "1H": None},
            "armed": False,
        }
        _atomic_write_text(meta_path, json.dumps(_json_sanitize(meta), indent=2) + "\n")
        _append_audit(audit_path, {"type": "WRITE_META", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "ok": True, "meta": str(meta_path)})
        return meta

    # -------- 1H (optional) --------
    p1h = _find_variant(discovered, sym, "1H")
    if p1h is None or not p1h.exists():
        out_1h = TfOutcome(
            tf="1H",
            status="SKIP_H1_NOT_ELIGIBLE",
            limited=False,
            gate={"passed": False, "status": "SKIP_H1_NOT_ELIGIBLE", "gate_version": gate_version_expected, "reasons": ["missing_parquet"], "passed_checks": [], "insufficient_evidence": []},
            parquet_path=None,
            parquet_sha256=None,
            parquet_rows=None,
            parquet_start=None,
            parquet_end=None,
            pkl_path=None,
            pkl_sha256=None,
        )
        _append_audit(audit_path, {"type": "SKIP", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "reason": "missing_parquet"})
    else:
        parquet_sha_1h = _sha256_file(p1h)
        _append_audit(audit_path, {"type": "LOAD", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "ok": True, "parquet_path": str(p1h), "parquet_sha256": parquet_sha_1h})

        # hourly-like check (strict)
        try:
            df_1h_check = load_parquet(p1h)
            hourly_like = _is_hourly_like_index(df_1h_check.index)
            rows_1h, start_1h, end_1h = _dataset_fingerprint(df_1h_check)
        except Exception as e:
            hourly_like = False
            rows_1h, start_1h, end_1h = None, None, None
            _append_audit(audit_path, {"type": "LOAD", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "ok": False, "reason": f"read_error:{str(e).splitlines()[0]}"})

        if not hourly_like:
            out_1h = TfOutcome(
                tf="1H",
                status="SKIP_H1_NOT_ELIGIBLE",
                limited=False,
                gate={"passed": False, "status": "SKIP_H1_NOT_ELIGIBLE", "gate_version": gate_version_expected, "reasons": ["not_hourly_like"], "passed_checks": [], "insufficient_evidence": []},
                parquet_path=str(p1h),
                parquet_sha256=parquet_sha_1h,
                parquet_rows=rows_1h,
                parquet_start=start_1h,
                parquet_end=end_1h,
                pkl_path=None,
                pkl_sha256=None,
            )
            _append_audit(audit_path, {"type": "SKIP", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "reason": "not_hourly_like"})
        else:
            cfg_1h = _stage_cfg(cfg, staging_1h)
            _append_audit(audit_path, {"type": "TRAIN_START", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H"})
            res_1h = train_evaluate_package(sym + "_1H", cfg_1h, state, run_id, safe_mode=False, smoke_test=False, parquet_path=str(p1h))
            _append_audit(audit_path, {"type": "TRAIN_END", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "passed": bool(getattr(res_1h, "passed", False)), "error": getattr(res_1h, "error", None)})

            gate_1h = _gate_fields(getattr(res_1h, "gate_result", None))
            if gate_version_expected and gate_1h.get("gate_version") and str(gate_1h.get("gate_version")) != str(gate_version_expected):
                gate_1h = {**gate_1h, "passed": False, "status": "ERROR", "reasons": list(gate_1h.get("reasons") or []) + [f"gate_version_mismatch:{gate_1h.get('gate_version')}!={gate_version_expected}"]}

            passed_1h, limited_1h = _tf_passed(gate_1h)
            pack_1h = getattr(res_1h, "pack_result", None) or {}
            pkl_1h_src = pack_1h.get("pkl") if isinstance(pack_1h, dict) else None
            pkl_1h_sha = pack_1h.get("pkl_sha") if isinstance(pack_1h, dict) else None

            out_1h_path = out_1h_dir / f"{sym}.pkl"
            if passed_1h:
                if not pkl_1h_src or not Path(pkl_1h_src).exists():
                    passed_1h = False
                    gate_1h = {**gate_1h, "passed": False, "status": "ERROR", "reasons": list(gate_1h.get("reasons") or []) + ["packaging_missing_pkl"]}
                else:
                    shutil.copy2(pkl_1h_src, out_1h_path)
                    pkl_1h_sha = _sha256_file(out_1h_path)
                    _append_audit(audit_path, {"type": "WRITE_PKL", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "pkl": str(out_1h_path), "pkl_sha256": pkl_1h_sha})
            else:
                if out_1h_path.exists():
                    out_1h_path.unlink()

            out_1h = TfOutcome(
                tf="1H",
                status="PASS" if passed_1h else ("ERROR" if getattr(res_1h, "error", None) else "FAIL"),
                limited=bool(limited_1h),
                gate=gate_1h,
                parquet_path=str(p1h),
                parquet_sha256=parquet_sha_1h,
                parquet_rows=rows_1h,
                parquet_start=start_1h,
                parquet_end=end_1h,
                pkl_path=str(out_1h_path) if passed_1h else None,
                pkl_sha256=str(pkl_1h_sha) if passed_1h else None,
            )

    armed = out_1d.status == "PASS" and out_1h.status in {"PASS", "SKIP_H1_NOT_ELIGIBLE"}

    meta = {
        "symbol": sym,
        "asset_profile": "stock",
        "run_id": run_id,
        "created_utc": _utc_now(),
        "gate_version": gate_version_expected,
        "timeframe_status": {"1D": out_1d.status, "1H": out_1h.status},
        "limited": {"1D": out_1d.limited, "1H": out_1h.limited},
        "datasets": {
            "1D": {"path": out_1d.parquet_path, "sha256": out_1d.parquet_sha256, "rows": out_1d.parquet_rows, "start": out_1d.parquet_start, "end": out_1d.parquet_end},
            "1H": None if out_1h.parquet_path is None else {"path": out_1h.parquet_path, "sha256": out_1h.parquet_sha256, "rows": out_1h.parquet_rows, "start": out_1h.parquet_start, "end": out_1h.parquet_end},
        },
        "models": {
            "1D": {"pkl": out_1d.pkl_path, "sha256": out_1d.pkl_sha256},
            "1H": None if out_1h.pkl_path is None else {"pkl": out_1h.pkl_path, "sha256": out_1h.pkl_sha256},
        },
        "gates": {"1D": out_1d.gate, "1H": out_1h.gate},
        "armed": bool(armed),
        "armed_mode": "ARMED_FULL" if (armed and out_1h.status == "PASS") else ("ARMED_1D_ONLY" if armed else "NOT_ARMED"),
    }

    _atomic_write_text(meta_path, json.dumps(_json_sanitize(meta), indent=2) + "\n")
    _append_audit(audit_path, {"type": "WRITE_META", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "ok": True, "meta": str(meta_path)})

    if armed:
        armed_payload = {
            "symbol": sym,
            "asset_profile": "stock",
            "run_id": run_id,
            "created_utc": _utc_now(),
            "armed_mode": meta["armed_mode"],
            "timeframe_status": meta["timeframe_status"],
            "pkl_paths": {"1D": out_1d.pkl_path, "1H": out_1h.pkl_path},
            "pkl_sha256": {"1D": out_1d.pkl_sha256, "1H": out_1h.pkl_sha256},
            "gate": {"1D": {"status": out_1d.gate.get("status"), "gate_version": out_1d.gate.get("gate_version")}, "1H": {"status": out_1h.gate.get("status"), "gate_version": out_1h.gate.get("gate_version")}},
        }
        _atomic_write_text(armed_path, json.dumps(_json_sanitize(armed_payload), indent=2) + "\n")
        _append_audit(audit_path, {"type": "ARMED", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "ok": True, "armed": str(armed_path)})
    else:
        # Fail-closed: if the symbol is not armed, ensure any previous marker is removed.
        try:
            if armed_path.exists():
                armed_path.unlink()
                _append_audit(audit_path, {"type": "DISARM", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "ok": True, "armed": str(armed_path)})
        except Exception:
            pass

    # Per-symbol report artifact under reports/ (easy grepping + CI artifacts)
    try:
        (reports_dir / f"{sym}.result.json").write_text(json.dumps(meta, indent=2) + "\n")
    except Exception:
        pass

    return meta


def validate_pkl_outputs(out_root: Path, symbols: Iterable[str]) -> None:
    out_root = Path(out_root)
    for s in symbols:
        sym = _norm_symbol(s)
        meta_path = out_root / "meta" / f"{sym}.meta.json"
        if not meta_path.exists():
            raise AssertionError(f"missing meta for {sym}: {meta_path}")
        meta = json.loads(meta_path.read_text())
        tf = meta.get("timeframe_status") or {}
        s1d = tf.get("1D")
        s1h = tf.get("1H")

        p1d = out_root / "1D" / f"{sym}.pkl"
        p1h = out_root / "1H" / f"{sym}.pkl"
        armed = out_root / "ARMED" / f"{sym}.ok"

        if s1d == "PASS":
            if not p1d.exists():
                raise AssertionError(f"1D PASS but pkl missing: {sym}")
        else:
            if p1d.exists():
                raise AssertionError(f"1D not PASS but pkl exists: {sym}")

        if s1h == "PASS":
            if not p1h.exists():
                raise AssertionError(f"1H PASS but pkl missing: {sym}")
        else:
            if p1h.exists():
                raise AssertionError(f"1H not PASS but pkl exists: {sym}")

        should_armed = (s1d == "PASS") and (s1h in {"PASS", "SKIP_H1_NOT_ELIGIBLE"})
        if should_armed and not armed.exists():
            raise AssertionError(f"should be ARMED but missing marker: {sym}")
        if (not should_armed) and armed.exists():
            raise AssertionError(f"ARMED marker exists but should not: {sym}")


def _process_worker_train_one_symbol(
    symbol: str,
    config_path: str,
    state_dir: str,
    run_id: str,
    parquet_root: str,
    out_root: str,
    gate_version_expected: str,
    reports_dir: str,
    discovered: list[dict],
    train_1h_even_if_1d_fails: bool,
) -> dict:
    cfg_local = load_config(config_path) if config_path else load_config()
    return train_one_symbol(
        symbol=symbol,
        cfg=cfg_local,
        state_dir=Path(state_dir),
        run_id=run_id,
        parquet_root=Path(parquet_root),
        out_root=Path(out_root),
        gate_version_expected=gate_version_expected,
        reports_dir=Path(reports_dir),
        discovered=discovered,
        train_1h_even_if_1d_fails=bool(train_1h_even_if_1d_fails),
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    # Default to the 1D Global Gate pass list. This avoids accidentally pre-filtering 1H via a G1 recheck list.
    ap.add_argument("--symbols-file", default="reports/pass_symbols_stock.txt")
    ap.add_argument("--parquet-root", required=True)
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--gate-version", required=True)
    ap.add_argument("--max-workers", type=int, default=1)
    ap.add_argument("--config", default=None)
    ap.add_argument(
        "--state-dir",
        default="",
        help="Optional override for StateRegistry directory (isolates state.db for verification runs).",
    )
    ap.add_argument("--write-ndjson", action="store_true", help="Write reports/training_1d1h/<run_id>/stocks_1d1h.ndjson")
    ap.add_argument("--train-1h-even-if-1d-fails", action="store_true", help="Train/pack 1H even if 1D FAIL/ERROR (still does NOT ARMED unless 1D PASS)")
    args = ap.parse_args()

    run_id = str(args.run_id)
    symbols_file = Path(str(args.symbols_file))
    symbols = _read_symbols(symbols_file)
    parquet_root = Path(args.parquet_root)
    out_root = Path(args.out_root)

    reports_dir = Path("reports") / "training_1d1h" / run_id
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Sanity: 1D Global Gate should be the only pre-filter. If someone accidentally feeds a G1 recheck list,
    # 1H would be pre-filtered and the cascade intent is violated.
    sf_lower = str(symbols_file).lower()
    warn = None
    if "g1" in sf_lower and "recheck" in sf_lower:
        warn = {
            "type": "symbols_file_suspect",
            "message": "symbols-file looks like a G1 recheck list; intended input is the 1D Global Gate pass list (e.g. reports/pass_symbols_stock.txt).",
            "symbols_file": str(symbols_file),
        }
        print(f"WARNING: {warn['message']} (symbols_file={symbols_file})", file=sys.stderr)

    cfg = load_config(args.config) if args.config else load_config()

    # State dir: allow override to avoid mutating shared state.db during verification.
    state_dir = Path(str(args.state_dir)).expanduser() if str(args.state_dir).strip() else Path(cfg.paths.state_dir)

    (reports_dir / "runner.meta.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "created_utc": _utc_now(),
                "symbols_file": str(symbols_file),
                "symbols_count": len(symbols),
                "symbols": symbols,
                "state_dir": str(state_dir),
                "warning": warn,
            },
            indent=2,
        )
        + "\n"
    )

    # Pre-discover parquet inventory once (deterministic + avoids IO thrash in parallel).
    # IMPORTANT: avoid hashing every parquet here; hashing is done only for the selected files.
    discovered: list[dict] = []
    for p in parquet_root.rglob("*.parquet"):
        parts_upper = {str(x).upper() for x in p.parts}
        if "PKL" in parts_upper:
            continue
        discovered.append({"symbol": sanitize_symbol(p.stem), "path": str(p)})

    max_workers = max(1, int(args.max_workers or 1))

    # Run sequentially by default (HF safe default). If max-workers>1, run in subprocesses.
    results: list[dict] = []
    if max_workers == 1:
        for sym in symbols:
            results.append(
                train_one_symbol(
                    symbol=sym,
                    cfg=cfg,
                    state_dir=state_dir,
                    run_id=run_id,
                    parquet_root=parquet_root,
                    out_root=out_root,
                    gate_version_expected=str(args.gate_version),
                    reports_dir=reports_dir,
                    discovered=discovered,
                    train_1h_even_if_1d_fails=bool(args.train_1h_even_if_1d_fails),
                )
            )
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futs = {
                ex.submit(
                    _process_worker_train_one_symbol,
                    s,
                    str(args.config or ""),
                    str(state_dir),
                    run_id,
                    str(parquet_root),
                    str(out_root),
                    str(args.gate_version),
                    str(reports_dir),
                    discovered,
                    bool(args.train_1h_even_if_1d_fails),
                ): s
                for s in symbols
            }
            for fut in as_completed(futs):
                results.append(fut.result())

    # Summary counts
    def st(meta: dict, k: str) -> str:
        try:
            return str((meta.get("timeframe_status") or {}).get(k) or "").upper()
        except Exception:
            return ""

    summary = {
        "run_id": run_id,
        "created_utc": _utc_now(),
        "total": len(symbols),
        "counts": {
            "1D": {"PASS": 0, "FAIL": 0, "ERROR": 0},
            "1H": {"PASS": 0, "FAIL": 0, "ERROR": 0, "SKIP_H1_NOT_ELIGIBLE": 0},
        },
        "armed": 0,
        "armed_symbols": [],
    }

    for meta in results:
        s = str(meta.get("symbol") or "").upper()
        s1d = st(meta, "1D")
        s1h = st(meta, "1H")
        if s1d in summary["counts"]["1D"]:
            summary["counts"]["1D"][s1d] += 1
        else:
            summary["counts"]["1D"]["ERROR"] += 1
        if s1h in summary["counts"]["1H"]:
            summary["counts"]["1H"][s1h] += 1
        else:
            summary["counts"]["1H"]["ERROR"] += 1
        if bool(meta.get("armed")):
            summary["armed"] += 1
            summary["armed_symbols"].append(s)

    (reports_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    # Optional NDJSON (1 record per symbol)
    if bool(getattr(args, "write_ndjson", False)):
        nd = reports_dir / "stocks_1d1h.ndjson"
        for meta in results:
            sym = str(meta.get("symbol") or "").upper()
            rec = {
                "symbol": sym,
                "asset_profile": "stock",
                "run_id": run_id,
                "created_utc": meta.get("created_utc"),
                "timeframe_status": meta.get("timeframe_status"),
                "limited": meta.get("limited"),
                "armed": bool(meta.get("armed")),
                "armed_mode": meta.get("armed_mode"),
                "models": meta.get("models"),
                "datasets": meta.get("datasets"),
                "gates": meta.get("gates"),
            }
            _append_ndjson(nd, rec)

    # Validate invariants
    validate_pkl_outputs(out_root, symbols)

    print(json.dumps({"run_id": run_id, "total": summary["total"], "armed": summary["armed"], "counts": summary["counts"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
