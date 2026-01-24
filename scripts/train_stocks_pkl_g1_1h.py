#!/usr/bin/env python3
"""Train + package STOCKS tradeable PKLs for G1 1H (recheck gate), PASS-only.

This script is intentionally narrow:
- Input is a symbol list (typically the G1 recheck PASS list).
- Trains/evaluates on the 1H parquet for each symbol.
- Writes a tradeable PKL ONLY when the 1H gate PASSes.

Outputs under out_root:
- 1H/<SYMBOL>.pkl (PASS-only)
- meta/<SYMBOL>.meta.json
- meta/<SYMBOL>.audit.jsonl

Run reports:
- reports/training_g1_1h/<run_id>/summary.json
- reports/training_g1_1h/<run_id>/g1_1h.ndjson (optional)

Notes:
- Uses the base symbol (not suffixed) to match the original g1 recheck behavior.
- Disables pipeline recent-pass short-circuiting (skip_window_days=0) for determinism.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from octa_training.core.config import TrainingConfig, load_config
from octa_training.core.io_parquet import load_parquet
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry

ALLOWED_PASS_STATUSES = {"PASS_FULL", "PASS_LIMITED_STATISTICAL_CONFIDENCE"}


def _json_sanitize(obj: Any) -> Any:
    """Convert common non-JSON-native scalars/containers to plain Python types."""
    try:
        import numpy as np  # type: ignore

        if isinstance(obj, np.generic):
            return obj.item()
    except Exception:
        pass

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


def _read_symbols(path: Path) -> List[str]:
    rows = []
    for ln in path.read_text(encoding="utf-8").splitlines():
        s = _norm_symbol(ln)
        if s:
            rows.append(s)
    out: List[str] = []
    seen = set()
    for s in rows:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


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


def _append_jsonl(path: Path, rec: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, default=str) + "\n")
        f.flush()


def _gate_fields(gate_obj: Any) -> Dict[str, Any]:
    if gate_obj is None:
        return {
            "passed": False,
            "status": "ERROR",
            "gate_version": None,
            "reasons": ["missing_gate"],
            "passed_checks": [],
            "insufficient_evidence": [],
        }
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
        "robustness": getattr(gate_obj, "robustness", None),
        "diagnostics": getattr(gate_obj, "diagnostics", None),
    }


def _tf_passed(gate: Dict[str, Any]) -> Tuple[bool, bool]:
    status = str(gate.get("status") or "").upper()
    passed = bool(gate.get("passed"))
    if status in ALLOWED_PASS_STATUSES:
        return True, status == "PASS_LIMITED_STATISTICAL_CONFIDENCE"
    return passed and status.startswith("PASS"), False


def _stage_cfg_for_run(cfg: TrainingConfig, pkl_dir: Path) -> TrainingConfig:
    base = cfg.model_dump() if hasattr(cfg, "model_dump") else (cfg.dict() if hasattr(cfg, "dict") else {})
    base.setdefault("paths", {})
    base["paths"]["pkl_dir"] = str(pkl_dir)

    # Disable recent-pass short-circuit for determinism.
    base.setdefault("retrain", {})
    base["retrain"]["skip_window_days"] = 0

    # Keep strict fail-closed packaging behavior (PASS-only tradeable artifacts).
    try:
        base.setdefault("packaging", {})
        base["packaging"]["save_debug_on_fail"] = False
    except Exception:
        pass

    return TrainingConfig(**base)


@dataclass
class Outcome:
    symbol: str
    status: str  # PASS/FAIL/ERROR
    limited: bool
    gate: Dict[str, Any]
    parquet_path: str
    parquet_sha256: str
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


def train_one_symbol_1h(
    *,
    symbol: str,
    cfg: TrainingConfig,
    state_dir: Path,
    run_id: str,
    parquet_root: Path,
    out_root: Path,
    gate_version_expected: Optional[str],
    reports_dir: Path,
) -> Dict[str, Any]:
    sym = _norm_symbol(symbol)

    audit_path = out_root / "meta" / f"{sym}.audit.jsonl"
    meta_path = out_root / "meta" / f"{sym}.meta.json"

    out_1h_dir = out_root / "1H"
    out_1h_dir.mkdir(parents=True, exist_ok=True)

    state = StateRegistry(str(state_dir))

    _append_jsonl(audit_path, {"type": "START", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "asset_profile": "stock", "tf": "1H"})

    p1h = parquet_root / f"{sym}_1H.parquet"
    if not p1h.exists():
        meta = {
            "symbol": sym,
            "asset_profile": "stock",
            "run_id": run_id,
            "created_utc": _utc_now(),
            "gate_version": gate_version_expected,
            "timeframe_status": {"1H": "ERROR"},
            "limited": {"1H": False},
            "datasets": {"1H": None},
            "models": {"1H": None},
            "gates": {"1H": {"passed": False, "status": "ERROR", "gate_version": gate_version_expected, "reasons": ["missing_parquet"], "passed_checks": [], "insufficient_evidence": []}},
        }
        _atomic_write_text(meta_path, json.dumps(_json_sanitize(meta), indent=2) + "\n")
        _append_jsonl(audit_path, {"type": "LOAD", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "ok": False, "reason": "missing_parquet", "parquet_path": str(p1h)})
        return meta

    parquet_sha = _sha256_file(p1h)
    _append_jsonl(audit_path, {"type": "LOAD", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "ok": True, "parquet_path": str(p1h), "parquet_sha256": parquet_sha})

    try:
        df = load_parquet(p1h)
        rows, start, end = _dataset_fingerprint(df)
    except Exception as e:
        rows, start, end = None, None, None
        _append_jsonl(audit_path, {"type": "LOAD", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "ok": False, "reason": f"read_error:{str(e).splitlines()[0]}"})

    staging_dir = out_root / "_staging" / run_id / sym / "1H"
    cfg_1h = _stage_cfg_for_run(cfg, staging_dir)

    _append_jsonl(audit_path, {"type": "TRAIN_START", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H"})
    res = train_evaluate_package(sym, cfg_1h, state, run_id, safe_mode=False, smoke_test=False, parquet_path=str(p1h))
    _append_jsonl(audit_path, {"type": "TRAIN_END", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "passed": bool(getattr(res, "passed", False)), "error": getattr(res, "error", None)})

    gate = _gate_fields(getattr(res, "gate_result", None))
    if gate_version_expected and gate.get("gate_version") and str(gate.get("gate_version")) != str(gate_version_expected):
        gate = {**gate, "passed": False, "status": "ERROR", "reasons": list(gate.get("reasons") or []) + [f"gate_version_mismatch:{gate.get('gate_version')}!={gate_version_expected}"]}

    passed, limited = _tf_passed(gate)
    pack = getattr(res, "pack_result", None) or {}
    pkl_src = pack.get("pkl") if isinstance(pack, dict) else None

    out_pkl = out_1h_dir / f"{sym}.pkl"
    pkl_sha = None

    if passed:
        if not pkl_src or not Path(pkl_src).exists():
            passed = False
            gate = {**gate, "passed": False, "status": "ERROR", "reasons": list(gate.get("reasons") or []) + ["packaging_missing_pkl"]}
        else:
            shutil.copy2(pkl_src, out_pkl)
            pkl_sha = _sha256_file(out_pkl)
            _append_jsonl(audit_path, {"type": "WRITE_PKL", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "tf": "1H", "pkl": str(out_pkl), "pkl_sha256": pkl_sha})
    else:
        if out_pkl.exists():
            out_pkl.unlink()

    status = "PASS" if passed else ("ERROR" if getattr(res, "error", None) else "FAIL")

    meta = {
        "symbol": sym,
        "asset_profile": "stock",
        "run_id": run_id,
        "created_utc": _utc_now(),
        "gate_version": gate_version_expected,
        "timeframe_status": {"1H": status},
        "limited": {"1H": bool(limited)},
        "datasets": {"1H": {"path": str(p1h), "sha256": parquet_sha, "rows": rows, "start": start, "end": end}},
        "models": {"1H": None if not passed else {"pkl": str(out_pkl), "sha256": pkl_sha}},
        "gates": {"1H": gate},
    }

    _atomic_write_text(meta_path, json.dumps(_json_sanitize(meta), indent=2) + "\n")
    _append_jsonl(audit_path, {"type": "WRITE_META", "ts_utc": _utc_now(), "run_id": run_id, "symbol": sym, "ok": True, "meta": str(meta_path)})

    # Per-symbol report (for grepping)
    try:
        (reports_dir / f"{sym}.result.json").write_text(json.dumps(_json_sanitize(meta), indent=2) + "\n")
    except Exception:
        pass

    return meta


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--symbols-file", default="reports/stocks_g1_recheck_passed28.txt")
    ap.add_argument("--parquet-root", default="raw/Stock_parquet")
    ap.add_argument("--out-root", default="/home/n-b/Octa/raw/PKL/stocks_g1")
    # Important: default to the project TrainingConfig (octa_training/config/training.yaml).
    # Using configs/dev.yaml changes horizons/splits defaults and can invalidate a g1_recheck PASS list.
    ap.add_argument("--config", default="octa_training/config/training.yaml")
    ap.add_argument("--gate-version", default="", help="If set, enforce gate_version match")
    ap.add_argument("--write-ndjson", action="store_true")
    args = ap.parse_args()

    run_id = str(args.run_id)
    symbols = _read_symbols(Path(args.symbols_file))
    parquet_root = Path(args.parquet_root)
    out_root = Path(args.out_root)

    reports_dir = Path("reports") / "training_g1_1h" / run_id
    reports_dir.mkdir(parents=True, exist_ok=True)

    cfg = load_config(args.config) if args.config else load_config("octa_training/config/training.yaml")
    state_dir = Path(cfg.paths.state_dir)

    gate_version_expected = str(args.gate_version).strip() or None

    results: List[dict] = []
    interrupted = False
    try:
        for sym in symbols:
            results.append(
                train_one_symbol_1h(
                    symbol=sym,
                    cfg=cfg,
                    state_dir=state_dir,
                    run_id=run_id,
                    parquet_root=parquet_root,
                    out_root=out_root,
                    gate_version_expected=gate_version_expected,
                    reports_dir=reports_dir,
                )
            )
    except KeyboardInterrupt:
        interrupted = True

    def st(meta: dict) -> str:
        try:
            return str((meta.get("timeframe_status") or {}).get("1H") or "").upper()
        except Exception:
            return ""

    summary = {
        "run_id": run_id,
        "created_utc": _utc_now(),
        "total": len(symbols),
        "processed": len(results),
        "interrupted": bool(interrupted),
        "counts": {"PASS": 0, "FAIL": 0, "ERROR": 0},
        "pass_symbols": [],
    }

    for meta in results:
        sym = str(meta.get("symbol") or "").upper()
        s1h = st(meta)
        if s1h in summary["counts"]:
            summary["counts"][s1h] += 1
        else:
            summary["counts"]["ERROR"] += 1
        if s1h == "PASS":
            summary["pass_symbols"].append(sym)


    (reports_dir / "summary.json").write_text(json.dumps(_json_sanitize(summary), indent=2) + "\n")

    if bool(getattr(args, "write_ndjson", False)):
        nd = reports_dir / "g1_1h.ndjson"
        for meta in results:
            _append_jsonl(nd, meta)

    print(json.dumps({"run_id": run_id, "total": summary["total"], "processed": summary["processed"], "interrupted": summary["interrupted"], "counts": summary["counts"]}, indent=2))
    return 130 if interrupted else 0


if __name__ == "__main__":
    raise SystemExit(main())
