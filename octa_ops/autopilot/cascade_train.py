from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from octa_training.core.config import load_config
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry

from .types import GateDecision, normalize_timeframe


@dataclass
class CascadePolicy:
    order: List[str]


def _find_parquet_for_tf(parquet_paths: Dict[str, str], tf: str) -> Optional[str]:
    tf = normalize_timeframe(tf)
    if not parquet_paths:
        return None
    # common normalization (30m stored as 30M, etc)
    return parquet_paths.get(tf)


def run_cascade_training(
    *,
    run_id: str,
    config_path: str,
    symbol: str,
    asset_class: str,
    parquet_paths: Dict[str, str],
    cascade: CascadePolicy,
    safe_mode: bool,
    reports_dir: str,
) -> Tuple[List[GateDecision], Dict[str, Any]]:
    cfg = load_config(config_path)
    state = StateRegistry(str(Path(cfg.paths.state_dir) / "state.db"))

    decisions: List[GateDecision] = []
    metrics_by_tf: Dict[str, Any] = {}

    prev_pass = True
    for tf in [normalize_timeframe(t) for t in cascade.order]:
        # IMPORTANT: packaging writes <pkl_dir>/<symbol>.pkl. To support multi-timeframe
        # cascades without overwriting, we stage a per-timeframe PKL directory.
        try:
            cfg_layer = cfg.copy(deep=True)
        except Exception:
            cfg_layer = cfg
        try:
            pkl_root = Path(getattr(cfg_layer.paths, "pkl_dir", cfg.paths.pkl_dir))
            # Structure: <pkl_root>/<asset_class>/<tf>/<SYMBOL>.pkl
            tf_pkl_dir = pkl_root / str(asset_class) / str(tf)
            tf_pkl_dir.mkdir(parents=True, exist_ok=True)
            cfg_layer.paths.pkl_dir = tf_pkl_dir
        except Exception:
            # Fail-closed behavior is handled by the caller (no promotion without PKL files).
            cfg_layer = cfg_layer

        pq = _find_parquet_for_tf(parquet_paths, tf)
        if not prev_pass:
            decisions.append(GateDecision(symbol=symbol, timeframe=tf, stage="train", status="SKIP", reason="cascade_previous_not_pass"))
            continue
        if not pq:
            decisions.append(GateDecision(symbol=symbol, timeframe=tf, stage="train", status="SKIP", reason="missing_parquet"))
            prev_pass = False
            continue

        try:
            res = train_evaluate_package(
                symbol=symbol,
                cfg=cfg_layer,
                state=state,
                run_id=run_id,
                safe_mode=bool(safe_mode),
                smoke_test=False,
                parquet_path=str(pq),
                dataset=asset_class,
            )
            passed = bool(getattr(res, "passed", False))
            gate_obj = getattr(res, "gate_result", None)
            metrics_obj = getattr(res, "metrics", None)
            gate_dump = gate_obj.model_dump() if hasattr(gate_obj, "model_dump") else (gate_obj.dict() if hasattr(gate_obj, "dict") else None)
            metrics_dump = metrics_obj.model_dump() if hasattr(metrics_obj, "model_dump") else (metrics_obj.dict() if hasattr(metrics_obj, "dict") else None)
            metrics_by_tf[tf] = {
                "gate": gate_dump,
                "metrics": metrics_dump,
                "pack": getattr(res, "pack_result", None),
                "parquet_path": str(pq),
                "pkl_dir": str(getattr(getattr(cfg_layer, "paths", None), "pkl_dir", "")),
            }
            decisions.append(GateDecision(symbol=symbol, timeframe=tf, stage="train", status="PASS" if passed else "FAIL", reason=None if passed else "gate_failed", details={"gate": gate_dump}))
            prev_pass = passed
        except Exception as e:
            decisions.append(GateDecision(symbol=symbol, timeframe=tf, stage="train", status="FAIL", reason="train_exception", details={"error": str(e)}))
            prev_pass = False

    # write per-symbol metrics bundle
    out_dir = Path(reports_dir) / "autopilot" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"model_metrics_{symbol}.json"
    p.write_text(json.dumps(metrics_by_tf, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    return decisions, metrics_by_tf


def write_gate_matrix(*, run_dir: str, decisions: List[GateDecision], cascade_order: List[str]) -> str:
    out_dir = Path(run_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows: Dict[str, Dict[str, str]] = {}
    for d in decisions:
        if d.stage != "train":
            continue
        rows.setdefault(d.symbol, {})[normalize_timeframe(d.timeframe)] = d.status

    cols = [normalize_timeframe(t) for t in cascade_order]
    mat = []
    for sym in sorted(rows.keys()):
        r = {"symbol": sym}
        for tf in cols:
            r[tf] = rows[sym].get(tf, "")
        mat.append(r)
    df = pd.DataFrame(mat)
    p = out_dir / "gate_matrix.csv"
    df.to_csv(p, index=False)
    return str(p)
