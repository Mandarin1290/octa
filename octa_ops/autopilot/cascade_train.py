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
    model_root: Optional[str] = None,
) -> Tuple[List[GateDecision], Dict[str, Any]]:
    cfg = load_config(config_path)
    base_pkl_root = Path(getattr(cfg.paths, "pkl_dir", "pkl"))
    base_state_root = Path(getattr(cfg.paths, "state_dir", "state")) if getattr(cfg, "paths", None) else Path("state")

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
        stage_state = None
        stage_state_dir = None
        orig_pkl_dir = None
        orig_state_dir = None
        try:
            if model_root:
                pkl_root = Path(model_root)
            else:
                pkl_root = Path(getattr(cfg_layer.paths, "pkl_dir", base_pkl_root))
            # Structure: <pkl_root>/<asset_class>/<tf>/<SYMBOL>.pkl
            tf_pkl_dir = pkl_root / str(asset_class) / str(tf)
            tf_pkl_dir.mkdir(parents=True, exist_ok=True)
            orig_pkl_dir = getattr(cfg_layer.paths, "pkl_dir", None)
            cfg_layer.paths.pkl_dir = tf_pkl_dir

            # Scope state dir per asset_class/timeframe to avoid collisions.
            stage_state_dir = base_state_root / str(asset_class) / str(tf)
            stage_state_dir.mkdir(parents=True, exist_ok=True)
            orig_state_dir = getattr(cfg_layer.paths, "state_dir", None)
            cfg_layer.paths.state_dir = stage_state_dir
            stage_state = StateRegistry(str(stage_state_dir / "state.db"))
        except Exception:
            # Fail-closed behavior is handled by the caller (no promotion without PKL files).
            stage_state = None

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
                state=stage_state if stage_state is not None else StateRegistry(str(base_state_root / "state.db")),
                run_id=run_id,
                safe_mode=bool(safe_mode),
                smoke_test=False,
                parquet_path=str(pq),
                dataset=asset_class,
                asset_class=asset_class,
            )
            passed = bool(getattr(res, "passed", False))
            gate_obj = getattr(res, "gate_result", None)
            metrics_obj = getattr(res, "metrics", None)
            gate_dump = gate_obj.model_dump() if hasattr(gate_obj, "model_dump") else (gate_obj.dict() if hasattr(gate_obj, "dict") else None)
            metrics_dump = metrics_obj.model_dump() if hasattr(metrics_obj, "model_dump") else (metrics_obj.dict() if hasattr(metrics_obj, "dict") else None)
            pack = getattr(res, "pack_result", None)
            features_used = None
            altdata_sources = None
            model_artifacts = None
            altdata_enabled = None
            training_window = None
            altdata_meta = None
            monte_carlo = None
            walk_forward = None
            regime_stability = None
            cost_stress = None
            liquidity = None
            leakage_audit = None
            if isinstance(pack, dict):
                features_used = pack.get("features_used")
                altdata_sources = pack.get("altdata_sources_used")
                model_artifacts = pack.get("model_artifacts")
                altdata_enabled = pack.get("altdata_enabled")
                training_window = pack.get("training_window")
                altdata_meta = pack.get("altdata_meta")
                leakage_audit = pack.get("leakage_audit")
            try:
                if isinstance(gate_dump, dict):
                    rob = gate_dump.get("robustness")
                    if isinstance(rob, dict):
                        details = rob.get("details") or {}
                        mc = details.get("monte_carlo")
                        if isinstance(mc, dict):
                            monte_carlo = mc
                        wf = details.get("walk_forward")
                        if isinstance(wf, dict):
                            walk_forward = wf
                        rg = details.get("regime_stability")
                        if isinstance(rg, dict):
                            regime_stability = rg
                        cs = details.get("cost_stress")
                        if isinstance(cs, dict):
                            cost_stress = cs
                        liq = details.get("liquidity")
                        if isinstance(liq, dict):
                            liquidity = liq
            except Exception:
                monte_carlo = None
            metrics_by_tf[tf] = {
                "gate": gate_dump,
                "metrics": metrics_dump,
                "pack": pack,
                "features_used": features_used,
                "altdata_sources_used": altdata_sources,
                "altdata_enabled": altdata_enabled,
                "altdata_meta": altdata_meta,
                "model_artifacts": model_artifacts,
                "training_window": training_window,
                "monte_carlo": monte_carlo,
                "walk_forward": walk_forward,
                "regime_stability": regime_stability,
                "cost_stress": cost_stress,
                "liquidity": liquidity,
                "leakage_audit": leakage_audit,
                "parquet_path": str(pq),
                "asset_class": str(asset_class),
                "pkl_dir": str(getattr(getattr(cfg_layer, "paths", None), "pkl_dir", "")),
            }
            fail_reason = None
            fail_status = "PASS"
            if not passed:
                err_text = str(getattr(res, "error", "") or "")
                is_exception = bool("Traceback (most recent call last)" in err_text)
                if is_exception:
                    fail_status = "TRAIN_ERROR"
                    fail_reason = "train_error"
                else:
                    fail_status = "GATE_FAIL"
                    reasons = []
                    if isinstance(gate_dump, dict):
                        rr = gate_dump.get("reasons")
                        if isinstance(rr, list):
                            reasons = [str(x) for x in rr if str(x)]
                    if reasons:
                        fail_reason = reasons[0]
                    elif err_text:
                        fail_reason = err_text
                    else:
                        fail_reason = "gate_failed"
            decisions.append(
                GateDecision(
                    symbol=symbol,
                    timeframe=tf,
                    stage="train",
                    status=fail_status,
                    reason=None if passed else fail_reason,
                    details={"gate": gate_dump, "error": getattr(res, "error", None), "leakage_audit": leakage_audit},
                )
            )
            prev_pass = passed
        except Exception as e:
            decisions.append(GateDecision(symbol=symbol, timeframe=tf, stage="train", status="TRAIN_ERROR", reason="train_exception", details={"error": str(e)}))
            prev_pass = False
        finally:
            if cfg_layer is cfg and orig_pkl_dir is not None:
                try:
                    cfg_layer.paths.pkl_dir = orig_pkl_dir
                except Exception:
                    pass
            if cfg_layer is cfg and orig_state_dir is not None:
                try:
                    cfg_layer.paths.state_dir = orig_state_dir
                except Exception:
                    pass

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
