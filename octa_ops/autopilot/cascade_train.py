from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from octa.core.data.quality.series_validator import validate_price_series
from octa_training.core.io_parquet import load_parquet
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


def _walkforward_resolver(cfg: Any) -> Dict[str, Any]:
    splits_cfg = getattr(cfg, "splits", {}) if hasattr(cfg, "splits") else {}
    n_folds = int(splits_cfg.get("n_folds", 5))
    train_window = int(splits_cfg.get("train_window", 1000))
    test_window = int(splits_cfg.get("test_window", 200))
    step = int(splits_cfg.get("step", 200))
    purge_size = int(splits_cfg.get("purge_size", 10))
    embargo_size = int(splits_cfg.get("embargo_size", 5))
    min_train_size = int(splits_cfg.get("min_train_size", 500))
    min_test_size = int(splits_cfg.get("min_test_size", 100))
    min_folds_required = int(splits_cfg.get("min_folds_required", 1))
    expanding = bool(splits_cfg.get("expanding", True))
    fallback_min_train = max(100, max(1, min_train_size // 2))
    fallback_min_test = max(30, max(1, min_test_size // 2))
    return {
        "n_folds": n_folds,
        "train_window": train_window,
        "test_window": test_window,
        "step": step,
        "purge_size": purge_size,
        "embargo_size": embargo_size,
        "min_train_size": min_train_size,
        "min_test_size": min_test_size,
        "min_folds_required": min_folds_required,
        "expanding": expanding,
        "fallback_min_train_size": fallback_min_train,
        "fallback_min_test_size": fallback_min_test,
        "required_bars_strict": max(train_window + test_window, min_train_size + min_test_size),
        "required_bars_fallback": fallback_min_train + fallback_min_test,
    }


def _parquet_num_rows(path: str) -> Optional[int]:
    try:
        import pyarrow.parquet as pq  # type: ignore

        pf = pq.ParquetFile(str(path))
        return int(pf.metadata.num_rows)
    except Exception:
        return None


def _check_futures_parquet_columns(df: Any, *, parquet_path: Optional[str] = None) -> Optional[str]:
    """Return an error reason string if required futures columns are missing, else None.

    Reads column names from the raw parquet file metadata when parquet_path is provided,
    because load_parquet strips non-OHLCV columns before returning the DataFrame.
    Falls back to checking df.columns if pyarrow metadata read fails.
    """
    # Primary: read raw schema from file (load_parquet strips extra columns)
    if parquet_path is not None:
        try:
            import pyarrow.parquet as _pq
            raw_cols = set(str(c).lower() for c in _pq.ParquetFile(str(parquet_path)).schema_arrow.names)
            if "roll_flag" not in raw_cols:
                return "missing_required_futures_columns:roll_flag"
            # Validate roll_flag not all-NaN by reading just that column
            roll_series = _pq.read_table(str(parquet_path), columns=["roll_flag"]).to_pandas()["roll_flag"]
            if hasattr(roll_series, "isna") and bool(roll_series.isna().all()):
                return "roll_flag_all_nan"
            return None
        except Exception:
            pass
    # Fallback: check the loaded DataFrame (may be stripped of roll_flag)
    try:
        cols = set(str(c).lower() for c in df.columns)
        if "roll_flag" not in cols:
            return "missing_required_futures_columns:roll_flag"
        rf = df["roll_flag"] if "roll_flag" in df.columns else df.get("roll_flag")
        if rf is not None:
            try:
                if hasattr(rf, "isna") and bool(rf.isna().all()):
                    return "roll_flag_all_nan"
            except Exception:
                pass
    except Exception:
        pass
    return None


def _walkforward_eligibility(*, parquet_path: str, cfg: Any, asset_class: str = "unknown") -> Dict[str, Any]:
    resolved = _walkforward_resolver(cfg)
    available_bars = None
    data_invalid_reason = None
    data_health_stats = None
    try:
        df = load_parquet(
            Path(parquet_path),
            nan_threshold=getattr(getattr(cfg, "parquet", {}), "nan_threshold", 0.2),
            allow_negative_prices=getattr(getattr(cfg, "parquet", {}), "allow_negative_prices", False),
            resample_enabled=getattr(getattr(cfg, "parquet", {}), "resample_enabled", False),
            resample_bar_size=getattr(getattr(cfg, "parquet", {}), "resample_bar_size", "1D"),
        )
        # Futures: enforce roll_flag column presence
        if str(asset_class).lower() in {"future", "futures"}:
            col_err = _check_futures_parquet_columns(df, parquet_path=parquet_path)
            if col_err:
                return {
                    "eligible": False,
                    "reason": col_err,
                    "available_bars": 0,
                    "required_bars": int(resolved["required_bars_fallback"]),
                    "resolver": resolved,
                    "proof": "futures_schema_check",
                    "asset_class": str(asset_class),
                }
        health = validate_price_series(df, close_col="close")
        data_health_stats = dict(health.stats)
        if not bool(health.ok):
            data_invalid_reason = f"DATA_INVALID:{health.code}"
            available_bars = int(health.stats.get("rows_clean", 0) or 0)
        else:
            available_bars = int(health.stats.get("rows_clean", len(df)) or len(df))
    except Exception:
        available_bars = _parquet_num_rows(parquet_path)
    if data_invalid_reason:
        return {
            "eligible": False,
            "reason": data_invalid_reason,
            "available_bars": int(available_bars or 0),
            "required_bars": int(resolved["required_bars_fallback"]),
            "resolver": resolved,
            "proof": "data_health_validator",
            "data_health": data_health_stats,
        }
    if available_bars is None:
        return {
            "eligible": True,
            "reason": None,
            "available_bars": None,
            "required_bars": int(resolved["required_bars_fallback"]),
            "resolver": resolved,
            "proof": "parquet_row_count_unavailable",
        }
    required = int(resolved["required_bars_fallback"])
    eligible = int(available_bars) >= required
    return {
        "eligible": bool(eligible),
        "reason": None if eligible else "insufficient_history_for_walkforward",
        "available_bars": int(available_bars),
        "required_bars": required,
        "resolver": resolved,
        "proof": "cleaned_bars_count" if data_health_stats is not None else "parquet_metadata_row_count",
        "data_health": data_health_stats,
    }


def _stable_sha256_obj(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _is_structural_failure(*, reason: Optional[str], error_text: Optional[str], gate_dump: Optional[Dict[str, Any]]) -> bool:
    blob_parts: list[str] = []
    if reason:
        blob_parts.append(str(reason))
    if error_text:
        blob_parts.append(str(error_text))
    if isinstance(gate_dump, dict):
        rr = gate_dump.get("reasons")
        if isinstance(rr, list):
            blob_parts.extend([str(x) for x in rr if x is not None])
    blob = " | ".join(blob_parts).lower()
    structural_markers = (
        "train_error",
        "train_exception",
        "internal_exception",
        "missing_parquet",
        "no_parquet",
        "insufficient_history",
        "empty_after_filters",
        "data_invalid",
        "data_load_failed",
    )
    return any(marker in blob for marker in structural_markers)


def _trace_emit(trace_dir: Optional[str], payload: Dict[str, Any]) -> None:
    if not trace_dir:
        return
    p = Path(str(trace_dir)) / "train_step_progress.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def _trace_write_json(trace_dir: Optional[str], name: str, payload: Dict[str, Any]) -> None:
    if not trace_dir:
        return
    p = Path(str(trace_dir)) / str(name)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str), encoding="utf-8")


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
    config_overrides: Optional[Dict[str, Any]] = None,
    trace_dir: Optional[str] = None,
    underlying_performance_pass: Optional[bool] = None,
) -> Tuple[List[GateDecision], Dict[str, Any]]:
    """Run cascade training for one symbol.

    underlying_performance_pass:
        For asset_class="option" symbols, the caller must set this to
        True/False to indicate whether the base asset's 1D stage passed.
        When False, all cascade stages are immediately skipped with
        reason="underlying_cascade_not_passed" (spec §3.1 hard requirement).
        When None (default), the check is skipped (backward compatible).
    """
    # --- Options base-asset cascade dependency check (spec §3.1) ---
    if str(asset_class).lower() in {"option", "options"} and underlying_performance_pass is False:
        normalized_order = [normalize_timeframe(t) for t in cascade.order]
        skip_decisions = [
            GateDecision(
                symbol=symbol,
                timeframe=tf,
                stage="train",
                status="SKIP",
                reason="underlying_cascade_not_passed",
                details={
                    "structural_pass": False,
                    "performance_pass": False,
                    "underlying_performance_pass": False,
                },
            )
            for tf in normalized_order
        ]
        return skip_decisions, {}

    root_timer = time.monotonic()
    _trace_emit(trace_dir, {"ts": time.time(), "step": "run_cascade_training", "event": "start", "elapsed_s": 0.0, "symbol": str(symbol)})
    durations: Dict[str, float] = {}
    cfg_start = time.monotonic()
    cfg = load_config(config_path)
    durations["load_config"] = float(time.monotonic() - cfg_start)
    _trace_emit(trace_dir, {"ts": time.time(), "step": "load_config", "event": "end", "elapsed_s": float(time.monotonic() - root_timer), "duration_s": float(durations["load_config"])})
    if config_overrides:
        try:
            merged = cfg.dict() if hasattr(cfg, "dict") else {}
            if isinstance(merged, dict):
                _deep_merge_dict(merged, dict(config_overrides))
                cfg = load_config(config_path, override=merged)  # type: ignore[arg-type]
        except TypeError:
            # Backward-compatible path when load_config does not support override argument.
            try:
                merged = cfg.dict() if hasattr(cfg, "dict") else {}
                if isinstance(merged, dict):
                    _deep_merge_dict(merged, dict(config_overrides))
                    cfg = type(cfg)(**merged)
            except Exception:
                pass
        except Exception:
            pass
    base_pkl_root = Path(getattr(cfg.paths, "pkl_dir", "pkl"))
    base_state_root = Path(getattr(cfg.paths, "state_dir", "state")) if getattr(cfg, "paths", None) else Path("state")

    decisions: List[GateDecision] = []
    metrics_by_tf: Dict[str, Any] = {}

    prev_structural_pass = True
    normalized_order = [normalize_timeframe(t) for t in cascade.order]
    for idx, tf in enumerate(normalized_order):
        stage_timer = time.monotonic()
        _trace_emit(trace_dir, {"ts": time.time(), "step": "timeframe_stage", "event": "start", "elapsed_s": float(time.monotonic() - root_timer), "timeframe": str(tf)})
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
        upstream_tf = normalized_order[idx - 1] if idx > 0 else None
        if not prev_structural_pass:
            upstream_decision = None
            for dd in reversed(decisions):
                if dd.symbol == symbol and dd.stage == "train":
                    upstream_decision = dd
                    break
            upstream_metrics = metrics_by_tf.get(upstream_tf) if upstream_tf else None
            upstream_structural_pass = None
            upstream_performance_pass = None
            if isinstance(upstream_decision.details, dict):
                upstream_structural_pass = upstream_decision.details.get("structural_pass")
                upstream_performance_pass = upstream_decision.details.get("performance_pass")
            details = {
                "expected_upstream_timeframe": upstream_tf,
                "upstream_stage_status": str(upstream_decision.status) if upstream_decision else "not_available",
                "upstream_stage_reason": str(upstream_decision.reason) if upstream_decision and upstream_decision.reason else None,
                "upstream_artifact_present": bool(upstream_metrics is not None),
                "upstream_artifact_hash": _stable_sha256_obj(upstream_metrics) if upstream_metrics is not None else None,
                "upstream_structural_pass": upstream_structural_pass,
                "upstream_performance_pass": upstream_performance_pass,
                "structural_pass": False,
                "performance_pass": False,
            }
            decisions.append(
                GateDecision(
                    symbol=symbol,
                    timeframe=tf,
                    stage="train",
                    status="SKIP",
                    reason="cascade_previous_not_structural_pass",
                    details=details,
                )
            )
            continue
        if not pq:
            decisions.append(
                GateDecision(
                    symbol=symbol,
                    timeframe=tf,
                    stage="train",
                    status="SKIP",
                    reason="missing_parquet",
                    details={"structural_pass": False, "performance_pass": False},
                )
            )
            prev_structural_pass = False
            continue
        wf_elig = _walkforward_eligibility(parquet_path=str(pq), cfg=cfg_layer, asset_class=asset_class)
        durations[f"{tf}.walkforward_eligibility"] = float(time.monotonic() - stage_timer)
        _trace_emit(
            trace_dir,
            {
                "ts": time.time(),
                "step": "walkforward_eligibility",
                "event": "end",
                "elapsed_s": float(time.monotonic() - root_timer),
                "timeframe": str(tf),
                "eligible": bool(wf_elig.get("eligible", False)),
                "available_bars": wf_elig.get("available_bars"),
                "required_bars": wf_elig.get("required_bars"),
                "duration_s": float(durations[f"{tf}.walkforward_eligibility"]),
            },
        )
        if not bool(wf_elig.get("eligible", True)):
            metrics_by_tf[tf] = {
                "walk_forward": {
                    "enabled": True,
                    "passed": False,
                    "reason": "insufficient_history_for_walkforward",
                    "walkforward_meta": {
                        "history_bars": wf_elig.get("available_bars"),
                        "required_bars_for_fallback": wf_elig.get("required_bars"),
                        "resolver": wf_elig.get("resolver"),
                        "proof": wf_elig.get("proof"),
                    },
                },
                "parquet_path": str(pq),
                "asset_class": str(asset_class),
                "eligibility": wf_elig,
            }
            decisions.append(
                GateDecision(
                    symbol=symbol,
                    timeframe=tf,
                    stage="train",
                    status="SKIP",
                    reason="insufficient_history_for_walkforward",
                    details={**wf_elig, "structural_pass": False, "performance_pass": False},
                )
            )
            prev_structural_pass = False
            continue

        try:
            gate_overrides = None
            try:
                if isinstance(config_overrides, dict):
                    g_over = config_overrides.get("gates", {})
                    if isinstance(g_over, dict):
                        g_global = g_over.get("global", {})
                        g_tf_map = g_over.get("global_by_timeframe", {})
                        gate_overrides = {}
                        if isinstance(g_global, dict):
                            for k, v in g_global.items():
                                if v is not None:
                                    gate_overrides[str(k)] = v
                        if isinstance(g_tf_map, dict):
                            tf_spec = g_tf_map.get(tf, {})
                            if isinstance(tf_spec, dict):
                                for k, v in tf_spec.items():
                                    if v is not None:
                                        gate_overrides[str(k)] = v
                        if not gate_overrides:
                            gate_overrides = None
            except Exception:
                gate_overrides = None
            train_start = time.monotonic()
            _trace_emit(trace_dir, {"ts": time.time(), "step": "train_evaluate_package", "event": "start", "elapsed_s": float(time.monotonic() - root_timer), "timeframe": str(tf)})
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
                gate_overrides=gate_overrides,
            )
            durations[f"{tf}.train_evaluate_package"] = float(time.monotonic() - train_start)
            _trace_emit(
                trace_dir,
                {
                    "ts": time.time(),
                    "step": "train_evaluate_package",
                    "event": "end",
                    "elapsed_s": float(time.monotonic() - root_timer),
                    "timeframe": str(tf),
                    "duration_s": float(durations[f"{tf}.train_evaluate_package"]),
                },
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
            structural_pass = bool(passed)
            performance_pass = bool(passed)
            if not passed:
                err_text = str(getattr(res, "error", "") or "")
                is_exception = bool("Traceback (most recent call last)" in err_text)
                if is_exception:
                    fail_status = "TRAIN_ERROR"
                    fail_reason = "train_error"
                    structural_pass = False
                    performance_pass = False
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
                    structural_pass = not _is_structural_failure(
                        reason=fail_reason,
                        error_text=err_text,
                        gate_dump=gate_dump if isinstance(gate_dump, dict) else None,
                    )
                    performance_pass = False
            decisions.append(
                GateDecision(
                    symbol=symbol,
                    timeframe=tf,
                    stage="train",
                    status=fail_status,
                    reason=None if passed else fail_reason,
                    details={
                        "gate": gate_dump,
                        "error": getattr(res, "error", None),
                        "leakage_audit": leakage_audit,
                        "structural_pass": bool(structural_pass),
                        "performance_pass": bool(performance_pass),
                    },
                )
            )
            _trace_emit(
                trace_dir,
                {
                    "ts": time.time(),
                    "step": "decision_assembly",
                    "event": "end",
                    "elapsed_s": float(time.monotonic() - root_timer),
                    "timeframe": str(tf),
                    "status": str(fail_status),
                    "passed": bool(passed),
                },
            )
            metrics_by_tf[tf]["structural_pass"] = bool(structural_pass)
            metrics_by_tf[tf]["performance_pass"] = bool(performance_pass)
            prev_structural_pass = bool(structural_pass)
        except Exception as e:
            _trace_write_json(
                trace_dir,
                "exception.json",
                {
                    "step": "train_evaluate_package",
                    "timeframe": str(tf),
                    "error": str(e),
                },
            )
            _trace_emit(trace_dir, {"ts": time.time(), "step": "train_evaluate_package", "event": "error", "elapsed_s": float(time.monotonic() - root_timer), "timeframe": str(tf), "error": str(e)})
            decisions.append(
                GateDecision(
                    symbol=symbol,
                    timeframe=tf,
                    stage="train",
                    status="TRAIN_ERROR",
                    reason="train_exception",
                    details={"error": str(e), "structural_pass": False, "performance_pass": False},
                )
            )
            prev_structural_pass = False
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
    _trace_write_json(trace_dir, "train_step_durations.json", durations)
    _trace_emit(trace_dir, {"ts": time.time(), "step": "run_cascade_training", "event": "end", "elapsed_s": float(time.monotonic() - root_timer), "symbol": str(symbol)})

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


def _deep_merge_dict(base: Dict[str, Any], overlay: Dict[str, Any]) -> None:
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge_dict(base[k], v)
        else:
            base[k] = v
