from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from octa.core.data.quality.series_validator import validate_price_series
from octa.core.governance.governance_audit import (
    GovernanceAudit,
    EVENT_SCOPE_GUARD_FAILED,
    EVENT_SCOPE_GUARD_PASSED,
)
from octa_training.core.io_parquet import load_parquet
from octa_training.core.config import load_config
from octa_training.core.pipeline import (
    train_evaluate_adaptive,
    train_evaluate_package,
    train_regime_ensemble,
    RegimeEnsemble,
)
from octa_training.core.state import StateRegistry

from .autopilot_types import GateDecision, normalize_timeframe


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
        # I3: row-count unknown → FAIL-CLOSED (cannot validate data availability)
        return {
            "eligible": False,
            "reason": "ROWCOUNT_UNKNOWN",
            "available_bars": None,
            "required_bars": int(resolved["required_bars_fallback"]),
            "resolver": resolved,
            "proof": "parquet_row_count_unavailable_fail_closed",
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
        "missing_metrics_fail_closed",
        "no_models",
        "feature_matrix_empty",
    )
    return any(marker in blob for marker in structural_markers)


_cascade_log = logging.getLogger(__name__)


def _write_exclusion_record(
    *,
    run_id: str,
    symbol: str,
    tf: str,
    reason_code: str,
    detail: str,
    evidence_root: Optional[str] = None,
) -> Optional[str]:
    """Write a per-symbol/TF exclusion record under octa/var/evidence/exclusions/.

    Idempotent — overwrites any prior record for the same (symbol, tf, run_id).
    Returns the path written, or None on failure (never raises).
    """
    import datetime

    try:
        base = Path(evidence_root) if evidence_root else Path("octa/var/evidence")
        excl_dir = base / "exclusions"
        excl_dir.mkdir(parents=True, exist_ok=True)
        fname = f"{symbol}_{tf}_{run_id}.json"
        payload = {
            "symbol": symbol,
            "tf": tf,
            "run_id": run_id,
            "reason_code": reason_code,
            "detail": detail,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        p = excl_dir / fname
        p.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return str(p)
    except Exception:
        return None


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


def _load_scope_config(config_path: str) -> Optional[Dict[str, Any]]:
    """Return the ``scope`` section from *config_path* as a plain dict, or ``None`` if absent.

    Raises ``RuntimeError`` with ``reason_code=SCOPE_CONFIG_MISSING`` when the
    YAML file itself cannot be found.  A missing *scope* key is not an error —
    the guards simply become no-ops in that case.
    """
    import yaml  # type: ignore  # pyyaml — always available in this environment

    p = Path(str(config_path))
    if not p.exists():
        raise RuntimeError(
            f"SCOPE_CONFIG_MISSING: config file not found: {config_path!r} "
            f"(reason_code=SCOPE_CONFIG_MISSING)"
        )
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return raw.get("scope")  # None when key is absent — guards are no-ops


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
        True to indicate whether the base asset's 1D stage passed.
        When False or None, all cascade stages are immediately skipped
        (I7: options must be SKIP unless underlying_cascade_passed is True).
        None (default) is now FAIL-CLOSED: options cannot train without an
        explicit underlying_performance_pass=True from the caller.
    """
    ac_lower = str(asset_class).lower()

    # --- I4: Unknown asset class is FAIL-CLOSED ---
    if ac_lower in {"unknown", ""}:
        normalized_order = [normalize_timeframe(t) for t in cascade.order]
        fail_decisions = [
            GateDecision(
                symbol=symbol,
                timeframe=tf,
                stage="train",
                status="GATE_FAIL",
                reason="UNKNOWN_ASSET_CLASS",
                details={
                    "structural_pass": False,
                    "performance_pass": False,
                    "asset_class": str(asset_class),
                },
            )
            for tf in normalized_order
        ]
        return fail_decisions, {}

    # --- I7: Options base-asset cascade dependency check (spec §3.1) ---
    # underlying_performance_pass must be explicitly True; False or None → SKIP
    if ac_lower in {"option", "options"} and underlying_performance_pass is not True:
        normalized_order = [normalize_timeframe(t) for t in cascade.order]
        skip_reason = (
            "underlying_cascade_not_passed"
            if underlying_performance_pass is False
            else "underlying_cascade_pass_not_provided"
        )
        skip_decisions = [
            GateDecision(
                symbol=symbol,
                timeframe=tf,
                stage="train",
                status="SKIP",
                reason=skip_reason,
                details={
                    "structural_pass": False,
                    "performance_pass": False,
                    "underlying_performance_pass": underlying_performance_pass,
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
                cfg = type(cfg)(**merged)
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

    # I1: prev_stage_pass tracks whether the previous stage's performance gate passed.
    # Only a genuine PASS (performance_pass=True) authorises promotion to the next TF.
    prev_stage_pass = True
    normalized_order = [normalize_timeframe(t) for t in cascade.order]

    # ------------------------------------------------------------------ #
    # Scope Guards (S1 + S2): TF and Asset-Class enforcement              #
    #                                                                      #
    # Source of truth: scope.timeframes / scope.asset_classes in the      #
    # YAML at config_path.  Missing scope section → no-op (guards pass).  #
    # Missing YAML file → RuntimeError(SCOPE_CONFIG_MISSING).             #
    # OCTA_SCOPE_OVERRIDE=1 bypasses hard failures with an explicit       #
    # warning — never silent.                                              #
    # ------------------------------------------------------------------ #
    _scope_log = logging.getLogger(__name__)
    _scope_override = os.environ.get("OCTA_SCOPE_OVERRIDE", "").strip() == "1"
    _scope_cfg: Optional[Dict[str, Any]] = _load_scope_config(config_path)
    _gov_scope: Optional[GovernanceAudit] = None
    try:
        _gov_scope = GovernanceAudit(run_id=run_id)
    except Exception:
        pass  # audit chain unavailable — guard logic still runs

    if _scope_cfg is not None:
        _allowed_tfs_raw = _scope_cfg.get("timeframes")
        _allowed_ac_raw = _scope_cfg.get("asset_classes")

        # S1 — Timeframe scope ---------------------------------------------
        if _allowed_tfs_raw is not None:
            _allowed_tfs_norm = {normalize_timeframe(str(t)) for t in _allowed_tfs_raw}
            _violation_tfs = [t for t in normalized_order if t not in _allowed_tfs_norm]
            if _violation_tfs:
                _tf_fail_payload: Dict[str, Any] = {
                    "reason_code": "SCOPE_VIOLATION_TF",
                    "symbol": symbol,
                    "violation_tfs": _violation_tfs,
                    "allowed_tfs": sorted(_allowed_tfs_norm),
                    "cascade_order": list(normalized_order),
                    "config_path": str(config_path),
                }
                try:
                    if _gov_scope is not None:
                        _gov_scope.emit(EVENT_SCOPE_GUARD_FAILED, _tf_fail_payload)
                except Exception:
                    pass
                _tf_err = (
                    f"SCOPE_VIOLATION_TF: cascade contains timeframes {_violation_tfs!r} "
                    f"not in allowed scope {sorted(_allowed_tfs_norm)!r} "
                    f"(reason_code=SCOPE_VIOLATION_TF, config={config_path!r})"
                )
                if _scope_override:
                    _scope_log.warning(
                        "OCTA_SCOPE_OVERRIDE=1 — bypassing SCOPE_VIOLATION_TF "
                        "symbol=%s violation_tfs=%s",
                        symbol,
                        _violation_tfs,
                    )
                else:
                    raise RuntimeError(_tf_err)

        # S2 — Asset-class scope -------------------------------------------
        if _allowed_ac_raw is not None:
            _allowed_ac_norm = {str(a).lower() for a in _allowed_ac_raw}
            # Expand allowed set with canonical aliases so that config "Stock"
            # matches inventory "equities" and vice-versa.
            try:
                from octa.support.ops.universe_preflight import ASSET_CLASS_ALIASES as _AC_ALIASES
                _allowed_ac_canon = {_AC_ALIASES.get(a, a) for a in _allowed_ac_norm}
                _asset_class_lower = str(asset_class).lower()
                _asset_class_canon = _AC_ALIASES.get(_asset_class_lower, _asset_class_lower)
            except Exception:
                _allowed_ac_canon = set()
                _asset_class_lower = str(asset_class).lower()
                _asset_class_canon = _asset_class_lower
            _allowed_ac_all = _allowed_ac_norm | _allowed_ac_canon
            if _asset_class_lower not in _allowed_ac_all and _asset_class_canon not in _allowed_ac_all:
                _ac_fail_payload: Dict[str, Any] = {
                    "reason_code": "SCOPE_VIOLATION_ASSET_CLASS",
                    "symbol": symbol,
                    "asset_class": str(asset_class),
                    "allowed_asset_classes": sorted(_allowed_ac_norm),
                    "config_path": str(config_path),
                }
                try:
                    if _gov_scope is not None:
                        _gov_scope.emit(EVENT_SCOPE_GUARD_FAILED, _ac_fail_payload)
                except Exception:
                    pass
                if _scope_override:
                    _scope_log.warning(
                        "OCTA_SCOPE_OVERRIDE=1 — bypassing SCOPE_VIOLATION_ASSET_CLASS "
                        "symbol=%s asset_class=%s",
                        symbol,
                        asset_class,
                    )
                else:
                    # Asset-class violation: SKIP all TFs with a structured record
                    # (does not abort the whole run — other symbols continue).
                    return [
                        GateDecision(
                            symbol=symbol,
                            timeframe=tf,
                            stage="train",
                            status="SKIP",
                            reason="SCOPE_VIOLATION_ASSET_CLASS",
                            details={
                                "structural_pass": False,
                                "performance_pass": False,
                                "asset_class": str(asset_class),
                                "allowed_asset_classes": sorted(_allowed_ac_norm),
                                "reason_code": "SCOPE_VIOLATION_ASSET_CLASS",
                            },
                        )
                        for tf in normalized_order
                    ], {}

    # All scope guards cleared (or no scope section defined in config).
    try:
        if _gov_scope is not None:
            _gov_scope.emit(
                EVENT_SCOPE_GUARD_PASSED,
                {
                    "symbol": symbol,
                    "asset_class": str(asset_class),
                    "cascade_order": list(normalized_order),
                    "config_path": str(config_path),
                    "scope_enforced": _scope_cfg is not None,
                },
            )
    except Exception:
        pass
    # ------------------------------------------------------------------ #

    for idx, tf in enumerate(normalized_order):
        stage_timer = time.monotonic()
        _trace_emit(trace_dir, {"ts": time.time(), "step": "timeframe_stage", "event": "start", "elapsed_s": float(time.monotonic() - root_timer), "timeframe": str(tf)})
        # IMPORTANT: packaging writes <pkl_dir>/<symbol>.pkl. To support multi-timeframe
        # cascades without overwriting, we stage a per-timeframe PKL directory.
        try:
            cfg_layer = cfg.copy(deep=True)
        except Exception:
            cfg_layer = cfg
        # Apply per-TF cat_params overrides (e.g. fewer iterations for 1H to reduce overfitting)
        try:
            _cat_by_tf = getattr(cfg, 'cat_params_by_timeframe', None) or {}
            if isinstance(_cat_by_tf, dict):
                _tf_cat_spec = (
                    _cat_by_tf.get(str(tf), {})
                    or _cat_by_tf.get(str(tf).upper(), {})
                    or {}
                )
                if _tf_cat_spec:
                    _base_cat = dict(getattr(cfg_layer, 'cat_params', None) or {})
                    cfg_layer.cat_params = {**_base_cat, **_tf_cat_spec}
        except Exception:
            pass
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
            stage_state = StateRegistry(stage_state_dir / "state.db")
        except Exception:
            # Fail-closed behavior is handled by the caller (no promotion without PKL files).
            stage_state = None

        pq = _find_parquet_for_tf(parquet_paths, tf)
        upstream_tf = normalized_order[idx - 1] if idx > 0 else None
        # I1: only a genuine performance PASS on the previous stage authorises promotion
        if not prev_stage_pass:
            upstream_decision = None
            for dd in reversed(decisions):
                if dd.symbol == symbol and dd.stage == "train":
                    upstream_decision = dd
                    break
            upstream_metrics = metrics_by_tf.get(upstream_tf) if upstream_tf else None
            upstream_structural_pass = None
            upstream_performance_pass = None
            if upstream_decision is not None and isinstance(upstream_decision.details, dict):
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
                    reason="cascade_previous_stage_not_passed",
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
            prev_stage_pass = False
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
            prev_stage_pass = False
            continue

        try:
            gate_overrides: Dict[str, Any] | None = None
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

            # v0.0.0 Regime-Ensemble dispatch: when cfg.regime_ensemble.enabled=True,
            # train one CatBoost submodel per regime (bull/bear/crisis) and gate on
            # regimes_trained >= min_regimes_trained.  Downstream code receives a
            # PipelineResult-compatible object (same .passed / .pack_result attrs).
            _re_cfg = getattr(cfg_layer, "regime_ensemble", None)
            if _re_cfg is not None and bool(getattr(_re_cfg, "enabled", False)):
                ensemble: RegimeEnsemble = train_regime_ensemble(
                    symbol=symbol,
                    timeframe=str(tf),
                    cfg=cfg_layer,
                    state=stage_state if stage_state is not None else StateRegistry(base_state_root / "state.db"),
                    run_id=run_id,
                    parquet_path=str(pq),
                    dataset=asset_class,
                    asset_class=asset_class,
                    gov_audit=_gov_scope,
                )
                # Synthesize PipelineResult-compatible attributes from RegimeEnsemble.
                # Critical: downstream code (run_full_cascade_training_from_parquets.py)
                # requires .metrics and .gate_result to validate a passing result.
                # Pick the representative submodel: first passing one (priority order),
                # fallback to first submodel if none pass.
                from octa_training.core.pipeline import PipelineResult as _PR
                _sub_artifacts: list = []
                _sub_features: list = []
                _rep_sub = None
                for _r_key in ["neutral", "bull", "bear", "crisis"]:
                    _s = ensemble.submodels.get(_r_key)
                    if _s is not None and getattr(_s, "passed", False):
                        _rep_sub = _s
                        break
                if _rep_sub is None and ensemble.submodels:
                    _rep_sub = next(iter(ensemble.submodels.values()))
                for _regime_res in ensemble.submodels.values():
                    _sub_pack = getattr(_regime_res, "pack_result", None) or {}
                    _sub_artifacts.extend(_sub_pack.get("model_artifacts") or [])
                    _sub_features.extend(_sub_pack.get("features_used") or [])
                # Propagate pack_result fields from representative submodel so that
                # monte_carlo / walk_forward / regime_stability / cost_stress /
                # liquidity are visible to run_full_cascade validation.
                _rep_pack = (getattr(_rep_sub, "pack_result", None) or {}) if _rep_sub else {}
                _merged_pack = dict(_rep_pack)
                # Include per-regime pkls and router manifest so promotion copies them
                for _art_path in ensemble.regime_artifact_paths.values():
                    _sub_artifacts.append(_art_path)
                    _sha_path = str(Path(_art_path).with_suffix(".sha256"))
                    if Path(_sha_path).exists():
                        _sub_artifacts.append(_sha_path)
                if ensemble.router_path and Path(ensemble.router_path).exists():
                    _sub_artifacts.append(ensemble.router_path)
                _merged_pack["model_artifacts"] = list(dict.fromkeys(_sub_artifacts))
                _merged_pack["features_used"] = list(dict.fromkeys(_sub_features))
                _merged_pack["regime_ensemble"] = {
                    "regimes_trained": ensemble.regimes_trained,
                    "detector_path": ensemble.detector_path,
                    "submodels": {r: getattr(v, "passed", False) for r, v in ensemble.submodels.items()},
                    "regime_artifact_paths": dict(ensemble.regime_artifact_paths),
                    "router_path": ensemble.router_path,
                }
                # Set error when ensemble did not reach min_regimes_trained threshold
                # so that cascade_train produces a readable fail_reason (not "gate_failed").
                _ens_error = ensemble.error
                if not ensemble.passed and not _ens_error:
                    _min_req = int(getattr(_re_cfg, "min_regimes_trained", 2))
                    _ens_error = f"insufficient_regime_diversity:{ensemble.regimes_trained}/{_min_req}"
                res = _PR(
                    symbol=symbol,
                    run_id=run_id,
                    passed=ensemble.passed,
                    error=_ens_error,
                    metrics=getattr(_rep_sub, "metrics", None) if _rep_sub else None,
                    gate_result=getattr(_rep_sub, "gate_result", None) if _rep_sub else None,
                    pack_result=_merged_pack,
                )
            else:
                res = train_evaluate_adaptive(
                    symbol=symbol,
                    cfg=cfg_layer,
                    state=stage_state if stage_state is not None else StateRegistry(base_state_root / "state.db"),
                    run_id=run_id,
                    safe_mode=bool(safe_mode),
                    smoke_test=False,
                    parquet_path=str(pq),
                    dataset=asset_class,
                    asset_class=asset_class,
                    gate_overrides=gate_overrides,
                    fast=bool(getattr(cfg_layer, "proof_mode", False)),
                    robustness_profile="risk_overlay" if bool(getattr(cfg_layer, "proof_mode", False)) else "full",
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
            if gate_obj is None:
                gate_dump = None
            elif hasattr(gate_obj, "model_dump"):
                gate_dump = gate_obj.model_dump()
            elif hasattr(gate_obj, "dict"):
                gate_dump = gate_obj.dict()
            else:
                gate_dump = None
            if metrics_obj is None:
                metrics_dump = None
            elif hasattr(metrics_obj, "model_dump"):
                metrics_dump = metrics_obj.model_dump()
            elif hasattr(metrics_obj, "dict"):
                metrics_dump = metrics_obj.dict()
            else:
                metrics_dump = None
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
                asset_profile = pack.get("asset_profile")
                asset_profile_kind = pack.get("asset_profile_kind")
                asset_profile_hash = pack.get("asset_profile_hash")
                asset_profile_source = pack.get("asset_profile_source")
                asset_profile_legacy_fallback = pack.get("asset_profile_legacy_fallback")
                training_policy = pack.get("training_policy")
                training_policy_source = pack.get("training_policy_source")
            else:
                asset_profile = None
                asset_profile_kind = None
                asset_profile_hash = None
                asset_profile_source = None
                asset_profile_legacy_fallback = None
                training_policy = None
                training_policy_source = None
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
                "asset_profile": asset_profile,
                "asset_profile_kind": asset_profile_kind,
                "asset_profile_hash": asset_profile_hash,
                "asset_profile_source": asset_profile_source,
                "asset_profile_legacy_fallback": asset_profile_legacy_fallback,
                "training_policy": training_policy,
                "training_policy_source": training_policy_source,
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
            # I1: only a real performance PASS advances the cascade
            prev_stage_pass = bool(performance_pass)
            # Write exclusion record when metrics are missing so downstream
            # analysis can distinguish silent fails from gate fails.
            if metrics_dump is None and not passed:
                _excl_rc = str(getattr(res, "error", None) or fail_reason or "METRICS_MISSING")
                _write_exclusion_record(
                    run_id=run_id,
                    symbol=symbol,
                    tf=tf,
                    reason_code=_excl_rc,
                    detail=str(getattr(res, "error", "") or ""),
                )
        except Exception as e:
            _cascade_log.error("Training failed for %s/%s: %s", symbol, tf, repr(e))
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
            metrics_by_tf[tf] = {
                "gate": None,
                "metrics": None,
                "reason_code": "TRAINING_EXCEPTION",
                "detail": repr(e),
                "parquet_path": str(pq) if pq else None,
                "asset_class": str(asset_class),
                "structural_pass": False,
                "performance_pass": False,
            }
            _write_exclusion_record(
                run_id=run_id,
                symbol=symbol,
                tf=tf,
                reason_code="TRAINING_EXCEPTION",
                detail=repr(e),
            )
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
            prev_stage_pass = False
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
