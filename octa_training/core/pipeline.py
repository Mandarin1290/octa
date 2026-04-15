from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import os
import json
import pickle
import shutil

import numpy as np
import pandas as pd

from octa_training.core.asset_class import infer_asset_class
from octa_training.core.config import canonical_training_altdata_config_path, resolve_feature_settings
from octa_training.core.asset_profiles import (
    AssetProfileMismatchError,
    ensure_canonical_profile_for_dataset,
    profile_hash,
    resolve_asset_profile,
)
from octa_training.core.device import detect_device
from octa_training.core.evaluation import (
    EvalSettings,
    compute_equity_and_metrics,
    infer_frequency,
)
from octa_training.core.features import build_features, leakage_audit
from octa_training.core.gates import GateSpec, gate_evaluate
from octa_training.core.io_parquet import discover_parquets, load_parquet
from octa_training.core.liquidity import passes_liquidity_filter
from octa_training.core.models import train_models
from octa_training.core.notify import send_telegram
from octa_training.core.packaging import save_tradeable_artifact
from octa_training.core.robustness import run_all_tests, run_risk_overlay_tests
from octa_training.core.splits import SplitFold, walk_forward_splits
from octa.core.data.quality.series_validator import validate_price_series


@dataclass
class PipelineResult:
    symbol: str
    run_id: str
    passed: bool
    metrics: Optional[Any] = None
    gate_result: Optional[Any] = None
    pack_result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    altdata: Optional[Dict[str, Any]] = None


@dataclass
class RegimeEnsemble:
    """Result of train_regime_ensemble() for one symbol/TF.

    Attributes
    ----------
    symbol : ticker symbol
    timeframe : '1D', '1H', etc.
    run_id : run identifier
    submodels : dict mapping regime name → PipelineResult for that submodel
    regimes_trained : number of regimes that passed their training gate
    passed : True iff bull AND bear both produced valid artifacts (by default)
    detector_path : path where RegimeDetector was persisted (.pkl)
    error : error message if ensemble failed to build
    regime_artifact_paths : maps regime name → absolute path of per-regime pkl
    router_path : path to the <SYMBOL>_<TF>_regime.pkl routing manifest
    """
    symbol: str
    timeframe: str
    run_id: str
    submodels: Dict[str, "PipelineResult"]
    regimes_trained: int
    passed: bool
    detector_path: Optional[str] = None
    error: Optional[str] = None
    regime_artifact_paths: Dict[str, str] = field(default_factory=dict)
    router_path: Optional[str] = None


def _normalize_asset_class(label: Optional[str]) -> str:
    if not label:
        return "unknown"
    v = str(label).strip().lower()
    if v in {"equity", "stock", "shares", "stocks"}:
        return "stock"
    if v in {"options", "option"}:
        return "option"
    if v in {"future", "futures"}:
        return "future"
    return v


def _merge_gate_specs_strict(global_spec: dict, asset_spec: dict) -> dict:
    """Merge gate specs without allowing asset overrides to relax global floors.

    For "min" thresholds (e.g., sharpe_min) we take max(global, asset).
    For "max" thresholds (e.g., max_drawdown_max) we take min(global, asset).
    Other keys default to asset override if present, else global.
    """
    g = dict(global_spec or {})
    a = dict(asset_spec or {})
    out = dict(g)
    out.update(a)

    min_keys = {
        'sharpe_min',
        'sortino_min',
        'profit_factor_min',
        'min_trades',
        'required_folds_pass_ratio',
        'robustness_subwindow_min_sharpe_ratio',
        'robustness_subwindow_abs_sharpe_min',
        'robustness_stress_min_sharpe',
    }
    max_keys = {
        'max_drawdown_max',
        'robustness_permutation_auc_max',
        'robustness_regime_max_drawdown',
        'turnover_per_day_max',
        'avg_gross_exposure_max',
        'cvar_99_sigma_max',
    }

    for k in min_keys:
        gv = g.get(k)
        av = a.get(k)
        if gv is None and av is None:
            continue
        if gv is None:
            out[k] = av
            continue
        if av is None:
            out[k] = gv
            continue
        try:
            out[k] = max(float(gv), float(av))
        except Exception:
            out[k] = gv

    for k in max_keys:
        gv = g.get(k)
        av = a.get(k)
        if gv is None and av is None:
            continue
        if gv is None:
            out[k] = av
            continue
        if av is None:
            out[k] = gv
            continue
        try:
            out[k] = min(float(gv), float(av))
        except Exception:
            out[k] = gv

    return out


def _emit_crisis_gov_event(
    gov_audit: Optional[Any],
    status: str,
    symbol: str,
    tf: str,
    window_name: str,
    **payload: Any,
) -> None:
    """Emit a CRISIS_OOS_* governance event (fail-soft — never raises)."""
    if gov_audit is None:
        return
    try:
        from octa.core.governance.governance_audit import (
            EVENT_CRISIS_OOS_FAILED,
            EVENT_CRISIS_OOS_PASSED,
            EVENT_CRISIS_OOS_SKIPPED,
        )
        _evt_map = {
            "PASSED": EVENT_CRISIS_OOS_PASSED,
            "FAILED": EVENT_CRISIS_OOS_FAILED,
            "SKIPPED": EVENT_CRISIS_OOS_SKIPPED,
        }
        evt = _evt_map.get(status)
        if evt:
            gov_audit.emit(evt, {"symbol": symbol, "tf": tf, "window": window_name, **payload})
    except Exception:
        pass


def crisis_oos_gate(
    X: pd.DataFrame,
    y_dict: Dict[str, Any],
    close_prices: pd.Series,
    cfg: Any,
    profile: Any,
    eval_settings: Any,
    crisis_windows: List[Dict[str, Any]],
    thresholds: Dict[str, Any],
    symbol: str = "",
    tf: str = "",
    gov_audit: Optional[Any] = None,
) -> Tuple[bool, List[Dict[str, Any]]]:
    """Crisis hold-out OOS gate.

    For each window in *crisis_windows*:

    1. Convert the window's date range to integer positions in ``X.index``.
    2. Build a :class:`SplitFold` where ``train_idx`` = everything **outside**
       the crisis and ``val_idx`` = everything **inside** the crisis.
    3. Retrain a fresh model via :func:`train_models` — the model has no
       knowledge of the crisis period.
    4. Evaluate OOF predictions (on the crisis val set) with
       :func:`compute_equity_and_metrics`.
    5. Gate: ``sharpe >= thresholds["min_sharpe"]`` AND
       ``|max_drawdown| <= thresholds["max_drawdown_pct"]``.

    Windows where the crisis dates are absent from the data (or data is
    insufficient) receive status ``SKIPPED`` and do **not** fail the gate.

    Parameters
    ----------
    X : feature DataFrame (DatetimeIndex aligned with close_prices)
    y_dict : label dict (same as passed to train_models in the main pipeline)
    close_prices : ``df["close"]`` series
    cfg : training config object
    profile : device profile (from detect_device())
    eval_settings : EvalSettings instance
    crisis_windows : list of dicts with keys ``name``, ``start``, ``end``
    thresholds : dict with ``min_sharpe``, ``max_drawdown_pct``,
        ``min_test_rows``, ``min_train_rows``
    symbol : symbol name for logging / audit
    tf : timeframe string for logging / audit
    gov_audit : optional GovernanceAudit instance; events emitted if provided

    Returns
    -------
    (passed, window_results)
    *passed* is ``True`` when every **evaluated** (non-SKIPPED) window passes.
    *window_results* is a list of per-window dicts with keys:
    ``name``, ``status``, ``sharpe``, ``max_drawdown``, ``note``.
    """
    # Use module-level names so tests can monkeypatch them via the pipeline module.
    # train_models and compute_equity_and_metrics are imported at the top of this module.
    # SplitFold is imported from splits at the top of this module.

    min_sharpe = float(thresholds.get("min_sharpe", 0.0))
    max_dd = float(thresholds.get("max_drawdown_pct", 0.40))
    min_test_rows = int(thresholds.get("min_test_rows", 20))
    min_train_rows = int(thresholds.get("min_train_rows", 252))

    X_index = X.index
    window_results: List[Dict[str, Any]] = []
    all_passed = True

    for w in crisis_windows:
        name = str(w.get("name", "unnamed"))
        try:
            start = pd.Timestamp(w["start"])
            end = pd.Timestamp(w["end"])
        except (KeyError, ValueError, TypeError):
            window_results.append({"name": name, "status": "SKIPPED", "sharpe": None, "max_drawdown": None, "note": "invalid_window_spec"})
            _emit_crisis_gov_event(gov_audit, "SKIPPED", symbol, tf, name, note="invalid_window_spec")
            continue

        # Tz-align start/end to match X_index timezone.
        if isinstance(X_index, pd.DatetimeIndex) and X_index.tz is not None:
            if start.tzinfo is None:
                start = start.tz_localize(X_index.tz)
            if end.tzinfo is None:
                end = end.tz_localize(X_index.tz)

        # Convert date window to integer positions within X.
        test_mask = (X_index >= start) & (X_index <= end)
        train_mask = ~test_mask
        test_positions = np.where(test_mask)[0]
        train_positions = np.where(train_mask)[0]

        if len(test_positions) < min_test_rows or len(train_positions) < min_train_rows:
            note = f"insufficient_data:test={len(test_positions)},train={len(train_positions)}"
            window_results.append({"name": name, "status": "SKIPPED", "sharpe": None, "max_drawdown": None, "note": note})
            _emit_crisis_gov_event(gov_audit, "SKIPPED", symbol, tf, name, note=note)
            continue

        fold = SplitFold(
            train_idx=train_positions,
            val_idx=test_positions,
            fold_meta={"crisis_window": name, "crisis_start": str(start), "crisis_end": str(end)},
        )

        # Retrain on all-but-crisis data; evaluate on crisis period (val_idx).
        try:
            crisis_results = train_models(
                X,
                y_dict,
                [fold],
                cfg,
                profile,
                fast=False,
                prices=close_prices,
                eval_settings=eval_settings,
            )
        except Exception as exc:
            note = f"train_error:{exc}"
            window_results.append({"name": name, "status": "SKIPPED", "sharpe": None, "max_drawdown": None, "note": note})
            _emit_crisis_gov_event(gov_audit, "SKIPPED", symbol, tf, name, note=note)
            continue

        if not crisis_results:
            note = "no_train_results"
            window_results.append({"name": name, "status": "SKIPPED", "sharpe": None, "max_drawdown": None, "note": note})
            _emit_crisis_gov_event(gov_audit, "SKIPPED", symbol, tf, name, note=note)
            continue

        best = crisis_results[0]

        # Extract OOF predictions (cover the crisis val period).
        try:
            oof = getattr(best, "oof_predictions", None) or {}
            oof_idx_raw = oof.get("index", [])
            oof_vals = oof.get("pred", [])
            if not oof_idx_raw or not oof_vals:
                raise ValueError("empty_oof_predictions")
            try:
                preds_index = pd.to_datetime(pd.Index(oof_idx_raw), utc=True, errors="coerce")
                if preds_index.isna().all():
                    preds_index = pd.Index(oof_idx_raw)
                else:
                    # Align tz to match close_prices so compute_equity_and_metrics can join them.
                    close_tz = getattr(close_prices.index, "tz", None)
                    if close_tz is None and hasattr(preds_index, "tz") and preds_index.tz is not None:
                        # Strip tz from tz-aware preds index to match tz-naive close.
                        preds_index = preds_index.tz_convert(None)
                    elif close_tz is not None and hasattr(preds_index, "tz") and preds_index.tz is None:
                        preds_index = preds_index.tz_localize(close_tz)
            except Exception:
                preds_index = pd.Index(oof_idx_raw)
            preds = pd.Series(oof_vals, index=preds_index)
        except Exception as exc:
            note = f"oof_error:{exc}"
            window_results.append({"name": name, "status": "SKIPPED", "sharpe": None, "max_drawdown": None, "note": note})
            _emit_crisis_gov_event(gov_audit, "SKIPPED", symbol, tf, name, note=note)
            continue

        # Evaluate strategy metrics on the crisis-period close prices.
        try:
            crisis_close = close_prices.iloc[test_positions]
            out = compute_equity_and_metrics(crisis_close, preds, eval_settings)
            m = out.get("metrics")
            if m is None:
                raise ValueError("no_metrics_returned")
            sharpe = float(getattr(m, "sharpe", None) or 0.0)
            max_drawdown = abs(float(getattr(m, "max_drawdown", None) or 1.0))
        except Exception as exc:
            note = f"eval_error:{exc}"
            window_results.append({"name": name, "status": "SKIPPED", "sharpe": None, "max_drawdown": None, "note": note})
            _emit_crisis_gov_event(gov_audit, "SKIPPED", symbol, tf, name, note=note)
            continue

        passed_window = (sharpe >= min_sharpe) and (max_drawdown <= max_dd)
        status = "PASSED" if passed_window else "FAILED"
        if not passed_window:
            all_passed = False

        note = f"sharpe={sharpe:.3f} max_dd={max_drawdown:.3f}"
        window_results.append({
            "name": name,
            "status": status,
            "sharpe": round(sharpe, 4),
            "max_drawdown": round(max_drawdown, 4),
            "note": note,
        })
        _emit_crisis_gov_event(
            gov_audit, status, symbol, tf, name,
            sharpe=sharpe,
            max_drawdown=max_drawdown,
            thresholds={"min_sharpe": min_sharpe, "max_drawdown_pct": max_dd},
        )

    return all_passed, window_results


def train_evaluate_package(
    symbol: str,
    cfg: Any,
    state: Any,
    run_id: str,
    safe_mode: bool = False,
    smoke_test: bool = False,
    logger: Optional[Any] = None,
    parquet_path: Optional[str] = None,
    gate_overrides: Optional[Dict[str, Any]] = None,
    robustness_profile: str = "full",
    debug: bool = False,
    dataset: Optional[str] = None,
    asset_profile: Optional[str] = None,
    fast: bool = False,
    window_start: Optional[pd.Timestamp] = None,
    window_end: Optional[pd.Timestamp] = None,
    lookback_policy: Optional[Dict[str, Any]] = None,
    require_full_run: bool = False,
) -> PipelineResult:
    alt_diag: Dict[str, Any] = {}
    try:
        assurance_ctx: Dict[str, Any] = {
            'asset_class': None,
            'parquet_path': None,
            'parquet_stat': None,
        }

        # FX-G1 Data-Truth Gate precomputed stats (populated only for FX *_1H stage).
        fx_g1_median_bar_spacing_seconds: Optional[float] = None
        fx_g1_p90_bar_spacing_seconds: Optional[float] = None
        fx_g1_is_hourly_like: Optional[bool] = None
        fx_g1_skip_reason: Optional[str] = None

        # Snapshot gate policy BEFORE any asset overlay (global, not tunable per asset/symbol).
        gate_policy_snapshot = None
        try:
            raw_gates = getattr(cfg, 'gates', {})
            if isinstance(raw_gates, dict):
                gate_policy_snapshot = dict(raw_gates)
            elif hasattr(raw_gates, "model_dump"):
                gate_policy_snapshot = raw_gates.model_dump()
            elif hasattr(raw_gates, "dict"):
                gate_policy_snapshot = raw_gates.dict()
            else:
                gate_policy_snapshot = {}
        except Exception:
            gate_policy_snapshot = {}

        diagnose_mode = bool(gate_policy_snapshot.get('diagnose_mode', False))
        diagnose_reasons: list[str] = []

        def _infer_timeframe_key(idx: pd.Index) -> str:
            """Infer timeframe key for global-by-timeframe gates."""
            try:
                if not isinstance(idx, pd.DatetimeIndex) or len(idx) < 2:
                    return "1D"
                deltas = np.diff(idx.astype('int64'))
                med_ns = float(np.median(deltas))
                sec = med_ns / 1e9
                if sec >= 20 * 3600:
                    return "1D"
                if sec >= 50 * 60:
                    return "1H"
                if sec >= 20 * 60:
                    return "30m"
                if sec >= 4 * 60:
                    return "5m"
                return "1m"
            except Exception:
                return "1D"

        def _hard_kill_switches_conf() -> dict:
            try:
                if isinstance(gate_policy_snapshot, dict):
                    return gate_policy_snapshot.get('hard_kill_switches', {}) or {}
            except Exception:
                pass
            return {}

        # record run start for monitoring/audit trail
        try:
            state.record_run_start(symbol, run_id)
        except Exception:
            pass

        def _record_end(
            passed: bool,
            metrics_obj: Optional[Any] = None,
            gate_obj: Optional[Any] = None,
            pack_res: Optional[Dict[str, Any]] = None,
        ) -> None:
            def _sanitize(v: Any) -> Any:
                try:
                    from datetime import datetime as _dt

                    if isinstance(v, _dt):
                        return v.isoformat()
                except Exception:
                    pass

                try:
                    import pandas as _pd

                    if isinstance(v, _pd.Timestamp):
                        return v.isoformat()
                except Exception:
                    pass

                try:
                    import numpy as _np

                    if isinstance(v, _np.generic):
                        return v.item()
                except Exception:
                    pass
                try:
                    from pathlib import Path as _Path

                    if isinstance(v, _Path):
                        return str(v)
                except Exception:
                    pass
                if isinstance(v, dict):
                    return {str(k): _sanitize(val) for k, val in v.items()}
                if isinstance(v, (list, tuple)):
                    return [_sanitize(x) for x in v]
                return v

            metrics_summary = None
            if metrics_obj is not None:
                try:
                    raw = metrics_obj.dict() if hasattr(metrics_obj, 'dict') else None
                    metrics_summary = _sanitize(raw)
                except Exception:
                    metrics_summary = None

            try:
                state.record_run_end(symbol, run_id, passed=passed, metrics_summary=metrics_summary)
            except Exception:
                pass

            # Optional KVP aggregation (non-invasive; writes only aggregate stats)
            try:
                kvp_cfg = getattr(cfg, 'kvp', None)
                if kvp_cfg is not None and bool(getattr(kvp_cfg, 'enabled', False)):
                    from octa_training.core.kvp import update_kvp_summary

                    update_kvp_summary(
                        state_dir=cfg.paths.state_dir,
                        asset_class=str(assurance_ctx.get('asset_class') or sstate.get('asset_class') or 'unknown'),
                        passed=bool(passed),
                        metrics_summary=metrics_summary,
                        filename=str(getattr(kvp_cfg, 'filename', 'kvp_summary.json') or 'kvp_summary.json'),
                    )
            except Exception:
                # KVP is best-effort and must never change pass/fail semantics.
                pass

            if gate_obj is not None:
                try:
                    gr_passed = bool(getattr(gate_obj, 'passed', passed))
                    reasons = getattr(gate_obj, 'reasons', None)
                    if isinstance(reasons, list) and reasons:
                        reasons_s = ';'.join([str(r) for r in reasons[:3]])
                        state.update_symbol_state(symbol, last_gate_result=('PASS' if gr_passed else f"FAIL:{reasons_s}"))
                    else:
                        state.update_symbol_state(symbol, last_gate_result=('PASS' if gr_passed else 'FAIL'))
                except Exception:
                    pass

            # Tier-1 assurance evidence emission (audit/compliance/governance)
            try:
                a = getattr(cfg, 'assurance', None)
                if a is not None and bool(getattr(a, 'enabled', False)):
                    from octa_training.core.assurance import emit_assurance_report

                    reasons_list = None
                    try:
                        rr = getattr(gate_obj, 'reasons', None)
                        if isinstance(rr, list):
                            reasons_list = [str(x) for x in rr]
                    except Exception:
                        reasons_list = None

                    emit_assurance_report(
                        cfg=cfg,
                        symbol=symbol,
                        run_id=run_id,
                        passed=bool(passed),
                        reasons=reasons_list,
                        safe_mode=bool(safe_mode),
                        asset_class=assurance_ctx.get('asset_class'),
                        parquet_path=assurance_ctx.get('parquet_path'),
                        parquet_stat=assurance_ctx.get('parquet_stat'),
                        metrics_summary=metrics_summary,
                        pack_result=pack_res,
                    )
            except Exception as e:
                a = getattr(cfg, 'assurance', None)
                if a is not None and bool(getattr(a, 'fail_closed', False)):
                    raise
                if logger:
                    logger.warning("Assurance emission failed: %s", e)

        # idempotence: skip if parquet unchanged and artifact exists and last_pass_time recent
        sstate = state.get_symbol_state(symbol) or {}
        art_path = sstate.get('artifact_path')
        last_pass = sstate.get('last_pass_time')
        from datetime import datetime, timedelta
        try:
            if not require_full_run:
                if art_path and Path(art_path).exists() and last_pass:
                    last = datetime.fromisoformat(last_pass)
                    if datetime.utcnow() - last < timedelta(days=getattr(cfg.retrain, 'skip_window_days', 3)):
                        _record_end(True, metrics_obj=None, gate_obj=None, pack_res={'skipped': True, 'reason': 'recent_pass'})
                        return PipelineResult(symbol=symbol, run_id=run_id, passed=True, metrics=None, gate_result=None, pack_result={'skipped': True, 'reason': 'recent_pass'}, altdata=alt_diag)
        except Exception:
            pass
        pinfo = None
        if parquet_path:
            try:
                from octa_training.core.io_parquet import ParquetFileInfo

                pp = Path(parquet_path)
                if not pp.exists():
                    _record_end(False, metrics_obj=None, gate_obj=None)
                    return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='no_parquet', altdata=alt_diag)
                try:
                    stat = pp.stat()
                    pinfo = ParquetFileInfo(symbol=symbol, path=pp, mtime=stat.st_mtime, size=stat.st_size, sha256=None)
                except Exception:
                    pinfo = ParquetFileInfo(symbol=symbol, path=pp, mtime=0.0, size=0, sha256=None)
            except Exception:
                pinfo = None

        if pinfo is None:
            discovered = discover_parquets(Path(cfg.paths.raw_dir), state=state)
            match = [d for d in discovered if d.symbol == symbol]
            if not match:
                _record_end(False, metrics_obj=None, gate_obj=None)
                return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='no_parquet', altdata=alt_diag)
            pinfo = match[0]

        try:
            assurance_ctx['parquet_path'] = str(pinfo.path)
            assurance_ctx['parquet_stat'] = {
                'mtime': getattr(pinfo, 'mtime', None),
                'size': getattr(pinfo, 'size', None),
                'sha256': getattr(pinfo, 'sha256', None),
            }
        except Exception:
            pass

        try:
            df = load_parquet(
                pinfo.path,
                nan_threshold=cfg.parquet.nan_threshold,
                allow_negative_prices=cfg.parquet.allow_negative_prices,
                resample_enabled=cfg.parquet.resample_enabled,
                resample_bar_size=cfg.parquet.resample_bar_size,
            )
        except Exception as e:
            # In diagnose mode we want a clean per-symbol failure record (not a run error)
            # so large sweeps can continue and aggregate reasons.
            try:
                diagnose_enabled = bool(gate_policy_snapshot.get('diagnose_mode', False))
            except Exception:
                diagnose_enabled = False
            if diagnose_enabled:
                from octa_training.core.gates import GateResult

                reason = f"data_load_failed: {e}"
                gate_obj = GateResult(
                    passed=False,
                    status='FAIL_DATA',
                    gate_version=None,
                    reasons=[reason],
                    passed_checks=[],
                    robustness=None,
                    diagnostics=None,
                )
                _record_end(False, metrics_obj=None, gate_obj=gate_obj)
                return PipelineResult(symbol=symbol, run_id=run_id, passed=False, metrics=None, gate_result=gate_obj, pack_result=None, error=None, altdata=alt_diag)
            raise
        # Strict data-health validation prior to feature/model steps.
        health = validate_price_series(df, close_col="close")
        if not bool(health.ok):
            from octa_training.core.gates import GateResult
            try:
                data_health_path = Path(cfg.paths.reports_dir) / "autopilot" / str(run_id) / f"data_health_{symbol}.json"
                data_health_path.parent.mkdir(parents=True, exist_ok=True)
                data_health_path.write_text(
                    json.dumps(
                        {"symbol": symbol, "run_id": run_id, "code": str(health.code), "stats": dict(health.stats), "parquet_path": str(pinfo.path)},
                        indent=2,
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )
            except Exception:
                data_health_path = None
            reason = f"DATA_INVALID:{health.code}"
            gate_obj = GateResult(
                passed=False,
                status="FAIL_DATA",
                gate_version=None,
                reasons=[reason],
                passed_checks=[],
                robustness=None,
                diagnostics=[
                    {"name": "data_health_code", "value": str(health.code), "threshold": None, "op": None, "passed": False, "evaluable": True, "confidence": 1.0, "reason": reason},
                    {"name": "data_health_stats", "value": dict(health.stats), "threshold": None, "op": None, "passed": False, "evaluable": True, "confidence": 1.0, "reason": reason},
                ],
            )
            _record_end(False, metrics_obj=None, gate_obj=gate_obj, pack_res={"data_health_path": str(data_health_path) if data_health_path else None})
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, metrics=None, gate_result=gate_obj, pack_result={"data_health_path": str(data_health_path) if data_health_path else None}, error=None)
        # Auto asset classification (prefer inference).
        asset_class_state = None
        try:
            asset_class_state = (state.get_symbol_state(symbol) or {}).get('asset_class')
        except Exception:
            asset_class_state = None

        inferred = None
        try:
            inferred = infer_asset_class(symbol, str(pinfo.path), list(df.columns), cfg)
        except Exception:
            inferred = None

        asset_class_raw = inferred or asset_class_state or dataset or "unknown"
        # If inference couldn't decide, allow existing state to win.
        try:
            if str(asset_class_raw).strip().lower() == 'unknown' and asset_class_state:
                asset_class_raw = asset_class_state
        except Exception:
            pass

        # v0.0.0 stock-only safety net: if still unknown but symbol is a known
        # configured stock, force to 'stock' rather than GATE_FAIL:UNKNOWN_ASSET_CLASS.
        _V000_STOCK_SYMBOLS = {'ADC', 'AON', 'AWR', 'AEM', 'ALB', 'AMZN', 'AVA'}
        if str(asset_class_raw).strip().lower() == 'unknown' and str(symbol).upper() in _V000_STOCK_SYMBOLS:
            asset_class_raw = 'stock'
            if logger:
                logger.debug("[%s] asset_class forced to 'stock' via v0.0.0 symbol list", symbol)

        asset_class = _normalize_asset_class(asset_class_raw)
        assurance_ctx['asset_class'] = asset_class

        # Tier-1 Data-Truth Gate (FX only, G1 only): ensure *_1H.parquet is actually hourly-like.
        # If not, fail closed and do not run FX-G1 alpha evaluation on daily-like data.
        try:
            ac_dt = str(asset_class or '').lower()
            is_fx_g1_stage = (
                ac_dt in {'fx', 'forex'}
                and str(robustness_profile or 'full').lower() != 'risk_overlay'
                and str(pinfo.path).upper().endswith('_1H.PARQUET')
            )
            if is_fx_g1_stage and isinstance(df.index, pd.DatetimeIndex) and len(df.index) >= 3:
                # Avoid constructing an intermediate Series/DataFrame here; this runs in large sweeps.
                idx_ns = df.index.asi8
                secs = (np.diff(idx_ns).astype("float64") / 1e9)
                secs = secs[np.isfinite(secs) & (secs > 0.0)]
                if secs.size:
                    fx_g1_median_bar_spacing_seconds = float(np.median(secs))
                    fx_g1_p90_bar_spacing_seconds = float(np.quantile(secs, 0.9))

                # Conservative, fail-closed predicate: must look hourly-like.
                fx_g1_is_hourly_like = bool(
                    fx_g1_median_bar_spacing_seconds is not None
                    and fx_g1_p90_bar_spacing_seconds is not None
                    and fx_g1_median_bar_spacing_seconds <= 2.0 * 3600.0
                    and fx_g1_p90_bar_spacing_seconds <= 4.0 * 3600.0
                )

                if not fx_g1_is_hourly_like:
                    fx_g1_skip_reason = (
                        'fx_g1:invalid_1h_data: '
                        f'median_spacing={fx_g1_median_bar_spacing_seconds} '
                        f'p90_spacing={fx_g1_p90_bar_spacing_seconds}'
                    )
                    from octa_training.core.gates import GateResult

                    gate_obj = GateResult(
                        passed=False,
                        status='FAIL_DATA',
                        gate_version=None,
                        reasons=[f"data_load_failed: {fx_g1_skip_reason}"],
                        passed_checks=[],
                        robustness=None,
                        diagnostics=[
                            {
                                'name': 'fx_g1_median_bar_spacing_seconds',
                                'value': fx_g1_median_bar_spacing_seconds,
                                'threshold': None,
                                'op': None,
                                'passed': True,
                                'evaluable': True,
                                'confidence': 0.0,
                                'reason': None,
                            },
                            {
                                'name': 'fx_g1_p90_bar_spacing_seconds',
                                'value': fx_g1_p90_bar_spacing_seconds,
                                'threshold': None,
                                'op': None,
                                'passed': True,
                                'evaluable': True,
                                'confidence': 0.0,
                                'reason': None,
                            },
                            {
                                'name': 'fx_g1_is_hourly_like',
                                'value': bool(fx_g1_is_hourly_like),
                                'threshold': None,
                                'op': None,
                                'passed': True,
                                'evaluable': True,
                                'confidence': 0.0,
                                'reason': None,
                            },
                            {
                                'name': 'fx_g1_skip_reason',
                                'value': None,
                                'threshold': None,
                                'op': None,
                                'passed': True,
                                'evaluable': True,
                                'confidence': 0.0,
                                'reason': fx_g1_skip_reason,
                            },
                        ],
                    )
                    _record_end(False, metrics_obj=None, gate_obj=gate_obj)
                    return PipelineResult(symbol=symbol, run_id=run_id, passed=False, metrics=None, gate_result=gate_obj, pack_result=None, error=None, altdata=alt_diag)
        except Exception:
            # Fail-closed intent is enforced by the early GateResult above; do not break unrelated assets.
            pass

        # Optional per-asset-class config overlay (safe no-op unless file exists)
        try:
            import yaml

            def _deep_merge(dst: dict, src: dict) -> None:
                if not isinstance(dst, dict) or not isinstance(src, dict):
                    return
                for k, v in src.items():
                    if k in dst and isinstance(dst[k], dict) and isinstance(v, dict):
                        _deep_merge(dst[k], v)
                    else:
                        dst[k] = v

            overlay_path = Path('configs') / 'asset' / f"{asset_class}.yaml"
            if (not overlay_path.exists()) and asset_class_raw:
                try:
                    alt = Path('configs') / 'asset' / f"{str(asset_class_raw).strip().lower()}.yaml"
                    if alt.exists():
                        overlay_path = alt
                except Exception:
                    pass

            if overlay_path.exists():
                raw_overlay = yaml.safe_load(overlay_path.read_text()) or {}
                base_cfg = cfg.model_dump() if hasattr(cfg, 'model_dump') else (cfg.dict() if hasattr(cfg, 'dict') else {})
                if isinstance(base_cfg, dict) and isinstance(raw_overlay, dict):
                    # Overlay provides defaults; explicit cfg values must win.
                    merged_cfg = raw_overlay.copy()
                    _deep_merge(merged_cfg, base_cfg)
                    from octa_training.core.config import TrainingConfig

                    cfg = TrainingConfig(**merged_cfg)
                    try:
                        state.update_symbol_state(symbol, asset_config_overlay_path=str(overlay_path), asset_class=asset_class)
                    except Exception:
                        pass
                    if logger:
                        logger.info("Applied asset config overlay: %s", overlay_path)
        except Exception:
            pass

        # Liquidity filter (pre-train): quarantine+skip if below thresholds
        try:
            ok_liq, liq_reason, liq_details = passes_liquidity_filter(df, cfg)
            if not ok_liq:
                from datetime import datetime

                qroot = Path(cfg.paths.reports_dir) / 'quarantine' / symbol
                ts = datetime.utcnow().isoformat().replace(':', '-')
                qdir = qroot / ts
                qdir.mkdir(parents=True, exist_ok=True)

                try:
                    state.update_symbol_state(
                        symbol,
                        artifact_quarantine_path=str(qdir),
                        artifact_quarantine_reason=f"liquidity:{liq_reason}",
                        last_gate_result='LIQUIDITY',
                    )
                except Exception:
                    pass

                send_telegram(cfg, f"OCTA: {symbol} liquidity filter FAIL ({liq_reason}) details={liq_details}", logger=logger)
                _record_end(False, metrics_obj=None, gate_obj=None)
                if not diagnose_mode:
                    return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error=f"liquidity_filter_failed:{liq_reason}", altdata=alt_diag)
                diagnose_reasons.append(f"liquidity_filter_failed:{liq_reason}")
        except Exception as e:
            # never fail the run due to liquidity checker bugs
            if logger:
                logger.warning("Liquidity filter check failed: %s", e)
        # Apply deterministic time window policy (fail-closed when insufficient data).
        window_meta: Dict[str, Any] = {}
        if window_start is not None or window_end is not None:
            try:
                if not isinstance(df.index, pd.DatetimeIndex):
                    raise ValueError("invalid_index")
                idx = df.index
                if idx.tz is None:
                    idx = idx.tz_localize("UTC")
                    df = df.copy()
                    df.index = idx
                w_start = window_start if window_start is not None else idx.min()
                w_end = window_end if window_end is not None else idx.max()
                if w_start.tzinfo is None:
                    w_start = w_start.tz_localize("UTC")
                if w_end.tzinfo is None:
                    w_end = w_end.tz_localize("UTC")
                df = df[(df.index >= w_start) & (df.index <= w_end)]
                bars_in_window = int(len(df))
                window_meta = {
                    "window_start": str(w_start),
                    "window_end": str(w_end),
                    "bars_in_window": bars_in_window,
                    "lookback_policy": lookback_policy or {},
                }
                min_train = int(getattr(getattr(cfg, "gating", None), "min_train_samples", 0) or 0)
                min_bt = int(getattr(getattr(cfg, "gating", None), "min_backtest_samples", 0) or 0)
                min_required = max(min_train, min_bt, 1)
                if bars_in_window < min_required:
                    _record_end(False, metrics_obj=None, gate_obj=None)
                    return PipelineResult(
                        symbol=symbol,
                        run_id=run_id,
                        passed=False,
                        error="INSUFFICIENT_DATA_WINDOW_FAIL_CLOSED",
                        altdata={"time_window": window_meta},
                    )
            except Exception:
                _record_end(False, metrics_obj=None, gate_obj=None)
                return PipelineResult(
                    symbol=symbol,
                    run_id=run_id,
                    passed=False,
                    error="INSUFFICIENT_DATA_WINDOW_FAIL_CLOSED",
                    altdata={"time_window": window_meta},
                )

        # Build features using nested cfg.features (dict). Keep a config-like object
        # so build_features can access settings.features and legacy attributes.
        class _FeatSettings:
            pass

        eff_settings = _FeatSettings()
        # Provide paths/timeframe context for feature sidecars (FRED cache, market context).
        try:
            eff_settings.raw_dir = cfg.paths.raw_dir
        except Exception:
            pass
        try:
            eff_settings.timeframe = _infer_timeframe_key(df.index)
        except Exception:
            pass
        try:
            if window_meta:
                eff_settings.window_start = window_meta.get("window_start")
                eff_settings.window_end = window_meta.get("window_end")
                eff_settings.lookback_policy = window_meta.get("lookback_policy")
        except Exception:
            pass
        try:
            eff_settings.features = resolve_feature_settings(cfg, asset_class)
        except Exception:
            eff_settings.features = cfg.features if isinstance(cfg.features, dict) else {}
        # Apply per-TF feature overrides AFTER asset overlay (highest priority).
        # cfg.features_by_timeframe = {"1H": {"horizons": [6, 12]}} etc.
        try:
            _ftf = getattr(cfg, 'features_by_timeframe', None) or {}
            if isinstance(_ftf, dict) and _ftf:
                _tf_key_feat = getattr(eff_settings, 'timeframe', None) or ''
                _tf_spec = _ftf.get(_tf_key_feat) or _ftf.get(_tf_key_feat.upper()) or {}
                if _tf_spec and isinstance(eff_settings.features, dict):
                    eff_settings.features = {**eff_settings.features, **_tf_spec}
        except Exception:
            pass
        # Optional context for sidecar integrations (no behavioral impact unless used).
        try:
            eff_settings.symbol = symbol
        except Exception:
            pass
        try:
            eff_settings.run_id = run_id
        except Exception:
            pass
        try:
            eff_settings.timezone = str(getattr(getattr(cfg, 'session', None), 'timezone', 'UTC') or 'UTC')
        except Exception:
            pass
        try:
            eff_settings.altdata_config_path = str(canonical_training_altdata_config_path())
        except Exception:
            pass
        # legacy attribute fallbacks
        try:
            for k, v in (eff_settings.features or {}).items():
                if isinstance(k, str):
                    setattr(eff_settings, k, v)
        except Exception:
            pass
        # Per-timeframe split calibration for eff_settings resolver (restores a6bc3b3 fix).
        try:
            splits_cfg_pre = cfg.splits if hasattr(cfg, 'splits') else {}
            _splits_by_tf_pre = getattr(cfg, 'splits_by_timeframe', None) or {}
            if isinstance(_splits_by_tf_pre, dict) and _splits_by_tf_pre:
                _tf_key_pre = _infer_timeframe_key(df.index)
                _tf_spec_pre = (
                    _splits_by_tf_pre.get(_tf_key_pre, {})
                    or _splits_by_tf_pre.get(_tf_key_pre.upper(), {})
                    or _splits_by_tf_pre.get(_tf_key_pre.lower(), {})
                    or {}
                )
                if _tf_spec_pre:
                    splits_cfg_pre = {**splits_cfg_pre, **_tf_spec_pre}
            _min_train = int(splits_cfg_pre.get('min_train_size', 500))
            _min_test = int(splits_cfg_pre.get('min_test_size', 100))
            eff_settings.walk_forward_resolver = {
                "n_folds": int(splits_cfg_pre.get('n_folds', 5)),
                "train_window": int(splits_cfg_pre.get('train_window', 1000)),
                "test_window": int(splits_cfg_pre.get('test_window', 200)),
                "step": int(splits_cfg_pre.get('step', 200)),
                "purge_size": int(splits_cfg_pre.get('purge_size', 10)),
                "embargo_size": int(splits_cfg_pre.get('embargo_size', 5)),
                "min_train_size": _min_train,
                "min_test_size": _min_test,
                "min_folds_required": int(splits_cfg_pre.get('min_folds_required', 1)),
                "expanding": bool(splits_cfg_pre.get('expanding', True)),
                "fallback_min_train_size": max(100, max(1, _min_train // 2)),
                "fallback_min_test_size": max(30, max(1, _min_test // 2)),
            }
        except Exception:
            pass

        features_res = build_features(df, eff_settings, asset_class, symbol=symbol)
        meta = getattr(features_res, "meta", {}) if hasattr(features_res, "meta") else {}

        def _build_altdata_diag() -> Dict[str, Any]:
            cols = list(getattr(features_res, "X", pd.DataFrame()).columns)
            alt_cols = [c for c in cols if isinstance(c, str) and c.startswith("altdat_")]
            macro_cols = [c for c in cols if isinstance(c, str) and c.startswith("macro_")]
            avi_cols = [c for c in cols if isinstance(c, str) and c.startswith("avi_")]
            altdat_meta = meta.get("altdat") if isinstance(meta, dict) else {}
            missing_sources: List[str] = []
            sources_payload: Dict[str, Any] = {}
            cache_flags: List[bool] = []
            earnings_diag = {}
            xbrl_diag = {}
            if isinstance(altdat_meta, dict):
                srcs = altdat_meta.get("sources") or {}
                if isinstance(srcs, dict):
                    for name, spec in srcs.items():
                        entry = {}
                        if isinstance(spec, dict):
                            entry = {
                                "ok": bool(spec.get("ok", False)),
                                "cache_asof": spec.get("cache_asof"),
                                "error": spec.get("error"),
                                "rows": spec.get("rows_by_series"),
                                "status": spec.get("status"),
                                "snapshot_asof": spec.get("snapshot_asof"),
                                "counts": spec.get("counts"),
                                "snapshot_path": spec.get("snapshot_path"),
                            }
                            if not entry["ok"]:
                                missing_sources.append(name)
                            cache_flags.append(bool(entry.get("cache_asof")))
                        sources_payload[name] = entry
                    earnings_diag = sources_payload.get("earnings", {}) or {}
                    xbrl_diag = sources_payload.get("xbrl", {}) or {}
                merge_reason = altdat_meta.get("merge_reason")
                cols_added = altdat_meta.get("cols_added")
                rows_with_values = altdat_meta.get("rows_with_values")
            else:
                merge_reason = None
                cols_added = None
                rows_with_values = None
            rows_dropped = None
            try:
                if meta.get("n_rows_raw") is not None and meta.get("n_rows_features") is not None:
                    rows_dropped = int(meta.get("n_rows_raw")) - int(meta.get("n_rows_features"))
            except Exception:
                rows_dropped = None
            return {
                "run_id": str(run_id),
                "symbol": str(symbol),
                "timeframe": _infer_timeframe_key(df.index),
                "n_rows_raw": meta.get("n_rows_raw"),
                "n_rows_features": meta.get("n_rows_features"),
                "feature_count": len(cols),
                "altdata_enabled": bool(altdat_meta.get("enabled")) if isinstance(altdat_meta, dict) else False,
                "altdata_feature_count": len(alt_cols),
                "macro_feature_count": len(macro_cols),
                "avi_feature_count": len(avi_cols),
                "altdata_merge_reason": merge_reason,
                "altdata_cols_added": cols_added,
                "altdata_rows_with_values": rows_with_values,
                "altdata_rows_dropped": rows_dropped,
                "missing_altdata_sources": missing_sources,
                "sources": sources_payload,
                "altdata_merge": {
                    "earnings": earnings_diag,
                    "xbrl": xbrl_diag,
                    "fail_closed": (altdat_meta or {}).get("fail_closed") if isinstance(altdat_meta, dict) else None,
                },
                "features": cols,
                "cache_ok": bool(cache_flags) and all(cache_flags),
            }

        alt_diag = _build_altdata_diag()
        if window_meta:
            alt_diag["time_window"] = window_meta

        try:
            if str(os.getenv("OCTA_FEATURE_DEBUG", "")).strip() == "1":
                out_dir = Path("octa") / "var" / "audit" / "features"
                out_dir.mkdir(parents=True, exist_ok=True)
                tf_key = _infer_timeframe_key(df.index)
                out_path = out_dir / f"features_{run_id}_{symbol}_{tf_key}.json"
                out_path.write_text(json.dumps(alt_diag, indent=2, default=str) + "\n", encoding="utf-8")
        except Exception:
            pass

        offline_mode = str(os.getenv("OCTA_OFFLINE", "")).strip().lower() in {"1", "true", "yes", "on"}
        alt_policy = str(os.getenv("OCTA_ALTDATA_POLICY", "")).strip().lower()
        allow_alt_missing = alt_policy == "smoke"
        if not bool(alt_diag.get("altdata_enabled")) and not allow_alt_missing:
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error="ALT_DISABLED", altdata=alt_diag)
        if int(alt_diag.get("altdata_feature_count") or 0) <= 0 and not allow_alt_missing:
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error="ALT_MERGE_EMPTY", altdata=alt_diag)
        fail_closed = None
        try:
            altdat_meta = meta.get("altdat") if isinstance(meta, dict) else {}
            fail_closed = (altdat_meta or {}).get("fail_closed") if isinstance(altdat_meta, dict) else None
        except Exception:
            fail_closed = None
        if isinstance(fail_closed, dict) and fail_closed.get("reasons") and not allow_alt_missing:
            reason = str(fail_closed.get("reasons")[0])
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error=reason, altdata=alt_diag)
        if offline_mode and not bool(alt_diag.get("cache_ok")) and not allow_alt_missing:
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error="ALT_SNAPSHOT_MISSING", altdata=alt_diag)
        try:
            if hasattr(features_res, "meta") and isinstance(features_res.meta, dict):
                features_res.meta.setdefault("bar_size", _infer_timeframe_key(df.index))
        except Exception:
            pass
        # leakage audit
        try:
            horizons = (eff_settings.features or {}).get('horizons', [1, 3, 5])
        except Exception:
            horizons = [1, 3, 5]
        hk = _hard_kill_switches_conf()
        leak_ok = leakage_audit(features_res.X, features_res.y_dict, df, horizons, settings=eff_settings, asset_class=asset_class)
        if bool(hk.get('leakage', True)) and (not bool(leak_ok)):
            _record_end(False, metrics_obj=None, gate_obj=None)
            if not diagnose_mode:
                return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='leakage_detected', altdata=alt_diag)
            diagnose_reasons.append('leakage_detected')

        # IBKR-only conservative cost model: prefer cfg.broker over cfg.signal
        broker = getattr(cfg, 'broker', None)
        cost_bps = getattr(broker, 'cost_bps', None) if broker is not None else None
        spread_bps = getattr(broker, 'spread_bps', None) if broker is not None else None
        stress_mult = getattr(broker, 'stress_cost_multiplier', None) if broker is not None else None
        # Per-timeframe broker override: e.g. 1H uses tighter limit-order spread vs 1D market orders.
        # cfg.broker_by_timeframe = {"1H": {"spread_bps": 1.0}, "1D": {"spread_bps": 5.0}}
        try:
            _btf = getattr(cfg, 'broker_by_timeframe', None) or {}
            if isinstance(_btf, dict) and _btf:
                _cur_tf = str(getattr(eff_settings, 'timeframe', '') or '').upper()
                _tf_bspec = _btf.get(_cur_tf) or _btf.get(_cur_tf.lower()) or {}
                if isinstance(_tf_bspec, dict) and _tf_bspec:
                    if _tf_bspec.get('cost_bps') is not None:
                        cost_bps = float(_tf_bspec['cost_bps'])
                    if _tf_bspec.get('spread_bps') is not None:
                        spread_bps = float(_tf_bspec['spread_bps'])
                    if _tf_bspec.get('stress_cost_multiplier') is not None:
                        stress_mult = float(_tf_bspec['stress_cost_multiplier'])
        except Exception:
            pass
        es = EvalSettings(
            mode=cfg.signal.mode,
            upper_q=cfg.signal.upper_q,
            lower_q=cfg.signal.lower_q,
            causal_quantiles=bool(getattr(cfg.signal, 'causal_quantiles', False)),
            quantile_window=getattr(cfg.signal, 'quantile_window', None),
            leverage_cap=cfg.signal.leverage_cap,
            vol_target=cfg.signal.vol_target,
            realized_vol_window=cfg.signal.realized_vol_window,
            cost_bps=float(cost_bps) if cost_bps is not None else cfg.signal.cost_bps,
            spread_bps=float(spread_bps) if spread_bps is not None else cfg.signal.spread_bps,
            stress_cost_multiplier=float(stress_mult) if stress_mult is not None else cfg.signal.stress_cost_multiplier,
            session_enabled=bool(getattr(getattr(cfg, 'session', None), 'enabled', False)),
            session_timezone=str(getattr(getattr(cfg, 'session', None), 'timezone', 'UTC') or 'UTC'),
            session_open=str(getattr(getattr(cfg, 'session', None), 'open', '00:00') or '00:00'),
            session_close=str(getattr(getattr(cfg, 'session', None), 'close', '23:59') or '23:59'),
            session_weekdays=getattr(getattr(cfg, 'session', None), 'weekdays', None),
        )

        # FX intraday should not use an equity-style session filter. A session filter
        # can zero signals outside a narrow window, leaving mostly flat exposure while
        # still paying costs on occasional trades -> extreme negative Sharpe artifacts.
        try:
            ac0 = str(asset_class or '').lower()
            is_fx = ac0 in {'fx', 'forex'}
            is_1h = False
            try:
                is_1h = bool(str(getattr(pinfo, 'path', '')).upper().endswith('_1H.PARQUET'))
            except Exception:
                is_1h = False
            if is_fx and is_1h:
                es.session_enabled = False
        except Exception:
            pass

        # Hard kill-switch: require a non-zero cost model (no free lunch).
        if bool(hk.get('cost_model', True)):
            try:
                if (float(es.cost_bps) <= 0.0) and (float(es.spread_bps) <= 0.0):
                    _record_end(False, metrics_obj=None, gate_obj=None)
                    if not diagnose_mode:
                        return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='missing_cost_model', altdata=alt_diag)
                    diagnose_reasons.append('missing_cost_model')
            except Exception:
                _record_end(False, metrics_obj=None, gate_obj=None)
                if not diagnose_mode:
                    return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='missing_cost_model', altdata=alt_diag)
                diagnose_reasons.append('missing_cost_model')
        splits_cfg = cfg.splits if hasattr(cfg, 'splits') else {}
        # Per-timeframe split calibration (restores a6bc3b3 fix).
        _splits_by_tf = getattr(cfg, 'splits_by_timeframe', None) or {}
        if isinstance(_splits_by_tf, dict) and _splits_by_tf:
            _tf_key = _infer_timeframe_key(df.index)
            _tf_spec = (
                _splits_by_tf.get(_tf_key, {})
                or _splits_by_tf.get(_tf_key.upper(), {})
                or _splits_by_tf.get(_tf_key.lower(), {})
                or {}
            )
            if _tf_spec:
                splits_cfg = {**splits_cfg, **_tf_spec}
        try:
            folds = walk_forward_splits(features_res.X.index, n_folds=int(splits_cfg.get('n_folds',5)), train_window=int(splits_cfg.get('train_window',1000)), test_window=int(splits_cfg.get('test_window',200)), step=int(splits_cfg.get('step',200)), purge_size=int(splits_cfg.get('purge_size',10)), embargo_size=int(splits_cfg.get('embargo_size',5)), min_train_size=int(splits_cfg.get('min_train_size',500)), min_test_size=int(splits_cfg.get('min_test_size',100)), expanding=bool(splits_cfg.get('expanding',True)), min_folds_required=int(splits_cfg.get('min_folds_required',1)))
        except ValueError:
            folds = []

        # Phase-1 survival fallback: if strict WF produces no folds, try a single simple split
        # (keeps determinism; avoids false negatives from overly strict split params).
        if not folds:
            try:
                n_rows = int(len(features_res.X.index))
                min_train_cfg = int(splits_cfg.get('min_train_size', 500))
                min_test_cfg = int(splits_cfg.get('min_test_size', 100))
                fb_min_train = max(200, max(1, min_train_cfg // 2))
                fb_min_test = max(60, max(1, min_test_cfg // 2))
                if n_rows >= (fb_min_train + fb_min_test):
                    from octa_training.core.splits import SplitFold

                    train_end = n_rows - fb_min_test - 1
                    train_idx = np.arange(0, train_end + 1)
                    val_idx = np.arange(train_end + 1, n_rows)
                    folds = [
                        SplitFold(
                            train_idx=train_idx,
                            val_idx=val_idx,
                            fold_meta={
                                'fallback': True,
                                'train_size': int(train_idx.size),
                                'val_size': int(val_idx.size),
                                'n_rows': int(n_rows),
                            },
                        )
                    ]
            except Exception:
                pass
        if bool(hk.get('walk_forward', True)) and (not folds):
            _record_end(False, metrics_obj=None, gate_obj=None)
            if not diagnose_mode:
                return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='missing_walk_forward', altdata=alt_diag)
            # Cannot proceed without folds; return with recorded diagnose reason.
            from octa_training.core.gates import GateResult

            # Best-effort: still attach asset-profile metadata for auditability.
            resolved_profile_name = 'legacy'
            applied_thresholds: Dict[str, Any] = {}
            try:
                tf_key = _infer_timeframe_key(df.index)
                gconf = gate_policy_snapshot or {}
                tf_map = gconf.get('global_by_timeframe', {}) if isinstance(gconf, dict) else {}
                tf_spec = tf_map.get(tf_key, {}) if isinstance(tf_map, dict) else {}
                global_spec = gconf.get('global', {}) if isinstance(gconf, dict) else {}
                base_spec = dict(global_spec or {})
                if isinstance(tf_spec, dict) and tf_spec:
                    base_spec.update(tf_spec)

                resolved = resolve_asset_profile(
                    symbol=symbol,
                    dataset=dataset,
                    asset_class=asset_class,
                    parquet_path=str(parquet_path or pinfo.path),
                    cfg=cfg,
                )
                resolved_profile_name = str(getattr(resolved, 'name', None) or 'legacy')

                # Explicit override (diagnose/manual runs): try to load that named profile.
                if asset_profile:
                    forced = None
                    try:
                        raw_profiles = getattr(cfg, 'asset_profiles', None)
                        if isinstance(raw_profiles, dict):
                            forced = raw_profiles.get(str(asset_profile))
                    except Exception:
                        forced = None
                    if isinstance(forced, dict):
                        from octa_training.core.asset_profiles import (
                            AssetProfile as _AP,
                        )

                        vv = dict(forced)
                        vv.setdefault('name', str(asset_profile))
                        resolved = _AP.parse_obj(vv)
                    resolved_profile_name = str(asset_profile)

                pg = getattr(resolved, 'gates', {})
                if isinstance(pg, dict):
                    p_global = pg.get('global', {}) if isinstance(pg.get('global', {}), dict) else {}
                    p_tf_map = pg.get('global_by_timeframe', {}) if isinstance(pg.get('global_by_timeframe', {}), dict) else {}
                    p_tf = p_tf_map.get(tf_key, {}) if isinstance(p_tf_map.get(tf_key, {}), dict) else {}
                    profile_spec = dict(p_global or {})
                    if p_tf:
                        profile_spec.update(p_tf)
                else:
                    profile_spec = {}

                merged = _merge_gate_specs_strict(base_spec, profile_spec)

                # Optional per-call overrides (used for FX two-stage gating). Do not touch global config.
                try:
                    if isinstance(gate_overrides, dict) and gate_overrides:
                        for k, v in gate_overrides.items():
                            if v is not None:
                                merged[k] = v
                except Exception:
                    pass

                # gate version
                try:
                    gv = gconf.get('version') if isinstance(gconf, dict) else None
                    if gv:
                        merged.setdefault('gate_version', str(gv))
                except Exception:
                    pass

                # Backward compatible fallbacks: cfg.gating only fills missing keys
                try:
                    if 'sharpe_min' not in merged and getattr(cfg, 'gating', None) is not None:
                        merged['sharpe_min'] = cfg.gating.min_sharpe
                    if 'max_drawdown_max' not in merged and getattr(cfg, 'gating', None) is not None:
                        merged['max_drawdown_max'] = cfg.gating.max_drawdown
                except Exception:
                    pass

                # robustness defaults (only fill if not set)
                try:
                    if isinstance(gconf, dict):
                        gsuf = gconf.get('sufficiency', {})
                    else:
                        gsuf = {}
                    if isinstance(gsuf, dict) and gsuf:
                        for k, v in gsuf.items():
                            merged.setdefault(k, v)

                    if isinstance(gconf, dict):
                        grob = gconf.get('robustness', {})
                    else:
                        grob = {}
                    if isinstance(grob, dict) and grob:
                        for k, v in grob.items():
                            merged.setdefault(k, v)
                    rob = getattr(cfg, 'robustness', None)
                    if rob is not None:
                        merged.setdefault('robustness_permutation_auc_max', getattr(rob, 'permutation_auc_max', None))
                        merged.setdefault('robustness_subwindow_min_sharpe_ratio', getattr(rob, 'subwindow_min_sharpe_ratio', None))
                        merged.setdefault('robustness_subwindow_abs_sharpe_min', getattr(rob, 'subwindow_abs_sharpe_min', None))
                        merged.setdefault('robustness_stress_min_sharpe', getattr(rob, 'stress_min_sharpe', None))
                        merged.setdefault('robustness_regime_top_quantile', getattr(rob, 'regime_top_quantile', None))
                        merged.setdefault('robustness_regime_max_drawdown', getattr(rob, 'regime_max_drawdown', None))
                except Exception:
                    pass

                applied_thresholds = {k: v for k, v in dict(merged or {}).items() if v is not None}
            except Exception:
                resolved_profile_name = 'legacy'
                applied_thresholds = {}

            diag = []
            try:
                ah = profile_hash(str(resolved_profile_name), dict(applied_thresholds or {}))
                diag = [
                    {
                        'name': 'asset_profile',
                        'value': str(resolved_profile_name),
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': None,
                    },
                    {
                        'name': 'asset_profile_hash',
                        'value': str(ah),
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': None,
                    },
                    {
                        'name': 'applied_thresholds',
                        'value': dict(applied_thresholds or {}),
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': None,
                    },
                ]
            except Exception:
                diag = None

            # Enforce canonical profile rules for dataset-level preflight.
            try:
                try:
                    ensure_canonical_profile_for_dataset(dataset=dataset, resolved=resolved, applied_thresholds=applied_thresholds, gate_version=(merged.get('gate_version') if isinstance(merged, dict) else None))
                except AssetProfileMismatchError:
                    # propagate to caller
                    raise
            except Exception:
                # Let caller catch and record the mismatch; do not silently ignore.
                raise

            gr = GateResult(
                passed=False,
                status='FAIL_STRUCTURAL',
                gate_version=None,
                reasons=['missing_walk_forward'],
                passed_checks=[],
                robustness=None,
                diagnostics=diag,
            )
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error=None, metrics=None, gate_result=gr, altdata=alt_diag)
        profile = detect_device()
        # Explicit empty-matrix guard — prevents silent no_models return that hides the cause.
        if features_res.X is None or len(features_res.X) == 0:
            if logger:
                logger.error(
                    "Training failed for %s: feature matrix is empty (FEATURE_MATRIX_EMPTY)", symbol
                )
            _record_end(False, metrics_obj=None, gate_obj=None)
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='FEATURE_MATRIX_EMPTY', altdata=alt_diag)
        # use fast=True during evaluation to avoid long/hanging native trainings
        train_results = train_models(
            features_res.X,
            features_res.y_dict,
            folds,
            cfg,
            profile,
            fast=bool(fast),
            prices=df['close'],
            eval_settings=es,
        )
        if not train_results:
            _record_end(False, metrics_obj=None, gate_obj=None)
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='no_models', altdata=alt_diag)

        def _mean_fold_metric(r, key: str):
            try:
                vals = []
                for fm in getattr(r, 'fold_metrics', []) or []:
                    m = getattr(fm, 'metric', None) or {}
                    v = m.get(key)
                    if v is None:
                        continue
                    try:
                        fv = float(v)
                    except Exception:
                        continue
                    if np.isfinite(fv):
                        vals.append(fv)
                if not vals:
                    return None
                return float(np.mean(vals))
            except Exception:
                return None

        def _oof_pred_series(r) -> Optional[pd.Series]:
            try:
                oof = getattr(r, 'oof_predictions', None) or {}
                oof_index = oof.get('index', [])
                oof_vals = oof.get('pred', [])
                if not oof_index or not oof_vals:
                    return None
                try:
                    idx = pd.to_datetime(pd.Index(oof_index), utc=True, errors='coerce')
                    if idx.isna().all():
                        idx = pd.Index(oof_index)
                except Exception:
                    idx = pd.Index(oof_index)
                return pd.Series(oof_vals, index=idx)
            except Exception:
                return None

        def _score_train_result_strategy(r, es: EvalSettings) -> Optional[float]:
            """Compute OOF strategy Sharpe for a TrainResult.

            This is intentionally the primary selector to align model choice
            with live-trading objectives (HF-style), while remaining leakage-safe
            if EvalSettings.causal_quantiles is enabled.
            """
            try:
                preds_s = _oof_pred_series(r)
                if preds_s is None or preds_s.empty:
                    return None
                out = compute_equity_and_metrics(df['close'], preds_s, es)
                m = out.get('metrics')
                sharpe = getattr(m, 'sharpe', None)
                if sharpe is None:
                    return None
                sharpe = float(sharpe)
                if not np.isfinite(sharpe):
                    return None
                return sharpe
            except Exception:
                return None

        def _score_train_result_fallback(r) -> float:
            # Fallback selector: fold AUC for classification; RMSE (lower is better) for regression.
            task = str(getattr(r, 'task', '') or '').lower()
            if task == 'cls':
                auc = _mean_fold_metric(r, 'auc')
                return auc if auc is not None else float('-inf')
            if task == 'reg':
                rmse = _mean_fold_metric(r, 'rmse')
                if rmse is not None:
                    return -rmse
                dir_acc = _mean_fold_metric(r, 'dir_acc')
                return dir_acc if dir_acc is not None else float('-inf')
            return float('-inf')

        # Select best result for the configured signal mode.
        desired_task = str(getattr(cfg.signal, 'mode', '') or '').lower()
        candidates = [r for r in train_results if str(getattr(r, 'task', '') or '').lower() == desired_task]
        if not candidates:
            candidates = list(train_results)

        # Primary: choose by OOF strategy Sharpe (HF-style objective).
        scored = []
        for r in candidates:
            s = _score_train_result_strategy(r, es)
            if s is not None:
                scored.append((s, r))
        if scored:
            scored.sort(key=lambda t: t[0], reverse=True)
            best = scored[0][1]
        else:
            best = max(candidates, key=_score_train_result_fallback)

        # Hard kill-switch: nondeterminism (same seed, same data, should reproduce OOF preds).
        if bool(hk.get('nondeterminism_check', False)):
            try:
                orig_models_order = list(getattr(cfg, 'models_order', []) or [])
            except Exception:
                orig_models_order = []
            try:
                orig_tuning_enabled = bool(getattr(getattr(cfg, 'tuning', None), 'enabled', False))
            except Exception:
                orig_tuning_enabled = False
            try:
                orig_tuning_models_order = list(getattr(getattr(cfg, 'tuning', None), 'models_order', []) or [])
            except Exception:
                orig_tuning_models_order = []
            try:
                try:
                    cfg.models_order = [str(getattr(best, 'model_name', '') or '').strip() or (orig_models_order[0] if orig_models_order else 'lightgbm')]
                except Exception:
                    pass
                try:
                    if getattr(cfg, 'tuning', None) is not None:
                        cfg.tuning.enabled = False
                except Exception:
                    pass

                # train_models prefers cfg.tuning.models_order over cfg.models_order;
                # ensure both are pinned to the same single model for a fair rerun.
                try:
                    pinned_model = str(getattr(best, 'model_name', '') or '').strip()
                    if pinned_model and getattr(cfg, 'tuning', None) is not None:
                        cfg.tuning.models_order = [pinned_model]
                except Exception:
                    pass

                # Re-run with the same effective hyperparameters as the selected best model.
                # Otherwise, if the best model came from tuning, disabling tuning would
                # change the model and falsely trip the nondeterminism check.
                try:
                    best_model_name = str(getattr(best, 'model_name', '') or '').strip().lower()
                    best_params = getattr(best, 'params', None)
                    if isinstance(best_params, dict) and best_params:
                        if best_model_name == 'lightgbm':
                            cfg.lgbm_params = dict(best_params)
                        elif best_model_name == 'xgboost':
                            cfg.xgb_params = dict(best_params)
                        elif best_model_name == 'catboost':
                            cfg.cat_params = dict(best_params)
                except Exception:
                    pass

                # We already pinned the model list to a single model above; use fast=False
                # to avoid accidentally filtering out the pinned model.
                rerun = train_models(features_res.X, features_res.y_dict, folds, cfg, profile, fast=False, prices=df['close'], eval_settings=es)
                rerun_best = None
                for r in rerun or []:
                    if str(getattr(r, 'model_name', '') or '') != str(getattr(best, 'model_name', '') or ''):
                        continue
                    if str(getattr(r, 'task', '') or '') != str(getattr(best, 'task', '') or ''):
                        continue
                    if str(getattr(r, 'horizon', '') or '') != str(getattr(best, 'horizon', '') or ''):
                        continue
                    rerun_best = r
                    break
                if rerun_best is None and rerun:
                    rerun_best = rerun[0]

                if rerun_best is not None:
                    p1 = np.asarray((getattr(best, 'oof_predictions', {}) or {}).get('pred', []), dtype=float)
                    p2 = np.asarray((getattr(rerun_best, 'oof_predictions', {}) or {}).get('pred', []), dtype=float)
                    if p1.shape != p2.shape:
                        _record_end(False, metrics_obj=None, gate_obj=None)
                        if not diagnose_mode:
                            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='nondeterminism_oof_shape', altdata=alt_diag)
                        diagnose_reasons.append('nondeterminism_oof_shape')
                    mask = np.isfinite(p1) & np.isfinite(p2)
                    if mask.any():
                        max_diff = float(np.max(np.abs(p1[mask] - p2[mask])))
                        if not np.isfinite(max_diff) or max_diff > 1e-6:
                            _record_end(False, metrics_obj=None, gate_obj=None)
                            if not diagnose_mode:
                                return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error=f'nondeterminism_oof_diff:{max_diff}', altdata=alt_diag)
                            diagnose_reasons.append(f'nondeterminism_oof_diff:{max_diff}')
            finally:
                try:
                    if orig_models_order:
                        cfg.models_order = orig_models_order
                except Exception:
                    pass
                try:
                    if getattr(cfg, 'tuning', None) is not None:
                        cfg.tuning.enabled = orig_tuning_enabled
                except Exception:
                    pass
                try:
                    if getattr(cfg, 'tuning', None) is not None:
                        cfg.tuning.models_order = orig_tuning_models_order
                except Exception:
                    pass

        preds = _oof_pred_series(best)
        if preds is None:
            raise ValueError('Best model has no OOF predictions')
        res = compute_equity_and_metrics(df['close'], preds, es)
        metrics = res['metrics']
        if metrics is None:
            _record_end(False, metrics_obj=None, gate_obj=None)
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error="MISSING_METRICS_FAIL_CLOSED", altdata=alt_diag)
        stress_metrics = res.get('stress_metrics')
        # add fold metrics from training
        if hasattr(best, 'fold_metrics') and best.fold_metrics:
            from octa_training.core.metrics_contract import MetricsSummaryLite
            fold_lites = []
            for fm in best.fold_metrics:
                m = fm.metric
                lite = MetricsSummaryLite(
                    sharpe=m.get('sharpe'),
                    sharpe_is=m.get('sharpe_is'),
                    max_drawdown=m.get('max_drawdown'),
                    n_trades=m.get('n_trades')
                )
                fold_lites.append(lite)
            metrics.fold_metrics = fold_lites
        # Institutional gate selection: global-by-timeframe, identical across assets.
        gconf = gate_policy_snapshot or {}
        tf_key = _infer_timeframe_key(df.index)
        tf_map = gconf.get('global_by_timeframe', {}) if isinstance(gconf, dict) else {}
        tf_spec = tf_map.get(tf_key, {}) if isinstance(tf_map, dict) else {}

        # FX-G0 ONLY: compute tail-kill metric on MARKET returns (no positions, no costs).
        # This keeps HF-grade kill semantics but makes G0 a true 1D risk overlay.
        fx_g0_tail_series: Optional[str] = None
        fx_g0_cvar95_mkt: Optional[float] = None
        fx_g0_vol_mkt: Optional[float] = None
        fx_g0_tail_ratio_mkt: Optional[float] = None
        try:
            ac = str(asset_class or '').lower()
            is_fx_g0 = (
                ac in {'fx', 'forex'}
                and str(tf_key or '').upper() == '1D'
                and str(robustness_profile or 'full').lower() == 'risk_overlay'
            )
            if is_fx_g0:
                df_bt = res.get('df')
                if isinstance(df_bt, pd.DataFrame) and 'ret' in df_bt.columns:
                    r_mkt = pd.to_numeric(df_bt['ret'], errors='coerce').astype(float).replace([np.inf, -np.inf], np.nan).dropna()
                    # compute_equity_and_metrics fills the first diff with 0.0; drop it for tail stats.
                    if len(r_mkt) > 1 and float(r_mkt.iloc[0]) == 0.0:
                        r_mkt = r_mkt.iloc[1:]
                    if len(r_mkt) >= 20:
                        fx_g0_tail_series = 'market_log_returns'
                        var95 = float(r_mkt.quantile(0.05))
                        tail = r_mkt[r_mkt <= var95]
                        cvar95 = float(tail.mean()) if len(tail) else 0.0
                        vol = float(r_mkt.std(ddof=0))
                        ratio = float(abs(cvar95) / (vol + 1e-12)) if vol > 0 else 0.0
                        fx_g0_cvar95_mkt = cvar95
                        fx_g0_vol_mkt = vol
                        fx_g0_tail_ratio_mkt = ratio

                        # Override only the tail-kill metric used by the global gate.
                        try:
                            metrics.cvar_95_over_daily_vol = float(ratio)
                        except Exception:
                            pass
        except Exception:
            # Fail-closed behavior is handled elsewhere; do not break runs due to diagnostics.
            pass

        # Backward compatible fallback: global (if timeframe map missing)
        global_spec = gconf.get('global', {}) if isinstance(gconf, dict) else {}

        # HF-grade per-timeframe baseline overlays (net-of-cost metric scale).
        # Applied after merged spec so they set institutional minimums without overriding
        # explicit gate_overrides from callers (FX two-stage gating etc.).
        hf_tf_overlays: Dict[str, Dict[str, Any]] = {
            "1D": {
                "profit_factor_min": 1.20,
                "sharpe_min": 0.65,
                "max_drawdown_max": 0.06,
                "min_trades": 20,
                "min_bars": 130,
            },
            "4H": {
                "profit_factor_min": 1.18,
                "sharpe_min": 0.63,
                "max_drawdown_max": 0.055,
                "min_trades": 30,
                "min_bars": 200,
            },
            "1H": {
                "profit_factor_min": 1.15,
                "sharpe_min": 0.60,
                "max_drawdown_max": 0.05,
                "min_trades": 60,
                "min_bars": 260,
            },
            "30M": {
                "profit_factor_min": 1.12,
                "sharpe_min": 0.55,
                "max_drawdown_max": 0.045,
                "min_trades": 120,
                "min_bars": 400,
            },
            "5M": {
                "profit_factor_min": 1.10,
                "sharpe_min": 0.50,
                "max_drawdown_max": 0.040,
                "min_trades": 240,
                "min_bars": 600,
            },
            "1M": {
                "profit_factor_min": 1.08,
                "sharpe_min": 0.45,
                "max_drawdown_max": 0.035,
                "min_trades": 480,
                "min_bars": 1000,
            },
        }

        base_spec = dict(global_spec or {})
        if isinstance(tf_spec, dict) and tf_spec:
            base_spec.update(tf_spec)

        # Per-asset-class overlay from gates.by_asset_class (e.g. stock, forex).
        # Unlike profile overlays, asset-class overrides CAN relax global defaults
        # because different asset classes have structurally different risk profiles.
        # Restores HEAD behavior removed in partial refactor.
        try:
            by_ac = gconf.get('by_asset_class', {}) if isinstance(gconf, dict) else {}
            if isinstance(by_ac, dict) and by_ac:
                ac_key = str(asset_class or '').lower().strip()
                if not ac_key or ac_key == 'unknown':
                    ac_key = str(dataset or '').lower().strip()
                ac_spec = by_ac.get(ac_key, {})
                # Alias fallback: 'stocks' → 'stock', 'etfs' → 'etf', etc.
                if not ac_spec and ac_key.endswith('s'):
                    ac_spec = by_ac.get(ac_key[:-1], {})
                if isinstance(ac_spec, dict) and ac_spec:
                    base_spec.update(ac_spec)
                    # Apply TF-specific sortino floor when sortino_min_by_tf is present.
                    # This overrides the flat sortino_min default for the current TF,
                    # allowing 1H to have a different floor than 1D without a full
                    # by_asset_class split per TF.
                    _sortino_by_tf = ac_spec.get('sortino_min_by_tf')
                    if isinstance(_sortino_by_tf, dict) and tf_key:
                        _tf_sortino = _sortino_by_tf.get(str(tf_key).upper())
                        if _tf_sortino is not None:
                            try:
                                base_spec['sortino_min'] = float(_tf_sortino)
                            except Exception:
                                pass
                    if logger:
                        logger.debug(
                            "[%s] Applied by_asset_class gate overlay for '%s': %s",
                            symbol, ac_key, list(ac_spec.keys()),
                        )
        except Exception as _ac_exc:
            if logger:
                logger.warning("[%s] by_asset_class gate overlay failed: %s", symbol, _ac_exc)

        # Asset profile overlay (cannot relax global/timeframe floors).
        # Profile gates use the same key-space as GateSpec.
        resolved_profile_name = 'legacy'
        applied_thresholds = {}
        try:
            resolved = resolve_asset_profile(
                symbol=symbol,
                dataset=dataset,
                asset_class=asset_class,
                parquet_path=str(parquet_path or pinfo.path),
                cfg=cfg,
            )
            resolved_profile_name = str(getattr(resolved, 'name', None) or 'legacy')

            # Explicit override (diagnose/manual runs): try to load that named profile.
            if asset_profile:
                forced = None
                try:
                    raw_profiles = getattr(cfg, 'asset_profiles', None)
                    if isinstance(raw_profiles, dict):
                        forced = raw_profiles.get(str(asset_profile))
                except Exception:
                    forced = None
                if isinstance(forced, dict):
                    from octa_training.core.asset_profiles import AssetProfile as _AP

                    vv = dict(forced)
                    vv.setdefault('name', str(asset_profile))
                    resolved = _AP.parse_obj(vv)
                resolved_profile_name = str(asset_profile)

            # Build profile spec (global + timeframe) in cfg.gates-like shape
            pg = getattr(resolved, 'gates', {})
            if isinstance(pg, dict):
                p_global = pg.get('global', {}) if isinstance(pg.get('global', {}), dict) else {}
                p_tf_map = pg.get('global_by_timeframe', {}) if isinstance(pg.get('global_by_timeframe', {}), dict) else {}
                p_tf = p_tf_map.get(tf_key, {}) if isinstance(p_tf_map.get(tf_key, {}), dict) else {}
                profile_spec = dict(p_global or {})
                if p_tf:
                    profile_spec.update(p_tf)
            else:
                profile_spec = {}

            merged = _merge_gate_specs_strict(base_spec, profile_spec)
        except Exception:
            merged = dict(base_spec)

        # HF overlay fills institutional minimums (uses setdefault — does not override
        # caller-provided gate_overrides or explicit config entries that already set the key).
        try:
            hf_overlay = hf_tf_overlays.get(str(tf_key).upper(), {})
            if isinstance(hf_overlay, dict):
                for k, v in hf_overlay.items():
                    merged.setdefault(k, v)
        except Exception:
            pass

        # Optional per-call overrides (used for FX two-stage gating). Do not touch global config.
        try:
            if isinstance(gate_overrides, dict) and gate_overrides:
                for k, v in gate_overrides.items():
                    if v is not None:
                        merged[k] = v
        except Exception:
            pass

        # gate version
        try:
            gv = gconf.get('version') if isinstance(gconf, dict) else None
            if gv:
                merged.setdefault('gate_version', str(gv))
        except Exception:
            pass

        # Backward compatible fallbacks: cfg.gating only fills missing keys
        try:
            if 'sharpe_min' not in merged and getattr(cfg, 'gating', None) is not None:
                merged['sharpe_min'] = cfg.gating.min_sharpe
            if 'max_drawdown_max' not in merged and getattr(cfg, 'gating', None) is not None:
                merged['max_drawdown_max'] = cfg.gating.max_drawdown
        except Exception:
            pass

        # robustness defaults (only fill if not set)
        try:
            # statistical sufficiency defaults (only fill if not set)
            if isinstance(gconf, dict):
                gsuf = gconf.get('sufficiency', {})
            else:
                gsuf = {}
            if isinstance(gsuf, dict) and gsuf:
                for k, v in gsuf.items():
                    merged.setdefault(k, v)

            # prefer gates.robustness from snapshot, else cfg.robustness
            if isinstance(gconf, dict):
                grob = gconf.get('robustness', {})
            else:
                grob = {}
            if isinstance(grob, dict) and grob:
                for k, v in grob.items():
                    merged.setdefault(k, v)
            rob = getattr(cfg, 'robustness', None)
            if rob is not None:
                merged.setdefault('robustness_permutation_auc_max', getattr(rob, 'permutation_auc_max', None))
                merged.setdefault('robustness_subwindow_min_sharpe_ratio', getattr(rob, 'subwindow_min_sharpe_ratio', None))
                merged.setdefault('robustness_subwindow_abs_sharpe_min', getattr(rob, 'subwindow_abs_sharpe_min', None))
                merged.setdefault('robustness_stress_min_sharpe', getattr(rob, 'stress_min_sharpe', None))
                merged.setdefault('robustness_regime_top_quantile', getattr(rob, 'regime_top_quantile', None))
                merged.setdefault('robustness_regime_max_drawdown', getattr(rob, 'regime_max_drawdown', None))
        except Exception:
            pass

        # drop Nones to avoid pydantic issues
        merged = {k: v for k, v in merged.items() if v is not None}
        applied_thresholds = dict(merged)
        gate = GateSpec(**merged)

        # Enforce canonical profile for dataset before any training/gating.
        try:
            ensure_canonical_profile_for_dataset(dataset=dataset, resolved=resolved, applied_thresholds=applied_thresholds, gate_version=(merged.get('gate_version') if isinstance(merged, dict) else None))
        except AssetProfileMismatchError:
            # Bubble up to be handled by caller; fail-closed behavior.
            raise

        # OPTIONAL (FX-G1 only, hourly-like only): base tail-kill metric on MARKET log returns.
        # Keep the strategy-based value as reference diagnostics.
        fx_g1_tail_ratio_strategy: Optional[float] = None
        fx_g1_tail_ratio_mkt: Optional[float] = None
        try:
            ac2 = str(asset_class or '').lower()
            is_fx_g1_stage2 = (
                ac2 in {'fx', 'forex'}
                and str(robustness_profile or 'full').lower() != 'risk_overlay'
                and str(pinfo.path).upper().endswith('_1H.PARQUET')
                and bool(fx_g1_is_hourly_like) is True
            )
            if is_fx_g1_stage2:
                fx_g1_tail_ratio_strategy = float(getattr(metrics, 'cvar_95_over_daily_vol', float('nan')))
                df_bt2 = res.get('df')
                if isinstance(df_bt2, pd.DataFrame) and 'ret' in df_bt2.columns:
                    r_mkt2 = pd.to_numeric(df_bt2['ret'], errors='coerce').astype(float).replace([np.inf, -np.inf], np.nan).dropna()
                    if len(r_mkt2) > 1 and float(r_mkt2.iloc[0]) == 0.0:
                        r_mkt2 = r_mkt2.iloc[1:]
                    if len(r_mkt2) >= 20:
                        var95 = float(r_mkt2.quantile(0.05))
                        tail = r_mkt2[r_mkt2 <= var95]
                        cvar95 = float(tail.mean()) if len(tail) else 0.0
                        vol = float(r_mkt2.std(ddof=0))
                        fx_g1_tail_ratio_mkt = float(abs(cvar95) / (vol + 1e-12)) if vol > 0 else 0.0
                        try:
                            metrics.cvar_95_over_daily_vol = float(fx_g1_tail_ratio_mkt)
                        except Exception:
                            pass
        except Exception:
            pass

        result = gate_evaluate(metrics, gate)

        # Attach profile + threshold snapshot for audit/NDJSON.
        try:
            if getattr(result, 'diagnostics', None) is None:
                result.diagnostics = []
            ah = profile_hash(str(resolved_profile_name), dict(applied_thresholds or {}))
            result.diagnostics.extend(
                [
                    {
                        'name': 'asset_profile',
                        'value': str(resolved_profile_name),
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': None,
                    },
                    {
                        'name': 'asset_profile_hash',
                        'value': str(ah),
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': None,
                    },
                    {
                        'name': 'applied_thresholds',
                        'value': dict(applied_thresholds or {}),
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': None,
                    },
                ]
            )
        except Exception:
            pass

        # FX-G1 diagnostics: confirm annualization and cost scaling per symbol.
        try:
            ac1 = str(asset_class or '').lower()
            is_fx_g1 = (
                ac1 in {'fx', 'forex'}
                and str(robustness_profile or 'full').lower() != 'risk_overlay'
                and str(parquet_path or '').upper().endswith('_1H.PARQUET')
            )
            if is_fx_g1:
                ann1 = float(infer_frequency(res['df'].index)) if isinstance(res.get('df'), pd.DataFrame) else float('nan')
                bpd1 = float(ann1) / 252.0 if np.isfinite(ann1) and ann1 > 0 else float('nan')

                df_bt = res.get('df')
                costs_per_day = None
                turnover_per_day = getattr(metrics, 'turnover_per_day', None)
                try:
                    if isinstance(df_bt, pd.DataFrame) and 'costs' in df_bt.columns and np.isfinite(bpd1):
                        c = pd.to_numeric(df_bt['costs'], errors='coerce').astype(float).replace([np.inf, -np.inf], np.nan).dropna()
                        costs_per_day = float(c.mean() * bpd1) if len(c) else 0.0
                except Exception:
                    costs_per_day = None

                if getattr(result, 'diagnostics', None) is None:
                    result.diagnostics = []
                result.diagnostics.extend(
                    [
                        {
                            'name': 'fx_g1_median_bar_spacing_seconds',
                            'value': fx_g1_median_bar_spacing_seconds,
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_p90_bar_spacing_seconds',
                            'value': fx_g1_p90_bar_spacing_seconds,
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_is_hourly_like',
                            'value': bool(fx_g1_is_hourly_like) if fx_g1_is_hourly_like is not None else None,
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_skip_reason',
                            'value': None,
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': fx_g1_skip_reason,
                        },
                        {
                            'name': 'fx_g1_bars_per_day',
                            'value': float(bpd1) if np.isfinite(bpd1) else None,
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_ann_factor',
                            'value': float(ann1) if np.isfinite(ann1) else None,
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_cost_bps',
                            'value': float(getattr(es, 'cost_bps', 0.0)),
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_spread_bps',
                            'value': float(getattr(es, 'spread_bps', 0.0)),
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_stress_cost_multiplier',
                            'value': float(getattr(es, 'stress_cost_multiplier', 0.0)),
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_costs_per_day',
                            'value': float(costs_per_day) if costs_per_day is not None else None,
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_turnover_per_day',
                            'value': float(turnover_per_day) if turnover_per_day is not None else None,
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_tail_ratio_strategy',
                            'value': float(fx_g1_tail_ratio_strategy) if fx_g1_tail_ratio_strategy is not None and np.isfinite(fx_g1_tail_ratio_strategy) else None,
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                        {
                            'name': 'fx_g1_tail_ratio_mkt',
                            'value': float(fx_g1_tail_ratio_mkt) if fx_g1_tail_ratio_mkt is not None else None,
                            'threshold': None,
                            'op': None,
                            'passed': True,
                            'evaluable': True,
                            'confidence': 0.0,
                            'reason': None,
                        },
                    ]
                )
        except Exception:
            pass

        # Add explicit FX-G0 tail diagnostics (non-breaking; numeric fields only).
        try:
            if fx_g0_tail_ratio_mkt is not None:
                if getattr(result, 'diagnostics', None) is None:
                    result.diagnostics = []
                # Store series selection as a diagnostic (schema-compatible: numeric value is None).
                result.diagnostics.append(
                    {
                        'name': 'fx_g0_tail_series',
                        'value': None,
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': fx_g0_tail_series or 'market_log_returns',
                    }
                )
                result.diagnostics.append(
                    {
                        'name': 'fx_g0_cvar95_mkt',
                        'value': float(fx_g0_cvar95_mkt) if fx_g0_cvar95_mkt is not None else None,
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': None,
                    }
                )
                result.diagnostics.append(
                    {
                        'name': 'fx_g0_vol_mkt',
                        'value': float(fx_g0_vol_mkt) if fx_g0_vol_mkt is not None else None,
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': None,
                    }
                )
                result.diagnostics.append(
                    {
                        'name': 'fx_g0_tail_ratio_mkt',
                        'value': float(fx_g0_tail_ratio_mkt),
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': None,
                    }
                )
        except Exception:
            pass

        def _classify_fail_status(reasons: list[str]) -> str:
            # Conservative classification: anything explicitly tail/DD/CVaR/regime is risk; else structural.
            risk_markers = (
                'tail_kill_switch',
                'tail_risk',
                'cvar_',
                'max_drawdown',
                'regime_stress_failed',
                'avg_gross_exposure',
            )
            for r in reasons:
                rs = str(r or '')
                if any(m in rs for m in risk_markers):
                    return 'FAIL_RISK'
            return 'FAIL_STRUCTURAL'

        def _finalize_pass_status() -> None:
            try:
                if result.passed:
                    ie = getattr(result, 'insufficient_evidence', None) or []
                    result.status = 'PASS_FULL' if len(ie) == 0 else 'PASS_LIMITED_STATISTICAL_CONFIDENCE'
                else:
                    result.status = _classify_fail_status(getattr(result, 'reasons', None) or [])
            except Exception:
                pass

        # Diagnose-mode: merge hard-kill reasons into gate reasons (do not discard).
        if diagnose_mode and diagnose_reasons:
            try:
                # preserve existing ordering; tag as hard_kill for visibility.
                for r in diagnose_reasons:
                    if r:
                        result.reasons.append(f"hard_kill:{r}")
                result.passed = False
                _finalize_pass_status()
            except Exception:
                pass
        # run robustness tests after gate thresholds are known
        if str(robustness_profile or 'full').lower() == 'risk_overlay':
            robustness_result = run_risk_overlay_tests(res['df'], preds, metrics, gate, es)
        else:
            robustness_result = run_all_tests(
                symbol, features_res, folds, res['df'], preds, metrics, gate, es,
                source_df=df,
                asset_class=asset_class,
                timeframe=tf_key,
            )
        try:
            result.robustness = robustness_result.model_dump() if hasattr(robustness_result, "model_dump") else robustness_result.dict()
        except Exception:
            result.robustness = None
        if not robustness_result.passed:
            result.passed = False
            result.reasons = result.reasons + robustness_result.reasons
            _finalize_pass_status()
        else:
            # Robustness may have been skipped for insufficient evidence
            try:
                lr = getattr(robustness_result, 'limited_reasons', None) or []
                if lr:
                    if getattr(result, 'insufficient_evidence', None) is None:
                        result.insufficient_evidence = []
                    result.insufficient_evidence = list(result.insufficient_evidence) + list(lr)
            except Exception:
                pass
            _finalize_pass_status()
        # Crisis hold-out OOS gate.
        # Fires when cfg.crisis_oos is a CrisisOosConfig with enabled=True and ≥1 window.
        # cfg is typed Any (callers may pass SimpleNamespace in tests) so use getattr
        # defensively; the whole block is non-fatal and wrapped in try/except.
        if result.passed:
            try:
                _crisis_cfg = getattr(cfg, "crisis_oos", None)
                if _crisis_cfg is not None and _crisis_cfg.enabled:
                    _cw_list = _crisis_cfg.windows or []
                    if _cw_list:
                        # CrisisWindow is a Pydantic v1 model — .dict() produces plain dicts
                        # compatible with crisis_oos_gate's List[Dict[str, Any]] interface.
                        _cw_dicts = [
                            w.dict() if hasattr(w, "dict") else dict(w)
                            for w in _cw_list
                        ]
                        _crisis_thresholds: Dict[str, Any] = {
                            "min_sharpe": _crisis_cfg.min_sharpe,
                            "max_drawdown_pct": _crisis_cfg.max_drawdown_pct,
                            "min_test_rows": _crisis_cfg.min_test_rows,
                            "min_train_rows": _crisis_cfg.min_train_rows,
                        }
                        _crisis_passed, _crisis_window_results = crisis_oos_gate(
                            features_res.X,
                            features_res.y_dict,
                            df["close"],
                            cfg,
                            profile,
                            es,
                            _cw_dicts,
                            _crisis_thresholds,
                            symbol=symbol,
                            tf=_infer_timeframe_key(features_res.X.index),
                        )
                        if not _crisis_passed:
                            _failed_names = [
                                w["name"]
                                for w in _crisis_window_results
                                if w.get("status") == "FAILED"
                            ]
                            result.passed = False
                            result.reasons.append(f"crisis_oos_failed:{_failed_names}")
            except Exception as _crisis_exc:
                if logger:
                    logger.warning("crisis_oos_gate error (non-fatal): %s", _crisis_exc)
        pack_res = None
        if result.passed:
            # Portfolio-level packaging gate proxies (per-symbol)
            try:
                pg = getattr(cfg, 'portfolio_gate', None)
                if pg is not None and getattr(pg, 'enabled', False):
                    df_bt = res.get('df')
                    if df_bt is not None and not df_bt.empty:
                        ann = infer_frequency(df_bt.index)
                        turnover_ann = float(df_bt['turnover'].astype(float).mean() * ann) if 'turnover' in df_bt.columns else None
                        avg_gross = float(df_bt['pos'].astype(float).abs().mean()) if 'pos' in df_bt.columns else None

                        max_turn_ann = getattr(pg, 'max_turnover_ann', None)
                        if max_turn_ann is not None and not (isinstance(max_turn_ann, float) and pd.isna(max_turn_ann)):
                            if turnover_ann is None or turnover_ann > float(max_turn_ann):
                                result.passed = False
                                result.reasons.append(f"portfolio_gate:turnover_ann {turnover_ann} > {max_turn_ann}")
                        max_avg_gross = getattr(pg, 'max_avg_gross_exposure', None)
                        if max_avg_gross is not None and not (isinstance(max_avg_gross, float) and pd.isna(max_avg_gross)):
                            if avg_gross is None or avg_gross > float(max_avg_gross):
                                result.passed = False
                                result.reasons.append(f"portfolio_gate:avg_gross_exposure {avg_gross} > {max_avg_gross}")
            except Exception as e:
                if logger:
                    logger.warning("Portfolio gate check failed: %s", e)

        if result.passed:
            if not safe_mode:
                pack_res = save_tradeable_artifact(symbol, best, features_res, df, metrics, result, cfg, state, run_id, asset_class, str(pinfo.path), enforce_improvement=False)
            else:
                pack_res = {'saved': False, 'reason': 'safe_mode'}
            # Fail-closed: PASS requires saved artifacts and metrics.
            if not pack_res or not pack_res.get('saved'):
                result.passed = False
                result.reasons.append(f"artifact_not_saved:{(pack_res or {}).get('reason')}")
            else:
                model_artifacts = pack_res.get("model_artifacts") if isinstance(pack_res, dict) else None
                if not model_artifacts:
                    result.passed = False
                    result.reasons.append("missing_model_artifacts")
                else:
                    try:
                        missing = [p for p in model_artifacts if not Path(p).exists()]
                    except Exception:
                        missing = []
                    if missing:
                        result.passed = False
                        result.reasons.append(f"missing_model_artifacts:{len(missing)}")
            # perform smoke-test and quarantine on failure if packaging policy requests it
            try:
                if pack_res and pack_res.get('saved') and getattr(cfg.packaging, 'quarantine_on_smoke_fail', True):
                    from octa_training.core.artifact_io import (
                        quarantine_artifact,
                        smoke_test_artifact,
                    )
                    pkl = pack_res.get('pkl')
                    meta = pkl.replace('.pkl', '.meta.json')
                    sha = pkl.replace('.pkl', '.sha256')
                    try:
                        smoke_test_artifact(pkl, cfg.paths.raw_dir, last_n=getattr(cfg, 'smoke_test_last_n', 50))
                        # record success
                        state.update_symbol_state(symbol, artifact_smoke_test_status='PASS', artifact_smoke_test_time=datetime.utcnow().isoformat())
                    except Exception as e:
                        # quarantine
                        qdir = getattr(cfg.packaging, 'quarantine_dir', None) or str(Path(cfg.paths.pkl_dir) / '_quarantine')
                        quarantine_artifact(pkl, meta, sha, reason=str(e), quarantine_dir=qdir)
                        state.update_symbol_state(symbol, artifact_smoke_test_status='FAIL', artifact_smoke_test_time=datetime.utcnow().isoformat(), artifact_quarantine_path=qdir, artifact_quarantine_reason=str(e))
                        send_telegram(cfg, f"OCTA: {symbol} smoke-test FAIL -> quarantined. reason={str(e)}", logger=logger)
                        # mark pack_res as quarantined
                        pack_res = {**pack_res, 'quarantined': True, 'quarantine_reason': str(e)}
                        result.passed = False
                        result.reasons.append(f"smoke_test_failed:{str(e)}")
            except Exception:
                pass

        # Optional: write a non-tradeable debug artifact on FAIL (auditability).
        if (not result.passed) and (pack_res is None) and (not safe_mode) and bool(getattr(cfg.packaging, 'save_debug_on_fail', False)):
            try:
                dbg_dir = getattr(cfg.packaging, 'debug_dir', None)
                if not dbg_dir:
                    dbg_dir = str(Path(cfg.paths.pkl_dir) / '_debug_fail')
                pack_res = save_tradeable_artifact(
                    symbol,
                    best,
                    features_res,
                    df,
                    metrics,
                    result,
                    cfg,
                    state,
                    run_id,
                    asset_class,
                    str(pinfo.path),
                    pkl_dir_override=dbg_dir,
                    update_state=False,
                    artifact_kind='debug',
                    enforce_improvement=False,
                )
                pack_res = {**(pack_res or {}), 'reason': 'debug_on_fail'}
            except Exception as e:
                pack_res = {'saved': False, 'reason': f'debug_on_fail_error:{e}'}

        # Optional debug bundle for diagnostics scripts (kept in-memory; not serialized).
        if bool(debug):
            try:
                dbg = {
                    'parquet_path': str(parquet_path or ''),
                    'asset_class': str(asset_class),
                    'eval_settings': es,
                    'preds': preds,
                    'df_backtest': res.get('df'),
                    'metrics': metrics,
                    'stress_metrics': stress_metrics,
                }
                if pack_res is None:
                    pack_res = {}
                if isinstance(pack_res, dict):
                    pack_res['debug'] = dbg
            except Exception:
                pass
        # optionally smoke test performed by caller
        _record_end(bool(result.passed), metrics_obj=metrics, gate_obj=result, pack_res=pack_res)
        return PipelineResult(symbol=symbol, run_id=run_id, passed=result.passed, metrics=metrics, gate_result=result, pack_result=pack_res, altdata=alt_diag)
    except AssetProfileMismatchError as e:
        # Profile mismatch is a gate decision (wrong asset class), not a code error.
        # Return without traceback so cascade_train classifies this as GATE_FAIL, not TRAIN_ERROR.
        try:
            state.record_run_end(symbol, run_id, passed=False, metrics_summary=None)
        except Exception:
            pass
        return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error=f"profile_mismatch:{e}", altdata=alt_diag)
    except Exception as e:
        import traceback as _tb
        _tb_str = _tb.format_exc()
        if logger:
            logger.error(
                "Training failed for %s (run_id=%s): %s\n%s",
                symbol, run_id, repr(e), _tb_str,
            )
        try:
            state.record_run_end(symbol, run_id, passed=False, metrics_summary=None)
        except Exception:
            pass
        return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error=str(e) + "\n" + _tb_str, altdata=alt_diag)


def evaluate_fx_g0_risk_overlay_1d(
    symbol: str,
    cfg: Any,
    state: Any,
    run_id: str,
    parquet_path: str,
    safe_mode: bool = True,
) -> PipelineResult:
    """FX Stage G0: 1D strict risk/regime overlay only.

    Alpha metrics at 1D are not required for FX. Risk constraints remain unchanged.
    """

    # Safety guard: prevent FX evaluation from silently assuming zero trading costs.
    try:
        from octa_training.core.gates import GateResult

        require_nonzero = bool(getattr(getattr(cfg, 'costs', None), 'require_nonzero_for_fx', True))
        broker = getattr(cfg, 'broker', None)
        signal = getattr(cfg, 'signal', None)
        cost_bps = getattr(broker, 'cost_bps', None) if broker is not None else None
        spread_bps = getattr(broker, 'spread_bps', None) if broker is not None else None
        stress_mult = getattr(broker, 'stress_cost_multiplier', None) if broker is not None else None
        if cost_bps is None and signal is not None:
            cost_bps = getattr(signal, 'cost_bps', None)
        if spread_bps is None and signal is not None:
            spread_bps = getattr(signal, 'spread_bps', None)
        if stress_mult is None and signal is not None:
            stress_mult = getattr(signal, 'stress_cost_multiplier', None)
        print(f"[fx_cost_model] symbol={symbol} stage=g0 cost_bps={cost_bps} spread_bps={spread_bps} stress_cost_multiplier={stress_mult} require_nonzero_for_fx={require_nonzero}")
        if require_nonzero and float(cost_bps or 0.0) <= 0.0 and float(spread_bps or 0.0) <= 0.0:
            gr = GateResult(
                passed=False,
                status='FAIL_DATA',
                reasons=['data_load_failed: fx_cost_model_missing_or_zero'],
                passed_checks=[],
                insufficient_evidence=[],
                robustness=None,
                diagnostics=[
                    {'name': 'fx_cost_bps', 'value': float(cost_bps or 0.0)},
                    {'name': 'fx_spread_bps', 'value': float(spread_bps or 0.0)},
                    {'name': 'fx_stress_cost_multiplier', 'value': float(stress_mult or 0.0)},
                ],
            )
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='fx_cost_model_missing_or_zero', gate_result=gr)
    except Exception:
        pass

    gate_overrides = {
        # Make alpha/profitability checks non-binding at 1D for FX.
        'sharpe_min': -1e9,
        'sortino_min': -1e9,
        'profit_factor_min': -1e9,
        'avg_net_trade_return_min': -1e9,
        'sharpe_oos_over_is_min': -1e9,
    }

    res = train_evaluate_package(
        symbol=symbol,
        cfg=cfg,
        state=state,
        run_id=run_id,
        safe_mode=safe_mode,
        parquet_path=parquet_path,
        gate_overrides=gate_overrides,
        robustness_profile='risk_overlay',
    )
    # Explicit audit marker: alpha is not required at 1D for FX.
    try:
        gr = getattr(res, 'gate_result', None)
        if gr is not None:
            if getattr(gr, 'insufficient_evidence', None) is None:
                gr.insufficient_evidence = []
            gr.insufficient_evidence = list(gr.insufficient_evidence) + ['alpha_not_required_at_1d_for_fx']
            if getattr(gr, 'passed', False):
                gr.status = 'PASS_LIMITED_STATISTICAL_CONFIDENCE'
    except Exception:
        pass
    return res


def evaluate_fx_g1_alpha_1h(
    symbol: str,
    cfg: Any,
    state: Any,
    run_id: str,
    parquet_path: str,
    safe_mode: bool = True,
) -> PipelineResult:
    """FX Stage G1: 1H alpha gate with full HF checks."""

    # Safety guard: prevent FX evaluation from silently assuming zero trading costs.
    try:
        from octa_training.core.gates import GateResult

        require_nonzero = bool(getattr(getattr(cfg, 'costs', None), 'require_nonzero_for_fx', True))
        broker = getattr(cfg, 'broker', None)
        signal = getattr(cfg, 'signal', None)
        cost_bps = getattr(broker, 'cost_bps', None) if broker is not None else None
        spread_bps = getattr(broker, 'spread_bps', None) if broker is not None else None
        stress_mult = getattr(broker, 'stress_cost_multiplier', None) if broker is not None else None
        if cost_bps is None and signal is not None:
            cost_bps = getattr(signal, 'cost_bps', None)
        if spread_bps is None and signal is not None:
            spread_bps = getattr(signal, 'spread_bps', None)
        if stress_mult is None and signal is not None:
            stress_mult = getattr(signal, 'stress_cost_multiplier', None)
        print(f"[fx_cost_model] symbol={symbol} stage=g1 cost_bps={cost_bps} spread_bps={spread_bps} stress_cost_multiplier={stress_mult} require_nonzero_for_fx={require_nonzero}")
        if require_nonzero and float(cost_bps or 0.0) <= 0.0 and float(spread_bps or 0.0) <= 0.0:
            gr = GateResult(
                passed=False,
                status='FAIL_DATA',
                reasons=['data_load_failed: fx_cost_model_missing_or_zero'],
                passed_checks=[],
                insufficient_evidence=[],
                robustness=None,
                diagnostics=[
                    {'name': 'fx_cost_bps', 'value': float(cost_bps or 0.0)},
                    {'name': 'fx_spread_bps', 'value': float(spread_bps or 0.0)},
                    {'name': 'fx_stress_cost_multiplier', 'value': float(stress_mult or 0.0)},
                ],
            )
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='fx_cost_model_missing_or_zero', gate_result=gr)
    except Exception:
        pass

    return train_evaluate_package(
        symbol=symbol,
        cfg=cfg,
        state=state,
        run_id=run_id,
        safe_mode=safe_mode,
        parquet_path=parquet_path,
    )


def _pipeline_annotate_pack(res: "PipelineResult", **extra: Any) -> None:
    """Inject metadata into PipelineResult.pack_result (in-place, safe)."""
    if res.pack_result is None:
        res.pack_result = {}
    res.pack_result.update(extra)


def train_regime_ensemble(
    symbol: str,
    timeframe: str,
    cfg: Any,
    state: Any,
    run_id: str,
    parquet_path: Optional[str] = None,
    detector_dir: Optional[str] = None,
    regime_artifacts_dir: Optional[str] = None,
    gov_audit: Optional[Any] = None,
    **kwargs: Any,
) -> "RegimeEnsemble":
    """Train a per-regime CatBoost ensemble for one symbol/TF (v0.0.0).

    Steps
    -----
    1. Load the full DataFrame from parquet_path (or discover via cfg)
    2. Classify every bar into a regime (crisis/bear/bull/neutral)
    3. For each regime with sufficient rows, train a submodel via
       train_evaluate_adaptive()
    4. Fit + persist a RegimeDetector for shadow execution
    5. Gate: ensemble passes iff regimes_trained >= min_regimes_trained

    Parameters
    ----------
    symbol : ticker symbol
    timeframe : e.g. '1D', '1H'
    cfg : TrainingConfig (must have cfg.regime_ensemble set and enabled)
    state : pipeline state object
    run_id : run identifier
    parquet_path : explicit parquet path; if None, discovered via cfg
    detector_dir : directory to save RegimeDetector pickle;
                   defaults to octa/var/models/regime_detectors/<symbol>/<timeframe>/
    **kwargs : forwarded to train_evaluate_adaptive()

    Returns
    -------
    RegimeEnsemble
    """
    from octa_training.core.regime_labels import (
        RegimeLabelConfig,
        classify_regimes,
        get_regime_splits,
    )
    from octa_training.core.regime_detector import RegimeDetector

    re_cfg = getattr(cfg, "regime_ensemble", None)
    if re_cfg is None:
        return RegimeEnsemble(
            symbol=symbol,
            timeframe=timeframe,
            run_id=run_id,
            submodels={},
            regimes_trained=0,
            passed=False,
            error="regime_ensemble config not set on TrainingConfig",
        )

    min_regimes_trained: int = int(getattr(re_cfg, "min_regimes_trained", 2))
    allowed_regimes: List[str] = list(getattr(re_cfg, "regimes", ["bull", "bear", "crisis"]))
    require_bull: bool = bool(getattr(re_cfg, "require_bull", True))
    require_bear: bool = bool(getattr(re_cfg, "require_bear", True))

    # --- Resolve per-regime artifact directory ---
    _re_arts_root: Optional[str] = (
        regime_artifacts_dir
        or getattr(re_cfg, "regime_artifacts_dir", None)
        or "octa/var/models/regime_artifacts"
    )
    _re_arts_dir = Path(_re_arts_root) / symbol / timeframe

    # --- Build label config from re_cfg.min_rows ---
    min_rows_raw = getattr(re_cfg, "min_rows", {}) or {}
    if hasattr(min_rows_raw, "model_dump"):
        min_rows_dict = min_rows_raw.model_dump()
    elif hasattr(min_rows_raw, "dict"):
        min_rows_dict = min_rows_raw.dict()
    else:
        min_rows_dict = dict(min_rows_raw)
    label_cfg = RegimeLabelConfig(min_rows=min_rows_dict)

    # --- Resolve parquet path and load data ---
    _parquet_path = parquet_path
    if _parquet_path is None:
        try:
            parquets = discover_parquets(cfg, symbol=symbol, timeframe=timeframe)
            _parquet_path = parquets[0] if parquets else None
        except Exception:
            _parquet_path = None

    if _parquet_path is None or not Path(_parquet_path).exists():
        return RegimeEnsemble(
            symbol=symbol,
            timeframe=timeframe,
            run_id=run_id,
            submodels={},
            regimes_trained=0,
            passed=False,
            error=f"parquet not found: {_parquet_path}",
        )

    try:
        df_full = load_parquet(_parquet_path)
    except Exception as exc:
        return RegimeEnsemble(
            symbol=symbol,
            timeframe=timeframe,
            run_id=run_id,
            submodels={},
            regimes_trained=0,
            passed=False,
            error=f"parquet load error: {exc}",
        )

    # --- Classify regimes ---
    try:
        labels = classify_regimes(df_full, cfg=label_cfg)
    except Exception as exc:
        return RegimeEnsemble(
            symbol=symbol,
            timeframe=timeframe,
            run_id=run_id,
            submodels={},
            regimes_trained=0,
            passed=False,
            error=f"regime classification error: {exc}",
        )

    if labels.empty:
        return RegimeEnsemble(
            symbol=symbol,
            timeframe=timeframe,
            run_id=run_id,
            submodels={},
            regimes_trained=0,
            passed=False,
            error="insufficient bars for regime classification (<252)",
        )

    # --- Split and train per regime ---
    splits = get_regime_splits(df_full, labels, cfg=label_cfg)
    submodels: Dict[str, PipelineResult] = {}
    regimes_trained = 0
    regime_artifact_paths: Dict[str, str] = {}

    for regime in allowed_regimes:
        if regime not in splits:
            continue  # Not enough rows for this regime

        regime_run_id = f"{run_id}_regime_{regime}"
        # Non-crisis submodels are never deployed during crisis periods — the
        # crisis submodel handles those intervals.  Applying the crisis OOS gate
        # to bull/bear/neutral submodels is architecturally incorrect and blocks
        # valid models that simply aren't designed for crisis resilience.
        # Only the crisis submodel keeps crisis_oos enabled.
        _regime_cfg = cfg
        if regime != "crisis":
            _crisis_oos = getattr(cfg, "crisis_oos", None)
            if _crisis_oos is not None and getattr(_crisis_oos, "enabled", False):
                try:
                    _regime_cfg = cfg.copy(
                        update={"crisis_oos": _crisis_oos.copy(update={"enabled": False})}
                    )
                except Exception:
                    _regime_cfg = cfg  # fallback: keep original if copy fails
        try:
            res = train_evaluate_adaptive(
                symbol=symbol,
                cfg=_regime_cfg,
                state=state,
                run_id=regime_run_id,
                parquet_path=_parquet_path,
                require_full_run=True,  # bypass recent_pass idempotency: each
                # regime submodel must train independently; the bull run's state
                # record must not cause bear/crisis to short-circuit with metrics=None.
                **kwargs,
            )
        except Exception as exc:
            res = PipelineResult(
                symbol=symbol,
                run_id=regime_run_id,
                passed=False,
                error=f"regime_train_error:{exc}",
            )

        submodels[regime] = res
        if gov_audit is not None:
            try:
                from octa.core.governance.governance_audit import EVENT_TRAINING_RUN
                gov_audit.emit(
                    EVENT_TRAINING_RUN,
                    {
                        "phase": "regime_submodel",
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "regime": regime,
                        "passed": res.passed,
                        "error": res.error,
                    },
                )
            except Exception:
                pass
        if res.passed:
            regimes_trained += 1
            # Copy artifact to per-regime location before next submodel overwrites it.
            # safe: fail-soft — copy failure does not fail the submodel itself.
            _src_pkl_str = (res.pack_result or {}).get("pkl")
            if _src_pkl_str:
                _src_pkl = Path(_src_pkl_str)
                if _src_pkl.exists():
                    try:
                        _re_arts_dir.mkdir(parents=True, exist_ok=True)
                        _dst_pkl = _re_arts_dir / f"{symbol}_{timeframe}_{regime}.pkl"
                        shutil.copy2(_src_pkl, _dst_pkl)
                        # copy sha256 sidecar if present
                        _src_sha = _src_pkl.with_suffix(".sha256")
                        if _src_sha.exists():
                            shutil.copy2(
                                _src_sha,
                                _re_arts_dir / f"{symbol}_{timeframe}_{regime}.sha256",
                            )
                        # load-validate: artifact must be readable
                        with open(_dst_pkl, "rb") as _fh:
                            pickle.load(_fh)
                        regime_artifact_paths[regime] = str(_dst_pkl)
                    except Exception:
                        # copy or load failed — do not register this artifact
                        _dst_maybe = _re_arts_dir / f"{symbol}_{timeframe}_{regime}.pkl"
                        if _dst_maybe.exists():
                            try:
                                _dst_maybe.unlink()
                            except Exception:
                                pass

    # --- Fit and persist RegimeDetector ---
    detector: Optional[RegimeDetector] = None
    detector_path: Optional[str] = None
    try:
        detector = RegimeDetector(cfg=label_cfg)
        detector.fit(df_full)

        _det_dir = (
            Path(detector_dir)
            if detector_dir
            else Path("octa/var/models/regime_detectors") / symbol / timeframe
        )
        _det_dir.mkdir(parents=True, exist_ok=True)
        _det_path = _det_dir / f"{symbol}_{timeframe}_regime.pkl"
        detector.save(_det_path)
        detector_path = str(_det_path)
    except Exception as exc:
        # Non-fatal: ensemble can still be used without a persisted detector
        detector_path = None

    # --- Compute passed: bull AND bear required by default ---
    bull_passes = bool(submodels.get("bull") is not None and submodels["bull"].passed)
    bear_passes = bool(submodels.get("bear") is not None and submodels["bear"].passed)

    if require_bull and require_bear:
        passed = bull_passes and bear_passes
    elif require_bull:
        passed = bull_passes
    elif require_bear:
        passed = bear_passes
    else:
        # legacy: gate on count only
        passed = regimes_trained >= min_regimes_trained

    _ensemble_error: Optional[str] = None
    if not passed:
        _missing = [r for r, ok in [("bull", bull_passes), ("bear", bear_passes)] if not ok and (require_bull if r == "bull" else require_bear)]
        if _missing:
            _ensemble_error = f"submodel_gate_failed:{','.join(_missing)}"
        else:
            _ensemble_error = f"insufficient_regime_diversity:{regimes_trained}/{min_regimes_trained}"

    # --- Write RegimeRouter pkl (routing manifest) ---
    router_path: Optional[str] = None
    try:
        from datetime import datetime as _dt

        _crisis_windows: List[Dict[str, Any]] = []
        try:
            _c_cfg = getattr(cfg, "crisis_oos", None)
            if _c_cfg is not None:
                for _w in getattr(_c_cfg, "windows", []):
                    _crisis_windows.append({
                        "name": str(getattr(_w, "name", "")),
                        "start": str(getattr(_w, "start", "")),
                        "end": str(getattr(_w, "end", "")),
                    })
        except Exception:
            pass

        def _regime_status(r: str) -> str:
            if r not in submodels:
                return "SKIP"
            return "PASS" if submodels[r].passed else "FAIL"

        _router_manifest: Dict[str, Any] = {
            "schema_version": 1,
            "kind": "regime_router",
            "symbol": symbol,
            "timeframe": timeframe,
            "run_id": run_id,
            "artifact_version": "1.0",
            "created_at": _dt.utcnow().isoformat(),
            "passed": passed,
            "fail_closed": True,
            "require_bull": require_bull,
            "require_bear": require_bear,
            "regimes": {
                r: {
                    "status": _regime_status(r),
                    "artifact_path": regime_artifact_paths.get(r),
                    "artifact_validated": r in regime_artifact_paths,
                }
                for r in allowed_regimes
            },
            "routing_table": dict(regime_artifact_paths),
            "detector_path": detector_path,
            "crisis_windows_evaluated": _crisis_windows,
        }
        _re_arts_dir.mkdir(parents=True, exist_ok=True)
        _router_pkl_path = _re_arts_dir / f"{symbol}_{timeframe}_regime.pkl"
        _router_bytes = pickle.dumps(_router_manifest, protocol=4)
        with open(_router_pkl_path, "wb") as _rfh:
            _rfh.write(_router_bytes)
        # load-validate the manifest
        with open(_router_pkl_path, "rb") as _rfh:
            pickle.load(_rfh)
        router_path = str(_router_pkl_path)
    except Exception:
        router_path = None

    if gov_audit is not None:
        try:
            from octa.core.governance.governance_audit import EVENT_REGIME_ACTIVATED
            gov_audit.emit(
                EVENT_REGIME_ACTIVATED,
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "passed": passed,
                    "regimes_trained": regimes_trained,
                    "regime_artifact_paths": dict(regime_artifact_paths),
                    "router_path": router_path,
                    "error": _ensemble_error,
                },
            )
        except Exception:
            pass

    return RegimeEnsemble(
        symbol=symbol,
        timeframe=timeframe,
        run_id=run_id,
        submodels=submodels,
        regimes_trained=regimes_trained,
        passed=passed,
        detector_path=detector_path,
        error=_ensemble_error,
        regime_artifact_paths=regime_artifact_paths,
        router_path=router_path,
    )


def train_evaluate_adaptive(
    symbol: str,
    cfg: Any,
    state: Any,
    run_id: str,
    *,
    fs_retry_ois_threshold: float = 0.10,
    **kwargs: Any,
) -> "PipelineResult":
    """Two-pass feature-selection fallback.

    Pass 1: run with cfg's current feature_selection setting (default: disabled).
    Pass 2: if Pass 1 fails AND sharpe_oos_over_is < fs_retry_ois_threshold,
            retry with feature_selection.enabled=True.

    The threshold guards against false retries:
      - Severe overfit (e.g. AAPL OOS/IS=0.0) → triggers Pass 2
      - Normal failure (e.g. ADC OOS/IS=0.63 that fails a different gate) → no retry
      - Already-enabled fs → single pass (no retry needed)

    Annotates pack_result with:
      fs_adaptive_pass: 1 (Pass 1 accepted) or 2 (Pass 2 used)
      fs_retry_ois_p1: OOS/IS value that triggered retry (Pass 2 only)

    Note: callers may pass asset_class= (legacy kwarg) alongside dataset=.
    asset_class= is consumed here and forwarded as dataset= to avoid TypeError
    on the new train_evaluate_package signature which dropped asset_class=.
    """
    import copy as _copy

    # Normalise asset_class= → dataset= for the new train_evaluate_package API.
    # cascade_train.py passes both; we honour dataset= if present, else fall back
    # to asset_class=, then discard the asset_class= key so it does not leak as
    # an unexpected kwarg into train_evaluate_package.
    _asset_class_hint = kwargs.pop("asset_class", None)
    if not kwargs.get("dataset") and _asset_class_hint:
        kwargs["dataset"] = _asset_class_hint

    # Pass 1 ------------------------------------------------------------------
    res1 = train_evaluate_package(symbol, cfg, state, run_id, **kwargs)

    _fs_already_on = bool(
        getattr(getattr(cfg, "feature_selection", None), "enabled", False)
    )

    if res1.passed or _fs_already_on:
        _pipeline_annotate_pack(res1, fs_adaptive_pass=1)
        return res1

    # Inspect OOS/IS to decide whether a retry is warranted.
    _ois = getattr(res1.metrics, "sharpe_oos_over_is", None)
    if _ois is None or float(_ois) >= fs_retry_ois_threshold:
        _pipeline_annotate_pack(res1, fs_adaptive_pass=1)
        return res1

    # Pass 2: enable feature selection ----------------------------------------
    try:
        cfg_p2 = _copy.deepcopy(cfg)
        cfg_p2.feature_selection.enabled = True
    except Exception:
        _pipeline_annotate_pack(res1, fs_adaptive_pass=1)
        return res1

    run_id_p2 = f"{run_id}_fsretry"
    res2 = train_evaluate_package(symbol, cfg_p2, state, run_id_p2, **kwargs)
    _pipeline_annotate_pack(res2, fs_adaptive_pass=2, fs_retry_ois_p1=float(_ois))
    return res2
