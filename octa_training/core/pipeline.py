from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from octa_training.core.asset_class import infer_asset_class
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
from octa_training.core.splits import walk_forward_splits


@dataclass
class PipelineResult:
    symbol: str
    run_id: str
    passed: bool
    metrics: Optional[Any] = None
    gate_result: Optional[Any] = None
    pack_result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


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
) -> PipelineResult:
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
            gate_policy_snapshot = dict(getattr(cfg, 'gates', {}) or {}) if isinstance(getattr(cfg, 'gates', {}), dict) else {}
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
            if art_path and Path(art_path).exists() and last_pass:
                last = datetime.fromisoformat(last_pass)
                if datetime.utcnow() - last < timedelta(days=getattr(cfg.retrain, 'skip_window_days', 3)):
                    _record_end(True, metrics_obj=None, gate_obj=None, pack_res={'skipped': True, 'reason': 'recent_pass'})
                    return PipelineResult(symbol=symbol, run_id=run_id, passed=True, metrics=None, gate_result=None, pack_result={'skipped': True, 'reason': 'recent_pass'})
        except Exception:
            pass
        pinfo = None
        if parquet_path:
            try:
                from octa_training.core.io_parquet import ParquetFileInfo

                pp = Path(parquet_path)
                if not pp.exists():
                    _record_end(False, metrics_obj=None, gate_obj=None)
                    return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='no_parquet')
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
                return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='no_parquet')
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
                return PipelineResult(symbol=symbol, run_id=run_id, passed=False, metrics=None, gate_result=gate_obj, pack_result=None, error=None)
            raise
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

        asset_class_raw = inferred or asset_class_state or "unknown"
        # If inference couldn't decide, allow existing state to win.
        try:
            if str(asset_class_raw).strip().lower() == 'unknown' and asset_class_state:
                asset_class_raw = asset_class_state
        except Exception:
            pass

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
                    return PipelineResult(symbol=symbol, run_id=run_id, passed=False, metrics=None, gate_result=gate_obj, pack_result=None, error=None)
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
                    return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error=f"liquidity_filter_failed:{liq_reason}")
                diagnose_reasons.append(f"liquidity_filter_failed:{liq_reason}")
        except Exception as e:
            # never fail the run due to liquidity checker bugs
            if logger:
                logger.warning("Liquidity filter check failed: %s", e)
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
            eff_settings.features = cfg.features if isinstance(cfg.features, dict) else {}
        except Exception:
            eff_settings.features = {}
        # Optional context for sidecar integrations (no behavioral impact unless used).
        try:
            eff_settings.symbol = symbol
        except Exception:
            pass
        try:
            eff_settings.timezone = str(getattr(getattr(cfg, 'session', None), 'timezone', 'UTC') or 'UTC')
        except Exception:
            pass
        # legacy attribute fallbacks
        try:
            for k, v in (eff_settings.features or {}).items():
                if isinstance(k, str):
                    setattr(eff_settings, k, v)
        except Exception:
            pass

        features_res = build_features(df, eff_settings, asset_class)
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
                return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='leakage_detected')
            diagnose_reasons.append('leakage_detected')

        # IBKR-only conservative cost model: prefer cfg.broker over cfg.signal
        broker = getattr(cfg, 'broker', None)
        cost_bps = getattr(broker, 'cost_bps', None) if broker is not None else None
        spread_bps = getattr(broker, 'spread_bps', None) if broker is not None else None
        stress_mult = getattr(broker, 'stress_cost_multiplier', None) if broker is not None else None
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
                        return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='missing_cost_model')
                    diagnose_reasons.append('missing_cost_model')
            except Exception:
                _record_end(False, metrics_obj=None, gate_obj=None)
                if not diagnose_mode:
                    return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='missing_cost_model')
                diagnose_reasons.append('missing_cost_model')
        splits_cfg = cfg.splits if hasattr(cfg, 'splits') else {}
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
                return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='missing_walk_forward')
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
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error=None, metrics=None, gate_result=gr)
        profile = detect_device()
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
            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='no_models')

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
                            return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error='nondeterminism_oof_shape')
                        diagnose_reasons.append('nondeterminism_oof_shape')
                    mask = np.isfinite(p1) & np.isfinite(p2)
                    if mask.any():
                        max_diff = float(np.max(np.abs(p1[mask] - p2[mask])))
                        if not np.isfinite(max_diff) or max_diff > 1e-6:
                            _record_end(False, metrics_obj=None, gate_obj=None)
                            if not diagnose_mode:
                                return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error=f'nondeterminism_oof_diff:{max_diff}')
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

        # Hedge-fund risk-first baseline overlays (net-of-cost metric scale).
        hf_tf_overlays: Dict[str, Dict[str, Any]] = {
            "1D": {
                "profit_factor_min": 1.20,
                "sharpe_min": 0.65,
                "max_drawdown_max": 0.06,
                "min_trades": 20,
                "min_bars": 252,
            },
            "1H": {
                "profit_factor_min": 1.15,
                "sharpe_min": 0.60,
                "max_drawdown_max": 0.05,
                "min_trades": 60,
                "min_bars": 500,
            },
            "30m": {
                "profit_factor_min": 1.12,
                "sharpe_min": 0.55,
                "max_drawdown_max": 0.045,
                "min_trades": 120,
                "min_bars": 800,
            },
            "5m": {
                "profit_factor_min": 1.10,
                "sharpe_min": 0.50,
                "max_drawdown_max": 0.040,
                "min_trades": 240,
                "min_bars": 1200,
            },
            "1m": {
                "profit_factor_min": 1.08,
                "sharpe_min": 0.45,
                "max_drawdown_max": 0.035,
                "min_trades": 480,
                "min_bars": 2000,
            },
        }

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

        base_spec = dict(global_spec or {})
        if isinstance(tf_spec, dict) and tf_spec:
            base_spec.update(tf_spec)

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
        try:
            hf_overlay = hf_tf_overlays.get(str(tf_key), {})
            if isinstance(hf_overlay, dict):
                for k, v in hf_overlay.items():
                    merged[k] = v
        except Exception:
            pass

        # Mandatory deterministic Monte Carlo gate defaults.
        merged.setdefault("monte_carlo_n", 600)
        merged.setdefault("monte_carlo_seed", 1337)
        merged.setdefault("monte_carlo_pf_p05_min", 1.05)
        merged.setdefault("monte_carlo_sharpe_p05_min", 0.40)
        merged.setdefault("monte_carlo_maxdd_mult", 1.5)
        merged.setdefault("monte_carlo_prob_loss_max", 0.40)

        merged = {k: v for k, v in merged.items() if v is not None}
        applied_thresholds = dict(merged)
        gate_kwargs = dict(merged)
        min_bars = int(gate_kwargs.pop("min_bars", 0) or 0)
        gate = GateSpec(**gate_kwargs)

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

        # Enforce minimum history bars as a strict structural gate.
        try:
            n_bars = int(len(df))
            if getattr(result, 'diagnostics', None) is None:
                result.diagnostics = []
            passed_bars = bool((min_bars <= 0) or (n_bars >= min_bars))
            result.diagnostics.append(
                {
                    'name': 'min_bars',
                    'value': float(n_bars),
                    'threshold': float(min_bars) if min_bars > 0 else None,
                    'op': '>=',
                    'passed': passed_bars,
                    'evaluable': True,
                    'confidence': 1.0 if passed_bars else 0.0,
                    'reason': None if passed_bars else f"insufficient_history_bars:{n_bars}<{min_bars}",
                }
            )
            if not passed_bars:
                result.passed = False
                result.reasons = list(getattr(result, 'reasons', []) or []) + [f"insufficient_history_bars:{n_bars}<{min_bars}"]
        except Exception:
            pass

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
                    {
                        'name': 'metric_scale_info',
                        'value': None,
                        'threshold': None,
                        'op': None,
                        'passed': True,
                        'evaluable': True,
                        'confidence': 0.0,
                        'reason': 'sharpe=annualized;profit_factor=net_of_cost_trade_pnl;max_drawdown=equity_drawdown_fraction;n_trades=position_change_count',
                    },
                    {
                        'name': 'net_of_cost',
                        'value': 1.0,
                        'threshold': 1.0,
                        'op': '==',
                        'passed': True,
                        'evaluable': True,
                        'confidence': 1.0,
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
            robustness_result = run_all_tests(symbol, features_res, folds, res['df'], preds, metrics, gate, es)
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

        # Ensure artifacts exist even on FAIL (auditability, fail-closed).
        if (not result.passed) and (pack_res is None) and (not safe_mode):
            try:
                dbg_dir = getattr(cfg.packaging, 'debug_dir', None)
                if not dbg_dir:
                    dbg_dir = str(Path(cfg.paths.pkl_dir))
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
        return PipelineResult(symbol=symbol, run_id=run_id, passed=result.passed, metrics=metrics, gate_result=result, pack_result=pack_res)
    except Exception as e:
        import traceback
        try:
            state.record_run_end(symbol, run_id, passed=False, metrics_summary=None)
        except Exception:
            pass
        return PipelineResult(symbol=symbol, run_id=run_id, passed=False, error=str(e) + "\n" + traceback.format_exc())


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
