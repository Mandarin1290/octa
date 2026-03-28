"""CLI to run training for symbols using octa_training foundation.

This script orchestrates runs: reads config, prepares run_id, records state, acquires locks,
and executes the configured training command per symbol. It does not implement model training itself.
"""
from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from uuid import uuid4

import pandas as pd

from core.training_safety_lock import TrainingSafetyLockError, assert_training_armed
from octa_training.core.asset_class import infer_asset_class
from octa_training.core.config import load_config
from octa_training.core.device import (
    apply_threading_policy,
    detect_device,
    profile_to_json,
)
from octa_training.core.features import build_features, leakage_audit
from octa_training.core.io_parquet import (
    discover_parquets,
    inspect_parquet,
    load_parquet,
)
from octa_training.core.locks import symbol_lock
from octa_training.core.logging import setup_logging
from octa_training.core.mem_profile import maybe_start as mem_maybe_start
from octa_training.core.mem_profile import snapshot as mem_snapshot
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.splits import describe_splits, walk_forward_splits
from octa_training.core.state import StateRegistry


def run_for_symbol(symbol: str, cfg_path: Optional[str], safe_mode: bool, run_id: str, logger) -> None:
    cfg = load_config(cfg_path)
    logger.info(f"Preparing run for symbol {symbol}", extra={"symbol": symbol, "run_id": run_id})
    state = StateRegistry(cfg.paths.state_dir)

    # discover matching parquet via discovery (sanitization stable)
    discovered = discover_parquets(Path(cfg.paths.raw_dir), state=state)
    match = [d for d in discovered if d.symbol == symbol]
    if not match:
        logger.warning("No parquet found for symbol; skipping", extra={"symbol": symbol})
        return
    pq_info = match[0]
    parquet_path = str(pq_info.path)

    # acquire per-symbol lock
    with symbol_lock(cfg.paths.state_dir, symbol):
        logger.info("Acquired lock", extra={"symbol": symbol})
        # perform inspection to get columns and basic info for asset class inference
        insp = inspect_parquet(pq_info.path, cfg=cfg.parquet.dict() if getattr(cfg, 'parquet', None) else {})
        cols = insp.get("columns", []) if insp.get("ok") else []

        asset_class = infer_asset_class(symbol, str(pq_info.path), cols, cfg)
        logger.info("Inferred asset class", extra={"symbol": symbol, "asset_class": asset_class})
        # detect device for this symbol-run and persist snapshot
        try:
            profile = detect_device()
            state.update_symbol_state(symbol, asset_class=asset_class, last_device_profile=profile_to_json(profile))
        except Exception:
            # fallback: persist asset class only
            state.update_symbol_state(symbol, asset_class=asset_class)

        # record run start
        state.record_run_start(symbol, run_id)

        # if inspect-only mode requested in config, perform inspect and return
        if getattr(cfg, "parquet", None) and getattr(cfg.parquet, "resample_enabled", False) and False:
            # placeholder: no-op; actual --inspect handled via CLI flag
            pass

        # build training command from config
        cmd_template = cfg.training_command or cfg.training.training_command if hasattr(cfg, 'training') else None
        if not cmd_template:
            # fallback: use default script driver
            cmd_template = "python3 -m scripts.train_and_save --parquet {parquet} --version {symbol} --backtest --cv-folds {cv_folds} --hyperopt"

        cmd = cmd_template.format(parquet=parquet_path, symbol=symbol, cv_folds=cfg.tuning.cv_folds)
        if safe_mode:
            logger.info("Safe mode enabled: will not execute external training command", extra={"symbol": symbol})
            # in safe mode, we just record the planned run
            state.update_symbol_state(symbol, last_seen_parquet_hash="safe-mode")
            return

        logger.info("Executing training command", extra={"symbol": symbol, "cmd": cmd})
        try:
            if not safe_mode:
                try:
                    assert_training_armed(cfg, symbol, "1D")
                except TrainingSafetyLockError as e:
                    logger.warning("Training blocked by safety lock", extra={"symbol": symbol, "reason": str(e)})
                    state.record_run_end(symbol, run_id, False, {"error": "training_safety_lock", "reason": str(e)})
                    return
            proc = subprocess.run(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False, text=True)
            out = proc.stdout
            rc = proc.returncode
            logger.info("Training finished", extra={"symbol": symbol, "returncode": rc, "output_len": len(out)})
            # basic metric extraction: attempt to parse model_card.json
            model_card = Path("artifacts/models/demo_model/regression") / symbol / 'model_card.json'
            metrics_summary = None
            if model_card.exists():
                try:
                    import json

                    metrics_summary = json.loads(model_card.read_text())
                except Exception:
                    metrics_summary = None
            passed = rc == 0
            state.record_run_end(symbol, run_id, passed, metrics_summary)
        except Exception as e:
            logger.exception("Training command failed", extra={"symbol": symbol})
            state.record_run_end(symbol, run_id, False, {"error": str(e)})


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OCTA run_train orchestration")
    p.add_argument("--all", action="store_true", help="Run for all symbols discovered in raw dir")
    p.add_argument("--symbol", type=str, help="Single symbol to run")
    p.add_argument("--inspect", action="store_true", help="Inspect parquet and print schema report instead of training")
    p.add_argument("--validate-metrics", type=str, help="Validate metrics JSON against gates and write report to reports dir")
    p.add_argument("--build-features", action="store_true", help="Build leakage-safe features for symbol and write report")
    p.add_argument("--train-models", action="store_true", help="Train models for symbol using built features and splits")
    p.add_argument("--evaluate", action="store_true", help="Train models to produce OOF predictions and run evaluation/gates for symbol")
    p.add_argument("--package", action="store_true", help="Run full pipeline and, on gate pass, package artifact to PKL directory")
    p.add_argument("--smoke-test-after-package", action="store_true", help="Run artifact smoke test immediately after successful packaging")
    p.add_argument("--task", type=str, choices=["cls","reg","both"], default="both", help="Task to train: classification, regression or both")
    p.add_argument("--asset-class", type=str, help="Asset class filter (not implemented advanced)" )
    p.add_argument("--config", type=str, help="Path to training.yaml")
    p.add_argument("--safe-mode", action="store_true", help="Do not execute external training commands")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(list(argv or sys.argv[1:]))
    cfg = load_config(args.config)
    run_id = str(uuid4())
    logger = setup_logging(cfg.paths.logs_dir, run_id=run_id)
    logger.info("Start run_train", extra={"run_id": run_id, "cli_args": vars(args)})

    # Optional memory profiling (no-op unless OCTA_MEM_PROFILE=1)
    if mem_maybe_start():
        mem_snapshot(label="run_train:start", logger=logger)

    state = StateRegistry(cfg.paths.state_dir)

    # detect device at run startup and apply threading policy
    try:
        run_profile = detect_device()
        logger.info("Device profile", extra={"device": run_profile.__dict__})
        # set threading envs
        apply_threading_policy(run_profile, cfg)
        # record a system-level snapshot
        try:
            state.update_symbol_state("__DEVICE__", last_device_profile=profile_to_json(run_profile))
        except Exception:
            logger.exception("Failed to persist device profile")
    except Exception:
        logger.exception("Device detection failed")

    if mem_maybe_start():
        mem_snapshot(label="run_train:after_device", logger=logger)

    symbols_to_run: List[str] = []
    if args.all:
        # discover parquet symbols using discovery (respects PKL ignore)
        discovered = discover_parquets(Path(cfg.paths.raw_dir), state=state)
        symbols_to_run = [d.symbol for d in discovered]
    elif args.symbol:
        symbols_to_run = [args.symbol]
    else:
        logger.error("No symbol specified and --all not used; exiting")
        return 2

    for sym in symbols_to_run:
        try:
            if args.inspect:
                # find parquet and inspect
                discovered = discover_parquets(Path(cfg.paths.raw_dir), state=state)
                match = [d for d in discovered if d.symbol == sym]
                if not match:
                    logger.warning("No parquet found for symbol; skipping inspect", extra={"symbol": sym})
                    continue
                info = inspect_parquet(match[0].path, cfg=cfg.parquet.dict() if getattr(cfg, 'parquet', None) else {})
                logger.info("Inspect result", extra={"symbol": sym, "inspect": info})
                continue
            if args.validate_metrics:
                # read metrics json, evaluate gates, write report
                import json

                from octa_training.core.gates import GateSpec, gate_evaluate
                from octa_training.core.metrics_contract import MetricsSummary

                jpath = Path(args.validate_metrics)
                if not jpath.exists():
                    logger.error("Metrics JSON not found", extra={"path": str(jpath)})
                    continue
                raw = json.loads(jpath.read_text())
                try:
                    metrics = MetricsSummary(**raw)
                except Exception as e:
                    logger.exception("Failed parsing MetricsSummary", extra={"error": str(e)})
                    continue
                # determine asset class: prefer state stored value, else infer
                s = state.get_symbol_state(sym) or {}
                asset_class = s.get("asset_class")
                if not asset_class:
                    # fallback infer using inspect if parquet present
                    discovered = discover_parquets(Path(cfg.paths.raw_dir), state=state)
                    match = [d for d in discovered if d.symbol == sym]
                    cols = []
                    if match:
                        cols = inspect_parquet(match[0].path, cfg=cfg.parquet.dict() if getattr(cfg, 'parquet', None) else {}).get("columns", [])
                        asset_class = infer_asset_class(sym, str(match[0].path), cols, cfg)
                # build gate spec from config (global < asset class)
                gconf = cfg.gates if hasattr(cfg, 'gates') else {}
                global_spec = gconf.get('global', {}) if isinstance(gconf, dict) else {}
                by_ac = gconf.get('by_asset_class', {}) if isinstance(gconf, dict) else {}
                ac_spec = by_ac.get(asset_class, {}) if asset_class else {}
                # merge
                merged = {**global_spec, **ac_spec}
                gate = GateSpec(**merged)
                result = gate_evaluate(metrics, gate)
                # write report
                report = {
                    'symbol': sym,
                    'asset_class': asset_class,
                    'gate': merged,
                    'result': result.dict(),
                }
                outp = Path(cfg.paths.reports_dir) / f"{sym}_gate_report_{run_id}.json"
                outp.write_text(json.dumps(report, ensure_ascii=False, indent=2))
                logger.info("Wrote gate report", extra={"path": str(outp), "symbol": sym})
                # persist gate result summary in state
                state.update_symbol_state(sym, last_gate_result=("PASS" if result.passed else "FAIL"), last_metrics_summary=raw)
                continue
            if args.build_features:
                # load parquet and build features for symbol
                discovered = discover_parquets(Path(cfg.paths.raw_dir), state=state)
                match = [d for d in discovered if d.symbol == sym]
                if not match:
                    logger.warning("No parquet found for symbol; skipping feature build", extra={"symbol": sym})
                    continue
                pinfo = match[0]
                try:
                    df = load_parquet(pinfo.path, nan_threshold=cfg.parquet.nan_threshold, allow_negative_prices=cfg.parquet.allow_negative_prices, resample_enabled=cfg.parquet.resample_enabled, resample_bar_size=cfg.parquet.resample_bar_size)
                except Exception as e:
                    logger.exception("Failed to load parquet for feature build", extra={"symbol": sym, "error": str(e)})
                    continue
                asset_class = (state.get_symbol_state(sym) or {}).get("asset_class") or infer_asset_class(sym, str(pinfo.path), list(df.columns), cfg)
                eff_settings = type("S", (), cfg.features)
                res = build_features(df, eff_settings, asset_class)
                # leakage audit (tolerant): treat a False return as a warning rather than crashing
                try:
                    audit_ok, audit_report = leakage_audit(
                        res.X,
                        res.y_dict,
                        df,
                        eff_settings.horizons,
                        settings=eff_settings,
                        asset_class=asset_class,
                        return_report=True,
                    )
                    res.meta["leakage_audit"] = audit_report
                except Exception as e:
                    logger.exception("Leakage audit threw exception", extra={"symbol": sym, "error": str(e)})
                    state.update_symbol_state(sym, last_gate_result="FEATURE_LEAKAGE_FAIL")
                    continue
                if not audit_ok:
                    logger.warning("Leakage audit flagged potential issues; continuing with WARN", extra={"symbol": sym})
                    state.update_symbol_state(sym, last_gate_result="FEATURE_LEAKAGE_WARN")
                # write report
                import json
                outp = Path(cfg.paths.reports_dir) / f"{sym}_features_{run_id}.json"
                report = {"symbol": sym, "asset_class": asset_class, "meta": res.meta}
                outp.write_text(json.dumps(report, ensure_ascii=False, indent=2))
                logger.info("Wrote feature report", extra={"path": str(outp), "symbol": sym})
                # persist in state
                state.update_symbol_state(sym, last_train_time=datetime.utcnow().isoformat(), last_metrics_summary=None)
                state.update_symbol_state(sym, last_gate_result="FEATURES_BUILT")
                # compute splits and write splits report if requested via inspect-splits flag
                splits_cfg = cfg.splits if hasattr(cfg, 'splits') else {}
                try:
                    folds = walk_forward_splits(res.X.index, n_folds=int(splits_cfg.get('n_folds',5)), train_window=int(splits_cfg.get('train_window',1000)), test_window=int(splits_cfg.get('test_window',200)), step=int(splits_cfg.get('step',200)), purge_size=int(splits_cfg.get('purge_size',10)), embargo_size=int(splits_cfg.get('embargo_size',5)), min_train_size=int(splits_cfg.get('min_train_size',500)), min_test_size=int(splits_cfg.get('min_test_size',100)), expanding=bool(splits_cfg.get('expanding',True)), min_folds_required=int(splits_cfg.get('min_folds_required',1)))
                    splits_report = describe_splits(folds, res.X.index)
                    outp2 = Path(cfg.paths.reports_dir) / f"{sym}_splits_{run_id}.json"
                    outp2.write_text(json.dumps(splits_report, ensure_ascii=False, indent=2))
                    logger.info("Wrote splits report", extra={"path": str(outp2), "symbol": sym})
                    # persist fold summary in state
                    state.update_symbol_state(sym, last_metrics_summary={'folds': splits_report.get('n_folds')}, last_gate_result='SPLITS_DONE')
                except Exception as e:
                    logger.exception("Failed to compute splits", extra={"symbol": sym, "error": str(e)})
                continue
            if args.train_models:
                # load parquet and build features if not already built
                discovered = discover_parquets(Path(cfg.paths.raw_dir), state=state)
                match = [d for d in discovered if d.symbol == sym]
                if not match:
                    logger.warning("No parquet found for symbol; skipping model train", extra={"symbol": sym})
                    continue
                pinfo = match[0]
                try:
                    df = load_parquet(pinfo.path, nan_threshold=cfg.parquet.nan_threshold, allow_negative_prices=cfg.parquet.allow_negative_prices, resample_enabled=cfg.parquet.resample_enabled, resample_bar_size=cfg.parquet.resample_bar_size)
                except Exception as e:
                    logger.exception("Failed to load parquet for model train", extra={"symbol": sym, "error": str(e)})
                    continue
                asset_class = (state.get_symbol_state(sym) or {}).get("asset_class") or infer_asset_class(sym, str(pinfo.path), list(df.columns), cfg)
                eff_settings = type("S", (), cfg.features)
                features_res = build_features(df, eff_settings, asset_class)
                # compute splits
                splits_cfg = cfg.splits if hasattr(cfg, 'splits') else {}
                try:
                    folds = walk_forward_splits(features_res.X.index, n_folds=int(splits_cfg.get('n_folds',5)), train_window=int(splits_cfg.get('train_window',1000)), test_window=int(splits_cfg.get('test_window',200)), step=int(splits_cfg.get('step',200)), purge_size=int(splits_cfg.get('purge_size',10)), embargo_size=int(splits_cfg.get('embargo_size',5)), min_train_size=int(splits_cfg.get('min_train_size',500)), min_test_size=int(splits_cfg.get('min_test_size',100)), expanding=bool(splits_cfg.get('expanding',True)), min_folds_required=int(splits_cfg.get('min_folds_required',1)))
                except Exception as e:
                    logger.exception("Failed to compute splits for model train", extra={"symbol": sym, "error": str(e)})
                    continue
                # prepare device profile
                profile = detect_device()
                from octa_training.core.models import train_models
                try:
                    assert_training_armed(cfg, sym, "1D")
                except TrainingSafetyLockError as e:
                    logger.warning("Training blocked by safety lock", extra={"symbol": sym, "reason": str(e)})
                    state.update_symbol_state(sym, last_gate_result="LOCK_BLOCKED")
                    continue
                train_results = train_models(features_res.X, features_res.y_dict, folds, cfg, profile)
                # write train report
                import json
                outp = Path(cfg.paths.reports_dir) / f"{sym}_model_train_{run_id}.json"
                rep = {"symbol": sym, "asset_class": asset_class, "results": [{"model_name": r.model_name, "task": r.task, "horizon": r.horizon, "fold_metrics": [f.__dict__ for f in r.fold_metrics], "oof_predictions": r.oof_predictions, "feature_importance": r.feature_importance, "params": r.params, "device_used": r.device_used} for r in train_results]}
                outp.write_text(json.dumps(rep, ensure_ascii=False, indent=2))
                logger.info("Wrote model train report", extra={"path": str(outp), "symbol": sym})
                # persist best model summary (pick first result as placeholder for best)
                if train_results:
                    best = train_results[0]
                    state.update_symbol_state(sym, last_train_time=datetime.utcnow().isoformat(), last_metrics_summary={"model": best.model_name, "task": best.task, "horizon": best.horizon}, last_gate_result="MODEL_TRAINED")
                continue
            if args.evaluate:
                if not args.safe_mode and not os.environ.get("OCTA_SKIP_SAFETY_LOCK"):
                    try:
                        assert_training_armed(cfg, sym, "1D")
                    except TrainingSafetyLockError as e:
                        logger.warning("Training blocked by safety lock", extra={"symbol": sym, "reason": str(e)})
                        state.update_symbol_state(sym, last_gate_result="LOCK_BLOCKED")
                        continue
                res = train_evaluate_package(sym, cfg, state, run_id, safe_mode=args.safe_mode)
                # write metrics report if available
                try:
                    import json
                    outp = Path(cfg.paths.reports_dir) / f"{sym}_metrics_{run_id}.json"
                    metrics_dump = None
                    if res.metrics is not None:
                        metrics_dump = res.metrics.model_dump() if hasattr(res.metrics, 'model_dump') else res.metrics.dict()
                    gate_dump = None
                    if res.gate_result is not None:
                        gate_dump = res.gate_result.model_dump() if hasattr(res.gate_result, 'model_dump') else res.gate_result.dict()

                    payload = {
                        "symbol": sym,
                        "asset_class": (state.get_symbol_state(sym) or {}).get('asset_class'),
                        "metrics": metrics_dump,
                        "gate_result": gate_dump,
                    }
                    # Be tolerant: pydantic / pandas / datetime objects may appear in metrics.
                    outp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
                    logger.info("Wrote metrics report", extra={"path": str(outp), "symbol": sym})
                    # Persist a JSON-safe snapshot into state registry.
                    safe_metrics_summary = None
                    if metrics_dump is not None:
                        safe_metrics_summary = json.loads(json.dumps(metrics_dump, ensure_ascii=False, default=str))
                    state.update_symbol_state(
                        sym,
                        last_metrics_summary=safe_metrics_summary,
                        last_gate_result=("PASS" if res.passed else "FAIL"),
                    )
                except Exception:
                    logger.exception("Failed to write metrics report")
                continue
            if args.package:
                # reuse evaluation flow
                discovered = discover_parquets(Path(cfg.paths.raw_dir), state=state)
                match = [d for d in discovered if d.symbol == sym]
                if not match:
                    logger.warning("No parquet found for symbol; skipping packaging", extra={"symbol": sym})
                    continue
                pinfo = match[0]
                try:
                    df = load_parquet(pinfo.path, nan_threshold=cfg.parquet.nan_threshold, allow_negative_prices=cfg.parquet.allow_negative_prices, resample_enabled=cfg.parquet.resample_enabled, resample_bar_size=cfg.parquet.resample_bar_size)
                except Exception as e:
                    logger.exception("Failed to load parquet for packaging", extra={"symbol": sym, "error": str(e)})
                    continue
                asset_class = (state.get_symbol_state(sym) or {}).get("asset_class") or infer_asset_class(sym, str(pinfo.path), list(df.columns), cfg)
                eff_settings = type("S", (), cfg.features)
                features_res = build_features(df, eff_settings, asset_class)
                # compute splits
                splits_cfg = cfg.splits if hasattr(cfg, 'splits') else {}
                try:
                    folds = walk_forward_splits(features_res.X.index, n_folds=int(splits_cfg.get('n_folds',5)), train_window=int(splits_cfg.get('train_window',1000)), test_window=int(splits_cfg.get('test_window',200)), step=int(splits_cfg.get('step',200)), purge_size=int(splits_cfg.get('purge_size',10)), embargo_size=int(splits_cfg.get('embargo_size',5)), min_train_size=int(splits_cfg.get('min_train_size',500)), min_test_size=int(splits_cfg.get('min_test_size',100)), expanding=bool(splits_cfg.get('expanding',True)), min_folds_required=int(splits_cfg.get('min_folds_required',1)))
                except Exception as e:
                    logger.exception("Failed to compute splits for packaging", extra={"symbol": sym, "error": str(e)})
                    continue
                # train to obtain OOF predictions
                profile = detect_device()
                from octa_training.core.models import train_models
                try:
                    assert_training_armed(cfg, sym, "1D")
                except TrainingSafetyLockError as e:
                    logger.warning("Training blocked by safety lock", extra={"symbol": sym, "reason": str(e)})
                    state.update_symbol_state(sym, last_gate_result="LOCK_BLOCKED")
                    continue
                train_results = train_models(features_res.X, features_res.y_dict, folds, cfg, profile)
                if not train_results:
                    logger.warning("No train results produced; skipping packaging", extra={"symbol": sym})
                    state.update_symbol_state(sym, last_gate_result="NO_MODELS")
                    continue
                best = train_results[0]
                try:
                    oof = best.oof_predictions
                    oof_index = oof.get('index', [])
                    oof_vals = oof.get('pred', [])
                    # convert index entries to datetime (handles ISO strings)
                    try:
                        idx = pd.to_datetime(oof_index, utc=True, errors='coerce')
                        preds = pd.Series(oof_vals, index=idx)
                        # align timezone with price series if needed
                        try:
                            if getattr(df.index, 'tz', None) is not None and getattr(preds.index, 'tz', None) is not None:
                                preds.index = preds.index.tz_convert(df.index.tz)
                        except Exception:
                            pass
                        # drop any predictions with invalid timestamps
                        preds = preds[~preds.index.isna()]
                    except Exception:
                        preds = pd.Series(oof_vals, index=pd.Index(oof_index))
                except Exception:
                    logger.exception("Failed to assemble OOF predictions", extra={"symbol": sym})
                    continue
                # compute evaluation
                from octa_training.core.evaluation import (
                    EvalSettings,
                    compute_equity_and_metrics,
                )
                es = EvalSettings(
                    mode='cls' if args.task in ('cls','both') else 'reg',
                    upper_q=cfg.signal.upper_q,
                    lower_q=cfg.signal.lower_q,
                    causal_quantiles=bool(getattr(cfg.signal, 'causal_quantiles', False)),
                    quantile_window=getattr(cfg.signal, 'quantile_window', None),
                    adaptive_density_quantiles=bool(getattr(cfg.signal, 'adaptive_density_quantiles', False)),
                    density_target=float(getattr(cfg.signal, 'density_target', 0.10) or 0.10),
                    density_window=getattr(cfg.signal, 'density_window', None),
                    density_relax_max=float(getattr(cfg.signal, 'density_relax_max', 0.0) or 0.0),
                    leverage_cap=cfg.signal.leverage_cap,
                    vol_target=cfg.signal.vol_target,
                    realized_vol_window=cfg.signal.realized_vol_window,
                    cost_bps=cfg.signal.cost_bps,
                    spread_bps=cfg.signal.spread_bps,
                    stress_cost_multiplier=cfg.signal.stress_cost_multiplier,
                )
                try:
                    res = compute_equity_and_metrics(df['close'], preds, es)
                except Exception as e:
                    logger.exception("Evaluation failed during packaging", extra={"symbol": sym, "error": str(e)})
                    continue
                metrics = res['metrics']
                # compute fold-level metrics using folds and OOF preds (more robust than placeholders)
                try:
                    from octa_training.core.metrics_contract import MetricsSummaryLite
                    fold_list = []
                    for f in folds:
                        try:
                            # val indices are positional indices into features_res.X
                            val_idx = f.val_idx
                            # try to map to timestamps from features_res.X
                            try:
                                val_times = features_res.X.index[val_idx]
                                prices_fold = df['close'].loc[val_times]
                                preds_fold = preds.loc[prices_fold.index]
                            except Exception:
                                # fallback to positional selection
                                prices_fold = df['close'].iloc[val_idx]
                                # attempt to pick preds by matching index positions, else align by iloc
                                try:
                                    preds_fold = preds.loc[prices_fold.index]
                                except Exception:
                                    preds_fold = pd.Series(preds.values[val_idx], index=prices_fold.index)

                            # require non-empty fold to compute metrics
                            if len(prices_fold) == 0 or len(preds_fold) == 0:
                                fold_list.append(MetricsSummaryLite(sharpe=None, max_drawdown=None, n_trades=0))
                                continue

                            sub = compute_equity_and_metrics(prices_fold, preds_fold, es)
                            m = sub['metrics']
                            fold_list.append(MetricsSummaryLite(sharpe=m.sharpe if getattr(m, 'sharpe', None) is not None else None, max_drawdown=m.max_drawdown if getattr(m, 'max_drawdown', None) is not None else None, n_trades=int(getattr(m, 'n_trades', 0) or 0)))
                        except Exception:
                            fold_list.append(MetricsSummaryLite(sharpe=None, max_drawdown=None, n_trades=0))
                    if fold_list:
                        metrics.fold_metrics = fold_list
                except Exception:
                    # leave fold_metrics empty if anything fails
                    pass
                # gate
                from octa_training.core.gates import GateSpec, gate_evaluate
                gconf = cfg.gates if hasattr(cfg, 'gates') else {}
                global_spec = gconf.get('global', {}) if isinstance(gconf, dict) else {}
                by_ac = gconf.get('by_asset_class', {}) if isinstance(gconf, dict) else {}
                ac_spec = by_ac.get(asset_class, {}) if asset_class else {}
                merged = {**global_spec, **ac_spec}
                gate = GateSpec(**merged)
                result = gate_evaluate(metrics, gate)
                if not result.passed:
                    # write fail report
                    import json
                    outp = Path(cfg.paths.reports_dir) / f"{sym}_gate_fail_{run_id}.json"
                    outp.write_text(json.dumps({"symbol": sym, "asset_class": asset_class, "metrics": metrics.dict(), "gate": result.dict()}, ensure_ascii=False, indent=2))
                    logger.info("Gate failed — wrote fail report", extra={"path": str(outp), "symbol": sym})
                    state.update_symbol_state(sym, last_gate_result="FAIL", last_metrics_summary=metrics.dict())
                    continue
                # passed — package
                from octa_training.core.packaging import save_tradeable_artifact
                try:
                    pack_res = save_tradeable_artifact(sym, best, features_res, df, metrics, result, cfg, state, run_id, asset_class, str(pinfo.path))
                    logger.info("Packaging result", extra={"symbol": sym, "pack_res": pack_res})
                    # optional smoke test after packaging
                    if args.smoke_test_after_package:
                        try:
                            from octa_training.core.artifact_io import (
                                smoke_test_artifact,
                            )
                            pkl_path = pack_res.get('pkl')
                            st = smoke_test_artifact(pkl_path, str(cfg.paths.raw_dir), last_n=getattr(cfg, 'smoke_test_last_n', 50))
                            status = 'PASS' if st.get('keys_ok') and st.get('nan_free') else 'FAIL'
                            state.update_symbol_state(sym, artifact_smoke_test_status=status, artifact_smoke_test_time=datetime.utcnow().isoformat())
                            if status == 'FAIL':
                                # quarantine or remove artifact based on config
                                quarantine = getattr(cfg.packaging, 'quarantine_on_smoke_fail', True)
                                qdir = getattr(cfg.packaging, 'quarantine_dir', None)
                                if quarantine:
                                    import shutil
                                    qbase = qdir or (Path(cfg.paths.pkl_dir) / 'quarantine')
                                    qbase = Path(qbase)
                                    qbase.mkdir(parents=True, exist_ok=True)
                                    for suf in ('.pkl', '.meta.json', '.sha256'):
                                        src = Path(pkl_path).with_suffix(suf)
                                        if src.exists():
                                            dst = qbase / src.name
                                            shutil.move(str(src), str(dst))
                                else:
                                    # remove files
                                    for suf in ('.pkl', '.meta.json', '.sha256'):
                                        src = Path(pkl_path).with_suffix(suf)
                                        if src.exists():
                                            try:
                                                src.unlink()
                                            except Exception:
                                                pass
                        except Exception as e:
                            logger.exception("Smoke test after packaging failed", extra={"symbol": sym, "error": str(e)})
                except Exception as e:
                    logger.exception("Packaging failed", extra={"symbol": sym, "error": str(e)})
                    continue
                continue
            run_for_symbol(sym, args.config, args.safe_mode, run_id, logger)
        except Exception:
            logger.exception("Error during run_for_symbol", extra={"symbol": sym})

    logger.info("Finished run_train", extra={"run_id": run_id})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
