from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import numpy as np
import pandas as pd
import yaml
import os
import pickle
import joblib

from octa.core.governance.model_release import decide_release, update_registry
from octa.core.research.robustness.monte_carlo import run_monte_carlo
from octa.core.research.scoring.scorer import score_run
from octa.core.research.validation.walk_forward import (
    Split,
    ValidationReport,
    make_walk_forward_splits,
    validate_model,
)
from octa.core.research.validation.purged_cv import purged_kfold_splits
from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training
from octa_ops.autopilot.universe import discover_universe
from octa_training.core.config import load_config
from octa_training.core.device import detect_device
from octa_training.core.evaluation import EvalSettings, compute_equity_and_metrics
from octa_training.core.features import build_features
from octa_training.core.io_parquet import load_parquet
from octa_training.core.models import train_models
from octa_training.core.splits import SplitFold


@dataclass(frozen=True)
class GateOutcome:
    gate: str
    timeframe: str
    validation: Dict[str, Any]
    scoring: Dict[str, Any]
    mc: Dict[str, Any]
    release: Dict[str, Any]


def _load_yaml(path: str) -> Dict[str, Any]:
    try:
        return yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _timeframe_gate_map() -> Dict[str, str]:
    return {
        "1D": "global_1d",
        "1H": "signal_1h",
        "30M": "structure_30m",
        "5M": "execution_5m",
        "1M": "micro_1m",
    }


def _select_best(results: List[Any]) -> Optional[Any]:
    if not results:
        return None
    best = None
    best_score = float("-inf")
    for r in results:
        scores = []
        for fm in getattr(r, "fold_metrics", []):
            try:
                scores.append(float(fm.metric.get("sharpe", float("nan"))))
            except Exception:
                continue
        score = float(np.nanmean(scores)) if scores else float("-inf")
        if score > best_score:
            best_score = score
            best = r
    return best or results[0]


def _pred_series(best: Any, index: pd.Index) -> pd.Series:
    preds = (getattr(best, "oof_predictions", {}) or {}).get("pred", [])
    idx = (getattr(best, "oof_predictions", {}) or {}).get("index", [])
    if idx and len(idx) == len(preds):
        try:
            dt_idx = pd.to_datetime(idx, utc=True, errors="coerce")
            return pd.Series(preds, index=dt_idx).reindex(index)
        except Exception:
            pass
    return pd.Series(preds, index=index[: len(preds)])


def _build_trades(df: pd.DataFrame) -> List[Dict[str, Any]]:
    trades = []
    if "turnover" not in df.columns:
        return trades
    for ts, row in df.iterrows():
        try:
            if float(row.get("turnover", 0.0)) <= 0:
                continue
            trades.append(
                {
                    "timestamp": ts,
                    "size_frac": float(row.get("turnover", 0.0)),
                    "price": float(row.get("price", row.get("close", 0.0))),
                }
            )
        except Exception:
            continue
    return trades


def _market_ctx(df: pd.DataFrame) -> Dict[str, Any]:
    vol = float(df["ret"].std(ddof=0)) if "ret" in df.columns else 0.0
    high = float(df["high"].iloc[-1]) if "high" in df.columns else None
    low = float(df["low"].iloc[-1]) if "low" in df.columns else None
    volume = float(df["volume"].median()) if "volume" in df.columns else 1.0
    return {"volatility": vol, "high": high, "low": low, "liquidity": max(volume, 1.0)}


def run_institutional_train(
    *,
    config_path: str,
    universe_size: int,
    timeframes: List[str],
    seed: int,
    bucket: str,
    parquet_root: str,
    mode: str,
) -> Dict[str, Any]:
    cfg = load_config(config_path)
    override_cfg_path = _prepare_config_override(config_path)
    if override_cfg_path:
        cfg = load_config(override_cfg_path)
    validation_cfg = _load_yaml("config/validation.yaml")
    scoring_cfg = _load_yaml("config/scoring.yaml")
    release_cfg = _load_yaml("config/release.yaml")
    robustness_cfg = _load_yaml("config/robustness.yaml")
    mode_key = str(mode or "paper").replace("-", "_")
    thresholds = release_cfg.get(mode_key, release_cfg) if isinstance(release_cfg, dict) else {}
    fast_mode = os.getenv("OCTA_INSTITUTIONAL_FAST", "").strip() == "1"
    if fast_mode:
        validation_cfg["walk_forward"] = {"train_days": 60, "test_days": 20, "step_days": 20, "warmup_days": 0, "embargo_bars": 1}
        validation_cfg["purged_cv"] = {"n_splits": 2, "purge_bars": 1, "embargo_bars": 1}
        robustness_cfg["n_sims"] = 20

    run_id = f"institutional_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    base = Path(parquet_root)
    universe = discover_universe(
        raw_root=str(base),
        stock_dir=str(base / "Stock_parquet"),
        fx_dir=str(base / "FX_parquet"),
        crypto_dir=str(base / "Crypto_parquet"),
        futures_dir=str(base / "Future_parquet"),
        limit=universe_size,
    )
    gate_map = _timeframe_gate_map()
    outcomes: List[GateOutcome] = []

    analysis_timeframes = timeframes
    cascade_timeframes = timeframes
    if fast_mode:
        analysis_timeframes = [tf for tf in timeframes if tf in {"1D", "1H"}]
        cascade_timeframes = analysis_timeframes

    for idx, u in enumerate(universe):
        if fast_mode and idx > 0:
            continue
        decisions, metrics_by_tf = run_cascade_training(
            run_id=run_id,
            config_path=override_cfg_path or config_path,
            symbol=u.symbol,
            asset_class=u.asset_class,
            parquet_paths=u.parquet_paths or {},
            cascade=CascadePolicy(order=cascade_timeframes),
            safe_mode=True,
            reports_dir="octa/var/artifacts",
        )
        for tf in analysis_timeframes:
            parquet_path = (u.parquet_paths or {}).get(tf)
            if not parquet_path:
                continue
            df = load_parquet(Path(parquet_path))
            if fast_mode and len(df) > 300:
                df = df.iloc[-300:]
            features_res = build_features(df, cfg, u.asset_class)
            eval_settings = EvalSettings(
                cost_bps=float(scoring_cfg.get("fee_bps", 1.0)),
                spread_bps=float(scoring_cfg.get("spread_bps", 0.5)),
            )
            wf_cfg = dict(validation_cfg.get("walk_forward", {}))
            wf_cfg["timeframe"] = tf
            wf_splits = make_walk_forward_splits(features_res.X.index, wf_cfg)
            purged_cfg = validation_cfg.get("purged_cv", {})
            purged_splits = purged_kfold_splits(
                features_res.X.index,
                n_splits=int(purged_cfg.get("n_splits", 5)),
                embargo=int(purged_cfg.get("embargo_bars", 5)),
                purge=int(purged_cfg.get("purge_bars", 5)),
            )
            device = detect_device()

            cached_results: Dict[str, Any] = {}
            split_map = {tuple(split.test_idx): i for i, split in enumerate(wf_splits)}
            split_folds = [
                SplitFold(
                    train_idx=np.array(split.train_idx),
                    val_idx=np.array(split.test_idx),
                    fold_meta=split.meta,
                )
                for split in wf_splits
            ]

            def _train_fn(train_idx, ctx):
                if cached_results.get("train_results") is None:
                    cached_results["train_results"] = train_models(
                        features_res.X,
                        features_res.y_dict,
                        split_folds,
                        cfg,
                        device,
                        prices=df["close"],
                        eval_settings=eval_settings,
                        fast=fast_mode,
                    )
                return cached_results["train_results"]

            def _eval_fn(model_results, test_idx, ctx):
                best = _select_best(model_results)
                fold_id = split_map.get(tuple(test_idx), None)
                if best is None or fold_id is None:
                    return {}
                for fm in best.fold_metrics:
                    if fm.fold == fold_id:
                        return {
                            "sharpe": fm.metric.get("sharpe"),
                            "max_drawdown": fm.metric.get("max_drawdown"),
                            "n_trades": fm.metric.get("n_trades"),
                        }
                return {}

            gate = gate_map.get(tf, tf)
            ctx = {"run_id": run_id, "gate": gate, "timeframe": tf}
            wf_report = validate_model(_train_fn, _eval_fn, wf_splits, seed, ctx)

            best = _select_best(cached_results.get("train_results", []) or [])
            if best is None:
                continue
            preds = _pred_series(best, df.index)
            out = compute_equity_and_metrics(df["close"], preds, eval_settings)
            df_bt = out["df"]
            gross = df_bt["pos_prev"] * df_bt["ret"]
            trades = _build_trades(df_bt)
            market_ctx = _market_ctx(df_bt)
            score_report = score_run(
                gross,
                trades,
                market_ctx,
                scoring_cfg,
                run_id=run_id,
                gate=gate,
                timeframe=tf,
                mode=mode_key,
            )
            mc_report = run_monte_carlo(
                gross.dropna().tolist(),
                robustness_cfg,
                seed,
                run_id=run_id,
                gate=gate,
                timeframe=tf,
            )
            decision = decide_release(wf_report.__dict__, score_report.__dict__, mc_report.__dict__, thresholds)
            registry_path = Path("octa") / "var" / "registry" / "models" / gate / tf / bucket / "champion.json"
            pack = (metrics_by_tf.get(tf, {}) or {}).get("pack", {}) if isinstance(metrics_by_tf, dict) else {}
            pkl_path = pack.get("pkl") if isinstance(pack, dict) else None
            joblib_path = _persist_joblib(pkl_path)
            update_registry(
                decision,
                {
                    "run_id": run_id,
                    "score": score_report.score,
                    "stability_ok": wf_report.aggregate_metrics.get("sharpe_cv", 0.0) <= float(thresholds.get("max_split_cv", 0.5)),
                    "pkl_path": pkl_path,
                    "joblib_path": joblib_path,
                    "artifact_sha": pack.get("pkl_sha") if isinstance(pack, dict) else None,
                },
                registry_path,
                thresholds,
            )
            outcomes.append(
                GateOutcome(
                    gate=gate,
                    timeframe=tf,
                    validation=wf_report.__dict__,
                    scoring=score_report.__dict__,
                    mc=mc_report.__dict__,
                    release=decision.__dict__,
                )
            )

    summary = {
        "run_id": run_id,
        "universe_size": len(universe),
        "outcomes": [o.__dict__ for o in outcomes],
    }
    _write_summary(summary)
    _write_audit(summary)
    return summary


def _write_summary(summary: Mapping[str, Any]) -> None:
    run_id = summary.get("run_id", "unknown")
    root = Path("octa") / "var" / "artifacts" / "summary" / run_id
    root.mkdir(parents=True, exist_ok=True)
    path = root / "institutional_summary.json"
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _write_audit(summary: Mapping[str, Any]) -> None:
    root = Path("octa") / "var" / "audit" / "institutional_train"
    root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    path = root / f"institutional_train_{safe_ts}.json"
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _prepare_config_override(config_path: str) -> Optional[str]:
    try:
        raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            return None
        paths = raw.get("paths") if isinstance(raw.get("paths"), dict) else {}
        state_dir = Path("octa") / "var" / "state_institutional"
        state_dir.mkdir(parents=True, exist_ok=True)
        paths["state_dir"] = str(state_dir)
        raw["paths"] = paths
        out_path = Path("octa") / "var" / "artifacts" / "institutional_training.yaml"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
        return str(out_path)
    except Exception:
        return None


def _persist_joblib(pkl_path: Optional[str]) -> Optional[str]:
    if not pkl_path:
        return None
    try:
        artifact = pickle.loads(Path(pkl_path).read_bytes())
        joblib_path = str(Path(pkl_path).with_suffix(".joblib"))
        joblib.dump(artifact, joblib_path)
        return joblib_path
    except Exception:
        return None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="octa_training/config/training.yaml")
    p.add_argument("--parquet-root", default="raw")
    p.add_argument("--bucket", default="default")
    p.add_argument("--universe-size", type=int, default=5)
    p.add_argument("--timeframes", default="1D,1H,30M,5M,1M")
    p.add_argument("--mode", default="paper", choices=["paper", "live-shadow"])
    p.add_argument("--resume", default="false")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    timeframes = [t.strip().upper() for t in str(args.timeframes).split(",") if t.strip()]
    _ = run_institutional_train(
        config_path=args.config,
        universe_size=args.universe_size,
        timeframes=timeframes,
        seed=args.seed,
        bucket=args.bucket,
        parquet_root=args.parquet_root,
        mode=args.mode,
    )


if __name__ == "__main__":
    main()
