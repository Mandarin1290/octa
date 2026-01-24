from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

import yaml

from octa_ops.autopilot.budgets import BudgetExceeded, ResourceBudgetController
from octa_ops.autopilot.cascade_train import (
    CascadePolicy,
    run_cascade_training,
    write_gate_matrix,
)
from octa_ops.autopilot.data_quality import (
    DataQualityPolicy,
    evaluate_data_quality,
    write_quality_outputs,
)
from octa_ops.autopilot.global_gate import (
    GlobalGatePolicy,
    evaluate_global_gate,
    write_global_outputs,
)
from octa_ops.autopilot.paper_runner import run_paper
from octa_ops.autopilot.registry import ArtifactRegistry
from octa_ops.autopilot.types import GateDecision, normalize_timeframe, now_utc_iso
from octa_ops.autopilot.universe import discover_universe


def _load_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}


def main() -> None:
    ap = argparse.ArgumentParser(description="OCTA Autopilot: universe→gates→cascade training→promote to paper")
    ap.add_argument("--config", required=True, help="Autopilot config YAML")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--run-paper", action="store_true", help="After promotion, attempt to run paper_runner (fail-closed if broker not wired)")
    args = ap.parse_args()

    cfg = _load_yaml(args.config)
    run_id = args.run_id or cfg.get("run_id") or now_utc_iso().replace(":", "").replace("-", "")[:15] + "Z"

    budgets = cfg.get("budgets", {}) if isinstance(cfg.get("budgets"), dict) else {}
    budget = ResourceBudgetController(
        max_runtime_s=int(budgets.get("max_runtime_s", 3600)),
        max_ram_mb=int(budgets.get("max_ram_mb", 12000)),
        max_threads=int(budgets.get("max_threads", 4)),
        max_disk_mb=int(budgets.get("max_disk_mb", 0) or 0),
        disk_root=str(Path("artifacts") / "runs" / run_id),
    )
    budget.apply_thread_caps()

    reg = ArtifactRegistry(root=str(cfg.get("registry_root", "artifacts")))
    reg.record_run_start(run_id, cfg)

    run_dir = Path("artifacts") / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # Universe discovery
    ucfg = cfg.get("universe", {}) if isinstance(cfg.get("universe"), dict) else {}
    universe = discover_universe(
        stock_dir=str(ucfg.get("stock_dir", "raw/Stock_parquet")),
        fx_dir=str(ucfg.get("fx_dir", "raw/FX_parquet")),
        crypto_dir=str(ucfg.get("crypto_dir", "raw/Crypto_parquet")),
        futures_dir=str(ucfg.get("futures_dir", "raw/Future_parquet")),
        asset_map_path=str(ucfg.get("asset_map_path", "assets/asset_map.yaml")),
        limit=int(args.limit or ucfg.get("limit", 0) or 0),
    )

    (run_dir / "universe_candidates.json").write_text(
        json.dumps([u.__dict__ for u in universe], ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    # Data quality gate
    dq_policy = DataQualityPolicy(**(cfg.get("data_quality", {}) or {}))
    tfs = [normalize_timeframe(t) for t in (cfg.get("timeframes") or ["1D", "1H", "30M", "5M", "1M"])]

    dq_decisions = []
    for u in universe:
        budget.checkpoint("dq")
        for tf in tfs:
            pq = (u.parquet_paths or {}).get(tf)
            if not pq:
                dq_decisions.append(GateDecision(u.symbol, tf, "data_quality", "SKIP", "missing_parquet", {"expected": tf}))
                continue
            dq_decisions.append(
                evaluate_data_quality(symbol=u.symbol, timeframe=tf, parquet_path=pq, asset_class=u.asset_class, policy=dq_policy)
            )
            reg.upsert_gate(run_id, u.symbol, tf, "data_quality", dq_decisions[-1].status, dq_decisions[-1].reason, json.dumps(dq_decisions[-1].details or {}, default=str))

    write_quality_outputs(run_dir=str(run_dir), decisions=dq_decisions, timeframes=tfs)

    # Global gate (1D only)
    gg_policy = GlobalGatePolicy(**(cfg.get("global_gate", {}) or {}))
    global_decisions = {}
    for u in universe:
        budget.checkpoint("global")
        pq1d = (u.parquet_paths or {}).get("1D")
        if not pq1d:
            global_decisions[u.symbol] = GateDecision(u.symbol, "1D", "global", "SKIP", "missing_1d", {})
            continue
        d = evaluate_global_gate(symbol=u.symbol, parquet_1d_path=pq1d, policy=gg_policy, cache_dir=str(run_dir / "global_features_store"))
        global_decisions[u.symbol] = d
        reg.upsert_gate(run_id, u.symbol, "1D", "global", d.status, d.reason, json.dumps(d.details or {}, default=str))

    write_global_outputs(run_dir=str(run_dir), decisions=global_decisions)

    # Cascaded training
    cascade_order = [normalize_timeframe(t) for t in (cfg.get("cascade_order") or ["1D", "1H", "30M", "5M", "1M"])]
    cascade = CascadePolicy(order=cascade_order)
    train_cfg_path = str(cfg.get("training_config", "configs/dev.yaml"))

    train_decisions = []
    for u in universe:
        budget.checkpoint("train")
        # Eligibility: require data_quality PASS for 1D + global PASS
        dq1d = next((d for d in dq_decisions if d.symbol == u.symbol and normalize_timeframe(d.timeframe) == "1D"), None)
        gg = global_decisions.get(u.symbol)
        if not dq1d or dq1d.status != "PASS":
            continue
        if not gg or gg.status != "PASS":
            continue
        dlist, _metrics = run_cascade_training(
            run_id=run_id,
            config_path=train_cfg_path,
            symbol=u.symbol,
            asset_class=u.asset_class,
            parquet_paths=u.parquet_paths or {},
            cascade=cascade,
            safe_mode=True,
            reports_dir=str(Path(cfg.get("reports_dir", "reports"))),
        )
        # Read the metrics bundle written by run_cascade_training for this symbol.
        metrics_path = Path(cfg.get("reports_dir", "reports")) / "autopilot" / run_id / f"model_metrics_{u.symbol}.json"
        try:
            metrics_bundle: Dict[str, Any] = json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.exists() else {}
        except Exception:
            metrics_bundle = {}

        for d in dlist:
            tf = normalize_timeframe(d.timeframe)
            # Persist metrics (best-effort)
            mb = metrics_bundle.get(tf) if isinstance(metrics_bundle, dict) else None
            if isinstance(mb, dict):
                reg.add_metrics(
                    run_id=run_id,
                    symbol=u.symbol,
                    timeframe=tf,
                    stage="train",
                    metrics_json=json.dumps(mb.get("metrics"), default=str) if mb.get("metrics") is not None else None,
                    gate_json=json.dumps(mb.get("gate"), default=str) if mb.get("gate") is not None else None,
                )

            # Fail-closed packaging invariant: a PASS decision must have a PKL.
            effective = d
            pack = (mb or {}).get("pack") if isinstance(mb, dict) else None
            pkl_path = str((pack or {}).get("pkl") or "") if isinstance(pack, dict) else ""
            sha_txt = str((pack or {}).get("pkl_sha") or "") if isinstance(pack, dict) else ""

            if d.status == "PASS":
                pkl_ok = bool(pkl_path) and Path(pkl_path).exists() and Path(pkl_path.replace(".pkl", ".sha256")).exists()
                if not pkl_ok:
                    effective = GateDecision(u.symbol, tf, d.stage, "FAIL", "packaging_missing_pkl", {"expected_pkl": pkl_path})

            train_decisions.append(effective)
            reg.upsert_gate(run_id, u.symbol, tf, "train", effective.status, effective.reason, json.dumps(effective.details or {}, default=str))

            # Fail-closed demotion: if the latest run does not PASS, remove any existing paper promotion.
            if effective.status in {"FAIL", "SKIP"}:
                try:
                    reg.clear_promotion(symbol=u.symbol, timeframe=tf, level="paper")
                except Exception:
                    pass

            # Auto-promote PASS->paper using the actual packaged artifact path (per timeframe)
            if effective.status == "PASS" and pkl_path:
                meta_path = pkl_path.replace(".pkl", ".meta.json")
                meta_json = None
                try:
                    if Path(meta_path).exists():
                        meta_json = Path(meta_path).read_text(encoding="utf-8")
                except Exception:
                    meta_json = None
                art_id = reg.add_artifact(run_id, u.symbol, tf, "tradeable", pkl_path, sha_txt, 1, meta_json=meta_json)
                reg.promote(u.symbol, tf, art_id, level="paper")

    write_gate_matrix(run_dir=str(run_dir), decisions=train_decisions, cascade_order=cascade_order)

    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "universe": len(universe),
                "dq": len(dq_decisions),
                "global": len(global_decisions),
                "train": len(train_decisions),
                "registry_db": str(reg.paths.db_path),
                "rss_peak_mb": budget.rss_peak_mb,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    reg.record_run_end(run_id, "OK")

    # Optional: run paper trading loop immediately after promotion.
    if bool(args.run_paper) or bool((cfg.get("paper") or {}).get("enabled", False)):
        paper_cfg = cfg.get("paper") if isinstance(cfg.get("paper"), dict) else {}
        _ = run_paper(
            run_id=run_id,
            config_path=train_cfg_path,
            registry_root=str(cfg.get("registry_root", "artifacts")),
            ledger_dir=str(paper_cfg.get("ledger_dir", "artifacts/ledger_paper")),
            level=str(paper_cfg.get("level", "paper")),
            live_enable=bool(paper_cfg.get("live_enable", False)),
            last_n_rows=int(paper_cfg.get("last_n_rows", 300)),
            paper_log_path=str(paper_cfg.get("paper_log_path", "artifacts/paper_trade_log.ndjson")),
            max_runtime_s=int((paper_cfg.get("budgets") or {}).get("max_runtime_s", 3600)),
            max_ram_mb=int((paper_cfg.get("budgets") or {}).get("max_ram_mb", 12000)),
            max_threads=int((paper_cfg.get("budgets") or {}).get("max_threads", 4)),
        )


if __name__ == "__main__":
    try:
        main()
    except BudgetExceeded as e:
        raise SystemExit(str(e)) from e
