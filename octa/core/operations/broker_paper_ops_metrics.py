from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def aggregate_broker_paper_ops_metrics(runs: Iterable[dict[str, Any]]) -> dict[str, Any]:
    run_list = list(runs)
    completed = [run for run in run_list if run.get("status") == "BROKER_PAPER_SESSION_COMPLETED"]
    failed = [run for run in run_list if run.get("status") != "BROKER_PAPER_SESSION_COMPLETED"]

    total_orders = 0
    total_fills = 0
    total_trades = 0
    aggregated_final_equity = 0.0
    aggregated_realized_pnl = 0.0
    max_drawdown = 0.0
    kill_switch_count = 0
    non_finite_flags: list[dict[str, Any]] = []

    for run in completed:
        evidence_dir = Path(run["evidence_dir"])
        metrics = _load_json(evidence_dir / "metrics.json")
        report = _load_json(evidence_dir / "broker_paper_report.json")
        policy = report.get("policy", {})
        total_orders += int(metrics.get("n_orders", 0))
        total_fills += int(metrics.get("n_fills", 0))
        total_trades += int(metrics.get("n_trades", metrics.get("total_trades", 0)))
        final_equity = float(metrics.get("final_equity", 0.0))
        aggregated_final_equity += final_equity
        aggregated_realized_pnl += final_equity - float(policy.get("paper_capital", 0.0))
        max_drawdown = max(max_drawdown, abs(float(metrics.get("max_drawdown", 0.0))))
        kill_switch_count += 1 if bool(metrics.get("kill_switch_triggered", False)) else 0
        metric_sources = [("executed_run", None, metrics)]
        source_evidence_dir = run.get("source_broker_paper_evidence_dir")
        if source_evidence_dir:
            source_path = Path(str(source_evidence_dir))
            source_metrics_path = source_path / "metrics.json"
            if source_metrics_path.exists():
                metric_sources.append(("source_broker_paper", str(source_path.resolve()), _load_json(source_metrics_path)))
        for origin, reference_path, source_metrics in metric_sources:
            for key, value in source_metrics.items():
                if isinstance(value, (int, float)) and not math.isfinite(float(value)):
                    non_finite_flags.append(
                        {
                            "origin": origin,
                            "run_sequence": int(run.get("sequence", 0)),
                            "reference_path": reference_path,
                            "metric": key,
                            "value": value,
                        }
                    )

    return {
        "n_runs_completed": len(completed),
        "n_runs_failed": len(failed),
        "n_total_orders": total_orders,
        "n_total_fills": total_fills,
        "n_total_trades": total_trades,
        "aggregated_final_equity": aggregated_final_equity,
        "aggregated_realized_pnl": aggregated_realized_pnl,
        "max_drawdown_across_runs": max_drawdown,
        "kill_switch_trigger_count": kill_switch_count,
        "non_finite_metric_flags": non_finite_flags,
    }


__all__ = ["aggregate_broker_paper_ops_metrics"]
