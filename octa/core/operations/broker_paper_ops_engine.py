from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from octa.core.broker_paper.broker_paper_adapter import InMemoryBrokerPaperAdapter
from octa.core.pipeline.broker_paper_runner import run_broker_paper
from octa.core.paper.market_data_adapter import InMemoryMarketDataAdapter

from .broker_paper_ops_metrics import aggregate_broker_paper_ops_metrics


def _build_market_data_adapter(replay_spec: Mapping[str, Any]) -> InMemoryMarketDataAdapter:
    if str(replay_spec.get("source", "")) != "inmemory_replay_v1":
        raise ValueError("only inmemory_replay_v1 is supported")
    bars = replay_spec.get("bars", {})
    if not isinstance(bars, Mapping):
        raise ValueError("replay bars must be a mapping")
    index = pd.to_datetime(list(bars["index"]), utc=True)
    frame = pd.DataFrame(
        {
            "open": list(bars["open"]),
            "high": list(bars["high"]),
            "low": list(bars["low"]),
            "close": list(bars["close"]),
            "volume": list(bars["volume"]),
        },
        index=index,
    )
    return InMemoryMarketDataAdapter.from_dataframe(str(replay_spec["symbol"]), frame)


def execute_broker_paper_ops(
    run_plan: Mapping[str, Any],
    *,
    evidence_root: str | Path = "octa/var/evidence",
    batch_run_id: str,
) -> dict[str, Any]:
    if str(run_plan.get("status", "")) != "OPS_PLAN_READY":
        return {
            "batch_status": "OPS_BLOCKED",
            "runs": [],
            "summary": {
                "reason": "run_plan_not_ready",
                "plan_status": run_plan.get("status"),
            },
        }

    policy = run_plan.get("policy", {})
    stop_on_first_failure = bool(policy.get("stop_on_first_failure", True))
    max_consecutive_failures = int(policy.get("max_consecutive_failures", 1))
    runs: list[dict[str, Any]] = []
    consecutive_failures = 0

    for planned in run_plan.get("planned_runs", []):
        try:
            market_data_adapter = _build_market_data_adapter(planned["market_data_replay"])
            broker_policy = dict(planned["broker_policy"])
            if str(broker_policy.get("require_broker_mode", "")) != "PAPER":
                raise ValueError("broker policy must require PAPER mode")
            broker_adapter = InMemoryBrokerPaperAdapter(
                mode="PAPER",
                fee_rate=float(broker_policy.get("paper_fee", 0.0)),
                slippage=float(broker_policy.get("paper_slippage", 0.0)),
            )
            run_id = f"{batch_run_id}_run{int(planned['sequence']):02d}"
            result = run_broker_paper(
                paper_session_evidence_dir=planned["paper_session_evidence_dir"],
                policy=broker_policy,
                market_data_adapter=market_data_adapter,
                broker_adapter=broker_adapter,
                evidence_root=evidence_root,
                run_id=run_id,
            )
            run_record = {
                "sequence": int(planned["sequence"]),
                "status": result["status"],
                "evidence_dir": result["evidence_dir"],
                "report_path": result["report_path"],
                "source_broker_paper_evidence_dir": planned["source_broker_paper_evidence_dir"],
            }
            runs.append(run_record)
            if result["status"] != "BROKER_PAPER_SESSION_COMPLETED":
                consecutive_failures += 1
                if stop_on_first_failure or consecutive_failures >= max_consecutive_failures:
                    aggregated = aggregate_broker_paper_ops_metrics(runs)
                    return {
                        "batch_status": "OPS_ABORTED",
                        "runs": runs,
                        "summary": {
                            "reason": "broker_paper_run_failed",
                            "consecutive_failures": consecutive_failures,
                            "aggregated_metrics": aggregated,
                        },
                    }
            else:
                consecutive_failures = 0
        except Exception as exc:
            consecutive_failures += 1
            runs.append(
                {
                    "sequence": int(planned["sequence"]),
                    "status": "BROKER_PAPER_SESSION_ABORTED",
                    "evidence_dir": None,
                    "report_path": None,
                    "source_broker_paper_evidence_dir": planned["source_broker_paper_evidence_dir"],
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            aggregated = aggregate_broker_paper_ops_metrics(runs)
            batch_status = "OPS_ABORTED" if stop_on_first_failure or consecutive_failures >= max_consecutive_failures else "OPS_COMPLETED"
            return {
                "batch_status": batch_status,
                "runs": runs,
                "summary": {
                    "reason": "exception_during_run",
                    "consecutive_failures": consecutive_failures,
                    "aggregated_metrics": aggregated,
                },
            }

    aggregated = aggregate_broker_paper_ops_metrics(runs)
    return {
        "batch_status": "OPS_COMPLETED",
        "runs": runs,
        "summary": {
            "n_runs": len(runs),
            "aggregated_metrics": aggregated,
        },
    }


__all__ = ["execute_broker_paper_ops"]
