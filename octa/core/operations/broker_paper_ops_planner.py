from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .broker_paper_ops_policy import BrokerPaperOpsPolicy


DEFAULT_REPLAY_SPEC = {
    "source": "inmemory_replay_v1",
    "symbol": "TEST",
    "bars": {
        "index": [
            "2026-02-01T00:00:00+00:00",
            "2026-02-02T00:00:00+00:00",
            "2026-02-03T00:00:00+00:00",
            "2026-02-04T00:00:00+00:00",
            "2026-02-05T00:00:00+00:00",
            "2026-02-06T00:00:00+00:00",
            "2026-02-07T00:00:00+00:00",
            "2026-02-08T00:00:00+00:00",
        ],
        "open": [100.0, 101.0, 102.0, 105.0, 105.5, 106.5, 110.0, 111.5],
        "high": [101.0, 102.0, 106.0, 106.0, 107.0, 111.0, 112.0, 113.0],
        "low": [99.0, 100.0, 101.0, 104.0, 105.0, 106.0, 109.0, 111.0],
        "close": [100.5, 101.5, 105.0, 105.5, 106.5, 110.0, 111.5, 112.0],
        "volume": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0],
    },
}


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def plan_broker_paper_runs(
    readiness_evidence_dir: str | Path,
    policy: BrokerPaperOpsPolicy | Mapping[str, Any],
) -> dict[str, Any]:
    resolved_policy = (
        policy if isinstance(policy, BrokerPaperOpsPolicy) else BrokerPaperOpsPolicy.from_mapping(policy)
    )
    readiness_dir = Path(readiness_evidence_dir)
    checks: list[dict[str, Any]] = []

    def add_check(name: str, passed: bool, value: Any, threshold: Any) -> None:
        checks.append(
            {
                "name": name,
                "status": "pass" if passed else "fail",
                "value": value,
                "threshold": threshold,
            }
        )

    required = (
        "readiness_report.json",
        "readiness_inventory.json",
        "readiness_governance_report.json",
        "readiness_metrics_report.json",
        "applied_readiness_policy.json",
        "evidence_manifest.json",
    )
    for name in required:
        add_check(f"required_file:{name}", (readiness_dir / name).exists(), (readiness_dir / name).exists(), True)
    if not all(item["status"] == "pass" for item in checks):
        return {
            "status": "OPS_PLAN_BLOCKED",
            "planned_runs": [],
            "checks": checks,
            "summary": {"reason": "missing_readiness_artifacts", "readiness_evidence_dir": str(readiness_dir.resolve())},
        }

    readiness_report = _load_json(readiness_dir / "readiness_report.json")
    inventory = _load_json(readiness_dir / "readiness_inventory.json")
    readiness_status = str(readiness_report.get("status", ""))
    add_check(
        "readiness_status_allowed",
        readiness_status == resolved_policy.require_readiness_status
        or (resolved_policy.allow_runs_when_not_ready and readiness_status == "BROKER_PAPER_NOT_READY"),
        readiness_status,
        resolved_policy.require_readiness_status,
    )
    add_check("paper_only_policy", resolved_policy.paper_only, resolved_policy.paper_only, True)
    add_check("forbid_live_mode_policy", resolved_policy.forbid_live_mode, resolved_policy.forbid_live_mode, True)
    add_check("max_runs_positive", resolved_policy.max_runs_per_batch > 0, resolved_policy.max_runs_per_batch, ">0")
    if not all(item["status"] == "pass" for item in checks):
        return {
            "status": "OPS_PLAN_BLOCKED",
            "planned_runs": [],
            "checks": checks,
            "summary": {"reason": "policy_block", "readiness_status": readiness_status},
        }

    chains = inventory.get("chains", [])
    positive_chains = [
        chain
        for chain in chains
        if str(chain.get("status", "")) == "BROKER_PAPER_SESSION_COMPLETED" and bool(chain.get("chain_complete", False))
    ]
    add_check("positive_chain_available", bool(positive_chains), len(positive_chains), ">=1")
    if not positive_chains:
        return {
            "status": "OPS_PLAN_BLOCKED",
            "planned_runs": [],
            "checks": checks,
            "summary": {"reason": "no_positive_broker_paper_chain"},
        }

    selected = positive_chains[: resolved_policy.max_runs_per_batch]
    planned_runs = []
    for index, chain in enumerate(selected, start=1):
        broker_report = _load_json(Path(chain["broker_paper_evidence_dir"]) / "broker_paper_report.json")
        planned_runs.append(
            {
                "sequence": index,
                "source_broker_paper_evidence_dir": chain["broker_paper_evidence_dir"],
                "paper_session_evidence_dir": chain["references"]["paper_session_evidence_dir"],
                "broker_policy": broker_report["policy"],
                "market_data_replay": DEFAULT_REPLAY_SPEC,
                "session_policy": {
                    "max_session_duration_minutes": resolved_policy.max_session_duration_minutes,
                    "paper_only": resolved_policy.paper_only,
                    "forbid_live_mode": resolved_policy.forbid_live_mode,
                },
                "cooldown_seconds_before_next_run": resolved_policy.min_cooldown_seconds_between_runs,
            }
        )

    return {
        "status": "OPS_PLAN_READY",
        "planned_runs": planned_runs,
        "checks": checks,
        "summary": {
            "readiness_evidence_dir": str(readiness_dir.resolve()),
            "readiness_status": readiness_status,
            "n_planned_runs": len(planned_runs),
        },
    }


__all__ = ["DEFAULT_REPLAY_SPEC", "plan_broker_paper_runs"]
