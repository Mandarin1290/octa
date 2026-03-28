from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class BrokerPaperOpsPolicy:
    require_readiness_status: str
    allow_runs_when_not_ready: bool
    max_runs_per_batch: int
    max_session_duration_minutes: int
    min_cooldown_seconds_between_runs: int
    paper_only: bool
    forbid_live_mode: bool
    stop_on_first_failure: bool
    max_consecutive_failures: int
    require_evidence_integrity: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "BrokerPaperOpsPolicy":
        required = (
            "require_readiness_status",
            "allow_runs_when_not_ready",
            "max_runs_per_batch",
            "max_session_duration_minutes",
            "min_cooldown_seconds_between_runs",
            "paper_only",
            "forbid_live_mode",
            "stop_on_first_failure",
            "max_consecutive_failures",
            "require_evidence_integrity",
        )
        missing = [key for key in required if key not in payload]
        if missing:
            raise ValueError(f"broker paper ops policy missing required keys: {missing}")
        return cls(
            require_readiness_status=str(payload["require_readiness_status"]),
            allow_runs_when_not_ready=bool(payload["allow_runs_when_not_ready"]),
            max_runs_per_batch=int(payload["max_runs_per_batch"]),
            max_session_duration_minutes=int(payload["max_session_duration_minutes"]),
            min_cooldown_seconds_between_runs=int(payload["min_cooldown_seconds_between_runs"]),
            paper_only=bool(payload["paper_only"]),
            forbid_live_mode=bool(payload["forbid_live_mode"]),
            stop_on_first_failure=bool(payload["stop_on_first_failure"]),
            max_consecutive_failures=int(payload["max_consecutive_failures"]),
            require_evidence_integrity=bool(payload["require_evidence_integrity"]),
        )


__all__ = ["BrokerPaperOpsPolicy"]
