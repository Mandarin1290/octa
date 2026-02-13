from __future__ import annotations

import datetime as _dt
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _utc_now_iso_z() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class PostMortemReport:
    incident_id: str
    timeline: List[Dict[str, Any]]
    root_cause: str
    failed_safeguards: List[str]
    remediation_actions: List[Dict[str, Any]]
    evidence: Dict[str, str]
    metadata: Dict[str, Any]
    created_at: str = field(default_factory=_utc_now_iso_z)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "timeline": self.timeline,
            "root_cause": self.root_cause,
            "failed_safeguards": self.failed_safeguards,
            "remediation_actions": self.remediation_actions,
            "evidence": self.evidence,
            "metadata": self.metadata,
            "created_at": self.created_at,
            "blame_free": True,
        }


def _parse_timeline(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Normalize and sort by timestamp (ISO strings expected)
    def _ts(e: Dict[str, Any]) -> str:
        return e.get("ts", "")

    sorted_events = sorted(events, key=_ts)
    return sorted_events


def _determine_root_cause(incident: Dict[str, Any]) -> str:
    # Heuristic: prefer explicit root_cause if present; else first failed safeguard; else first error event
    if incident.get("root_cause"):
        return incident["root_cause"]

    fs = incident.get("failed_safeguards") or []
    if fs:
        return fs[0]

    # scan timeline for error markers
    events = incident.get("events", [])
    for e in events:
        if e.get("type") == "error" or e.get("severity") == "critical":
            return e.get("message", "unknown_error")

    return "undetermined"


def _collect_failed_safeguards(incident: Dict[str, Any]) -> List[str]:
    return list(incident.get("failed_safeguards", []))


def _suggest_remediations(failed: List[str]) -> List[Dict[str, Any]]:
    suggestions: List[Dict[str, Any]] = []
    mapping = {
        "risk_limit_breach": {
            "action": "tighten_limits",
            "description": "Reduce risk limits and require multi-approval for overrides.",
        },
        "stale_data": {
            "action": "failover_to_fallback_feed",
            "description": "Switch to validated fallback data feed and run reconciliation.",
        },
        "execution_disconnected": {
            "action": "reconnect_exchanges",
            "description": "Restore connectivity and replay pending orders in dry-run mode.",
        },
        "partial_persistence": {
            "action": "verify_and_restore_snapshot",
            "description": "Verify snapshot integrity and restore from latest WORM snapshot.",
        },
    }

    for f in failed:
        suggestions.append(
            mapping.get(
                f, {"action": "investigate", "description": f"Investigate failure: {f}"}
            )
        )

    # add a generic continuous improvement action
    suggestions.append(
        {
            "action": "postmortem_review",
            "description": "Schedule blameless postmortem and track remediation to completion.",
        }
    )
    return suggestions


def _collect_evidence_hashes(incident: Dict[str, Any]) -> Dict[str, str]:
    evidence = incident.get("evidence", {})
    # If evidence items provided, compute deterministic hashes where missing
    out: Dict[str, str] = {}
    for k, v in evidence.items():
        if (
            isinstance(v, str)
            and len(v) == 64
            and all(c in "0123456789abcdef" for c in v.lower())
        ):
            out[k] = v
        else:
            out[k] = canonical_hash(v)
    # include snapshot of incident
    out.setdefault("incident_snapshot", canonical_hash(incident))
    return out


def generate_postmortem(
    incident: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None
) -> PostMortemReport:
    metadata = metadata or {}
    incident_id = incident.get("id") or canonical_hash(incident)
    timeline = _parse_timeline(incident.get("events", []))
    root = _determine_root_cause(incident)
    failed = _collect_failed_safeguards(incident)
    remediations = _suggest_remediations(failed)
    evidence = _collect_evidence_hashes(incident)

    report = PostMortemReport(
        incident_id=incident_id,
        timeline=timeline,
        root_cause=root,
        failed_safeguards=failed,
        remediation_actions=remediations,
        evidence=evidence,
        metadata=metadata,
    )

    return report


from dataclasses import dataclass, field
from datetime import datetime

from octa_ops.incidents import Incident, Severity


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class RemediationTask:
    id: str
    description: str
    owner: Optional[str]
    created_ts: str
    completed: bool = False
    completed_ts: Optional[str] = None


@dataclass
class Review:
    incident_id: str
    reviewer: str
    required: bool
    started_ts: str
    timeline: List[Dict[str, Any]] = field(default_factory=list)
    causes: List[Dict[str, Any]] = field(default_factory=list)
    tasks: List[RemediationTask] = field(default_factory=list)


class PostmortemManager:
    """Automate post-incident reviews for S2+ incidents.

    Hard rules:
    - Every S2+ incident requires a review.
    - A root cause categorization must be recorded.
    - Follow-up remediation tasks must be tracked until completion.
    """

    CAUSE_CATEGORIES = {"network", "data", "human", "process", "system", "third_party"}

    def __init__(self):
        self._reviews: Dict[str, Review] = {}

    def is_review_required(self, incident: Incident) -> bool:
        return incident.severity.value >= Severity.S2.value

    def start_review(self, incident: Incident, reviewer: str) -> Review:
        required = self.is_review_required(incident)
        r = Review(
            incident_id=incident.id,
            reviewer=reviewer,
            required=required,
            started_ts=_now_iso(),
        )
        # initial timeline event: incident recorded
        r.timeline.append(
            {
                "ts": incident.ts,
                "actor": incident.reporter,
                "event": "incident_reported",
                "title": incident.title,
                "severity": incident.severity.name,
            }
        )
        self._reviews[incident.id] = r
        return r

    def add_timeline_event(
        self,
        incident_id: str,
        event: str,
        actor: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        if incident_id not in self._reviews:
            raise KeyError("review not found")
        self._reviews[incident_id].timeline.append(
            {"ts": _now_iso(), "actor": actor, "event": event, "details": details or {}}
        )

    def categorize_cause(
        self,
        incident_id: str,
        category: str,
        rationale: str,
        actor: Optional[str] = None,
    ) -> None:
        if category not in self.CAUSE_CATEGORIES:
            raise ValueError("invalid cause category")
        if incident_id not in self._reviews:
            raise KeyError("review not found")
        self._reviews[incident_id].causes.append(
            {
                "ts": _now_iso(),
                "actor": actor,
                "category": category,
                "rationale": rationale,
            }
        )

    def add_remediation_task(
        self,
        incident_id: str,
        task_id: str,
        description: str,
        owner: Optional[str] = None,
    ) -> None:
        if incident_id not in self._reviews:
            raise KeyError("review not found")
        task = RemediationTask(
            id=task_id, description=description, owner=owner, created_ts=_now_iso()
        )
        self._reviews[incident_id].tasks.append(task)

    def complete_task(
        self, incident_id: str, task_id: str, actor: Optional[str] = None
    ) -> None:
        if incident_id not in self._reviews:
            raise KeyError("review not found")
        tasks = self._reviews[incident_id].tasks
        for t in tasks:
            if t.id == task_id:
                t.completed = True
                t.completed_ts = _now_iso()
                self.add_timeline_event(
                    incident_id, f"task_completed:{task_id}", actor=actor
                )
                return
        raise KeyError("task not found")

    def list_open_tasks(self, incident_id: str) -> List[RemediationTask]:
        if incident_id not in self._reviews:
            raise KeyError("review not found")
        return [t for t in self._reviews[incident_id].tasks if not t.completed]

    def get_review(self, incident_id: str) -> Review:
        return self._reviews[incident_id]
