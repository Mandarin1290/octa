from __future__ import annotations

import datetime
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class HardeningCertification:
    """Evidence-only hardening certification generator.

    Inputs must be machine-verifiable evidence (lists of test records, drill records,
    numeric resilience scores and enumerated unresolved risks). The generator
    produces a deterministic, canonical JSON report and a report hash suitable
    for audit/evidence storage.
    """

    evidence: Dict[str, Any]
    created_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    report_hash: Optional[str] = None

    def _normalize_evidence(self) -> Dict[str, Any]:
        # Only include the allowed top-level keys; preserve raw evidence values
        allowed = [
            "chaos_tests",  # list of {name, passed: bool, evidence: {...}}
            "kill_switch_drills",  # list of {name, passed: bool, evidence: {...}}
            "resilience_scores",  # list of {component, score: float}
            "unresolved_risks",  # list of {id, description}
            "evidence_store",  # optional mapping of evidence id -> item
        ]

        out: Dict[str, Any] = {}
        for k in allowed:
            if k in self.evidence:
                out[k] = self.evidence[k]

        # attach metadata about generation time
        out["generated_at"] = self.created_at
        return out

    def generate(self) -> Dict[str, Any]:
        normalized = self._normalize_evidence()

        # Summaries (counts) are evidence-derived facts, not subjective statements
        chaos = normalized.get("chaos_tests", [])
        drills = normalized.get("kill_switch_drills", [])
        scores = normalized.get("resilience_scores", [])
        risks = normalized.get("unresolved_risks", [])

        summary = {
            "chaos_tests_total": len(chaos),
            "chaos_tests_passed": sum(1 for t in chaos if t.get("passed") is True),
            "kill_switch_drills_total": len(drills),
            "kill_switch_drills_passed": sum(
                1 for d in drills if d.get("passed") is True
            ),
            "resilience_scores_count": len(scores),
            "unresolved_risks_count": len(risks),
        }

        report = {
            "evidence": normalized,
            "summary": summary,
            "created_at": self.created_at,
        }

        # deterministic canonical JSON and report hash
        report_json = json.dumps(
            report, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        self.report_hash = hashlib.sha256(report_json.encode("utf-8")).hexdigest()
        report["report_hash"] = self.report_hash
        return report

    def export_json(self) -> str:
        if self.report_hash is None:
            self.generate()
        # return canonical serialized JSON
        return json.dumps(
            {
                "evidence": self._normalize_evidence(),
                "summary": {
                    "chaos_tests_total": len(self.evidence.get("chaos_tests", [])),
                    "chaos_tests_passed": sum(
                        1
                        for t in self.evidence.get("chaos_tests", [])
                        if t.get("passed") is True
                    ),
                    "kill_switch_drills_total": len(
                        self.evidence.get("kill_switch_drills", [])
                    ),
                    "kill_switch_drills_passed": sum(
                        1
                        for d in self.evidence.get("kill_switch_drills", [])
                        if d.get("passed") is True
                    ),
                    "resilience_scores_count": len(
                        self.evidence.get("resilience_scores", [])
                    ),
                    "unresolved_risks_count": len(
                        self.evidence.get("unresolved_risks", [])
                    ),
                },
                "created_at": self.created_at,
                "report_hash": self.report_hash,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
