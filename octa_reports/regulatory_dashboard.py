from typing import Any, Dict, List


class RegulatoryDashboard:
    """Aggregate deterministic regulatory and audit status from core managers.

    Inputs (optional):
    - control_matrix: ControlMatrix
    - evidence_store: list of Evidence objects (from audit_evidence)
    - postmortem_manager: PostmortemManager
    - model_risk: ModelRiskManager
    - change_mgmt: ChangeManagement
    """

    def __init__(
        self,
        *,
        control_matrix=None,
        evidence_store=None,
        postmortem_manager=None,
        model_risk=None,
        change_mgmt=None,
    ):
        self.control_matrix = control_matrix
        self.evidence_store = evidence_store or []
        self.postmortem_manager = postmortem_manager
        self.model_risk = model_risk
        self.change_mgmt = change_mgmt

    def control_coverage(self) -> Dict[str, Any]:
        if self.control_matrix is None:
            return {"objectives": {}, "controls": {}}
        objs = sorted(self.control_matrix.objectives.keys())
        controls = {}
        for oid in objs:
            ctrls = self.control_matrix.get_controls_for_objective(oid)
            controls[oid] = [
                {
                    "id": c.id,
                    "owner": c.owner,
                    "testable": bool(c.testable),
                    "evidence_links": list(c.evidence_links),
                }
                for c in sorted(ctrls, key=lambda x: x.id)
            ]
        return {"objectives": objs, "controls": controls}

    def evidence_status(self) -> Dict[str, Any]:
        # map control_id -> evidence ids
        mapping: Dict[str, List[str]] = {}
        for e in sorted(self.evidence_store, key=lambda x: x.id):
            for cid in getattr(e, "control_ids", []):
                mapping.setdefault(cid, []).append(str(getattr(e, "id", "")))
        return {
            "evidence_by_control": mapping,
            "total_evidence": len(self.evidence_store),
        }

    def open_findings(self) -> List[Dict[str, Any]]:
        findings: List[Dict[str, Any]] = []
        if self.postmortem_manager is None:
            return findings
        # inspect reviews for open tasks
        for _rid, review in getattr(self.postmortem_manager, "_reviews", {}).items():
            open_tasks = [t for t in review.tasks if not t.completed]
            if open_tasks:
                findings.append(
                    {
                        "incident_id": review.incident_id,
                        "reviewer": review.reviewer,
                        "open_tasks": [
                            {"id": t.id, "desc": t.description, "owner": t.owner}
                            for t in open_tasks
                        ],
                    }
                )
        # deterministic ordering
        return sorted(findings, key=lambda f: (f["incident_id"]))

    def model_approvals(self) -> Dict[str, Any]:
        if self.model_risk is None:
            return {"models": []}
        models = sorted(
            self.model_risk.list_models(), key=lambda m: (m.name, m.version)
        )
        return {
            "models": [
                {
                    "id": m.id,
                    "name": m.name,
                    "version": m.version,
                    "approved": m.approved,
                    "approved_by": m.approved_by,
                }
                for m in models
            ]
        }

    def change_activity(self) -> Dict[str, Any]:
        if self.change_mgmt is None:
            return {"requests": [], "recent_audit": []}
        reqs = []
        for rid, cr in sorted(self.change_mgmt._store.items(), key=lambda kv: kv[0]):
            reqs.append(
                {
                    "id": rid,
                    "title": cr.title,
                    "proposer": cr.proposer,
                    "approved": cr.approved,
                    "emergency": cr.emergency,
                    "release_tag": cr.release_tag,
                }
            )
        # recent audit items deterministic by timestamp order
        recent = sorted(self.change_mgmt.audit_log, key=lambda e: e.get("ts"))[-50:]
        return {"requests": reqs, "recent_audit": recent}

    def snapshot(self) -> Dict[str, Any]:
        return {
            "control_coverage": self.control_coverage(),
            "evidence_status": self.evidence_status(),
            "open_findings": self.open_findings(),
            "model_approvals": self.model_approvals(),
            "change_activity": self.change_activity(),
        }
