from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from octa_reg.reg_map import control_requirements_for


@dataclass
class ScenarioReport:
    scenario: str
    gaps: List[str]
    mapped_controls: Dict[str, List[str]]


class RegulatorySimulator:
    """Simulate regulatory scenarios and map findings to controls.

    Rules:
    - Simulation only; does not change system state.
    - Findings map to control ids where possible.
    - Conservative: report gaps when evidence/controls missing.
    """

    def __init__(self):
        pass

    def _match_controls(
        self, controls: List[Any], required_keywords: List[str]
    ) -> List[str]:
        """Return control ids whose id or description contain any of the required keywords."""
        matched = []
        for c in controls:
            cid = getattr(c, "id", None)
            desc = getattr(c, "description", "")
            hay = f"{cid} {desc}".lower() if cid else desc.lower()
            hay_norm = hay.replace("_", "")
            for kw in required_keywords:
                kw_norm = kw.replace("_", "").lower()
                if kw_norm in hay_norm or kw.lower() in hay:
                    if cid:
                        matched.append(cid)
                    break
        return matched

    def simulate_rir(
        self, control_matrix, controls_list: Optional[List[Any]] = None
    ) -> ScenarioReport:
        """Simulate a Regulator Information Request focused on record keeping.

        If `controls_list` provided, it's used as the universe of controls (Control objects).
        Otherwise `control_matrix.controls` is inspected.
        """
        required = control_requirements_for("record_keeping")
        ctrls = (
            controls_list
            if controls_list is not None
            else list(control_matrix.controls.values())
        )

        # use keywords from required controls
        keywords = [k.replace("_", "") for k in required]
        mapped = self._match_controls(ctrls, keywords)
        gaps = [] if mapped else ["missing_record_keeping_controls"]
        return ScenarioReport(
            scenario="rir", gaps=gaps, mapped_controls={"record_keeping": mapped}
        )

    def simulate_sudden_rule_change(
        self, control_matrix, change_mgmt=None
    ) -> ScenarioReport:
        """Simulate sudden rule change: check whether risk controls exist and change_mgmt can process rapid change.

        Reports gaps if no `risk_management` controls or change_mgmt lacks ability to attach rollback plans.
        """
        required = control_requirements_for("risk_management")
        ctrls = list(control_matrix.controls.values())
        keywords = [k.replace("_", "") for k in required]
        mapped = self._match_controls(ctrls, keywords)
        gaps = []
        if not mapped:
            gaps.append("missing_risk_management_controls")

        if change_mgmt is None:
            gaps.append("no_change_mgmt")
        else:
            # quick heuristic: ensure change_mgmt has create_request method
            if not hasattr(change_mgmt, "create_request"):
                gaps.append("change_mgmt_incompatible")

        return ScenarioReport(
            scenario="sudden_rule_change",
            gaps=gaps,
            mapped_controls={"risk_management": mapped},
        )

    def simulate_on_site_audit(
        self, control_matrix, audit_evidence_store: Optional[List[Any]] = None
    ) -> ScenarioReport:
        """Simulate on-site audit: expects presence of audit evidence and record-keeping controls.

        `audit_evidence_store` is a list of evidence objects with `id` and `hash` attributes.
        """
        required = control_requirements_for("record_keeping")
        ctrls = list(control_matrix.controls.values())
        keywords = [k.replace("_", "") for k in required]
        mapped = self._match_controls(ctrls, keywords)

        gaps = []
        if not mapped:
            gaps.append("missing_record_keeping_controls")

        if not audit_evidence_store or len(audit_evidence_store) == 0:
            gaps.append("no_audit_evidence")

        mapped_controls = {
            "record_keeping": mapped,
            "evidence_count": [
                str(len(audit_evidence_store) if audit_evidence_store else 0)
            ],
        }
        return ScenarioReport(
            scenario="on_site_audit", gaps=gaps, mapped_controls=mapped_controls
        )


__all__ = ["RegulatorySimulator", "ScenarioReport"]
