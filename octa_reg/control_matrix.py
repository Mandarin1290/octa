from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class ControlObjective:
    id: str
    description: str
    domains: List[str] = field(default_factory=list)


@dataclass
class Control:
    id: str
    objective_id: str
    description: str
    owner: Optional[str] = None
    frequency: str = "manual"
    evidence_links: List[str] = field(default_factory=list)
    testable: bool = True
    created_ts: str = field(default_factory=_now_iso)


class ControlMatrix:
    """Registry of control objectives and controls, with validation and evidence linkage.

    Hard rules enforced programmatically:
    - Every objective must map to at least one control (flagged by `flag_missing_controls`).
    - Controls must have an `owner` (enforced by `enforce_ownership`).
    - Controls are testable (controls should set `testable=True`).
    """

    def __init__(self):
        self.objectives: Dict[str, ControlObjective] = {}
        self.controls: Dict[str, Control] = {}

    def register_objective(self, obj: ControlObjective) -> None:
        if obj.id in self.objectives:
            raise ValueError(f"objective already registered: {obj.id}")
        self.objectives[obj.id] = obj

    def register_control(self, ctrl: Control) -> None:
        if ctrl.id in self.controls:
            raise ValueError(f"control already registered: {ctrl.id}")
        if ctrl.objective_id not in self.objectives:
            raise ValueError(f"unknown objective for control: {ctrl.objective_id}")
        self.controls[ctrl.id] = ctrl

    def get_controls_for_objective(self, objective_id: str) -> List[Control]:
        return [c for c in self.controls.values() if c.objective_id == objective_id]

    def flag_missing_controls(self) -> List[str]:
        missing = [
            oid
            for oid in self.objectives.keys()
            if len(self.get_controls_for_objective(oid)) == 0
        ]
        return sorted(missing)

    def enforce_ownership(self) -> List[str]:
        """Return list of control ids missing owners. Caller may raise if desired."""
        missing = [cid for cid, c in self.controls.items() if not c.owner]
        return sorted(missing)

    def enforce_ownership_raise(self) -> None:
        missing = self.enforce_ownership()
        if missing:
            raise ValueError(f"controls missing owners: {missing}")

    def control_missing_for_any_objective(self) -> bool:
        return len(self.flag_missing_controls()) > 0

    def link_evidence(self, control_id: str, link: str) -> None:
        if control_id not in self.controls:
            raise KeyError("unknown control")
        self.controls[control_id].evidence_links.append(link)

    def controls_by_owner(self, owner: str) -> List[Control]:
        return [c for c in self.controls.values() if c.owner == owner]

    def validate_testable_controls(self) -> List[str]:
        """Return list of control ids that are not testable."""
        return [cid for cid, c in self.controls.items() if not c.testable]


__all__ = ["ControlObjective", "Control", "ControlMatrix"]
