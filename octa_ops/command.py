from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from octa_ops.incidents import IncidentManager


@dataclass
class CommandAudit:
    ts: str
    actor: str
    action: str
    detail: Dict[str, Any]


@dataclass
class CommandState:
    incident_id: str
    commander: Optional[str]
    commander_role: Optional[str]
    assigned_ts: str
    timeout_seconds: int
    escalation_index: int = 0
    audits: List[CommandAudit] = field(default_factory=list)


class CommandManager:
    """Centralized incident command logic.

    - Exactly one commander per incident enforced.
    - Escalation path derived from `IncidentManager.ESCALATION_RULES`.
    - Manual overrides require audit entries.
    """

    def __init__(
        self,
        incident_manager: IncidentManager,
        role_to_user: Dict[str, str] | None = None,
        now_fn: Callable[[], datetime] | None = None,
    ):
        self.im = incident_manager
        self.states: Dict[str, CommandState] = {}
        self.role_to_user = role_to_user or {}
        self.now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    def _iso_now(self) -> str:
        return self.now_fn().isoformat()

    def start_command(
        self, incident_id: str, initial_timeout: int = 300
    ) -> CommandState:
        if incident_id in self.states:
            raise KeyError("command already started for incident")
        if incident_id not in self.im._store:
            raise KeyError("unknown incident")
        ts = self._iso_now()
        st = CommandState(
            incident_id=incident_id,
            commander=None,
            commander_role=None,
            assigned_ts=ts,
            timeout_seconds=initial_timeout,
        )
        self.states[incident_id] = st
        return st

    def assign_commander(
        self,
        incident_id: str,
        commander: str,
        role: str,
        actor: str,
        reason: str = "assigned",
    ) -> None:
        st = self.states.get(incident_id)
        if not st:
            raise KeyError("command not started for incident")
        if st.commander is not None and st.commander != commander:
            raise RuntimeError(
                "exactly one command authority allowed; surrender or override required"
            )
        st.commander = commander
        st.commander_role = role
        st.assigned_ts = self._iso_now()
        st.audits.append(
            CommandAudit(
                ts=st.assigned_ts,
                actor=actor,
                action=reason,
                detail={"commander": commander, "role": role},
            )
        )

    def override_commander(
        self, incident_id: str, new_commander: str, actor: str, reason: str
    ) -> None:
        st = self.states.get(incident_id)
        if not st:
            raise KeyError("command not started for incident")
        # record audit before change
        ts = self._iso_now()
        st.audits.append(
            CommandAudit(
                ts=ts,
                actor=actor,
                action="override",
                detail={"from": st.commander, "to": new_commander, "reason": reason},
            )
        )
        st.commander = new_commander
        st.assigned_ts = ts

    def get_commander(self, incident_id: str) -> Optional[str]:
        st = self.states.get(incident_id)
        return st.commander if st else None

    def check_escalations(self) -> List[Dict[str, Any]]:
        """Check all command states and perform escalation if timeout exceeded.

        Returns list of escalation actions performed.
        """
        actions = []
        now = self.now_fn()
        for incident_id, st in list(self.states.items()):
            assigned_time = datetime.fromisoformat(st.assigned_ts)
            if (now - assigned_time).total_seconds() < st.timeout_seconds:
                continue
            # timeout exceeded -> escalate
            inc = self.im.get_incident(incident_id)
            escalation_roles = self.im.ESCALATION_RULES.get(inc.severity, [])
            # choose next role based on escalation_index
            next_index = min(st.escalation_index, len(escalation_roles) - 1)
            # advance to next role if possible
            if st.commander_role in escalation_roles:
                try:
                    cur_idx = escalation_roles.index(st.commander_role)
                except ValueError:
                    cur_idx = -1
                next_index = min(len(escalation_roles) - 1, cur_idx + 1)
            else:
                next_index = 0

            if next_index < 0 or next_index >= len(escalation_roles):
                continue

            next_role = escalation_roles[next_index]
            # map role to user if available
            next_user = self.role_to_user.get(next_role, f"role:{next_role}")
            # perform assignment (override allowed via escalation)
            ts = self._iso_now()
            st.audits.append(
                CommandAudit(
                    ts=ts,
                    actor="escalation_engine",
                    action="escalate",
                    detail={
                        "from_role": st.commander_role,
                        "to_role": next_role,
                        "assigned_to": next_user,
                    },
                )
            )
            st.commander = next_user
            st.commander_role = next_role
            st.assigned_ts = ts
            st.escalation_index = next_index + 1
            actions.append(
                {
                    "incident_id": incident_id,
                    "new_commander": next_user,
                    "role": next_role,
                }
            )

        return actions


__all__ = ["CommandManager", "CommandState", "CommandAudit"]
