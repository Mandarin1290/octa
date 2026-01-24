from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class FailureEvent:
    id: str
    hypothesis_id: str
    observed_modes: List[str]
    unexpected: List[str]
    details: Dict[str, Any]
    timestamp: str


class FailureModeRegistry:
    """Track allowed taxonomy and observed events linked to hypotheses."""

    def __init__(self, taxonomy: Optional[List[str]] = None):
        self.taxonomy = set(taxonomy or [])
        self._events: List[FailureEvent] = []

    def register_mode(self, mode: str) -> None:
        self.taxonomy.add(mode)

    def observe(
        self,
        hypothesis_id: str,
        observed_modes: List[str],
        details: Optional[Dict[str, Any]] = None,
    ) -> FailureEvent:
        details = details or {}
        unexpected = [m for m in observed_modes if m not in self.taxonomy]
        event = FailureEvent(
            id=f"evt-{len(self._events) + 1}",
            hypothesis_id=hypothesis_id,
            observed_modes=list(observed_modes),
            unexpected=unexpected,
            details=dict(details),
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        self._events.append(event)
        return event

    def get_events(self) -> List[FailureEvent]:
        return list(self._events)

    def get_events_for_hypothesis(self, hypothesis_id: str) -> List[FailureEvent]:
        return [e for e in self._events if e.hypothesis_id == hypothesis_id]

    def detect_unexpected(
        self, hypothesis_id: str, observed_modes: List[str]
    ) -> List[str]:
        return [m for m in observed_modes if m not in self.taxonomy]
