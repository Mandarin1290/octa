from typing import Dict, List


class TransitionError(Exception):
    pass


class LifecycleState:
    IDEA = "IDEA"
    PAPER = "PAPER"
    SHADOW = "SHADOW"
    LIVE = "LIVE"
    SUSPENDED = "SUSPENDED"
    RETIRED = "RETIRED"


ALLOWED_TRANSITIONS: Dict[str, List[str]] = {
    LifecycleState.IDEA: [LifecycleState.PAPER, LifecycleState.RETIRED],
    LifecycleState.PAPER: [
        LifecycleState.SHADOW,
        LifecycleState.SUSPENDED,
        LifecycleState.RETIRED,
    ],
    LifecycleState.SHADOW: [
        LifecycleState.LIVE,
        LifecycleState.SUSPENDED,
        LifecycleState.RETIRED,
    ],
    LifecycleState.LIVE: [LifecycleState.SUSPENDED, LifecycleState.RETIRED],
    LifecycleState.SUSPENDED: [
        LifecycleState.PAPER,
        LifecycleState.SHADOW,
        LifecycleState.RETIRED,
    ],
    LifecycleState.RETIRED: [],
}


def is_transition_allowed(from_state: str, to_state: str) -> bool:
    return to_state in ALLOWED_TRANSITIONS.get(from_state, [])
