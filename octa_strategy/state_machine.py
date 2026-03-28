from typing import Dict, List

# IMPORTANT: This module is RESEARCH LAYER ONLY.
# Not wired into production. See ARCHITECTURE_FORENSICS.md § D2, D6.
#
# Lifecycle order (must match octa/core/governance/lifecycle_controller.py):
#   IDEA → SHADOW → PAPER → LIVE
# Shadow validation comes BEFORE paper trading — this is a hard governance requirement.
# The previous (incorrect) order IDEA→PAPER→SHADOW was fixed 2026-03-21.


class TransitionError(Exception):
    pass


class LifecycleState:
    IDEA = "IDEA"
    SHADOW = "SHADOW"
    PAPER = "PAPER"
    LIVE = "LIVE"
    SUSPENDED = "SUSPENDED"
    RETIRED = "RETIRED"


ALLOWED_TRANSITIONS: Dict[str, List[str]] = {
    LifecycleState.IDEA: [LifecycleState.SHADOW, LifecycleState.RETIRED],
    LifecycleState.SHADOW: [
        LifecycleState.PAPER,
        LifecycleState.SUSPENDED,
        LifecycleState.RETIRED,
    ],
    LifecycleState.PAPER: [
        LifecycleState.LIVE,
        LifecycleState.SUSPENDED,
        LifecycleState.RETIRED,
    ],
    LifecycleState.LIVE: [LifecycleState.SUSPENDED, LifecycleState.RETIRED],
    LifecycleState.SUSPENDED: [
        LifecycleState.SHADOW,
        LifecycleState.PAPER,
        LifecycleState.RETIRED,
    ],
    LifecycleState.RETIRED: [],
}


def is_transition_allowed(from_state: str, to_state: str) -> bool:
    return to_state in ALLOWED_TRANSITIONS.get(from_state, [])
