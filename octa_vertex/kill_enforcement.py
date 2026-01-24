from typing import Callable, Dict

from octa_sentinel.kill_switch import get_kill_switch


def check_kill(audit_fn: Callable[[str, Dict], None] | None = None) -> bool:
    """Return True if kill-switch blocks execution."""
    ks = get_kill_switch(audit_fn=audit_fn)
    state = ks.get_state()
    return state.name in ("TRIGGERED", "LOCKED")
