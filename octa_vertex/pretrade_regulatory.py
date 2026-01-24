from typing import Callable, Dict, Optional, Tuple

from octa_sentinel.kill_switch import get_kill_switch
from octa_sentinel.regulatory_rules import RegulatoryRuleEngine


class PreTradeRegulator:
    def __init__(
        self,
        config: Dict | None = None,
        sentinel_api=None,
        audit_fn: Optional[Callable] = None,
    ):
        self.engine = RegulatoryRuleEngine(
            config=config or {}, sentinel_api=sentinel_api, audit_fn=audit_fn
        )
        # wire global kill switch
        self._kill = get_kill_switch(audit_fn=audit_fn)

    def pre_trade_check(
        self,
        order: Dict,
        positions_lookup: Optional[Callable[[str, str], float]] = None,
        locates_lookup: Optional[Callable[[str, str], bool]] = None,
    ) -> Tuple[bool, str]:
        # Hard kill-switch enforcement: non-bypassable
        state = self._kill.get_state()
        if state in (
            self._kill.__class__.state.__class__.__mro__[1].__class__ if False else (),
        ):
            pass
        # simpler check
        if state.name in ("TRIGGERED", "LOCKED"):
            return False, "kill_switch"

        return self.engine.pre_trade_check(
            order, positions_lookup=positions_lookup, locates_lookup=locates_lookup
        )

    def record_cancel(self, order: Dict):
        return self.engine.record_event("CANCEL", order)
