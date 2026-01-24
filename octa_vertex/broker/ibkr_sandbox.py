from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any, Dict, List

from octa_vertex.broker.ibkr_contract import IBKRConfig, IBKRContractAdapter


@dataclass
class SandboxScript:
    # mapping order_id -> list of status events to emit deterministically
    events: Dict[str, List[str]]


class IBKRSandboxDriver(IBKRContractAdapter):
    """Sandbox driver that replays scripted IBKR-like status sequences.

    Script example: {"o1": ["ACK", "SUBMITTED", "FILLED"], "o2": ["ACK", "REJECTED:INSUFFICIENT_MARGIN"]}
    """

    def __init__(
        self,
        script: SandboxScript | None = None,
        config: IBKRConfig | None = None,
        audit_fn=None,
        sentinel_api=None,
    ):
        super().__init__(config=config, audit_fn=audit_fn, sentinel_api=sentinel_api)
        self.script = script or SandboxScript(events={})
        # deterministic counters
        self._iters: Dict[str, Any] = {
            str(oid): itertools.cycle(seq) for oid, seq in self.script.events.items()
        }

    def submit_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        res = super().submit_order(order)
        oid = str(order.get("order_id") or "_unknown")
        # if scripted, immediately step the scripted status
        seq = self.script.events.get(oid)
        if seq:
            # If any scripted step is a rejection, treat as immediate reject to avoid ambiguous ACK-then-REJECT flows
            rej = next((s for s in seq if s.startswith("REJECTED")), None)
            if rej is not None:
                reason = rej.split(":", 1)[1] if ":" in rej else "REJECTED"
                self._orders[oid]["status"] = "REJECTED"
                self.audit("broker.submit_reject", {"order_id": oid, "reason": reason})
                return {"order_id": oid, "status": "REJECTED", "reason": reason}
            # set first status as provided by script
            first = seq[0]
            if first.startswith("REJECTED"):
                # convert to structured reject
                reason = first.split(":", 1)[1] if ":" in first else "REJECTED"
                self._orders[oid]["status"] = "REJECTED"
                self.audit("broker.submit_reject", {"order_id": oid, "reason": reason})
                return {"order_id": oid, "status": "REJECTED", "reason": reason}
            else:
                # ACK/SUBMITTED/FILLED etc
                self._orders[oid]["status"] = first
                self.audit("broker.order_status", {"order_id": oid, "status": first})
                # advance the internal iterator so the next `advance_order` call
                # yields the subsequent scripted status instead of repeating the first
                try:
                    it = self._iters.get(oid)
                    if it is None:
                        self._iters[oid] = itertools.cycle(seq)
                        it = self._iters[oid]
                    next(it)
                except Exception:
                    pass
                return {"order_id": oid, "status": first}
        return res

    def advance_order(self, order_id: str) -> Dict[str, Any]:
        seq = self.script.events.get(order_id)
        if not seq:
            return {"order_id": order_id, "status": "NO_SCRIPT"}
        # rotate
        it = self._iters.get(order_id)
        if not it:
            self._iters[order_id] = itertools.cycle(seq)
            it = self._iters[order_id]
        status = next(it)
        # process status
        if status.startswith("REJECTED"):
            reason = status.split(":", 1)[1] if ":" in status else "REJECTED"
            self._orders[order_id]["status"] = "REJECTED"
            self.audit(
                "broker.order_status",
                {"order_id": order_id, "status": "REJECTED", "reason": reason},
            )
            return {"order_id": order_id, "status": "REJECTED", "reason": reason}
        else:
            self._orders[order_id]["status"] = status
            self.audit("broker.order_status", {"order_id": order_id, "status": status})
            return {"order_id": order_id, "status": status}
