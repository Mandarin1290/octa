import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class BrokerCredentials:
    name: str
    api_key: Optional[str] = None
    secret: Optional[str] = None
    live: bool = False


class BrokerAdapter:
    """Central broker adapter enforcing SAFE MODE by default.

    - Default mode: SIMULATED. Any attempt to send a live order without
      explicit multi-approval is rejected.
    - Provides `place_order` and `simulate_order` API.
    - Thread-safe approval gating.
    """

    def __init__(self):
        self._mode = "SIMULATED"
        self._approved_live = False
        self._lock = threading.Lock()

    def enable_live(self, approvals: int = 0) -> None:
        """Enable live mode only when approvals >= required (policy enforced externally).

        This method must be called with multi-approval in production; tests may call it
        explicitly. Without calling, all orders remain simulated.
        """
        with self._lock:
            if approvals < 2:
                # enforce conservative default: require at least 2 approvals
                raise PermissionError("live enable requires multi-approval")
            self._approved_live = True
            self._mode = "LIVE"

    def disable_live(self) -> None:
        with self._lock:
            self._approved_live = False
            self._mode = "SIMULATED"

    def place_order(
        self, creds: BrokerCredentials, order: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Place order through adapter.

        If `creds.live` is True but adapter not approved, reject.
        """
        if creds.live:
            with self._lock:
                if not self._approved_live:
                    raise PermissionError(
                        "Attempt to use live broker without required approvals"
                    )
        # In SIMULATED mode or approved LIVE, route to simulated broker logic
        return self._route_to_simulator(creds, order)

    def _route_to_simulator(
        self, creds: BrokerCredentials, order: Dict[str, Any]
    ) -> Dict[str, Any]:
        # Minimal simulated execution: echo order with simulated id and status
        client_id = order.get("client_order_id") or f"sim-{id(order)}"
        return {
            "client_order_id": client_id,
            "status": "simulated",
            "broker": creds.name,
        }


__all__ = ["BrokerAdapter", "BrokerCredentials"]
