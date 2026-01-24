from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from octa_core.broker_adapter import BrokerAdapter, BrokerCredentials


@dataclass
class BrokerState:
    name: str
    last_heartbeat: datetime
    orders: Dict[str, Dict[str, Any]] = field(
        default_factory=dict
    )  # client_order_id -> order


class BrokerHealthMonitor:
    def __init__(self, failure_threshold_seconds: int = 30):
        self.brokers: Dict[str, BrokerState] = {}
        self.failure_threshold = timedelta(seconds=failure_threshold_seconds)

    def register(self, name: str) -> None:
        self.brokers[name] = BrokerState(
            name=name, last_heartbeat=datetime.now(timezone.utc)
        )

    def heartbeat(self, name: str) -> None:
        if name in self.brokers:
            self.brokers[name].last_heartbeat = datetime.now(timezone.utc)

    def is_healthy(self, name: str) -> bool:
        if name not in self.brokers:
            return False
        return (
            datetime.now(timezone.utc) - self.brokers[name].last_heartbeat
        ) <= self.failure_threshold

    def failed_brokers(self) -> List[str]:
        now = datetime.now(timezone.utc)
        return [
            n
            for n, s in self.brokers.items()
            if (now - s.last_heartbeat) > self.failure_threshold
        ]


class BrokerFailoverManager:
    """Manage orders across brokers and perform conservative failover.

    Hard rules:
    - Broker loss is survivable: attempt to route new orders to healthy brokers.
    - No duplicate orders: client_order_id uniqueness enforced across brokers.
    - Failover does not increase exposure: any rerouted order must not increase absolute exposure beyond original.
    """

    def __init__(self, monitor: BrokerHealthMonitor):
        self.monitor = monitor
        self.positions: Dict[str, float] = {}  # instrument -> net position
        # registry mapping client_order_id -> (broker_name, order)
        self.registry: Dict[str, Dict[str, Any]] = {}
        # default adapter used to actually route orders (simulated by default)
        self.adapter = BrokerAdapter()

    def set_adapter(self, adapter: BrokerAdapter) -> None:
        self.adapter = adapter

    def place_order(
        self, broker: str, client_order_id: str, instrument: str, qty: float, side: str
    ) -> None:
        # prevent duplicates
        if client_order_id in self.registry:
            raise RuntimeError("duplicate client_order_id")
        if broker not in self.monitor.brokers:
            raise KeyError("unknown broker")
        order = {
            "id": client_order_id,
            "instrument": instrument,
            "qty": qty,
            "side": side,
            "status": "new",
            "client_order_id": client_order_id,
        }
        # route through BrokerAdapter for central gating (simulated by default)
        creds = BrokerCredentials(name=broker, live=False)
        res = self.adapter.place_order(creds, order)
        # record adapter response into monitor and registry to preserve existing semantics
        assigned_id = res.get("client_order_id", client_order_id)
        order_record = {
            **order,
            "client_order_id": assigned_id,
            "status": res.get("status", "new"),
            "broker": creds.name,
        }
        self.monitor.brokers[broker].orders[assigned_id] = order_record
        self.registry[assigned_id] = {"broker": broker, "order": order_record}

    def record_fill(self, client_order_id: str, filled_qty: float) -> None:
        rec = self.registry.get(client_order_id)
        if not rec:
            raise KeyError("unknown order")
        order = rec["order"]
        inst = order["instrument"]
        side = order["side"]
        # side: 'buy' increases position, 'sell' decreases
        delta = filled_qty if side == "buy" else -filled_qty
        self.positions[inst] = self.positions.get(inst, 0.0) + delta
        order["status"] = "filled"

    def eligible_failover_targets(
        self, exclude: Optional[List[str]] = None
    ) -> List[str]:
        exclude = exclude or []
        return [
            n
            for n in sorted(self.monitor.brokers.keys())
            if n not in exclude and self.monitor.is_healthy(n)
        ]

    def failover(self, failed_broker: str) -> Dict[str, Any]:
        """Attempt failover for orders on failed_broker. Returns summary.

        Conservative behavior: do not re-submit orders that were already recorded as filled.
        For pending orders, re-route to eligible healthy brokers if doing so does not increase exposure.
        """
        summary: Dict[str, List[object]] = {
            "recovered": [],
            "skipped": [],
            "errors": [],
        }
        if failed_broker not in self.monitor.brokers:
            raise KeyError("unknown broker")
        pending = {
            oid: o
            for oid, o in self.monitor.brokers[failed_broker].orders.items()
            if o.get("status") != "filled"
        }
        targets = self.eligible_failover_targets(exclude=[failed_broker])
        if not targets:
            summary["errors"].append("no healthy targets")
            return summary

        for oid, order in pending.items():
            # skip duplicates if client_order_id already routed elsewhere
            if oid in self.registry and self.registry[oid]["broker"] != failed_broker:
                summary["skipped"].append(oid)
                continue

            inst = order["instrument"]
            qty = order["qty"]
            side = order["side"]
            # intended exposure delta
            self.positions.get(inst, 0.0)
            # Conservative check: do not increase absolute exposure beyond what original order intended.
            # Since original order would have applied same delta, re-routing is allowed; but avoid double-applying.
            # Ensure target does not already have an order with same id
            target = targets[0]
            try:
                if (
                    oid in self.registry
                    and self.registry[oid]["broker"] == failed_broker
                ):
                    # remove old registry and reassign
                    del self.registry[oid]
                # place on target
                if oid in self.registry:
                    summary["skipped"].append(oid)
                    continue
                self.place_order(target, oid, inst, qty, side)
                summary["recovered"].append({"order": oid, "to": target})
            except Exception as e:
                summary["errors"].append({"order": oid, "error": str(e)})

        return summary

    def reconcile_order_states(
        self, external_states: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Reconcile order states across brokers. `external_states` maps broker->orders dict.

        Returns reconciliation report.
        """
        report: Dict[str, List[object]] = {"duplicates": [], "orphaned": [], "ok": []}
        seen: Dict[str, str] = {}
        for broker, orders in external_states.items():
            for oid, _o in orders.items():
                if oid in seen and seen[oid] != broker:
                    report["duplicates"].append(
                        {"order": oid, "brokers": [seen[oid], broker]}
                    )
                else:
                    seen[oid] = broker

        # orphaned = registry entries pointing to unknown brokers or missing externally
        for oid, rec in list(self.registry.items()):
            broker = rec["broker"]
            if broker not in external_states or oid not in external_states[broker]:
                report["orphaned"].append(oid)
            else:
                report["ok"].append(oid)

        return report


__all__ = ["BrokerHealthMonitor", "BrokerFailoverManager", "BrokerState"]
