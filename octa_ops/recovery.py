from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class RecoveryManager:
    internal_positions: Dict[str, float]
    audit_log: List[Dict[str, Any]] = field(default_factory=list)
    in_recovery: bool = False
    checkpoints: List[Dict[str, float]] = field(default_factory=list)

    def _log(
        self, action: str, details: Dict[str, Any], actor: Optional[str] = None
    ) -> None:
        entry = {
            "ts": _utc_now_iso(),
            "actor": actor,
            "action": action,
            "details": details,
        }
        self.audit_log.append(entry)

    def create_checkpoint(self, actor: Optional[str] = None) -> int:
        snap = dict(self.internal_positions)
        self.checkpoints.append(snap)
        idx = len(self.checkpoints) - 1
        self._log("checkpoint_created", {"index": idx, "snapshot": snap}, actor)
        return idx

    def restore_checkpoint(self, index: int, actor: Optional[str] = None) -> None:
        if index < 0 or index >= len(self.checkpoints):
            raise IndexError("invalid checkpoint index")
        self.internal_positions = dict(self.checkpoints[index])
        self._log(
            "checkpoint_restored",
            {"index": index, "snapshot": self.internal_positions},
            actor,
        )

    def reconcile_with_broker(
        self,
        broker_positions: Dict[str, float],
        actor: Optional[str] = None,
        auto_resolve: bool = True,
    ) -> Dict[str, Any]:
        """Compare internal vs broker positions and optionally resolve mismatches.

        If `auto_resolve` is False, the manager enters recovery mode and trading is blocked until `resolve_mismatches` or `complete_recovery` is called.
        Returns a dict with `mismatches` list and `resolved` boolean.
        """
        self.create_checkpoint(actor=actor)
        mismatches = []
        all_assets = set(self.internal_positions.keys()) | set(broker_positions.keys())
        for a in sorted(all_assets):
            int_q = float(self.internal_positions.get(a, 0.0))
            brk_q = float(broker_positions.get(a, 0.0))
            if int_q != brk_q:
                mismatches.append({"asset": a, "internal": int_q, "broker": brk_q})

        if not mismatches:
            self._log("reconcile_ok", {"broker_snapshot": broker_positions}, actor)
            return {"mismatches": [], "resolved": True}

        self._log("mismatches_detected", {"mismatches": mismatches}, actor)
        self.in_recovery = True
        # block trading until resolved
        if not auto_resolve:
            return {"mismatches": mismatches, "resolved": False}

        try:
            self.resolve_mismatches(broker_positions, actor=actor)
            self.complete_recovery(actor=actor)
            return {"mismatches": mismatches, "resolved": True}
        except Exception as e:
            # keep in recovery if resolution fails
            self._log("resolution_failed", {"error": str(e)}, actor)
            self.in_recovery = True
            return {"mismatches": mismatches, "resolved": False}

    def resolve_mismatches(
        self, broker_positions: Dict[str, float], actor: Optional[str] = None
    ) -> None:
        """Apply a conservative resolution strategy: align internal positions to broker snapshot.

        Validation: broker positions must be non-negative. Raises on invalid data.
        """
        # validation
        for a, q in broker_positions.items():
            if q < 0:
                raise ValueError(f"negative broker position for {a}")

        # apply broker as source of truth
        self._log("resolution_started", {"broker_snapshot": broker_positions}, actor)
        self.internal_positions = {k: float(v) for k, v in broker_positions.items()}
        # verify exact match
        for k in set(self.internal_positions.keys()) | set(broker_positions.keys()):
            if float(self.internal_positions.get(k, 0.0)) != float(
                broker_positions.get(k, 0.0)
            ):
                raise RuntimeError("post-resolution mismatch")
        self._log(
            "resolution_completed",
            {"internal_positions": dict(self.internal_positions)},
            actor,
        )

    def complete_recovery(self, actor: Optional[str] = None) -> None:
        self.in_recovery = False
        self._log("recovery_completed", {}, actor)

    def allow_trading(self) -> bool:
        return not self.in_recovery
