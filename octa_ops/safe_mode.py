from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple


@dataclass
class AuditEntry:
    ts: str
    actor: str
    action: str
    detail: Dict[str, Any]


class SafeModeManager:
    """Manage global halt / safe mode behavior.

    Hard rules enforced:
    - Safe Mode (global halt) blocks entries; only exits that reduce risk are allowed.
    - Existing positions are protected (we only allow exits that reduce absolute exposure).
    - Exits that would increase exposure require explicit `risk_approved=True`.
    """

    def __init__(self, initial_positions: Dict[str, float] | None = None):
        self.global_halt: bool = False
        self.positions: Dict[str, float] = dict(initial_positions or {})
        self.audit_log: List[AuditEntry] = []

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def set_halt(self, flag: bool, actor: str, reason: str = "") -> None:
        self.global_halt = bool(flag)
        self.audit_log.append(
            AuditEntry(
                ts=self._now(),
                actor=actor,
                action="set_halt",
                detail={"flag": self.global_halt, "reason": reason},
            )
        )

    def allow_trade(
        self,
        instrument: str,
        delta: float,
        trade_type: str,
        risk_approved: bool = False,
    ) -> Tuple[bool, str]:
        """Decide whether a proposed trade is allowed under Safe Mode.

        - `delta` is positive to increase position (buy/increase long), negative to decrease (sell/reduce long).
        - `trade_type` is either 'entry' or 'exit' as declared by strategy.
        - Returns (allowed, reason).
        """

        current = float(self.positions.get(instrument, 0.0))
        new = current + float(delta)

        # compute whether absolute exposure increases (keep expression removed - unused)
        if not self.global_halt:
            return True, "ok"

        # Under global halt (Safe Mode) overrides strategy logic
        if trade_type == "entry":
            return False, "global_halt_blocks_entries"

        # exit trades
        # allow if exit reduces absolute exposure
        if abs(new) < abs(current):
            return True, "exit_reduces_exposure"

        # otherwise only allowed if risk_approved
        if risk_approved:
            return True, "risk_approved_override"

        return False, "exit_would_increase_exposure_and_not_approved"

    def execute_trade(
        self,
        instrument: str,
        delta: float,
        trade_type: str,
        actor: str,
        risk_approved: bool = False,
    ) -> None:
        allowed, reason = self.allow_trade(
            instrument, delta, trade_type, risk_approved=risk_approved
        )
        self.audit_log.append(
            AuditEntry(
                ts=self._now(),
                actor=actor,
                action="proposed_trade",
                detail={
                    "instrument": instrument,
                    "delta": delta,
                    "trade_type": trade_type,
                    "allowed": allowed,
                    "reason": reason,
                },
            )
        )
        if not allowed:
            raise RuntimeError(f"trade not allowed under Safe Mode: {reason}")

        # apply
        self.positions[instrument] = float(self.positions.get(instrument, 0.0)) + float(
            delta
        )
        self.audit_log.append(
            AuditEntry(
                ts=self._now(),
                actor=actor,
                action="executed_trade",
                detail={"instrument": instrument, "delta": delta},
            )
        )


__all__ = ["SafeModeManager", "AuditEntry"]
