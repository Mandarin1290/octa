from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from octa_ledger.incidents import IncidentStore
from octa_ledger.store import LedgerStore

from .actions import (
    ActionSignal,
    derisk_positions,
    flatten_and_kill,
    freeze_new_orders,
    warning,
)
from .policies import SentinelPolicy
from .state import GateState, StateStore


class Decision:
    def __init__(
        self, level: int, reason: str, action: Optional[ActionSignal] = None
    ) -> None:
        self.level = level
        self.reason = reason
        self.action = action


class SentinelEngine:
    def __init__(
        self,
        policy: SentinelPolicy,
        state_path: str,
        ledger_store: Optional[LedgerStore] = None,
    ) -> None:
        self.policy = policy
        self.state_store = StateStore(state_path)
        self.ledger_store = ledger_store

    def _audit_health_check(self) -> Optional[Decision]:
        # if ledger provided and chain not healthy, return configured level
        if self.ledger_store is None:
            return None
        ok = self.ledger_store.verify_chain()
        if not ok:
            lvl = self.policy.operational.audit_failure_level
            reason = "audit chain verification failed"
            if lvl >= 3:
                return Decision(lvl, reason, flatten_and_kill(reason))
            if lvl == 2:
                return Decision(lvl, reason, freeze_new_orders())
            return Decision(lvl, reason, warning(reason))
        return None

    def evaluate(self, inputs: Dict[str, Any]) -> Decision:
        # inputs: positions, pnl, volatility, health
        # 1) audit health must be authoritative
        audit_decision = self._audit_health_check()
        if audit_decision:
            self._record_state(audit_decision.level, audit_decision.reason)
            return audit_decision

        # drawdown checks
        pnl = inputs.get("pnl", {})
        # expect keys: current_nav, peak_nav, daily_loss
        current = float(pnl.get("current_nav", 0.0))
        peak = float(pnl.get("peak_nav", current))
        daily_loss = float(pnl.get("daily_loss", 0.0))
        if peak > 0:
            drawdown = max(0.0, (peak - current) / peak)
            if drawdown >= self.policy.drawdown.max_portfolio_drawdown:
                reason = f"drawdown {drawdown:.3f} >= {self.policy.drawdown.max_portfolio_drawdown}"
                dec = Decision(
                    (
                        self.policy.operational.data_integrity_failure_level
                        if False
                        else 2
                    ),
                    reason,
                    freeze_new_orders(),
                )
                self._record_state(dec.level, dec.reason)
                return dec
        if daily_loss >= self.policy.drawdown.max_daily_loss:
            reason = (
                f"daily loss {daily_loss:.3f} >= {self.policy.drawdown.max_daily_loss}"
            )
            dec = Decision(2, reason, freeze_new_orders())
            self._record_state(dec.level, dec.reason)
            return dec

        # exposure checks
        exposure = inputs.get("exposure", {})
        gross = float(exposure.get("gross", 0.0))
        net = float(exposure.get("net", 0.0))
        per_asset = float(exposure.get("max_asset", 0.0))
        if gross > self.policy.exposure.max_gross_exposure:
            reason = (
                f"gross exposure {gross} > {self.policy.exposure.max_gross_exposure}"
            )
            dec = Decision(1, reason, derisk_positions(reason))
            self._record_state(dec.level, dec.reason)
            return dec
        if net > self.policy.exposure.max_net_exposure:
            reason = f"net exposure {net} > {self.policy.exposure.max_net_exposure}"
            dec = Decision(1, reason, derisk_positions(reason))
            self._record_state(dec.level, dec.reason)
            return dec
        if per_asset > self.policy.exposure.max_per_asset:
            reason = (
                f"per-asset exposure {per_asset} > {self.policy.exposure.max_per_asset}"
            )
            dec = Decision(1, reason, derisk_positions(reason))
            self._record_state(dec.level, dec.reason)
            return dec

        # operational health triggers
        health = inputs.get("health", {})
        if not health.get("broker_connected", True):
            lvl = self.policy.operational.broker_disconnect_level
            reason = "broker disconnected"
            dec = Decision(
                lvl,
                reason,
                flatten_and_kill(reason) if lvl >= 3 else freeze_new_orders(),
            )
            self._record_state(dec.level, dec.reason)
            return dec

        # otherwise ok
        dec = Decision(0, "ok", warning("all good"))
        self._record_state(dec.level, dec.reason)
        return dec

    def _record_state(self, level: int, reason: str) -> None:
        gs = GateState(
            level=level, reason=reason, timestamp=datetime.now(timezone.utc).isoformat()
        )
        self.state_store.save(gs)
        # also log policy change or gate event to ledger if available
        if self.ledger_store is not None:
            try:
                # create an AuditEvent to append
                from octa_ledger.events import AuditEvent

                # If the level is elevated (L2+), create an incident record
                if level >= 2:
                    try:
                        incs = IncidentStore(self.ledger_store)
                        inc = incs.create_incident(
                            "RISK",
                            severity=level,
                            title=reason,
                            initial_notes=reason,
                            context={"level": level},
                        )
                        # add timeline entry referencing gate event
                        incs.append_timeline(
                            inc.incident_id, f"sentinel gate level {level}: {reason}"
                        )
                    except Exception:
                        # incident creation failure should escalate
                        self.state_store.save(
                            GateState(
                                level=3,
                                reason="incident write failure",
                                timestamp=datetime.now(timezone.utc).isoformat(),
                            )
                        )
                        raise

                ev = AuditEvent.create(
                    actor="sentinel",
                    action="gate_event",
                    payload={"level": level, "reason": reason},
                    severity="WARN",
                    prev_hash=None,
                )
                self.ledger_store.append(ev)
            except Exception:
                # cannot write to ledger -> escalate to kill switch
                self.state_store.save(
                    GateState(
                        level=3,
                        reason="audit write failure",
                        timestamp=datetime.now(timezone.utc).isoformat(),
                    )
                )
                raise
