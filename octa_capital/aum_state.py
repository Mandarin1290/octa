from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List


def _utcnow_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class AUMSnapshot:
    timestamp: str
    internal_capital: float
    external_capital: float
    portfolio_value: float
    computed_total: float
    reported_total: float
    reconciled: bool
    note: str = ""


@dataclass
class AUMChangeEvent:
    timestamp: str
    delta: float
    change_type: str  # 'pnl','inflow','outflow','set'
    source: str  # 'internal'|'external'|'portfolio'
    reason: str
    resulting_total: float


class AUMState:
    """A global, auditable, time-versioned AUM state engine.

    Usage:
      aum = AUMState(audit_fn=ledger.append)
      aum.set_internal_capital(1_000_000.0, reason="seed")
      aum.snapshot(portfolio_value=1_010_000.0)

    Consumers (allocator/capacity/risk) should call `get_current_total()`
    or subscribe via `subscribe(callback)` to receive snapshots on changes.
    """

    def __init__(
        self,
        audit_fn: Callable[[str, Dict[str, Any]], None] | None = None,
        initial_internal: float = 0.0,
        initial_external: float = 0.0,
    ):
        self._internal = float(initial_internal)
        self._external = float(initial_external)
        self._history: List[AUMSnapshot] = []
        self._events: List[AUMChangeEvent] = []
        self._subscribers: List[Callable[[AUMSnapshot], None]] = []
        self.audit_fn = audit_fn or (lambda event, payload: None)

    # --- Core interfaces ---
    def subscribe(self, cb: Callable[[AUMSnapshot], None]) -> None:
        self._subscribers.append(cb)

    def _emit_snapshot(self, snap: AUMSnapshot) -> None:
        self._history.append(snap)
        self.audit_fn("aum.snapshot", asdict(snap))
        for cb in list(self._subscribers):
            try:
                cb(snap)
            except Exception:
                # subscribers must not break the core
                pass

    def _record_event(self, ev: AUMChangeEvent) -> None:
        self._events.append(ev)
        self.audit_fn("aum.change", asdict(ev))

    # --- Mutators ---
    def set_internal_capital(self, amount: float, reason: str = "set_internal") -> None:
        prev = self._internal
        self._internal = float(amount)
        ev = AUMChangeEvent(
            timestamp=_utcnow_iso(),
            delta=self._internal - prev,
            change_type="set",
            source="internal",
            reason=reason,
            resulting_total=self._internal + self._external,
        )
        self._record_event(ev)

    def set_external_capital(self, amount: float, reason: str = "set_external") -> None:
        prev = self._external
        self._external = float(amount)
        ev = AUMChangeEvent(
            timestamp=_utcnow_iso(),
            delta=self._external - prev,
            change_type="set",
            source="external",
            reason=reason,
            resulting_total=self._internal + self._external,
        )
        self._record_event(ev)

    def apply_pnl(self, pnl: float, reason: str = "pnl") -> None:
        # By default PnL flows to internal capital
        self._internal += float(pnl)
        ev = AUMChangeEvent(
            timestamp=_utcnow_iso(),
            delta=float(pnl),
            change_type="pnl",
            source="portfolio",
            reason=reason,
            resulting_total=self._internal + self._external,
        )
        self._record_event(ev)

    def inflow(
        self, amount: float, source: str = "external", reason: str = "inflow"
    ) -> None:
        if source == "external":
            self._external += float(amount)
            src = "external"
        else:
            self._internal += float(amount)
            src = "internal"
        ev = AUMChangeEvent(
            timestamp=_utcnow_iso(),
            delta=float(amount),
            change_type="inflow",
            source=src,
            reason=reason,
            resulting_total=self._internal + self._external,
        )
        self._record_event(ev)

    def outflow(
        self, amount: float, source: str = "external", reason: str = "outflow"
    ) -> None:
        if source == "external":
            self._external -= float(amount)
            src = "external"
        else:
            self._internal -= float(amount)
            src = "internal"
        ev = AUMChangeEvent(
            timestamp=_utcnow_iso(),
            delta=-float(amount),
            change_type="outflow",
            source=src,
            reason=reason,
            resulting_total=self._internal + self._external,
        )
        self._record_event(ev)

    # --- Snapshots / reconciliation ---
    def snapshot(self, portfolio_value: float) -> AUMSnapshot:
        computed = self._internal + self._external
        reported = float(portfolio_value)
        reconciled = abs(computed - reported) < 1e-8
        snap = AUMSnapshot(
            timestamp=_utcnow_iso(),
            internal_capital=self._internal,
            external_capital=self._external,
            portfolio_value=reported,
            computed_total=computed,
            reported_total=reported,
            reconciled=reconciled,
        )
        self._emit_snapshot(snap)
        return snap

    # --- Queries ---
    def get_current_total(self) -> float:
        if self._history:
            return self._history[-1].reported_total
        return self._internal + self._external

    def get_latest_snapshot(self) -> AUMSnapshot:
        if not self._history:
            # create an initial snapshot reflecting current bookkeeping
            return AUMSnapshot(
                timestamp=_utcnow_iso(),
                internal_capital=self._internal,
                external_capital=self._external,
                portfolio_value=self._internal + self._external,
                computed_total=self._internal + self._external,
                reported_total=self._internal + self._external,
                reconciled=True,
            )
        return self._history[-1]

    def history(self) -> List[AUMSnapshot]:
        return list(self._history)

    def events(self) -> List[AUMChangeEvent]:
        return list(self._events)
