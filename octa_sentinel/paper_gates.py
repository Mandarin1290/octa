from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

from octa_ledger.events import AuditEvent
from octa_ledger.performance import max_drawdown
from octa_ledger.store import LedgerStore


class PaperGates:
    """Paper trading acceptance gates.

    All checks are quantitative and logged to the ledger as `paper_gate.evaluation` events.
    """

    def __init__(
        self,
        min_trading_days: int = 60,
        max_drawdown_threshold: float = 0.2,
        slippage_tolerance: float = 0.5,  # relative tolerance
    ) -> None:
        self.min_trading_days = min_trading_days
        self.max_drawdown_threshold = max_drawdown_threshold
        self.slippage_tolerance = slippage_tolerance

    def _collect_navs(self, ledger: LedgerStore) -> List[Tuple[str, float]]:
        res: List[Tuple[str, float]] = []
        for e in ledger.by_action("performance.nav"):
            p = e.get("payload", {})
            ts = str(p.get("date") or e.get("timestamp") or "")
            try:
                nav = float(p.get("nav", 0.0))
            except Exception:
                nav = 0.0
            res.append((ts, nav))
        res.sort(key=lambda x: x[0])
        return res

    def _count_trading_days(self, navs: List[Tuple[str, float]]) -> int:
        days = set()
        for ts, _ in navs:
            try:
                d = datetime.fromisoformat(ts).date()
            except Exception:
                # If timestamp missing or unparsable, skip
                continue
            days.add(d)
        return len(days)

    def _check_unresolved_critical_incidents(
        self, ledger: LedgerStore
    ) -> Tuple[bool, str]:
        created = ledger.by_action("incident.created")
        resolved = ledger.by_action("incident.resolved")
        resolved_ids = {r.get("payload", {}).get("incident_id") for r in resolved}
        for c in created:
            p = c.get("payload", {})
            sev = int(p.get("severity", 0))
            if sev >= 2:
                iid = c.get("event_id")
                if iid not in resolved_ids:
                    return False, f"unresolved critical incident {iid} severity={sev}"
        return True, "no unresolved critical incidents"

    def _check_slippage_stability(self, ledger: LedgerStore) -> Tuple[bool, str]:
        # expect events `slippage.forecast` and `slippage.observed` with numeric `slippage` payload
        forecasts = [
            float(e.get("payload", {}).get("slippage", 0.0))
            for e in ledger.by_action("slippage.forecast")
        ]
        observed = [
            float(e.get("payload", {}).get("slippage", 0.0))
            for e in ledger.by_action("slippage.observed")
        ]
        if not forecasts or not observed:
            return False, "insufficient slippage data"
        f_mean = sum(forecasts) / len(forecasts)
        o_mean = sum(observed) / len(observed)
        if f_mean == 0.0:
            ok = abs(o_mean) <= self.slippage_tolerance
        else:
            ok = abs(o_mean - f_mean) <= self.slippage_tolerance * abs(f_mean)
        return (
            (True, f"slippage stable (forecast={f_mean:.6f}, observed={o_mean:.6f})")
            if ok
            else (
                False,
                f"slippage instability forecast={f_mean:.6f} observed={o_mean:.6f}",
            )
        )

    def evaluate_promotion(self, ledger: LedgerStore) -> Dict[str, Any]:
        """Run all gates against the given ledger and append an audit event with the result.

        Returns a dict with `passed` (bool) and `details` (list of messages).
        """
        details: List[str] = []
        passed = True

        # 1) minimum paper runtime
        navs = self._collect_navs(ledger)
        days = self._count_trading_days(navs)
        if days < self.min_trading_days:
            passed = False
            details.append(
                f"insufficient_paper_runtime: {days} < {self.min_trading_days}"
            )
        else:
            details.append(f"paper_runtime_days: {days}")

        # 2) max drawdown
        prices = [nav for _, nav in navs]
        dd, dur = max_drawdown(prices)
        if dd > self.max_drawdown_threshold:
            passed = False
            details.append(
                f"max_drawdown {dd:.6f} > threshold {self.max_drawdown_threshold:.6f}"
            )
        else:
            details.append(f"max_drawdown {dd:.6f}")

        # 3) slippage stability
        slip_ok, slip_msg = self._check_slippage_stability(ledger)
        if not slip_ok:
            passed = False
            details.append(slip_msg)
        else:
            details.append(slip_msg)

        # 4) unresolved critical incidents
        inc_ok, inc_msg = self._check_unresolved_critical_incidents(ledger)
        if not inc_ok:
            passed = False
            details.append(inc_msg)
        else:
            details.append(inc_msg)

        # 5) audit integrity
        audit_ok = ledger.verify_chain()
        if not audit_ok:
            passed = False
            details.append("audit_integrity_failed")
        else:
            details.append("audit_integrity_ok")

        payload = {"passed": bool(passed), "details": details}
        ev = AuditEvent.create(
            actor="paper_gates",
            action="paper_gate.evaluation",
            payload=payload,
            severity="INFO" if passed else "ERROR",
        )
        ledger.append(ev)

        return {
            "passed": passed,
            "details": details,
            "evaluation_event_id": ev.event_id,
        }


__all__ = ["PaperGates"]
