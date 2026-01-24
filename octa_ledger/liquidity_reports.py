from __future__ import annotations

from typing import Any, Dict, List

from .store import LedgerStore


def get_latest_stress_reports(ledger: LedgerStore) -> List[Dict[str, Any]]:
    return ledger.by_action("liquidity_stress.report")


def summarize_latest(ledger: LedgerStore) -> Dict[str, Any]:
    reps = get_latest_stress_reports(ledger)
    if not reps:
        return {"count": 0, "latest": None}
    latest = reps[-1]
    return {"count": len(reps), "latest": latest}


__all__ = ["get_latest_stress_reports", "summarize_latest"]
