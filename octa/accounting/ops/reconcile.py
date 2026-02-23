"""CLI: Reconcile capital ledger.

Usage::

    python -m octa.accounting.ops.reconcile --asof 2026-02-18 --out report.json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from octa.accounting.capital_ledger import CapitalLedger
from octa.core.governance.governance_audit import EVENT_LEDGER_UPDATED, GovernanceAudit


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconcile capital ledger")
    ap.add_argument("--asof", default=None, help="As-of date (YYYY-MM-DD)")
    ap.add_argument("--ledger", default=None, help="Path to ledger JSONL file")
    ap.add_argument("--out", default=None, help="Write report JSON to this path")
    ap.add_argument("--run-id", default="reconcile", help="Governance audit run ID")
    args = ap.parse_args()

    ledger = CapitalLedger(Path(args.ledger)) if args.ledger else CapitalLedger()
    result = ledger.reconcile(as_of=args.asof)
    report = asdict(result)

    gov = GovernanceAudit(run_id=args.run_id)
    gov.emit(EVENT_LEDGER_UPDATED, {"reconciliation": report})

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")

    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
