from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from octa_ledger.store import LedgerStore
from octa_sentinel.paper_gates import PaperGates


@dataclass
class PaperEvalPolicy:
    min_trading_days: int = 20
    max_drawdown_threshold: float = 0.05
    slippage_tolerance: float = 0.5


def run_paper_eval(*, ledger_dir: str, out_dir: str, policy: PaperEvalPolicy) -> Dict[str, Any]:
    outp = Path(out_dir)
    outp.mkdir(parents=True, exist_ok=True)

    ledger = LedgerStore(ledger_dir)
    gates = PaperGates(
        min_trading_days=int(policy.min_trading_days),
        max_drawdown_threshold=float(policy.max_drawdown_threshold),
        slippage_tolerance=float(policy.slippage_tolerance),
    )
    res = gates.evaluate_promotion(ledger)

    # Build per symbol/timeframe status from order events.
    # Fail-closed promotion semantics: without explicit performance evidence, symbols remain HOLD.
    intents = ledger.by_action("paper.order_intent")
    by_key: Dict[str, Dict[str, Any]] = {}
    for e in intents:
        p = e.get("payload", {})
        sym = str(p.get("symbol") or "")
        tf = str(p.get("timeframe") or "")
        if not sym or not tf:
            continue
        k = f"{sym}|{tf}"
        by_key.setdefault(k, {"symbol": sym, "timeframe": tf, "intents": 0})
        by_key[k]["intents"] += 1

    rows: List[Dict[str, Any]] = []
    for _k, v in sorted(by_key.items()):
        # Placeholder gate: until realized PnL/nav-by-symbol is available, HOLD.
        rows.append({"symbol": v["symbol"], "timeframe": v["timeframe"], "status": "HOLD", "details": f"intents={v['intents']}"})

    # Add portfolio-level result as separate scope row.
    rows.append({"symbol": "__PORTFOLIO__", "timeframe": "ALL", "status": "PASS" if res.get("passed") else "FAIL", "details": "|".join(res.get("details") or [])})

    mat_path = outp / "paper_eval_matrix.csv"
    with mat_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["symbol", "timeframe", "status", "details"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    cand = {
        "portfolio": res,
        "candidates": [r for r in rows if r.get("status") == "PASS" and r.get("symbol") not in {"__PORTFOLIO__"}],
        "note": "Fail-closed: per-symbol promotion requires realized performance evidence; default HOLD.",
    }
    cand_path = outp / "promotion_candidates.json"
    cand_path.write_text(json.dumps(cand, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    return {"paper_eval_matrix": str(mat_path), "promotion_candidates": str(cand_path), "passed": bool(res.get("passed"))}
