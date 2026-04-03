from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

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


def evaluate_symbol_performance(
    symbol_pnl_path: str,
    *,
    min_trades: int = 5,
    min_win_rate: float = 0.45,
    max_loss_pct: float = 0.05,
    total_volume_ref: Optional[float] = None,
) -> Dict[str, str]:
    """Evaluate per-symbol performance from symbol_pnl.json.

    Returns dict mapping symbol → "PROMOTE" | "HOLD" | "DEMOTE".
    Fail-closed: any missing/invalid data defaults to HOLD.

    Args:
        symbol_pnl_path: Path to ``symbol_pnl.json`` produced by compute_symbol_pnl().
        min_trades: Minimum number of trades before a symbol is eligible for promotion.
        min_win_rate: Minimum win rate (0–1) to qualify for PROMOTE.
        max_loss_pct: Maximum loss as fraction of total_volume to qualify for PROMOTE.
            If total_volume_ref is None, uses the symbol's own total_volume.
        total_volume_ref: Optional reference denominator for loss pct (e.g. initial NAV).
    """
    path = Path(symbol_pnl_path)
    if not path.exists():
        return {}

    try:
        data: Dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    decisions: Dict[str, str] = {}
    for sym, entry in sorted(data.items()):
        if not isinstance(entry, dict):
            decisions[sym] = "HOLD"
            continue
        n_trades = int(entry.get("n_trades") or 0)
        win_rate = float(entry.get("win_rate") or 0.0)
        realized_pnl = float(entry.get("realized_pnl") or 0.0)
        total_volume = float(entry.get("total_volume") or 0.0)

        if n_trades < min_trades:
            decisions[sym] = "HOLD"
            continue

        # Loss fraction check
        denom = total_volume_ref if total_volume_ref else total_volume
        loss_pct = abs(realized_pnl) / denom if (denom > 0 and realized_pnl < 0) else 0.0

        if win_rate >= min_win_rate and loss_pct <= max_loss_pct:
            decisions[sym] = "PROMOTE"
        elif loss_pct > max_loss_pct:
            decisions[sym] = "DEMOTE"
        else:
            decisions[sym] = "HOLD"

    return decisions
