"""Module 4: Order/fill audit log.

Every order submitted to the broker is appended to fills.jsonl in the state
directory.  This provides a persistent, append-only record of all trade
intents and their broker responses — the foundation for per-trade P&L once
execution prices are available from the broker adapter.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

_FILLS_FILENAME = "fills.jsonl"


@dataclass
class FillEvent:
    order_id: str
    symbol: str
    strategy: str
    side: str
    qty: float
    status: str
    cycle: int
    timestamp_utc: str
    asset_class: str = ""
    raw_result: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class FillTracker:
    """Append-only order log persisted to state_dir/fills.jsonl."""

    def __init__(self, state_dir: Path) -> None:
        self._state_dir = state_dir
        self._path = state_dir / _FILLS_FILENAME
        state_dir.mkdir(parents=True, exist_ok=True)

    def record(self, fill: FillEvent) -> None:
        """Append one fill event.  Never raises — evidence must not block execution."""
        try:
            with self._path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(fill.to_dict(), default=str) + "\n")
        except Exception:
            pass

    def load_for_date(self, date_str: str) -> List[FillEvent]:
        """Return all fill events whose timestamp starts with date_str (YYYY-MM-DD)."""
        fills: List[FillEvent] = []
        if not self._path.exists():
            return fills
        try:
            with self._path.open(encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                        if str(d.get("timestamp_utc", "")).startswith(date_str):
                            raw = d.pop("raw_result", None)
                            fills.append(FillEvent(**d, raw_result=raw))
                    except Exception:
                        pass
        except Exception:
            pass
        return fills

    def count_submitted_today(self, date_str: str) -> int:
        return len(self.load_for_date(date_str))

    def count_filled_today(self, date_str: str) -> int:
        """Count orders that received a non-rejected, non-error broker status."""
        return sum(
            1
            for f in self.load_for_date(date_str)
            if f.status not in {"REJECTED", "ERROR", "BLOCKED", "SIMULATED_REJECT"}
        )

    def summary_for_date(self, date_str: str) -> Dict[str, Any]:
        fills = self.load_for_date(date_str)
        by_strategy: Dict[str, int] = {}
        by_symbol: Dict[str, int] = {}
        for f in fills:
            by_strategy[f.strategy] = by_strategy.get(f.strategy, 0) + 1
            by_symbol[f.symbol] = by_symbol.get(f.symbol, 0) + 1
        return {
            "date": date_str,
            "total": len(fills),
            "filled": self.count_filled_today(date_str),
            "by_strategy": by_strategy,
            "by_symbol": by_symbol,
        }
