"""Deterministic capital ledger — append-only journal.

Typed events:
  shareholder_loan_in, shareholder_equity_in,
  broker_funding_in, broker_funding_out,
  fees, pnl_realized, pnl_unrealized_snapshot

Each entry is hash-chained (via governance audit chain) for tamper detection.
The ledger file is a JSONL journal stored at:
  ``octa/var/accounting/capital_ledger.jsonl``
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from octa.core.governance.hashing import stable_hash

_DEFAULT_LEDGER_PATH = Path("octa") / "var" / "accounting" / "capital_ledger.jsonl"

VALID_EVENT_TYPES = frozenset({
    "shareholder_loan_in",
    "shareholder_equity_in",
    "broker_funding_in",
    "broker_funding_out",
    "fees",
    "pnl_realized",
    "pnl_unrealized_snapshot",
})


@dataclass(frozen=True)
class LedgerEntry:
    index: int
    timestamp_utc: str
    event_type: str
    amount: float
    currency: str
    description: str
    prev_hash: str
    entry_hash: str
    metadata: Dict[str, Any]


@dataclass(frozen=True)
class ReconciliationResult:
    as_of: str
    total_inflows: float
    total_outflows: float
    net_capital: float
    pnl_realized: float
    pnl_unrealized: float
    fees_total: float
    entry_count: int
    integrity_ok: bool
    breakdown: Dict[str, float]


class CapitalLedger:
    """Append-only, hash-chained capital ledger."""

    def __init__(self, ledger_path: Path = _DEFAULT_LEDGER_PATH) -> None:
        self._path = ledger_path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def append(
        self,
        event_type: str,
        amount: float,
        *,
        currency: str = "USD",
        description: str = "",
        metadata: Optional[Mapping[str, Any]] = None,
        ts: Optional[datetime] = None,
    ) -> LedgerEntry:
        """Append a journal entry."""
        if event_type not in VALID_EVENT_TYPES:
            raise ValueError(
                f"Unknown event type: {event_type!r}.  "
                f"Valid: {sorted(VALID_EVENT_TYPES)}"
            )
        timestamp = (ts or datetime.now(timezone.utc)).isoformat()
        index, prev_hash = self._last_index_hash()
        payload = {
            "index": index + 1,
            "timestamp_utc": timestamp,
            "event_type": event_type,
            "amount": amount,
            "currency": currency,
            "description": description,
            "prev_hash": prev_hash,
            "metadata": dict(metadata or {}),
        }
        entry_hash = stable_hash(payload)
        entry = LedgerEntry(
            index=index + 1,
            timestamp_utc=timestamp,
            event_type=event_type,
            amount=amount,
            currency=currency,
            description=description,
            prev_hash=prev_hash,
            entry_hash=entry_hash,
            metadata=dict(metadata or {}),
        )
        row = {
            "index": entry.index,
            "timestamp_utc": entry.timestamp_utc,
            "event_type": entry.event_type,
            "amount": entry.amount,
            "currency": entry.currency,
            "description": entry.description,
            "prev_hash": entry.prev_hash,
            "entry_hash": entry.entry_hash,
            "metadata": entry.metadata,
        }
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, sort_keys=True) + "\n")
        return entry

    def read_all(self) -> List[Dict[str, Any]]:
        """Read all journal entries."""
        if not self._path.exists():
            return []
        entries = []
        with self._path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    entries.append(json.loads(line))
        return entries

    def verify(self) -> bool:
        """Verify hash-chain integrity of the entire ledger."""
        entries = self.read_all()
        if not entries:
            return True
        prev_hash = "GENESIS"
        for i, entry in enumerate(entries):
            if entry.get("prev_hash") != prev_hash:
                return False
            if entry.get("index") != i + 1:
                return False
            payload = {
                "index": entry["index"],
                "timestamp_utc": entry["timestamp_utc"],
                "event_type": entry["event_type"],
                "amount": entry["amount"],
                "currency": entry["currency"],
                "description": entry["description"],
                "prev_hash": entry["prev_hash"],
                "metadata": entry.get("metadata", {}),
            }
            expected = stable_hash(payload)
            if entry.get("entry_hash") != expected:
                return False
            prev_hash = entry["entry_hash"]
        return True

    def reconcile(self, as_of: Optional[str] = None) -> ReconciliationResult:
        """Reconcile capital as of a given date."""
        entries = self.read_all()
        as_of_str = as_of or datetime.now(timezone.utc).strftime("%Y-%m-%d")

        inflows = 0.0
        outflows = 0.0
        pnl_realized = 0.0
        pnl_unrealized = 0.0
        fees = 0.0
        breakdown: Dict[str, float] = {}

        for entry in entries:
            ts = entry.get("timestamp_utc", "")
            if ts[:10] > as_of_str:
                continue
            et = entry.get("event_type", "")
            amt = float(entry.get("amount", 0.0))
            breakdown[et] = breakdown.get(et, 0.0) + amt

            if et in ("shareholder_loan_in", "shareholder_equity_in", "broker_funding_in"):
                inflows += amt
            elif et == "broker_funding_out":
                outflows += abs(amt)
            elif et == "fees":
                fees += abs(amt)
            elif et == "pnl_realized":
                pnl_realized += amt
            elif et == "pnl_unrealized_snapshot":
                pnl_unrealized = amt  # snapshot: use latest value

        net_capital = inflows - outflows - fees + pnl_realized + pnl_unrealized

        return ReconciliationResult(
            as_of=as_of_str,
            total_inflows=round(inflows, 2),
            total_outflows=round(outflows, 2),
            net_capital=round(net_capital, 2),
            pnl_realized=round(pnl_realized, 2),
            pnl_unrealized=round(pnl_unrealized, 2),
            fees_total=round(fees, 2),
            entry_count=len(entries),
            integrity_ok=self.verify(),
            breakdown={k: round(v, 2) for k, v in sorted(breakdown.items())},
        )

    def _last_index_hash(self) -> tuple[int, str]:
        if not self._path.exists():
            return 0, "GENESIS"
        last_line = ""
        with self._path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    last_line = line
        if not last_line:
            return 0, "GENESIS"
        data = json.loads(last_line)
        return int(data.get("index", 0)), str(data.get("entry_hash", "GENESIS"))
