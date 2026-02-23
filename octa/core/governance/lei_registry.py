"""LEI (Legal Entity Identifier) registry for derivatives governance.

Enforces that derivatives trading requires a valid, active LEI.
Equities are unaffected by this gate.

The registry loads from a JSON file::

    {
      "entities": [
        {
          "lei": "529900T8BM49AURSDO55",
          "legal_name": "Acme Trading GmbH",
          "status": "ACTIVE",
          "expiry_date": "2027-01-15"
        }
      ]
    }

If no LEI is found or the LEI is expired/inactive, derivatives are BLOCKED.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class LEIEntry:
    lei: str
    legal_name: str
    status: str  # ACTIVE, LAPSED, RETIRED, ...
    expiry_date: Optional[str]  # ISO date or None


@dataclass(frozen=True)
class LEICheckResult:
    valid: bool
    lei: Optional[str]
    reason: str


_VALID_STATUSES = frozenset({"ACTIVE"})


class LEIRegistry:
    """In-memory LEI registry loaded from JSON."""

    def __init__(self, entries: Optional[List[LEIEntry]] = None) -> None:
        self._entries: Dict[str, LEIEntry] = {}
        for e in (entries or []):
            self._entries[e.lei] = e

    @classmethod
    def from_file(cls, path: Path) -> "LEIRegistry":
        if not path.exists():
            return cls([])
        data = json.loads(path.read_text(encoding="utf-8"))
        entries = []
        for row in data.get("entities", []):
            entries.append(LEIEntry(
                lei=str(row.get("lei", "")).strip(),
                legal_name=str(row.get("legal_name", "")),
                status=str(row.get("status", "")).upper().strip(),
                expiry_date=row.get("expiry_date"),
            ))
        return cls(entries)

    def check(self, lei: Optional[str], *, as_of: Optional[date] = None) -> LEICheckResult:
        """Check if a given LEI is valid and active.

        Parameters
        ----------
        lei : str or None
            The LEI to validate.
        as_of : date, optional
            Date to check expiry against.  Defaults to today.
        """
        if not lei or not str(lei).strip():
            return LEICheckResult(valid=False, lei=None, reason="LEI_MISSING")

        lei = str(lei).strip()
        entry = self._entries.get(lei)
        if entry is None:
            return LEICheckResult(valid=False, lei=lei, reason="LEI_NOT_IN_REGISTRY")

        if entry.status not in _VALID_STATUSES:
            return LEICheckResult(
                valid=False, lei=lei,
                reason=f"LEI_STATUS_{entry.status}",
            )

        if entry.expiry_date:
            check_date = as_of or date.today()
            try:
                expiry = date.fromisoformat(entry.expiry_date)
                if check_date > expiry:
                    return LEICheckResult(
                        valid=False, lei=lei,
                        reason="LEI_EXPIRED",
                    )
            except ValueError:
                return LEICheckResult(
                    valid=False, lei=lei,
                    reason="LEI_EXPIRY_INVALID",
                )

        return LEICheckResult(valid=True, lei=lei, reason="LEI_VALID")

    def list_entries(self) -> List[Dict[str, Any]]:
        return [
            {
                "lei": e.lei,
                "legal_name": e.legal_name,
                "status": e.status,
                "expiry_date": e.expiry_date,
            }
            for e in sorted(self._entries.values(), key=lambda x: x.lei)
        ]
