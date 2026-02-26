"""Persistent capital state for cross-run NAV tracking.

Stores the last known NAV to disk so that successive execution runs can
detect anomalous NAV jumps (>NAV_DISCREPANCY_THRESHOLD) relative to what
was last persisted.  Any large discrepancy is governance-audited before
execution proceeds.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

NAV_DISCREPANCY_THRESHOLD: float = 0.01  # 1 % — configurable at module level

_FILENAME = "capital_state.json"


@dataclass(frozen=True)
class CapitalState:
    nav: float
    timestamp_utc: str
    source: str  # "broker" | "fallback" | "initial"

    @classmethod
    def load_or_init(cls, state_dir: Path) -> "CapitalState":
        """Load persisted capital state, or return a zero-nav initial state."""
        path = state_dir / _FILENAME
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                return cls(
                    nav=float(data["nav"]),
                    timestamp_utc=str(data.get("timestamp_utc", "")),
                    source=str(data.get("source", "persisted")),
                )
            except Exception:
                pass
        return cls(nav=0.0, timestamp_utc="", source="initial")

    def save(self, state_dir: Path) -> None:
        """Persist current capital state to disk."""
        state_dir.mkdir(parents=True, exist_ok=True)
        (state_dir / _FILENAME).write_text(
            json.dumps(asdict(self), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def discrepancy(self, broker_nav: float) -> float:
        """Fractional discrepancy between persisted NAV and broker NAV.

        Returns 0.0 if no previous state (nav == 0) or broker_nav == 0.
        """
        if self.nav <= 0 or broker_nav <= 0:
            return 0.0
        return abs(self.nav - broker_nav) / broker_nav
