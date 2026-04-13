"""Governance-level hash-chained audit writer.

Extends AuditChain with typed governance events, stored under
``octa/var/evidence/governance_hash_chain/<run_id>/chain.jsonl``.

Every governance-relevant action (model promotion, portfolio preflight,
key rotation, etc.) emits a typed record into this chain.  The chain is
append-only and hash-linked so that any tampering is detectable via
``GovernanceAudit.verify()``.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional

from .audit_chain import AuditChain, AuditRecord

# Canonical event types — extend as new governance controls land.
EVENT_EXECUTION_PREFLIGHT = "EXECUTION_PREFLIGHT"
EVENT_SIGNING_CONFIGURED = "SIGNING_CONFIGURED"
EVENT_MODEL_PROMOTED = "MODEL_PROMOTED"
EVENT_MODEL_LOAD_VERIFIED = "MODEL_LOAD_VERIFIED"
EVENT_MODEL_LOAD_REJECTED = "MODEL_LOAD_REJECTED"
EVENT_PORTFOLIO_PREFLIGHT = "PORTFOLIO_PREFLIGHT"
EVENT_GOVERNANCE_ENFORCED = "GOVERNANCE_ENFORCED"
EVENT_LEDGER_UPDATED = "LEDGER_UPDATED"
EVENT_KEY_ROTATED = "KEY_ROTATED"
EVENT_KEY_REVOKED = "KEY_REVOKED"
EVENT_POLICY_UPDATED = "POLICY_UPDATED"
EVENT_PROMOTION_REJECTED = "PROMOTION_REJECTED"
EVENT_DRIFT_BREACH = "DRIFT_BREACH"
EVENT_RISK_AGGREGATION = "RISK_AGGREGATION"
EVENT_INFERENCE_CYCLE = "INFERENCE_CYCLE"
EVENT_TRAINING_RUN = "TRAINING_RUN"
EVENT_SCOPE_GUARD_PASSED = "SCOPE_GUARD_PASSED"
EVENT_SCOPE_GUARD_FAILED = "SCOPE_GUARD_FAILED"
EVENT_CRISIS_OOS_PASSED = "CRISIS_OOS_PASSED"
EVENT_CRISIS_OOS_FAILED = "CRISIS_OOS_FAILED"
EVENT_CRISIS_OOS_SKIPPED = "CRISIS_OOS_SKIPPED"
EVENT_SHADOW_PARTIAL_FETCH_ABORT = "SHADOW_PARTIAL_FETCH_ABORT"
EVENT_UNIVERSE_PARQUET_REFRESH_COMPLETE = "UNIVERSE_PARQUET_REFRESH_COMPLETE"
EVENT_REGIME_ACTIVATED = "REGIME_ACTIVATED"

_KNOWN_EVENTS = frozenset({
    EVENT_EXECUTION_PREFLIGHT,
    EVENT_SIGNING_CONFIGURED,
    EVENT_MODEL_PROMOTED,
    EVENT_MODEL_LOAD_VERIFIED,
    EVENT_MODEL_LOAD_REJECTED,
    EVENT_PORTFOLIO_PREFLIGHT,
    EVENT_GOVERNANCE_ENFORCED,
    EVENT_LEDGER_UPDATED,
    EVENT_KEY_ROTATED,
    EVENT_KEY_REVOKED,
    EVENT_POLICY_UPDATED,
    EVENT_PROMOTION_REJECTED,
    EVENT_DRIFT_BREACH,
    EVENT_RISK_AGGREGATION,
    EVENT_INFERENCE_CYCLE,
    EVENT_TRAINING_RUN,
    EVENT_SCOPE_GUARD_PASSED,
    EVENT_SCOPE_GUARD_FAILED,
    EVENT_CRISIS_OOS_PASSED,
    EVENT_CRISIS_OOS_FAILED,
    EVENT_CRISIS_OOS_SKIPPED,
    EVENT_SHADOW_PARTIAL_FETCH_ABORT,
    EVENT_UNIVERSE_PARQUET_REFRESH_COMPLETE,
    EVENT_REGIME_ACTIVATED,
})

_DEFAULT_ROOT = Path("octa") / "var" / "evidence" / "governance_hash_chain"


class GovernanceAudit:
    """Typed governance audit chain.

    Parameters
    ----------
    run_id : str
        Unique identifier for this run / session.
    root : Path, optional
        Root directory.  Defaults to ``octa/var/evidence/governance_hash_chain``.
    """

    def __init__(self, run_id: str, root: Path = _DEFAULT_ROOT) -> None:
        if not run_id or not str(run_id).strip():
            raise ValueError("run_id must be non-empty")
        self._run_id = str(run_id).strip()
        self._chain_dir = root / self._run_id
        self._chain_dir.mkdir(parents=True, exist_ok=True)
        self._chain = AuditChain(self._chain_dir / "chain.jsonl")

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def chain_path(self) -> Path:
        return self._chain_dir / "chain.jsonl"

    def emit(
        self,
        event_type: str,
        payload: Mapping[str, Any],
        *,
        ts: Optional[datetime] = None,
    ) -> AuditRecord:
        """Append a governance event to the hash chain.

        Parameters
        ----------
        event_type : str
            Must be one of the ``EVENT_*`` constants.
        payload : Mapping
            Arbitrary JSON-serialisable event data.
        ts : datetime, optional
            Override timestamp (useful for deterministic tests).
        """
        if event_type not in _KNOWN_EVENTS:
            raise ValueError(
                f"Unknown governance event type: {event_type!r}.  "
                f"Known: {sorted(_KNOWN_EVENTS)}"
            )
        envelope = {
            "event_type": event_type,
            "run_id": self._run_id,
            "data": dict(payload),
        }
        return self._chain.append(envelope, ts=ts)

    def verify(self) -> bool:
        """Verify the integrity of the hash chain."""
        return self._chain.verify()

    def read_events(self) -> list[dict[str, Any]]:
        """Read all events from the chain (for diagnostics / tests)."""
        if not self.chain_path.exists():
            return []
        records: list[dict[str, Any]] = []
        with self.chain_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def summary(self) -> dict[str, Any]:
        """Return a summary of the chain state."""
        events = self.read_events()
        event_counts: dict[str, int] = {}
        for ev in events:
            et = ev.get("payload", {}).get("event_type", "UNKNOWN")
            event_counts[et] = event_counts.get(et, 0) + 1
        return {
            "run_id": self._run_id,
            "chain_path": str(self.chain_path),
            "total_events": len(events),
            "event_counts": event_counts,
            "integrity_ok": self.verify(),
        }
