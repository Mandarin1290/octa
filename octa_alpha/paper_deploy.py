import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict

from octa_core.decimal_utils import quantize_8

AuditFn = Callable[[str, Dict[str, Any]], None]


def _noop_audit(event: str, payload: Dict[str, Any]) -> None:
    return None


@dataclass
class LifecycleRecord:
    id: str
    state: str
    capital: Decimal
    created_at: str


class LifecycleEngine:
    """Minimal lifecycle engine to register strategy deployments."""

    def __init__(self):
        self._store: Dict[str, LifecycleRecord] = {}

    def register(
        self, deployment_id: str, state: str, capital: Decimal
    ) -> LifecycleRecord:
        cap_q = quantize_8(capital)
        rec = LifecycleRecord(
            id=deployment_id,
            state=state,
            capital=cap_q,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        self._store[deployment_id] = rec
        return rec

    def get(self, deployment_id: str) -> LifecycleRecord:
        return self._store[deployment_id]


class PaperDeploymentManager:
    """Automate deterministic paper deployments.

    - Deployment id is deterministic from `hypothesis_id` and `signal` to
      ensure reproducibility.
    - Deployments are registered into a `LifecycleEngine` with state 'PAPER'.
    - Optional `audit_fn` is called with `paper.deployed` event.
    """

    def __init__(
        self, lifecycle_engine: LifecycleEngine, audit_fn: AuditFn = _noop_audit
    ):
        self.lifecycle = lifecycle_engine
        self.audit_fn = audit_fn

    @staticmethod
    def _deterministic_id(hypothesis_id: str, signal: Decimal) -> str:
        # create a reproducible SHA1-based id from hypothesis and signal
        payload = f"{hypothesis_id}|{str(signal)}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def deploy(
        self, hypothesis_id: str, signal: Decimal, paper_capital: Decimal
    ) -> LifecycleRecord:
        # signal is expected as Decimal; avoid double-conversion
        dep_id = self._deterministic_id(hypothesis_id, signal)
        rec = self.lifecycle.register(dep_id, "PAPER", Decimal(paper_capital))
        try:
            self.audit_fn(
                "paper.deployed",
                {
                    "deployment_id": dep_id,
                    "hypothesis_id": hypothesis_id,
                    "signal": str(signal),
                    "paper_capital": str(paper_capital),
                },
            )
        except Exception:
            pass
        return rec
