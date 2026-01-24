import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List


@dataclass(frozen=True)
class LiveChecklistResult:
    ts: str
    results: Dict[str, bool]
    info: Dict[str, Any]
    passed: bool
    signature: str


class LiveChecklist:
    """Software-enforced pre-live compliance checklist.

    - run_checks(ctx) executes the required checks and stores an immutable result.
    - `enable_live()` only succeeds if last result passed 100%.
    - results are stored as `LiveChecklistResult` (frozen dataclass) to ensure immutability.
    """

    def __init__(
        self,
        required_shadow_days: int = 3,
        audit_fn: Callable[[str, dict], None] | None = None,
        signer_key: str = "live_check_key",
    ):
        self.required_shadow_days = int(required_shadow_days)
        self.audit = audit_fn or (lambda e, p: None)
        self.signer_key = signer_key
        self._results: List[LiveChecklistResult] = []
        self.live_enabled = False

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _sign(self, payload: str) -> str:
        m = hashlib.sha256()
        m.update(self.signer_key.encode())
        m.update(payload.encode())
        return m.hexdigest()

    def run_checks(self, ctx: Dict[str, Any]) -> LiveChecklistResult:
        # required checks
        checks = {}
        checks["paper_gates_passed"] = bool(ctx.get("paper_passed", False))
        checks["shadow_stable"] = (
            int(ctx.get("shadow_days", 0)) >= self.required_shadow_days
        )
        checks["no_unresolved_critical"] = int(ctx.get("critical_incidents", 0)) == 0
        checks["audit_chain_intact"] = bool(ctx.get("audit_chain_ok", False))
        checks["kill_switch_tested"] = bool(ctx.get("kill_tested", False))
        checks["capacity_liquidity_passed"] = bool(
            ctx.get("capacity_passed", False)
        ) and bool(ctx.get("liquidity_passed", False))

        passed = all(checks.values())
        ts = self._now()
        info = {"ctx": ctx}
        payload = f"{ts}|{checks}|{passed}"
        sig = self._sign(payload)
        result = LiveChecklistResult(
            ts=ts, results=checks, info=info, passed=passed, signature=sig
        )
        self._results.append(result)
        self.audit("live_checklist_run", {"result": result.__dict__})
        return result

    def latest(self) -> LiveChecklistResult:
        if not self._results:
            raise RuntimeError("no checklist run available")
        return self._results[-1]

    def enable_live(self) -> bool:
        # must have latest run and be all pass
        if not self._results:
            raise RuntimeError("no checklist run available")
        latest = self.latest()
        if not latest.passed:
            self.audit(
                "enable_live_failed",
                {"reason": "checklist_failed", "result": latest.__dict__},
            )
            return False
        self.live_enabled = True
        self.audit("live_enabled", {"ts": latest.ts, "signature": latest.signature})
        return True

    def history(self) -> List[LiveChecklistResult]:
        return list(self._results)
