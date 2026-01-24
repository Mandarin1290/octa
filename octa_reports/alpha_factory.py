from typing import Any, Dict, List


class AlphaFactoryDashboard:
    """Aggregate alpha pipeline artifacts into deterministic metrics.

    Methods are pure aggregators over provided registries/engines (no side effects).
    """

    def __init__(
        self,
        hypothesis_registry=None,
        audit_chain=None,
        lifecycle_engine=None,
        failure_registry=None,
    ):
        self.hypothesis_registry = hypothesis_registry
        self.audit_chain = audit_chain
        self.lifecycle_engine = lifecycle_engine
        self.failure_registry = failure_registry

    def active_hypotheses(self) -> List[Dict[str, Any]]:
        if not self.hypothesis_registry:
            return []
        return [
            {
                "hypothesis_id": hid,
                "economic_intuition": h.economic_intuition,
                "created_at": h.created_at,
            }
            for hid, h in self.hypothesis_registry.list().items()
        ]

    def pipeline_stage_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        if not self.audit_chain:
            return counts
        for blk in self.audit_chain.blocks():
            ev = blk.event
            counts[ev] = counts.get(ev, 0) + 1
        return counts

    def rejection_reasons(self) -> Dict[str, int]:
        reasons: Dict[str, int] = {}
        if not self.audit_chain:
            return reasons
        for blk in self.audit_chain.blocks():
            if blk.event.endswith(".rejected") or blk.event.endswith(".failed"):
                payload = blk.payload or {}
                reason = payload.get("reason") or payload.get("reasons") or "unknown"
                if isinstance(reason, list):
                    for r in reason:
                        reasons[r] = reasons.get(r, 0) + 1
                else:
                    reasons[str(reason)] = reasons.get(str(reason), 0) + 1
        return reasons

    def paper_deployment_flow(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        if not self.lifecycle_engine:
            return out
        # inspect lifecycle_engine internal store if available
        store = getattr(self.lifecycle_engine, "_store", {})
        for did, rec in store.items():
            out.append(
                {
                    "deployment_id": did,
                    "state": rec.state,
                    "capital": rec.capital,
                    "created_at": rec.created_at,
                }
            )
        return sorted(out, key=lambda x: x["created_at"])

    def failure_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {"total_events": 0, "unexpected_total": 0}
        if not self.failure_registry:
            return stats
        events = self.failure_registry.get_events()
        stats["total_events"] = len(events)
        stats["unexpected_total"] = sum(1 for e in events if e.unexpected)
        # breakdown by unexpected mode
        breakdown: Dict[str, int] = {}
        for e in events:
            for u in e.unexpected:
                breakdown[u] = breakdown.get(u, 0) + 1
        stats["unexpected_breakdown"] = breakdown
        return stats

    def summary(self) -> Dict[str, Any]:
        return {
            "active_hypotheses": self.active_hypotheses(),
            "pipeline_stage_counts": self.pipeline_stage_counts(),
            "rejection_reasons": self.rejection_reasons(),
            "paper_deployments": self.paper_deployment_flow(),
            "failure_stats": self.failure_statistics(),
        }
