from datetime import datetime, timezone
from typing import Dict, List, Optional


class StrategyFactoryReport:
    """Builds a deterministic oversight view from a registry and an audit ledger.

    The implementation is conservative and depends only on data recorded in the
    ledger (blocks' payloads) and the canonical registry. Ledger payloads are
    expected to include an `event` key when written via the central audit
    wrapper (e.g. `audit_fn(event, payload)` -> ledger.append({"event": event, **payload})).
    """

    def __init__(self, registry, ledger):
        self.registry = registry
        self.ledger = ledger

    def _chain(self):
        return getattr(self.ledger, "_chain", [])

    def _iter_payloads(self):
        for b in self._chain():
            yield b.payload

    def _collect_last(self, prefix: str) -> Dict[str, dict]:
        # collect last payload per strategy for events starting with prefix
        out: Dict[str, dict] = {}
        for p in self._iter_payloads():
            ev = p.get("event")
            if not ev:
                continue
            if not ev.startswith(prefix):
                continue
            sid = p.get("strategy_id")
            if not sid:
                continue
            out[sid] = p
        return out

    def _collect_all(self, prefix: str) -> List[dict]:
        res = []
        for p in self._iter_payloads():
            ev = p.get("event")
            if not ev:
                continue
            if not ev.startswith(prefix):
                continue
            res.append(p)
        return res

    @staticmethod
    def _estimate_capacity_from_params(params: dict) -> float:
        # same formula as `CapacityEngine.estimate_capacity`
        adv = float(params.get("adv", 0.0))
        turnover = float(params.get("turnover", 0.0))
        impact = float(params.get("impact", 0.0))
        adv_fraction = float(params.get("adv_fraction", 0.01))
        base_scaler = float(params.get("base_scaler", 1.0))
        if impact <= 0 or turnover <= 0:
            return 0.0
        cap = adv * adv_fraction * (1.0 / impact) * (1.0 / turnover) * base_scaler
        return cap

    def build(self) -> Dict:
        now = datetime.now(timezone.utc).isoformat()

        registry_entries = self.registry.list()

        # collect last risk.register and risk.usage events per strategy
        last_risk_reg = self._collect_last("risk.register")
        last_risk_usage = self._collect_last("risk.usage")

        # collect capacity params and latest aum from capacity.allocate
        last_capacity_reg = self._collect_last("capacity.register")
        last_capacity_aum = {}
        for p in self._collect_all("capacity.allocate"):
            sid = p.get("strategy_id")
            if not sid:
                continue
            # capacity.allocate emits 'new_aum' in payload
            last_capacity_aum[sid] = float(p.get("new_aum", p.get("aum", 0.0)))

        # collect promotion failures (paper/shadow) and suspension/kill events
        promotion_fail_events = self._collect_all(
            "paper_gates.failed"
        ) + self._collect_all("shadow_gates.failed")
        suspension_events = self._collect_all("risk.suspend") + self._collect_all(
            "capacity.block"
        )
        suspension_events += self._collect_all(
            "kill_rules.retire_immediate"
        ) + self._collect_all("kill_rules.auto_retire_due_to_persistence")

        strategies = []
        for sid in sorted(registry_entries.keys()):
            meta = registry_entries[sid]
            lifecycle = meta.get("lifecycle_state")

            # risk budget utilization
            risk_util: Dict[str, Optional[float]] = {
                "vol": None,
                "dd": None,
                "exposure": None,
                "max": None,
            }
            if sid in last_risk_reg and sid in last_risk_usage:
                bud = last_risk_reg[sid].get("budget", {})
                usage = last_risk_usage[sid].get("usage", {})
                try:
                    vol_util = (
                        float(usage.get("vol", 0.0))
                        / float(bud.get("vol_budget", float("nan")))
                        if bud.get("vol_budget") not in (0, None)
                        else float("inf")
                    )
                except Exception:
                    vol_util = None
                try:
                    dd_util = (
                        float(usage.get("dd", 0.0))
                        / float(bud.get("dd_budget", float("nan")))
                        if bud.get("dd_budget") not in (0, None)
                        else float("inf")
                    )
                except Exception:
                    dd_util = None
                try:
                    ex_util = (
                        float(usage.get("exposure", 0.0))
                        / float(bud.get("exposure_budget", float("nan")))
                        if bud.get("exposure_budget") not in (0, None)
                        else float("inf")
                    )
                except Exception:
                    ex_util = None
                vals = [v for v in (vol_util, dd_util, ex_util) if v is not None]
                max_util = max(vals) if vals else None
                risk_util = {
                    "vol": vol_util,
                    "dd": dd_util,
                    "exposure": ex_util,
                    "max": max_util,
                }

            # capacity utilisation
            cap_util = None
            if sid in last_capacity_reg:
                params = last_capacity_reg[sid].get("params", {})
                cap = self._estimate_capacity_from_params(params)
                aum = float(last_capacity_aum.get(sid, 0.0))
                if cap > 0:
                    cap_util = aum / cap
                else:
                    cap_util = float("inf")

            # promotion blockers: events referencing this strategy
            blockers = []
            for ev in promotion_fail_events:
                if ev.get("strategy_id") == sid:
                    blockers.append(f"{ev.get('event')}:{ev.get('failed', ev)}")
            # also include risk warnings/suspends as blockers
            for ev in (
                self._collect_all("risk.warn")
                + self._collect_all("risk.derisk")
                + self._collect_all("risk.suspend")
            ):
                if ev.get("strategy_id") == sid:
                    blockers.append(f"{ev.get('event')}:util={ev.get('util')}")

            # dedupe & sort for determinism
            blockers = sorted(list({b for b in blockers}))

            # suspension reasons
            susp_reasons = []
            for ev in suspension_events:
                if ev.get("strategy_id") == sid:
                    susp_reasons.append(f"{ev.get('event')}:{ev}")
            susp_reasons = sorted(list({s for s in susp_reasons}))

            strategies.append(
                {
                    "strategy_id": sid,
                    "lifecycle_state": lifecycle,
                    "risk_budget_utilization": risk_util,
                    "capacity_utilization": cap_util,
                    "promotion_blockers": blockers,
                    "suspension_reasons": susp_reasons,
                }
            )

        report = {"generated_at": now, "strategies": strategies}
        return report
