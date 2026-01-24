import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class EvidencePackBuilder:
    """Builds an immutable, reproducible evidence pack for a strategy.

    Inputs (optional): registry, ledger (AuditChain), risk_engine, capacity_engine,
    health_report (dict), alpha/regime/stability reports.

    The pack_id is the SHA256 of the canonical JSON payload (sorted keys).
    The builder appends an audit event `evidence_pack.generated` to the ledger
    with `pack_id` and `strategy_id` for complete linkage.
    """

    def __init__(
        self,
        registry,
        ledger,
        audit_fn=None,
        risk_engine=None,
        capacity_engine=None,
        health_scorer=None,
    ):
        self.registry = registry
        self.ledger = ledger
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.risk_engine = risk_engine
        self.capacity_engine = capacity_engine
        self.health_scorer = health_scorer

    def _canonical_hash(self, payload: Dict[str, Any]) -> str:
        b = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(b).hexdigest()

    def _ledger_payloads_for(self, strategy_id: str):
        out = []
        for b in getattr(self.ledger, "_chain", []):
            p = b.payload
            if isinstance(p, dict) and p.get("strategy_id") == strategy_id:
                out.append(p)
        return out

    def generate(
        self,
        strategy_id: str,
        include_ledger_events: bool = True,
        extra: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        meta = self.registry.get(strategy_id)
        if meta is None:
            raise ValueError("strategy not registered")

        # Build core payload used for canonical hashing (exclude generated_at)
        pack_core = {
            "strategy_id": strategy_id,
            "registry_meta": meta.as_dict(),
        }

        # risk
        if self.risk_engine is not None:
            bud = self.risk_engine.get_budget(strategy_id)
            usage = self.risk_engine.get_usage(strategy_id)
            pack_core["risk"] = {
                "budget": bud.__dict__ if bud is not None else None,
                "usage": usage,
            }

        # capacity
        if self.capacity_engine is not None:
            try:
                params = self.capacity_engine._params.get(strategy_id)
            except Exception:
                params = None
            aum = None
            try:
                aum = self.capacity_engine.get_aum(strategy_id)
            except Exception:
                aum = None
            pack_core["capacity"] = {
                "params": getattr(params, "__dict__", params),
                "aum": aum,
            }

        # health: if a scorer is provided, attempt to compute a health report using minimal inputs (best-effort)
        if self.health_scorer is not None:
            try:
                # consumer may pass precomputed parts via extra
                health_input = extra.get("health_input") if extra else {}
                hr = (
                    self.health_scorer.score(**health_input)
                    if hasattr(self.health_scorer, "score")
                    else None
                )
                pack_core["health"] = hr.explain if hr is not None else None
            except Exception:
                pack_core["health"] = None

        # ledger events (filtered and canonicalized)
        if include_ledger_events and self.ledger is not None:
            # exclude prior evidence_pack.generated audit events to keep pack reproducible
            events = [
                p
                for p in self._ledger_payloads_for(strategy_id)
                if p.get("event") != "evidence_pack.generated"
            ]
            pack_core["ledger_events"] = events

        # merge extras
        if extra:
            pack_core["extra"] = extra

        # compute immutable id from canonical core
        pack_id = self._canonical_hash(pack_core)

        # finalize pack with generation timestamp and pack_id (these are not part of the canonical hash)
        pack = dict(pack_core)
        pack["pack_id"] = pack_id
        pack["generated_at"] = datetime.now(timezone.utc).isoformat()

        # append audit event for evidence pack generation
        payload = {
            "event": "evidence_pack.generated",
            "strategy_id": strategy_id,
            "pack_id": pack_id,
        }
        try:
            if hasattr(self.ledger, "append"):
                self.ledger.append(payload)
            self.audit_fn("evidence_pack.generated", payload)
        except Exception:
            pass

        return pack
