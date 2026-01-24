from __future__ import annotations

import os
from typing import Any, Dict, List

from octa_atlas.registry import AtlasRegistry, FileIntegrityError
from octa_core.ids import generate_id
from octa_core.types import Identifier
from octa_ledger.api import LedgerAPI
from octa_sentinel.engine import SentinelEngine
from octa_sentinel.policies import SentinelPolicy
from octa_stream.manifest import AssetManifest
from octa_vertex.models import Order, OrderIntent, OrderSide, OrderStatus
from octa_vertex.sim_replay import Simulator
from octa_vertex.state_machine import OrderStateMachine


class PaperRuntime:
    def __init__(
        self,
        manifests_dir: str,
        atlas_root: str,
        ledger_dir: str,
        vertex_store: str,
        policy: SentinelPolicy | None = None,
        audit_enabled: bool = True,
        sentinel_engine: SentinelEngine | None = None,
    ) -> None:
        self.manifests_dir = manifests_dir
        self.atlas = AtlasRegistry(atlas_root)
        self.ledger = LedgerAPI(ledger_dir)
        self.vertex_store = vertex_store
        self.policy = policy or SentinelPolicy(schema_version=1, name="paper_default")
        self.sentinel = sentinel_engine or SentinelEngine(
            policy=self.policy,
            state_path=os.path.join(ledger_dir, "sentinel_state.json"),
            ledger_store=self.ledger.store,
        )
        self.oms = OrderStateMachine(self.ledger)
        self.sim = Simulator(self.oms)
        self.audit_enabled = audit_enabled

    def _load_manifests(self) -> List[AssetManifest]:
        out: List[AssetManifest] = []
        for fn in os.listdir(self.manifests_dir):
            if not fn.endswith((".yaml", ".yml", ".json")):
                continue
            try:
                m = AssetManifest.load(os.path.join(self.manifests_dir, fn))
                if m.ca_provided:
                    out.append(m)
            except Exception:
                # skip invalid manifests
                continue
        return out

    @staticmethod
    def _deterministic_signal(symbol: str) -> float:
        # deterministic, math-defined: normalized sum of ords mod 100 -> [0,1]
        s = sum(ord(c) for c in symbol)
        return (s % 100) / 100.0

    def run_once(self) -> Dict[str, Any]:
        trace: Dict[str, Any] = {
            "inference": [],
            "orders": [],
            "sentinel": None,
            "audit_events": 0,
        }
        manifests = self._load_manifests()
        # audit manifests loaded
        if self.audit_enabled:
            self.ledger.audit_or_fail(
                actor="paper_runtime",
                action="load_manifests",
                payload={"count": len(manifests)},
            )

        # assets -> artifacts
        assets = []
        for m in manifests:
            try:
                art, meta = self.atlas.load_latest(m.asset_id, "model")
                assets.append((m, art, meta))
                if self.audit_enabled:
                    self.ledger.audit_or_fail(
                        actor="paper_runtime",
                        action="load_artifact",
                        payload={"asset": m.asset_id, "version_meta": meta},
                    )
            except FileIntegrityError as e:
                # log and block
                self.ledger.audit_or_fail(
                    actor="paper_runtime",
                    action="artifact_integrity_failure",
                    payload={"asset": m.asset_id, "error": str(e)},
                )
                return {"blocked": True, "reason": "artifact_integrity"}

        # inference
        intents: List[OrderIntent] = []
        for m, _art, _meta in assets:
            score = self._deterministic_signal(m.symbol)
            self.ledger.audit_or_fail(
                actor="paper_runtime",
                action="inference",
                payload={"asset": m.asset_id, "symbol": m.symbol, "score": score},
            )
            trace["inference"].append({"asset": m.asset_id, "score": score})
            # simple portfolio rule: buy if score > 0.6
            if score > 0.6:
                qty = round(score * 10, 4)
                intent = OrderIntent(
                    intent_id=str(generate_id("intent")),
                    symbol=m.symbol,
                    side=OrderSide.BUY,
                    qty=qty,
                    price=None,
                    notional=None,
                )
                intents.append(intent)
                self.ledger.audit_or_fail(
                    actor="paper_runtime",
                    action="portfolio_intent",
                    payload={
                        "intent_id": intent.intent_id,
                        "symbol": intent.symbol,
                        "qty": intent.qty,
                    },
                )

        # sentinel pre-trade
        # aggregate inputs (simple): no pnl/exposure; set broker_connected True for paper
        dec = self.sentinel.evaluate({"health": {"broker_connected": True}})
        trace["sentinel"] = {"level": dec.level, "reason": dec.reason}
        if dec.level >= 2:
            self.ledger.audit_or_fail(
                actor="paper_runtime",
                action="pretrade_blocked",
                payload={"reason": dec.reason},
            )
            return {"blocked": True, "reason": dec.reason}

        # send to vertex (paper execution)
        for it in intents:
            oid = generate_id("order")
            order = Order(
                id=Identifier(str(oid)),
                intent_id=it.intent_id,
                symbol=it.symbol,
                side=it.side,
                qty=it.qty,
                price=it.price,
                status=OrderStatus.NEW,
            )
            self.ledger.audit_or_fail(
                actor="paper_runtime",
                action="order_created",
                payload={
                    "order_id": str(order.id),
                    "symbol": order.symbol,
                    "qty": order.qty,
                },
            )
            # simulate immediate fill in paper mode
            filled = order.qty
            self.oms.transition(order, OrderStatus.FILLED)
            self.ledger.audit_or_fail(
                actor="paper_runtime",
                action="order_filled",
                payload={"order_id": str(order.id), "filled_qty": filled},
            )
            trace["orders"].append(
                {"order_id": str(order.id), "symbol": order.symbol, "filled": filled}
            )

        trace["audit_events"] = len(self.ledger.last_n(50))
        return trace
