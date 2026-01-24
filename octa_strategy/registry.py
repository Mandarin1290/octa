from typing import Dict, Optional

from octa_strategy.models import StrategyMeta


class RegistryError(Exception):
    pass


class StrategyRegistry:
    """Canonical registry for strategies.

    - Entries immutable except lifecycle_state which may be updated via `update_lifecycle`.
    - Each update increments a version and is audited via `audit_fn`.
    """

    def __init__(self, audit_fn=None):
        self._store: Dict[str, Dict] = {}
        self.audit_fn = audit_fn or (lambda e, p: None)

    def register(self, meta: StrategyMeta) -> None:
        if meta.strategy_id in self._store:
            raise RegistryError(f"Duplicate strategy_id: {meta.strategy_id}")
        entry = {
            "meta": meta,
            "version": 1,
            "history": [{"version": 1, "meta": meta.as_dict()}],
        }
        self._store[meta.strategy_id] = entry
        self.audit_fn(
            "registry.register", {"strategy_id": meta.strategy_id, "version": 1}
        )

    def get(self, strategy_id: str) -> Optional[StrategyMeta]:
        entry = self._store.get(strategy_id)
        return entry["meta"] if entry else None

    def update_lifecycle(
        self, strategy_id: str, new_state: str, doc: Optional[str] = None
    ) -> None:
        entry = self._store.get(strategy_id)
        if not entry:
            raise RegistryError("Strategy not registered")
        old_meta: StrategyMeta = entry["meta"]
        # create a new StrategyMeta with updated lifecycle_state, preserve created_at
        updated = StrategyMeta(
            strategy_id=old_meta.strategy_id,
            owner=old_meta.owner,
            asset_classes=old_meta.asset_classes,
            risk_budget=old_meta.risk_budget,
            holding_period_days=old_meta.holding_period_days,
            expected_turnover_per_month=old_meta.expected_turnover_per_month,
            lifecycle_state=new_state,
            created_at=old_meta.created_at,
        )
        entry["version"] += 1
        entry["meta"] = updated
        entry["history"].append(
            {"version": entry["version"], "meta": updated.as_dict(), "doc": doc}
        )
        self.audit_fn(
            "registry.update_lifecycle",
            {
                "strategy_id": strategy_id,
                "version": entry["version"],
                "new_state": new_state,
                "doc": doc,
            },
        )

    def update_field(self, strategy_id: str, field_name: str, value) -> None:
        # Only lifecycle_state may change; other fields immutable
        if field_name == "lifecycle_state":
            return self.update_lifecycle(strategy_id, value, doc=None)
        raise RegistryError("Immutable field: cannot update registry field")

    def list(self):
        return {k: v["meta"].as_dict() for k, v in self._store.items()}
