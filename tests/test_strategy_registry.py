import pytest

from octa_strategy.models import StrategyMeta
from octa_strategy.registry import RegistryError, StrategyRegistry


def test_duplicate_ids_rejected():
    audit = []
    reg = StrategyRegistry(audit_fn=lambda e, p: audit.append((e, p)))
    meta = StrategyMeta(
        strategy_id="S1",
        owner="Alice",
        asset_classes=["equities"],
        risk_budget=0.1,
        holding_period_days=30,
        expected_turnover_per_month=2.0,
        lifecycle_state="IDEA",
    )
    reg.register(meta)
    with pytest.raises(RegistryError):
        reg.register(meta)


def test_registry_immutable_fields_enforced():
    reg = StrategyRegistry()
    meta = StrategyMeta(
        strategy_id="S2",
        owner="Bob",
        asset_classes=["futures"],
        risk_budget=0.2,
        holding_period_days=10,
        expected_turnover_per_month=5.0,
        lifecycle_state="IDEA",
    )
    reg.register(meta)
    # attempt to change owner should fail
    with pytest.raises(RegistryError):
        reg.update_field("S2", "owner", "Mallory")
    # lifecycle update should work
    reg.update_lifecycle("S2", "PAPER", doc="Paper approval")
    updated = reg.get("S2")
    assert updated.lifecycle_state == "PAPER"
