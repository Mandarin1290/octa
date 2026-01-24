import pytest

from octa_core.spin_off import ModuleDescriptor, SpinOffError, SpinOffManager


def test_dependency_isolation_allows_safe_spin():
    mgr = SpinOffManager()
    mgr.register_module(
        ModuleDescriptor(
            name="governance_framework",
            provides=["governance"],
            depends_on=[],
            critical=True,
        )
    )
    mgr.register_module(
        ModuleDescriptor(
            name="monitoring_stack",
            provides=["monitoring"],
            depends_on=["governance_framework"],
        )
    )
    mgr.register_module(
        ModuleDescriptor(
            name="risk_engine", provides=["risk_calc"], depends_on=["data_layer"]
        )
    )
    mgr.register_module(
        ModuleDescriptor(
            name="execution_engine",
            provides=["order_exec"],
            depends_on=["market_adapter"],
        )
    )
    mgr.register_module(
        ModuleDescriptor(name="data_layer", provides=["data"], depends_on=[])
    )
    mgr.register_module(
        ModuleDescriptor(name="market_adapter", provides=["market"], depends_on=[])
    )

    # allow explicit externals if any (none here)
    # propose spinning off risk_engine and execution_engine (their deps remain in core)
    manifest = mgr.propose_spin_off(["risk_engine", "execution_engine"])
    assert "spin_off" in manifest
    assert set(manifest["spin_off"]) == {"risk_engine", "execution_engine"}


def test_integrity_preserved_rejects_critical_and_unresolved():
    mgr = SpinOffManager()
    mgr.register_module(
        ModuleDescriptor(
            name="governance_framework",
            provides=["governance"],
            depends_on=[],
            critical=True,
        )
    )
    mgr.register_module(
        ModuleDescriptor(
            name="monitoring_stack",
            provides=["monitoring"],
            depends_on=["governance_framework"],
        )
    )

    # trying to spin off the critical governance module must be rejected
    with pytest.raises(SpinOffError):
        mgr.propose_spin_off(["governance_framework"])

    # register a module with unknown dependency and attempt to spin it off
    mgr.register_module(
        ModuleDescriptor(name="plugin", provides=["plugin"], depends_on=["unknown_dep"])
    )
    with pytest.raises(SpinOffError):
        mgr.propose_spin_off(["plugin"])
