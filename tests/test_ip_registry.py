import pytest

from octa_ip.ip_registry import IPRegistry, IPRegistryError


def test_registry_consistency_and_topo():
    reg = IPRegistry()
    # create assets
    reg.add_asset("libx", asset_id="libx")
    reg.add_asset("strategy_alpha", asset_id="alpha")

    # add baseline lib
    h_lib = reg.add_version("libx", "v1.2.0", metadata={"release": True})
    assert h_lib

    # add alpha baseline
    reg.add_version("alpha", "v1", metadata={"notes": "initial"})
    assert reg.verify_lineage("alpha", "v1")

    # add alpha v2 depending on libx@v1.2.0 and parent v1
    reg.add_version("alpha", "v2", parent="v1", dependencies=["libx@v1.2.0"])
    assert reg.verify_lineage("alpha", "v2")

    g = reg.dependency_graph()
    assert "alpha@v2" in g
    assert "libx@v1.2.0" in g["alpha@v2"]

    order = reg.topological_sort()
    # lib should come before alpha@v2
    assert order.index("libx@v1.2.0") < order.index("alpha@v2")


def test_version_immutability_and_cycle_detection():
    reg = IPRegistry()
    reg.add_asset("libx", asset_id="libx")
    reg.add_asset("alpha", asset_id="alpha")

    reg.add_version("libx", "v1")
    reg.add_version("alpha", "v1")

    # re-adding same version should raise
    with pytest.raises(IPRegistryError):
        reg.add_version("alpha", "v1")

    # create a dependency chain: libx@v2 -> alpha@v1
    reg.add_version("libx", "v2", dependencies=["alpha@v1"])
    # adding alpha@v2 that depends on libx@v2 should be allowed (no immediate cycle)
    h = reg.add_version("alpha", "v2", parent="v1", dependencies=["libx@v2"])
    assert h


def test_deprecate_and_lifecycle():
    reg = IPRegistry()
    reg.add_asset("alpha", asset_id="alpha")
    reg.add_version("alpha", "v1")
    reg.deprecate_version("alpha", "v1")
    v = reg.get_version("alpha", "v1")
    assert v.lifecycle == "deprecated"
