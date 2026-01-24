from octa_ip.module_map import ModuleMap
from octa_ip.spinoff_simulator import SpinoffSimulator


def test_missing_dependency_flagged():
    mm = ModuleMap()
    mm.add_module("mod_root", owner="team_root")
    # root depends on mod_dep which is missing from module map
    mm.add_dependency("mod_root", "mod_dep")

    sim = SpinoffSimulator()
    report = sim.simulate(mm, "mod_root")
    assert "mod_dep" in report.missing
    assert any("missing dependency" in a for a in report.adaptations)


def test_includes_transitive_deps_and_adaptation_cross_owner():
    mm = ModuleMap()
    mm.add_module("A", owner="team1")
    mm.add_module("B", owner="team2")
    mm.add_module("C", owner="team1")
    mm.add_dependency("A", "B")
    mm.add_dependency("B", "C")

    # C is depended on by B (owner team2) and is owned by team1 -> cross-owner
    sim = SpinoffSimulator()
    rep = sim.simulate(mm, "A")
    assert "B" in rep.included
    assert "C" in rep.included
    assert any(
        "cross-owner" in a or "cross-owner dependents" in a or "owners=" in a
        for a in rep.adaptations
    )
