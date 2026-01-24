from octa_ip.module_map import ModuleMap


def test_ownership_resolved():
    mm = ModuleMap()
    mm.add_module("octa_core", owner="CoreTeam", classification="core")
    mm.add_module("octa_alpha", owner="AlphaTeam", classification="internal")
    mm.add_module("octa_reports", owner="ReportsTeam", classification="licensable")
    assert "octa_core" in mm.modules
    assert mm.modules["octa_alpha"].owner == "AlphaTeam"


def test_illegal_dependency_blocked():
    mm = ModuleMap()
    mm.add_module("octa_core", owner="CoreTeam", classification="core")
    mm.add_module("octa_alpha", owner="AlphaTeam", classification="internal")
    mm.add_module("octa_fund", owner="FundTeam", classification="licensable")
    # add a dependency where octa_fund depends on octa_alpha (cross-owner internal)
    mm.add_dependency("octa_fund", "octa_alpha")
    violations = mm.detect_violations()
    assert any(v[0] == "octa_fund" and v[1] == "octa_alpha" for v in violations)
