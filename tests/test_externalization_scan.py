from octa_ip.externalization_scan import ExternalizationScanner
from octa_ip.module_map import ModuleMap


def test_tightly_coupled_module_rejected(tmp_path):
    mm = ModuleMap()
    mm.add_module("modA", owner="teamA")
    mm.add_module("modB", owner="teamB")
    # modA depends on modB -> tightly coupled
    mm.add_dependency("modA", "modB")

    # attach a harmless file for modA
    f = tmp_path / "modA_file.py"
    f.write_text("print('hello')\n")
    mm.add_file_to_module("modA", str(f))

    scanner = ExternalizationScanner()
    rep = scanner.analyze_module(mm, "modA")
    assert rep.ready is False
    assert "has_dependencies" in rep.reasons


def test_isolated_module_eligible(tmp_path):
    mm = ModuleMap()
    mm.add_module("modIso", owner="teamX")
    f = tmp_path / "iso.py"
    f.write_text("def foo():\n    return 1\n")
    mm.add_file_to_module("modIso", str(f))

    scanner = ExternalizationScanner()
    rep = scanner.analyze_module(mm, "modIso")
    assert rep.ready is True
    assert rep.purity_score == 1.0
