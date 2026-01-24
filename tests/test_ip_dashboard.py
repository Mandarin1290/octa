from octa_ip.externalization_scan import ExternalizationScanner
from octa_ip.ip_classifier import IPClassifier
from octa_ip.module_map import ModuleMap
from octa_ip.policy_enforcer import PolicyEnforcer
from octa_ip.valuation_meta import ValuationEngine
from octa_reports.ip_dashboard import IPDashboard


def test_dashboard_reconciles_with_classifier_and_deterministic():
    mm = ModuleMap()
    mm.add_module("A", owner="team1", classification="licensable")
    mm.add_module("B", owner="team2", classification="internal")
    mm.add_dependency("B", "A")

    classifier = IPClassifier()
    classifier.set_classification("A", "LICENSABLE")
    classifier.set_classification("B", "INTERNAL_ONLY")

    enforcer = PolicyEnforcer()
    scanner = ExternalizationScanner()
    valuation = ValuationEngine()

    dashboard = IPDashboard(mm, classifier, enforcer, scanner, valuation)

    usage = {"A": 10, "B": 1}
    revenue = {"A": 5000}

    s1 = dashboard.summary(usage, revenue)
    s2 = dashboard.summary(usage, revenue)
    assert s1 == s2

    # categories must match classifier classifications for existing modules
    for m in sorted(mm.modules.keys()):
        assert s1["ip_categories"][m] == classifier.classifications.get(m)

    # policy_violations should be a list (may be empty)
    assert isinstance(s1["policy_violations"], list)
