from octa_ip.module_map import ModuleMap
from octa_ip.valuation_meta import ValuationEngine


def test_metrics_reproducible_and_expected():
    mm = ModuleMap()
    mm.add_module("A", owner="team1", classification="licensable")
    mm.add_module("B", owner="team2", classification="internal")
    mm.add_module("C", owner="team3", classification="internal")
    # B and C depend on A
    mm.add_dependency("B", "A")
    mm.add_dependency("C", "A")

    usage = {"A": 100, "B": 50, "C": 10}
    revenue = {"A": 10000}

    ve = ValuationEngine()
    out1 = ve.compute_metadata(mm, usage, revenue)
    out2 = ve.compute_metadata(mm, usage, revenue)
    # reproducible
    assert out1 == out2

    # A should have higher dependency centrality than B and C
    assert out1["A"]["dependency_centrality"] > out1["B"]["dependency_centrality"]

    # revenue_relevance present for A only (licensable)
    assert out1["A"]["revenue_relevance"] == 10000.0
    assert out1["B"]["revenue_relevance"] == 0.0
