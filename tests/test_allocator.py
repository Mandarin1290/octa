from octa_core.allocator import allocate
from octa_core.strategy import StrategyOutput


def test_budget_respected():
    # two strategies, one with small budget
    s1 = ("s1", StrategyOutput(exposures={"A": 0.5}, confidence=0.9, rationale={}))
    s2 = ("s2", StrategyOutput(exposures={"A": 0.5}, confidence=0.9, rationale={}))
    current = {}
    risk_budgets = {"s1": 1.0, "s2": 0.1}
    prices = {"A": [100, 101, 102, 103, 104]}
    asset_classes = {"A": "EQUITY"}
    intent = allocate(
        [s1, s2],
        current,
        risk_budgets,
        prices,
        asset_classes,
        gross_cap=1.0,
        net_cap=1.0,
    )
    # s2 contribution should be much smaller
    a = intent.targets.get("A", 0.0)
    assert abs(a) <= 1.0
    assert intent.attribution["s2"]["A"] != intent.attribution["s1"]["A"]


def test_correlated_strategies_downweighted():
    # two strategies both long A and B with same signals; prices perfectly correlated
    s1 = (
        "s1",
        StrategyOutput(exposures={"A": 0.5, "B": 0.5}, confidence=0.9, rationale={}),
    )
    s2 = (
        "s2",
        StrategyOutput(exposures={"A": 0.5, "B": 0.5}, confidence=0.8, rationale={}),
    )
    current = {}
    risk_budgets = {"s1": 1.0, "s2": 1.0}
    # perfectly correlated price series
    prices = {"A": [100, 101, 102, 103, 104], "B": [200, 202, 204, 206, 208]}
    asset_classes = {"A": "EQUITY", "B": "EQUITY"}
    intent = allocate(
        [s1, s2],
        current,
        risk_budgets,
        prices,
        asset_classes,
        gross_cap=10.0,
        net_cap=10.0,
    )
    # because correlation is high, avg_abs_corr ~1, correlation scaling reduces exposures
    valA = abs(intent.targets.get("A", 0.0))
    valB = abs(intent.targets.get("B", 0.0))
    assert valA < 1.0 and valB < 1.0


def test_caps_enforced():
    s1 = (
        "s1",
        StrategyOutput(exposures={"A": 1.0, "B": 1.0}, confidence=0.9, rationale={}),
    )
    current = {}
    risk_budgets = {"s1": 1.0}
    prices = {"A": [100, 101, 102], "B": [50, 51, 52]}
    asset_classes = {"A": "EQUITY", "B": "EQUITY"}
    intent = allocate(
        [s1], current, risk_budgets, prices, asset_classes, gross_cap=0.5, net_cap=0.2
    )
    # gross cap should limit sum of abs exposures
    gross = sum(abs(v) for v in intent.targets.values())
    assert gross <= 0.5 + 1e-8
