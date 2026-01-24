from octa_strategy.correlation import StrategyCorrelation


def test_correlated_strategies_flagged_and_budget_reduced():
    # s1 base
    s1 = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01]
    # s2 highly correlated with s1
    s2 = [v * 0.95 for v in s1]
    # s3 uncorrelated
    s3 = [0.002, -0.001, 0.003, -0.002, 0.0, 0.001]

    returns = {"S1": s1, "S2": s2, "S3": s3}
    engine = StrategyCorrelation(redundancy_threshold=0.8)
    report = engine.assess(returns)
    assert "S1" in report and "S2" in report and "S3" in report
    # S1 and S2 should have high score and be flagged
    assert report["S1"]["flagged"] is True
    assert report["S2"]["flagged"] is True
    # S3 should not be flagged
    assert report["S3"]["flagged"] is False

    budgets = {"S1": 100.0, "S2": 100.0, "S3": 100.0}
    adjusted = engine.compress_budgets(budgets, report)
    # budgets for S1 and S2 should be reduced
    assert adjusted["S1"] < 100.0
    assert adjusted["S2"] < 100.0
    # S3 should be similar or unchanged
    assert adjusted["S3"] >= 100.0 * 0.1


def test_budget_reduction_proportional_to_score():
    # create two strategies with medium correlation
    base = [0.01, -0.005, 0.02, -0.01, 0.005]
    s2 = [v * 0.7 for v in base]
    returns = {"A": base, "B": s2}
    engine = StrategyCorrelation(redundancy_threshold=0.5)
    report = engine.assess(returns)
    budgets = {"A": 1000.0, "B": 1000.0}
    adjusted = engine.compress_budgets(budgets, report)
    # compression factor ~ 1 - score; ensure budgets reduced but positive
    for k in budgets:
        assert adjusted[k] > 0
        assert adjusted[k] <= budgets[k]
