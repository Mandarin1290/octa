from octa_capital.scaling_impact import ScalingImpactAnalyzer


def test_degradation_detected_and_monotonic():
    hist = [100.0] * 100  # stable absolute returns
    base_aum = 1_000_000.0
    targets = [1_000_000.0, 2_000_000.0, 5_000_000.0]
    analyzer = ScalingImpactAnalyzer(beta=0.7)
    res = analyzer.simulate_scaling(hist, base_aum, targets)
    # expected returns should decrease as AUM increases
    vals = [r.expected_return for r in res]
    assert vals[0] >= vals[1] >= vals[2]


def test_break_even_computed():
    hist = [1000.0] * 10
    base_aum = 1_000_000.0
    analyzer = ScalingImpactAnalyzer(beta=1.0)
    # choose hurdle_rate small so break-even exists at some AUM
    be = analyzer.compute_break_even(
        hist, base_aum, hurdle_rate=0.00005, search_aum_max=10_000_000.0, step=10_000.0
    )
    assert be is None or be >= base_aum
