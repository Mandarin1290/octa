from octa_strategy.stability import PerformanceStabilityAnalyzer


def gen_series(n, mu=0.001, sigma=0.0005, seed=1):
    import random

    rnd = random.Random(seed)
    return [rnd.gauss(mu, sigma) for _ in range(n)]


def test_unstable_series_flagged():
    # stable baseline then increasing volatility
    base = gen_series(200, mu=0.001, sigma=0.0005, seed=2)
    noisy = gen_series(60, mu=0.001, sigma=0.005, seed=3)
    data = base + noisy

    det = PerformanceStabilityAnalyzer(baseline_window=150, recent_window=40)
    rpt = det.analyze(data)
    assert rpt.stability_score > 0.6


def test_stable_series_not_flagged():
    data = gen_series(300, mu=0.001, sigma=0.0005, seed=4)
    det = PerformanceStabilityAnalyzer(baseline_window=150, recent_window=40)
    rpt = det.analyze(data)
    assert rpt.stability_score < 0.4
