from octa_strategy.drawdown_character import analyze_drawdown


def gen_series(mu=0.0, sigma=0.001, n=100, seed=1):
    import random

    rnd = random.Random(seed)
    return [rnd.gauss(mu, sigma) for _ in range(n)]


def test_prolonged_drawdown_detected():
    # create long shallow negative returns
    base = gen_series(mu=0.001, sigma=0.0005, n=100, seed=2)
    prolonged = gen_series(mu=-0.0008, sigma=0.0003, n=80, seed=3)
    data = base + prolonged
    res = analyze_drawdown(data, window=60)
    assert res["profile"]["classification"] in ("LONG_SHALLOW", "MIXED")


def test_fast_recovery_recognized():
    # simulate a sharp drop then quick recovery
    stable = gen_series(mu=0.001, sigma=0.0005, n=80, seed=4)
    crash = [-0.3]
    quick_recover = gen_series(
        mu=0.3, sigma=0.01, n=5, seed=5
    )  # large positive returns to recover
    data = (
        stable
        + crash
        + quick_recover
        + gen_series(mu=0.001, sigma=0.0005, n=20, seed=6)
    )
    res = analyze_drawdown(data, window=60)
    assert res["profile"]["classification"] in ("SHARP_CRASH", "QUICK_RECOVERY")
