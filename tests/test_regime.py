from octa_core.regime import RegimeDetector


def make_prices(n=200, start=100.0, drift=0.0, noise=0.0):
    # simple geometric walk
    prices = [start]
    import random

    for _i in range(1, n):
        ret = drift + random.gauss(0, noise)
        prices.append(prices[-1] * (1 + ret))
    return prices


def test_regime_transitions_detected():
    rd = RegimeDetector(short_window=10, long_window=50, trend_window=10)

    # Build series: first stable low-vol flat, then high-vol noisy, trend up later
    a = [100.0 + 0.01 * i for i in range(60)]  # gentle uptrend
    # insert high volatility spike region
    import random

    random.seed(1)
    for _i in range(60, 100):
        a.append(a[-1] * (1 + random.gauss(0, 0.05)))
    # resume with up trend
    for _i in range(100, 140):
        a.append(a[-1] * 1.002)

    prices = {"AAA": a, "BBB": [p * 0.9 for p in a]}  # correlated pair

    # check at index before spike: expect normal or low vol
    r1 = rd.detect_at_index(prices, idx=50)
    assert r1.volatility in ("low", "normal")

    # during spike: expect high volatility
    r2 = rd.detect_at_index(prices, idx=80)
    assert r2.volatility == "high"

    # later trending up: expect trend up
    r3 = rd.detect_at_index(prices, idx=130)
    assert r3.trend == "up"

    # correlation elevated because series are similar
    assert r2.correlation_stress == "elevated"


def test_no_lookahead():
    rd = RegimeDetector(short_window=5, long_window=20, trend_window=5)
    base = [100.0 * (1 + 0.001 * i) for i in range(50)]
    # attach a future spike beyond index 30
    future = base + [p * 2.0 for p in base[-5:]]
    prices = {"AAA": future}

    r_at_30 = rd.detect_at_index(prices, idx=30)
    r_at_30_later = rd.detect_at_index(prices, idx=30)
    # repeated calls deterministic
    assert r_at_30 == r_at_30_later

    # ensure detection at 30 not influenced by later spike at end (no lookahead): volatility should be low/normal
    assert r_at_30.volatility in ("low", "normal")
