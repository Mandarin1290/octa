import numpy as np
import pandas as pd

from octa_core.correlation_stress import detect_breakdown
from octa_sentinel.correlation_gates import CorrelationGates


def _mv_normal(n, corr, rng):
    cov = np.full((n, n), corr)
    np.fill_diagonal(cov, 1.0)
    return rng.multivariate_normal(np.zeros(n), cov)


def test_stable_independent_series():
    rng = np.random.default_rng(42)
    n_assets = 4
    rows = 200
    data = []
    for _ in range(rows):
        data.append(rng.normal(size=n_assets))
    df = pd.DataFrame(data, columns=[f"a{i}" for i in range(n_assets)])

    # window smaller than rows
    res = detect_breakdown(df, window=50)
    assert res["score"] < 0.3
    assert 0.1 <= res["recommended_compression"] <= 1.0


def test_increasing_correlation_triggers():
    rng = np.random.default_rng(123)
    n_assets = 4
    rows = 240
    data = []
    # first 180 rows low corr, last 60 high corr so previous window is low
    for _ in range(rows - 60):
        data.append(rng.normal(size=n_assets))
    for _ in range(60):
        cov = np.full((n_assets, n_assets), 0.95)
        np.fill_diagonal(cov, 1.0)
        data.append(rng.multivariate_normal(np.zeros(n_assets), cov))
    df = pd.DataFrame(data, columns=[f"s{i}" for i in range(n_assets)])

    res = detect_breakdown(df, window=60)
    # should show a meaningful increase vs. baseline independent series
    assert res["metrics"]["avg_pairwise"] > 0.4 or res["metrics"]["delta"] > 0.1
    top = res["top_pairs"]
    assert len(top) >= 1
    assert abs(top[0][2]) > 0.2


def test_correlation_gates_calls_apis():
    rng = np.random.default_rng(7)
    n_assets = 3
    rows = 180
    data = []
    # make previous window low-corr and last window high-corr
    for _ in range(rows - 60):
        data.append(rng.normal(size=n_assets))
    for _ in range(60):
        cov = np.full((n_assets, n_assets), 0.95)
        np.fill_diagonal(cov, 1.0)
        data.append(rng.multivariate_normal(np.zeros(n_assets), cov))
    df = pd.DataFrame(data, columns=[f"x{i}" for i in range(n_assets)])

    res = detect_breakdown(df, window=60)

    class MockSentinel:
        def __init__(self):
            self.last = None

        def set_gate(self, level, reason):
            self.last = (level, reason)

    class MockAllocator:
        def __init__(self):
            self.scaled = None

        def scale_risk(self, factor):
            self.scaled = factor

    sentinel = MockSentinel()
    allocator = MockAllocator()
    called = []

    def audit_fn(evt, payload):
        called.append((evt, payload))

    gates = CorrelationGates()
    action = gates.evaluate_and_act(
        res, sentinel_api=sentinel, allocator_api=allocator, audit_fn=audit_fn
    )

    assert "gate_level" in action
    assert sentinel.last is not None
    assert allocator.scaled is not None
    assert called, "audit_fn should be called"
