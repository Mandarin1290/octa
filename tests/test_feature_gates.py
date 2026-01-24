from octa_alpha.feature_gates import FeatureGates


def test_leaking_feature_rejected():
    fg = FeatureGates(allowed_transforms=["zscore"], leakage_threshold=0.1)
    feature = {
        "name": "leaker",
        "values": [1, 2, 3, 4],
        "correlation_with_future": 0.2,  # above threshold
        "transforms": ["zscore"],
    }
    res = fg.check(feature)
    assert not res.passed
    assert "leakage_detected" in res.reasons


def test_unstable_feature_blocked():
    fg = FeatureGates(allowed_transforms=["zscore"])
    # construct non-stationary series: mean jumps between halves
    series = [1] * 10 + [100] * 10
    feature = {
        "name": "unstable",
        "values": series,
        "series": series,
        "transforms": ["zscore"],
    }
    res = fg.check(feature)
    assert not res.passed
    assert "non_stationary" in res.reasons


def test_good_feature_passes():
    fg = FeatureGates(allowed_transforms=["zscore"], max_latency_ms=100)
    series = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    feature = {
        "name": "good",
        "values": series,
        "series": series,
        "latency_ms": 10,
        "transforms": ["zscore"],
        "correlation_with_future": 0.01,
    }
    res = fg.check(feature)
    assert res.passed
