from okta_altdat.weights import apply_quality_adjustments, normalize_weights


def test_weights_normalize_only_enabled_sources():
    base = {"edgar": 0.35, "macro": 0.25, "news": 0.15}
    enabled = {"edgar": True, "macro": False, "news": True}
    w = normalize_weights(base, enabled=enabled)
    assert set(w.keys()) == {"edgar", "news"}
    assert abs(sum(w.values()) - 1.0) < 1e-9


def test_quality_adjustment_zeroes_low_coverage_then_renormalizes():
    w0 = {"edgar": 0.5, "macro": 0.5}
    coverage = {"edgar": 0.9, "macro": 0.1}
    w1, reasons = apply_quality_adjustments(weights=w0, coverage=coverage, min_coverage=0.5)
    assert w1["macro"] == 0.0
    assert abs(w1["edgar"] - 1.0) < 1e-9
    assert reasons.get("macro")
