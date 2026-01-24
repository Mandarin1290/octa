from octa_core.concentration import ExposureGraph, evaluate_concentration


def test_duplicate_exposures_detected():
    g = ExposureGraph()
    # strategies are prefixed with s:, assets with a:
    g.add_edge("s:alpha", "a:US_STOCK_A", 0.05)
    g.add_edge("s:alpha", "a:US_STOCK_B", 0.05)

    g.add_edge("s:beta", "a:US_STOCK_A", 0.05)
    g.add_edge("s:beta", "a:US_STOCK_B", 0.05)

    res = evaluate_concentration(g, factor_map={})
    # duplicates should be detected
    assert res["duplicates"], "Expected duplicate strategies"
    # sentinel actions should include a duplicate-related entry
    dup_reasons = [
        s
        for s in res["actions"]["sentinel"]
        if "duplicate_strategies" in s.get("reason", "")
    ]
    assert dup_reasons


def test_factor_caps_enforced_when_missing_proxy():
    g = ExposureGraph()
    g.add_edge("s:one", "a:FX_USD", 0.1)
    g.add_edge("s:two", "a:FX_USD", 0.08)

    # no factor_map provided -> conservative cap expected
    res = evaluate_concentration(g, factor_map={})
    assert (
        "__missing_proxies__" in res["factor_results"]
        or res["actions"]["scale_recommendations"].get("global") <= 0.05
    )
