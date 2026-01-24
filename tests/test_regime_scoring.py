from octa_alpha.regime_scoring import score_alpha


def test_regime_mismatch_penalized():
    # signal positive but regime incompatible
    res = score_alpha(
        signal=0.8,
        base_confidence=1.0,
        regime="bear",
        regime_compatibility={"bull": 1.2, "bear": 0.2},
        regime_uncertainty=0.0,
    )
    assert abs(float(res["final_score"])) < abs(0.8)


def test_compatible_alpha_favored():
    res_bad = score_alpha(
        signal=0.5,
        base_confidence=1.0,
        regime="bear",
        regime_compatibility={"bull": 1.5, "bear": 0.5},
        regime_uncertainty=0.0,
    )
    res_good = score_alpha(
        signal=0.5,
        base_confidence=1.0,
        regime="bull",
        regime_compatibility={"bull": 1.5, "bear": 0.5},
        regime_uncertainty=0.0,
    )
    assert float(res_good["final_score"]) > float(res_bad["final_score"])
