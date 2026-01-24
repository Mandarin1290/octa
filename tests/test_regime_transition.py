from decimal import Decimal

from octa_alpha.regime_transition import RegimeTransitionEngine


def test_transition_reduces_exposure():
    eng = RegimeTransitionEngine(
        dampening_factor=Decimal("0.5"),
        uncertainty_increase=Decimal("0.2"),
        compression_periods=2,
    )
    prev = "bull"
    curr = "bear"
    exposure = Decimal("1000")
    unc = Decimal("0.1")
    new_exp, new_unc, compression = eng.handle_transition(prev, curr, exposure, unc)
    assert new_exp < exposure
    assert new_unc > unc
    assert compression == 2


def test_stability_preserved():
    eng = RegimeTransitionEngine(
        dampening_factor=Decimal("0.5"),
        uncertainty_increase=Decimal("0.2"),
        compression_periods=2,
    )
    prev = "bull"
    curr = "bull"
    exposure = Decimal("1000")
    unc = Decimal("0.1")
    new_exp, new_unc, compression = eng.handle_transition(prev, curr, exposure, unc)
    assert new_exp == exposure
    assert new_unc == unc
    assert compression == 0


def test_re_evaluate_score_lower_on_uncertainty():
    eng = RegimeTransitionEngine()
    # positive signal
    s = 0.8
    base_conf = 1.0
    regime = "bear"
    compat = {"bear": 1.0}
    low_unc = 0.0
    high_unc = 0.4
    r1 = eng.re_evaluate_score(s, base_conf, regime, compat, low_unc)
    r2 = eng.re_evaluate_score(s, base_conf, regime, compat, high_unc)
    assert abs(float(r2["final_score"])) < abs(float(r1["final_score"]))
