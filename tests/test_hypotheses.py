from dataclasses import FrozenInstanceError

import pytest

from octa_alpha.hypotheses import HypothesisRegistry


def test_missing_fields_rejected():
    r = HypothesisRegistry()
    with pytest.raises(ValueError):
        r.register(
            economic_intuition="",
            expected_regime="bull",
            expected_failure_modes="none",
            risk_assumptions="low",
            test_spec={"spec": "t"},
        )
    with pytest.raises(ValueError):
        r.register(
            economic_intuition="mean reversion",
            expected_regime="",
            expected_failure_modes="none",
            risk_assumptions="low",
            test_spec={"spec": "t"},
        )
    # missing test_spec
    with pytest.raises(ValueError):
        r.register(
            economic_intuition="x",
            expected_regime="y",
            expected_failure_modes="z",
            risk_assumptions="r",
            test_spec={},
        )


def test_immutable_hypotheses_and_duplicate_id():
    r = HypothesisRegistry()
    hyp = r.register(
        economic_intuition="momentum",
        expected_regime="trending",
        expected_failure_modes="mean reversion",
        risk_assumptions="liquidity ok",
        test_spec={"cases": ["t1"]},
    )
    # attempting to mutate should raise FrozenInstanceError
    with pytest.raises(FrozenInstanceError):
        hyp.economic_intuition = "changed"

    # duplicate id registration rejected
    with pytest.raises(ValueError):
        r.register(
            economic_intuition="a",
            expected_regime="b",
            expected_failure_modes="c",
            risk_assumptions="d",
            test_spec={"x": 1},
            hypothesis_id=hyp.hypothesis_id,
        )
