from octa_alpha.failure_modes import FailureModeRegistry
from octa_alpha.hypotheses import HypothesisRegistry


def test_unexpected_failure_flagged():
    fm = FailureModeRegistry(taxonomy=["mean_reversion", "liquidity_shortage"])
    # observed a mode not in taxonomy
    evt = fm.observe("hyp-1", ["data_leak"])
    assert "data_leak" in evt.unexpected


def test_linkage_to_hypothesis_registry():
    hr = HypothesisRegistry()
    hyp = hr.register(
        economic_intuition="momentum",
        expected_regime="trending",
        expected_failure_modes="mean_reversion",
        risk_assumptions="liquidity ok",
        test_spec={"cases": ["t1"]},
    )
    fm = FailureModeRegistry(taxonomy=["mean_reversion", "data_leak"])
    fm.observe(hyp.hypothesis_id, ["mean_reversion"])
    events = fm.get_events_for_hypothesis(hyp.hypothesis_id)
    assert len(events) == 1
    assert events[0].hypothesis_id == hyp.hypothesis_id
