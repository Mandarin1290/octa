import pytest

from octa_alpha.pipeline import AlphaPipeline, AlphaSource


def test_stage_order_enforced():
    p = AlphaPipeline()
    # calling a later stage directly without run should raise
    with pytest.raises(RuntimeError):
        p.data_sufficiency(None, None, {})


def test_bypass_impossible_and_full_run():
    p = AlphaPipeline()

    def gen():
        return {
            "features": [1, 2, 3],
            "data_points": 10,
            "min_data_points": 5,
            "risk_lb": 0,
            "risk_ub": 100,
        }

    src = AlphaSource("test", gen)
    hyp = src.propose()
    result = p.run(hyp)
    assert result["feature_eligible"] is True
    assert result["data_sufficient"] is True
    assert "signal" in result
    assert result["risk_ok"] is True
    assert "paper_deploy" in result

    # ensure attempting to call signal_construction outside run fails
    with pytest.raises(RuntimeError):
        p.signal_construction(None, None, {})
