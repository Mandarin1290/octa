from octa_alpha.data_sufficiency import estimate_sample_size, is_data_sufficient


def test_insufficient_data_blocks_pipeline():
    values = [0.1] * 5  # small sample
    res = is_data_sufficient(values, effect_size=0.5, sigma=1.0, alpha=0.05, power=0.8)
    assert not res["sufficient"]
    assert "underpowered" in res["reasons"]


def test_sufficient_data_passes():
    # generate larger sample approximating required n
    eff = 0.5
    sigma = 1.0
    req = estimate_sample_size(effect_size=eff, sigma=sigma, alpha=0.05, power=0.8)
    values = [0.0] * req
    res = is_data_sufficient(
        values, effect_size=eff, sigma=sigma, alpha=0.05, power=0.8
    )
    assert res["sufficient"] is True
