from octa_ledger.multi_asset_attribution import attribute_pnl


def test_reconciliation_exact():
    pnl = {
        "StratA": {
            "equities": {"pnl_local": 100.0, "currency": "USD"},
            "fx": {"pnl_local": 10.0, "currency": "EUR"},
        },
        "StratB": {"rates": {"pnl_local": -20.0, "currency": "USD"}},
    }
    hedges = {"HedgeStrat": {"equities": {"pnl_local": -5.0, "currency": "USD"}}}
    fx = {"USD": 1.0, "EUR": 1.2}
    out = attribute_pnl(pnl, fx, hedges=hedges)

    # compute expected
    eq_base = 100.0 * 1.0
    fx_local_base = 10.0 * 1.2
    rates_base = -20.0 * 1.0
    hedge_base = -5.0 * 1.0
    expected_total = eq_base + fx_local_base + rates_base + hedge_base

    assert out["reconciles"] is True
    assert abs(out["total_pnl"] - expected_total) < 1e-9
    # fx translation effect should equal fx_base - fx_local_sum
    assert (
        abs(
            out["fx_translation_effect"]
            - (eq_base + fx_local_base + rates_base - (100.0 + 10.0 - 20.0))
        )
        < 1e-9
    )
