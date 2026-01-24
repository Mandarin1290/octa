from octa_ledger.attribution import attribute_trades, reconcile
from octa_ledger.performance import (
    calmar,
    max_drawdown,
    periodic_returns_from_prices,
    sharpe,
    sortino,
)


def test_metrics_on_known_series():
    prices = [100, 110, 121]
    rets = periodic_returns_from_prices(prices)
    # returns are 0.1, 0.1
    assert abs(rets[0] - 0.1) < 1e-9
    assert abs(rets[1] - 0.1) < 1e-9
    dd, dur = max_drawdown([100, 110, 90, 95, 80])
    # max drawdown from 110 to 80 = 30/110 ~= 0.2727
    assert abs(dd - (30.0 / 110.0)) < 1e-6
    # Sharpe on constant positive returns should be large (vol small)
    s = sharpe(rets, risk_free=0.0, periods_per_year=1)
    assert s > 0
    so = sortino(rets, required_return=0.0, periods_per_year=1)
    assert so > 0
    ca = calmar(rets, [100, 110, 121], periods_per_year=1)
    assert ca > 0


def test_attribution_reconciles():
    trades = [
        {"strategy": "s1", "asset": "A", "pnl": 100.0, "fees": 1.0, "impact": 2.0},
        {"strategy": "s2", "asset": "B", "pnl": 50.0, "fees": 0.5, "impact": 1.0},
    ]
    att = attribute_trades(trades)
    assert att["gross_pnl"] == 150.0
    assert att["costs"]["total"] == 4.5
    assert att["net_pnl"] == 145.5
    assert reconcile(att) is True
