**PERFORMANCE METRICS**

This doc describes the implemented metrics in `octa_ledger.performance` and attribution helpers in `octa_ledger.attribution`.

Metrics
- `periodic_returns_from_prices(prices)` — compute period returns from price series.
- `volatility(returns)` — standard deviation of period returns.
- `sharpe(returns)` — annualized Sharpe ratio (default 252 periods/year).
- `sortino(returns)` — annualized Sortino ratio using downside deviation.
- `max_drawdown(prices)` — returns max drawdown fraction and duration.
- `calmar(returns, prices)` — annual_return / max_drawdown.

Attribution
- `attribute_trades(trades)` — aggregates PnL and costs by strategy and asset, returns gross and net PnL.
- `reconcile(attribution)` — verifies that sums match within numerical tolerance.

Notes
- All metrics are computed net-of-costs when provided with net returns series. Cost attribution is explicit via `attribution` helpers.
