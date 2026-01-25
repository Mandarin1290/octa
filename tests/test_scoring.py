from __future__ import annotations

import pandas as pd

from octa.core.research.scoring.scorer import score_run


def test_score_run_basic() -> None:
    pnl = pd.Series([0.01, -0.005, 0.002], index=pd.date_range("2020-01-01", periods=3, freq="D"))
    trades = [{"size_frac": 0.1, "price": 100.0}]
    report = score_run(
        pnl,
        trades,
        {"volatility": 0.02, "liquidity": 1.0},
        {"fee_bps": 1.0, "spread_bps": 0.5, "slippage_bps": 0.5},
        run_id="test",
        gate="global_1d",
        timeframe="1D",
    )
    assert report.ok
