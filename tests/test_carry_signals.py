from __future__ import annotations

from datetime import datetime, timezone

from octa.execution.carry import generate_carry_intents


def test_carry_signal_generation() -> None:
    cfg = {
        "min_rate_diff": 0.005,
        "enter_threshold": 0.005,
        "exit_threshold": 0.002,
        "max_volatility": 0.5,
        "rebalance_interval_hours": 1,
        "min_hold_days": 0,
        "instruments": [
            {
                "instrument": "EURUSD",
                "base_ccy": "EUR",
                "quote_ccy": "USD",
                "asset_class": "fx_carry",
                "funding_cost": 0.001,
                "volatility": 0.1,
            }
        ],
    }
    rates = {"USD": 0.05, "EUR": 0.02}
    intents, status = generate_carry_intents(carry_cfg=cfg, rates=rates, state={}, now_utc=datetime.now(timezone.utc))
    assert status["enabled"] is True
    assert len(intents) == 1
    assert intents[0].instrument == "EURUSD"


def test_carry_signal_respects_rebalance_interval() -> None:
    cfg = {
        "min_rate_diff": 0.005,
        "enter_threshold": 0.005,
        "exit_threshold": 0.002,
        "rebalance_interval_hours": 48,
        "min_hold_days": 0,
        "instruments": [{"instrument": "EURUSD", "base_ccy": "EUR", "quote_ccy": "USD"}],
    }
    rates = {"USD": 0.05, "EUR": 0.02}
    intents, _ = generate_carry_intents(
        carry_cfg=cfg,
        rates=rates,
        state={"last_rebalance_ts": datetime.now(timezone.utc).isoformat()},
        now_utc=datetime.now(timezone.utc),
    )
    assert intents == []
