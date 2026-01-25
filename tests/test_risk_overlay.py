from __future__ import annotations

from octa.core.risk.overlay import apply_overlay


def test_overlay_blocks_on_halt() -> None:
    signals = [{"symbol": "ABC", "qty": 0.05, "side": "BUY"}]
    adjusted = apply_overlay(
        signals=signals,
        portfolio_state={"exposure_used": 0.0},
        market_state={"asset_class": "equity"},
        overlay_cfg={"max_portfolio_exposure": 0.5},
        regime_state={"label": "HALT"},
    )
    assert adjusted == []
