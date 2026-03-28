from __future__ import annotations

from typing import Any


def enforce_risk_overlay(
    *,
    config: dict[str, Any],
    current_equity: float,
    peak_equity: float,
) -> dict[str, float | bool]:
    max_position_size = float(config.get("max_position_size", 1.0))
    position_size = float(config["position_size"])
    if position_size <= 0:
        raise ValueError("position_size must be positive")
    if max_position_size <= 0:
        raise ValueError("max_position_size must be positive")
    if position_size > max_position_size:
        raise ValueError(
            f"position_size exceeds max_position_size: {position_size} > {max_position_size}"
        )

    drawdown_limit = float(config.get("max_drawdown_limit", 1.0))
    if drawdown_limit < 0:
        raise ValueError("max_drawdown_limit must be non-negative")

    if peak_equity <= 0:
        drawdown = 0.0
    else:
        drawdown = max(0.0, 1.0 - (current_equity / peak_equity))
    kill_switch_triggered = drawdown > drawdown_limit
    return {
        "drawdown": float(drawdown),
        "kill_switch_triggered": bool(kill_switch_triggered),
        "max_position_size": float(max_position_size),
        "max_drawdown_limit": float(drawdown_limit),
    }


__all__ = ["enforce_risk_overlay"]
