from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping


@dataclass(frozen=True)
class RiskBudget:
    max_leverage: float
    target_vol: float
    risk_multiplier: float
    max_position_pct: float
    max_sector_pct: float
    max_single_asset_risk: float


def budget_from_cfg(cfg: Mapping[str, float], regime_scale: float) -> RiskBudget:
    max_leverage = float(cfg.get("max_leverage", 1.0))
    target_vol = float(cfg.get("target_vol", 0.12))
    max_pos = float(cfg.get("max_position_pct", 0.05))
    max_sector = float(cfg.get("max_sector_pct", 0.2))
    max_single = float(cfg.get("max_single_asset_risk", 0.02))
    return RiskBudget(
        max_leverage=max_leverage,
        target_vol=target_vol,
        risk_multiplier=regime_scale,
        max_position_pct=max_pos,
        max_sector_pct=max_sector,
        max_single_asset_risk=max_single,
    )
