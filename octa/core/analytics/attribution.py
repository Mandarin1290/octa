from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence


@dataclass(frozen=True)
class AttributionSummary:
    per_symbol: dict[str, float]
    per_gate: dict[str, float]
    per_regime: dict[str, float]
    per_session: dict[str, float]


def compute_attribution(
    symbol_pnl: Mapping[str, float],
    gate_pnl: Mapping[str, float],
    regime_pnl: Mapping[str, float],
    session_pnl: Mapping[str, float],
) -> AttributionSummary:
    return AttributionSummary(
        per_symbol=dict(symbol_pnl),
        per_gate=dict(gate_pnl),
        per_regime=dict(regime_pnl),
        per_session=dict(session_pnl),
    )
