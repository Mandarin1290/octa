from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class ChampionDecision:
    promote: bool
    reason: str
    delta_score: float
    diagnostics: Dict[str, Any]


def decide_champion(
    *,
    challenger_score: float,
    champion_score: float,
    min_improvement: float,
    stability_ok: bool,
) -> ChampionDecision:
    delta = challenger_score - champion_score
    if not stability_ok:
        return ChampionDecision(False, "stability_failed", delta, {"stability_ok": stability_ok})
    if delta >= min_improvement:
        return ChampionDecision(True, "improvement_met", delta, {})
    return ChampionDecision(False, "improvement_insufficient", delta, {"min_improvement": min_improvement})
