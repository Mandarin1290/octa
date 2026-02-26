from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import Sequence


@dataclass(frozen=True)
class DiagnosticsSummary:
    signal_accuracy: float
    slippage_mean: float
    risk_veto_rate: float
    capital_block_rate: float
    rejection_reasons: dict[str, int]


def compute_diagnostics(
    signal_hits: Sequence[bool],
    slippages: Sequence[float],
    risk_vetoes: Sequence[bool],
    capital_blocks: Sequence[bool],
    rejection_reasons: Sequence[str],
) -> DiagnosticsSummary:
    signal_accuracy = _rate(signal_hits)
    slippage_mean = mean(slippages) if slippages else 0.0
    risk_veto_rate = _rate(risk_vetoes)
    capital_block_rate = _rate(capital_blocks)
    reasons = _count(rejection_reasons)

    return DiagnosticsSummary(
        signal_accuracy=signal_accuracy,
        slippage_mean=slippage_mean,
        risk_veto_rate=risk_veto_rate,
        capital_block_rate=capital_block_rate,
        rejection_reasons=reasons,
    )


def _rate(flags: Sequence[bool]) -> float:
    if not flags:
        return 0.0
    return sum(1 for flag in flags if flag) / len(flags)


def _count(values: Sequence[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts
