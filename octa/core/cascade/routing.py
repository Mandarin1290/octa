from __future__ import annotations

from typing import Sequence

from .contracts import GateDecision, GateOutcome


def route_symbols(
    symbols: Sequence[str], outcome: GateOutcome
) -> tuple[list[str], list[str]]:
    if outcome.decision == GateDecision.PASS:
        eligible = list(outcome.eligible_symbols)
        if outcome.rejected_symbols:
            rejected = list(outcome.rejected_symbols)
        else:
            rejected = [symbol for symbol in symbols if symbol not in eligible]
        return eligible, rejected

    return [], list(symbols)
