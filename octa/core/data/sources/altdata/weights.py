from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass
class WeightResult:
    weights: Dict[str, float]
    reasons: Dict[str, str]


def normalize_weights(base: Dict[str, float], enabled: Dict[str, bool]) -> Dict[str, float]:
    # Drop disabled sources
    w = {k: float(v) for k, v in (base or {}).items() if bool(enabled.get(k, False)) and float(v) > 0}
    s = sum(w.values())
    if s <= 0:
        return {k: 0.0 for k in (base or {}).keys()}
    return {k: float(v) / float(s) for k, v in w.items()}


def apply_quality_adjustments(
    *,
    weights: Dict[str, float],
    coverage: Dict[str, float],
    min_coverage: float = 0.5,
) -> Tuple[Dict[str, float], Dict[str, str]]:
    out = dict(weights)
    reasons: Dict[str, str] = {}
    for src, _w in list(out.items()):
        cov = float(coverage.get(src, 1.0)) if coverage is not None else 1.0
        if cov < float(min_coverage):
            out[src] = 0.0
            reasons[src] = f"coverage<{min_coverage}"
    # renormalize
    s = sum(out.values())
    if s > 0:
        out = {k: float(v) / float(s) for k, v in out.items()}
    return out, reasons
