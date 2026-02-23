"""FeatureWeightingPolicy — per-asset-class, per-gate-layer feature group weights.

Resolves the correct weight table for (asset_class, timeframe, gate_layer) and
applies normalisation after zeroing out absent groups.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class FeatureGroupWeight:
    group: str      # e.g. "macro", "fundamental", "onchain", "basis"
    weight: float   # non-negative; 0.0 = disabled
    required: bool  # True → gate warns/fails if this group is empty


@dataclass(frozen=True)
class FeatureWeightingPolicy:
    asset_class: str  # "*" = wildcard
    timeframe: str    # "*" = wildcard
    gate_layer: str   # "*" = wildcard
    weights: Tuple[FeatureGroupWeight, ...]

    def stable_hash(self) -> str:
        payload = {
            "asset_class": self.asset_class,
            "timeframe": self.timeframe,
            "gate_layer": self.gate_layer,
            "weights": [
                {"group": w.group, "weight": w.weight, "required": w.required}
                for w in sorted(self.weights, key=lambda x: x.group)
            ],
        }
        raw = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def normalised_weights(self, present_groups: Sequence[str]) -> Dict[str, float]:
        """Return normalised weights for groups that are present."""
        effective = {
            w.group: w.weight
            for w in self.weights
            if w.group in present_groups and w.weight > 0.0
        }
        total = sum(effective.values())
        if total <= 0.0:
            return {}
        return {g: v / total for g, v in effective.items()}


# ---------------------------------------------------------------------------
# Canonical weight tables
# ---------------------------------------------------------------------------

_GLOBAL_1D = (
    FeatureGroupWeight("macro", 0.40, False),
    FeatureGroupWeight("event", 0.30, False),
    FeatureGroupWeight("flow", 0.30, False),
)
_STRUCTURE_30M = (
    FeatureGroupWeight("fundamental", 0.50, False),
    FeatureGroupWeight("event", 0.25, False),
    FeatureGroupWeight("flow", 0.25, False),
)
_SIGNAL_1H = (
    FeatureGroupWeight("sentiment", 0.40, False),
    FeatureGroupWeight("attention", 0.30, False),
    FeatureGroupWeight("flow", 0.30, False),
)
_EXECUTION = (FeatureGroupWeight("liquidity", 1.00, False),)

_DEFAULT_POLICIES: List[FeatureWeightingPolicy] = [
    FeatureWeightingPolicy("*", "*", "global_1d", _GLOBAL_1D),
    FeatureWeightingPolicy("*", "*", "structure_30m", _STRUCTURE_30M),
    FeatureWeightingPolicy("*", "*", "signal_1h", _SIGNAL_1H),
    FeatureWeightingPolicy("*", "*", "execution_5m", _EXECUTION),
    FeatureWeightingPolicy("*", "*", "micro_1m", _EXECUTION),
]

_CRYPTO_POLICIES: List[FeatureWeightingPolicy] = [
    FeatureWeightingPolicy(
        "crypto", "*", "global_1d",
        (FeatureGroupWeight("onchain_market", 0.40, False),
         FeatureGroupWeight("macro", 0.30, False),
         FeatureGroupWeight("flow", 0.30, False)),
    ),
    FeatureWeightingPolicy(
        "crypto", "*", "structure_30m",
        (FeatureGroupWeight("onchain_symbol", 0.40, False),
         FeatureGroupWeight("flow", 0.30, False),
         FeatureGroupWeight("event", 0.30, False)),
    ),
    FeatureWeightingPolicy(
        "crypto", "*", "signal_1h",
        (FeatureGroupWeight("funding_rate", 0.50, False),
         FeatureGroupWeight("sentiment", 0.30, False),
         FeatureGroupWeight("attention", 0.20, False)),
    ),
    FeatureWeightingPolicy("crypto", "*", "execution_5m", _EXECUTION),
    FeatureWeightingPolicy("crypto", "*", "micro_1m", _EXECUTION),
]

_FX_POLICIES: List[FeatureWeightingPolicy] = [
    FeatureWeightingPolicy(
        "fx", "*", "global_1d",
        (FeatureGroupWeight("eco_calendar", 0.40, False),
         FeatureGroupWeight("macro", 0.40, False),
         FeatureGroupWeight("flow", 0.20, False)),
    ),
    FeatureWeightingPolicy(
        "fx", "*", "structure_30m",
        (FeatureGroupWeight("cot_positioning", 0.50, False),
         FeatureGroupWeight("event", 0.30, False),
         FeatureGroupWeight("flow", 0.20, False)),
    ),
    FeatureWeightingPolicy(
        "fx", "*", "signal_1h",
        (FeatureGroupWeight("flow", 0.60, False),
         FeatureGroupWeight("attention", 0.40, False)),
    ),
    FeatureWeightingPolicy("fx", "*", "execution_5m", _EXECUTION),
    FeatureWeightingPolicy("fx", "*", "micro_1m", _EXECUTION),
]

_FUTURE_POLICIES: List[FeatureWeightingPolicy] = [
    FeatureWeightingPolicy(
        "future", "*", "global_1d",
        (FeatureGroupWeight("macro", 0.50, False),
         FeatureGroupWeight("flow", 0.30, False),
         FeatureGroupWeight("event", 0.20, False)),
    ),
    FeatureWeightingPolicy(
        "future", "*", "structure_30m",
        (FeatureGroupWeight("basis", 0.50, False),
         FeatureGroupWeight("flow", 0.30, False),
         FeatureGroupWeight("event", 0.20, False)),
    ),
    FeatureWeightingPolicy(
        "future", "*", "signal_1h",
        (FeatureGroupWeight("flow", 0.70, False),
         FeatureGroupWeight("attention", 0.30, False)),
    ),
    FeatureWeightingPolicy("future", "*", "execution_5m", _EXECUTION),
    FeatureWeightingPolicy("future", "*", "micro_1m", _EXECUTION),
]

_OPTION_POLICIES: List[FeatureWeightingPolicy] = [
    FeatureWeightingPolicy(
        "option", "*", "global_1d",
        (FeatureGroupWeight("macro", 0.50, False),
         FeatureGroupWeight("event", 0.50, False)),
    ),
    FeatureWeightingPolicy(
        "option", "*", "structure_30m",
        (FeatureGroupWeight("greeks", 0.60, False),
         FeatureGroupWeight("event", 0.40, False)),
    ),
    FeatureWeightingPolicy(
        "option", "*", "signal_1h",
        (FeatureGroupWeight("iv_surface", 0.70, False),
         FeatureGroupWeight("flow", 0.30, False)),
    ),
    FeatureWeightingPolicy("option", "*", "execution_5m", _EXECUTION),
    FeatureWeightingPolicy("option", "*", "micro_1m", _EXECUTION),
]

_ALL_POLICIES: List[FeatureWeightingPolicy] = (
    _CRYPTO_POLICIES + _FX_POLICIES + _FUTURE_POLICIES + _OPTION_POLICIES + _DEFAULT_POLICIES
)


def resolve_policy(
    asset_class: str,
    timeframe: str,
    gate_layer: str,
    extra_policies: Optional[List[FeatureWeightingPolicy]] = None,
) -> FeatureWeightingPolicy:
    """Resolve the most specific matching policy.

    Priority: exact asset_class > "*"; extra_policies checked first.
    Falls back to default if no match.
    """
    candidates = list(extra_policies or []) + _ALL_POLICIES

    def _score(p: FeatureWeightingPolicy) -> int:
        ac_match = (p.asset_class == asset_class)
        tf_match = (p.timeframe == timeframe)
        gl_match = (p.gate_layer == gate_layer)
        if not gl_match and p.gate_layer != "*":
            return -1
        if not ac_match and p.asset_class != "*":
            return -1
        score = 0
        if ac_match:
            score += 4
        if tf_match:
            score += 2
        if gl_match:
            score += 1
        return score

    best: Optional[FeatureWeightingPolicy] = None
    best_score = -1
    for p in candidates:
        s = _score(p)
        if s > best_score:
            best_score = s
            best = p

    if best is None:
        # Absolute fallback: empty policy
        return FeatureWeightingPolicy("*", "*", gate_layer, ())
    return best
