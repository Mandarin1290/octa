"""AltDataPack — typed container for altdata features delivered to gate evaluation.

This replaces the implicit (registry, payloads) pair with a single typed object
that carries market-wide and per-symbol features, missing-source tracking, and
provenance hashes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class AltDataPack:
    """Typed delivery contract for altdata features at a specific gate context."""

    run_id: str
    symbol: str
    asset_class: str          # from AssetProfile.kind (e.g. "stock", "crypto", "future")
    gate_layer: str           # e.g. "global_1d", "structure_30m"
    timeframe: str            # e.g. "1D", "30M"
    asof_ts: Optional[str]    # ISO-8601 UTC point-in-time cutoff; None = latest

    # Feature vectors (flat key → float, all values finite)
    market_features: Dict[str, float] = field(default_factory=dict)
    symbol_features: Dict[str, float] = field(default_factory=dict)

    # Missing-source tracking
    missing_required: List[str] = field(default_factory=list)
    missing_optional: List[str] = field(default_factory=list)

    # Provenance: source_name → content_hash
    provenance: Dict[str, str] = field(default_factory=dict)

    def is_empty(self) -> bool:
        """True if no features were produced at all."""
        return not self.market_features and not self.symbol_features

    def has_missing_required(self) -> bool:
        return bool(self.missing_required)

    def feature_count(self) -> int:
        return len(self.market_features) + len(self.symbol_features)
