"""Deterministic symbol eligibility filter for calibration and training.

Filters symbols by price floor, dollar-volume floor, spread proxy, and data quality.
All thresholds are configurable. No randomness. Deterministic output order.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


@dataclass(frozen=True)
class EligibilityRules:
    price_floor: float = 5.0
    dollar_volume_floor: float = 5_000_000.0
    min_valid_days_1d: int = 252
    spread_proxy_max: float = 0.03
    asset_class: str = "equity"


@dataclass
class EligibilityResult:
    symbol: str
    eligible: bool
    median_close: float = 0.0
    median_dollar_volume: float = 0.0
    valid_days: int = 0
    median_spread_proxy: float = 0.0
    exclusion_reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def compute_eligibility(
    symbol: str,
    closes: Sequence[float],
    volumes: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    rules: EligibilityRules,
) -> EligibilityResult:
    """Evaluate one symbol against eligibility rules. Pure function, no I/O."""
    reasons: List[str] = []

    # Clean data
    valid_closes = [c for c in closes if c is not None and math.isfinite(c) and c > 0]
    valid_volumes = [v for v in volumes if v is not None and math.isfinite(v) and v > 0]

    valid_days = len(valid_closes)
    if valid_days < 10:
        return EligibilityResult(
            symbol=symbol, eligible=False, valid_days=valid_days,
            exclusion_reasons=["insufficient_data"],
        )

    # Median close
    sorted_c = sorted(valid_closes)
    median_close = sorted_c[len(sorted_c) // 2]

    # Median dollar volume
    dollar_vols = []
    for c, v in zip(closes, volumes):
        if c is not None and v is not None and math.isfinite(c) and math.isfinite(v) and c > 0 and v > 0:
            dollar_vols.append(c * v)
    if dollar_vols:
        sorted_dv = sorted(dollar_vols)
        median_dollar_volume = sorted_dv[len(sorted_dv) // 2]
    else:
        median_dollar_volume = 0.0

    # Spread proxy: median((high-low)/close)
    spread_proxies = []
    for h, l, c in zip(highs, lows, closes):
        if (h is not None and l is not None and c is not None
                and math.isfinite(h) and math.isfinite(l) and math.isfinite(c) and c > 0):
            sp = (h - l) / c
            if math.isfinite(sp) and sp >= 0:
                spread_proxies.append(sp)
    if spread_proxies:
        sorted_sp = sorted(spread_proxies)
        median_spread_proxy = sorted_sp[len(sorted_sp) // 2]
    else:
        median_spread_proxy = 0.0

    # Apply rules
    if median_close < rules.price_floor:
        reasons.append(f"price_below_floor:{median_close:.2f}<{rules.price_floor}")
    if median_dollar_volume < rules.dollar_volume_floor:
        reasons.append(f"dollar_volume_below_floor:{median_dollar_volume:.0f}<{rules.dollar_volume_floor:.0f}")
    if valid_days < rules.min_valid_days_1d:
        reasons.append(f"insufficient_history:{valid_days}<{rules.min_valid_days_1d}")
    if median_spread_proxy > rules.spread_proxy_max:
        reasons.append(f"spread_too_wide:{median_spread_proxy:.4f}>{rules.spread_proxy_max}")

    return EligibilityResult(
        symbol=symbol,
        eligible=len(reasons) == 0,
        median_close=median_close,
        median_dollar_volume=median_dollar_volume,
        valid_days=valid_days,
        median_spread_proxy=median_spread_proxy,
        exclusion_reasons=reasons,
    )


def select_tier_symbols(
    eligible: List[EligibilityResult],
    n: int,
) -> List[str]:
    """Deterministic selection of top-N eligible symbols by (-median_dollar_volume, symbol)."""
    ranked = sorted(
        [e for e in eligible if e.eligible],
        key=lambda e: (-e.median_dollar_volume, e.symbol),
    )
    return [e.symbol for e in ranked[:n]]


def write_eligibility_evidence(
    results: List[EligibilityResult],
    rules: EligibilityRules,
    out_dir: Path,
    asset_class: str,
) -> None:
    """Write eligibility evidence artifacts to out_dir."""
    out_dir.mkdir(parents=True, exist_ok=True)

    eligible = [r for r in results if r.eligible]
    excluded = [r for r in results if not r.eligible]

    # Rules
    with open(out_dir / "eligibility_rules.json", "w") as f:
        json.dump(asdict(rules), f, indent=2)

    # Eligible symbols
    with open(out_dir / f"eligible_symbols_{asset_class}.json", "w") as f:
        json.dump(
            {"count": len(eligible), "symbols": [e.to_dict() for e in eligible]},
            f, indent=2,
        )

    # Excluded symbols with reason codes
    with open(out_dir / f"excluded_symbols_{asset_class}.json", "w") as f:
        json.dump(
            {"count": len(excluded), "symbols": [e.to_dict() for e in excluded]},
            f, indent=2,
        )

    # Summary
    with open(out_dir / "eligibility_summary.json", "w") as f:
        json.dump({
            "asset_class": asset_class,
            "total_evaluated": len(results),
            "eligible": len(eligible),
            "excluded": len(excluded),
            "exclusion_reason_counts": _count_reasons(excluded),
            "rules": asdict(rules),
        }, f, indent=2)


def _count_reasons(excluded: List[EligibilityResult]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for e in excluded:
        for r in e.exclusion_reasons:
            key = r.split(":")[0]
            counts[key] = counts.get(key, 0) + 1
    return counts
