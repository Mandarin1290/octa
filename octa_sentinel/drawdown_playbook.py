"""Drawdown recovery playbook: rule-based de-risking and re-risk checks.

Produces per-strategy compression factors, freeze lists and rationale metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass
class PlaybookConfig:
    kill_switch_threshold: float = 0.10
    ladder: Dict[float, Dict] | None = None
    re_risk_days: int = 3
    volatility_mult_threshold: float = 1.2
    correlation_threshold: float = 0.4

    def __post_init__(self):
        if self.ladder is None:
            # drawdown -> action presets
            self.ladder = {
                0.02: {"gross_reduce": 0.10, "freeze_new": False, "flatten_assets": []},
                0.05: {"gross_reduce": 0.25, "freeze_new": True, "flatten_assets": []},
                0.08: {
                    "gross_reduce": 0.5,
                    "freeze_new": True,
                    "flatten_assets": ["high_impact"],
                },
            }


def evaluate_drawdown(
    current_drawdown: float,
    strategy_gross: Dict[str, float],
    baseline_volatility: Dict[str, float],
    current_volatility: Dict[str, float],
    correlation_score: float,
    incidents_since: int,
    paper_gates_ok: bool,
    config: PlaybookConfig | None = None,
) -> Dict:
    """Evaluate drawdown ladder and produce actions.

    Inputs:
      - current_drawdown: portfolio drawdown as positive fraction (e.g. 0.05 for 5%)
      - strategy_gross: map strategy->gross risk (notional or risk units)
      - baseline_volatility, current_volatility: maps strategy->vol
      - correlation_score: 0..1 stress score (lower is better)
      - incidents_since: number of critical incidents within lookback window
      - paper_gates_ok: boolean (paper gates passing)

    Returns dict with keys: `compression`, `freeze_list`, `flatten_assets`, `rationale`, `incidents`.
    """
    cfg = config or PlaybookConfig()
    ladder = cfg.ladder or {}

    compression: Dict[str, float] = {}
    freeze_list: List[str] = []
    flatten_assets: List[str] = []
    rationale: List[str] = []

    # Determine ladder level
    applied = None
    for dd_thr in sorted(ladder.keys()):
        if current_drawdown >= dd_thr:
            applied = ladder[dd_thr]
    if current_drawdown >= cfg.kill_switch_threshold:
        # full flatten / kill switch
        for s in strategy_gross:
            compression[s] = 0.0
        rationale.append(f"kill_switch at {current_drawdown:.2%}")
        flatten_assets = ["all"]
        # create incident payload
        return {
            "compression": compression,
            "freeze_list": list(strategy_gross.keys()),
            "flatten_assets": flatten_assets,
            "rationale": rationale,
            "incident": {
                "level": 3,
                "reason": "kill_switch",
                "drawdown": current_drawdown,
            },
        }

    if applied is None:
        # no ladder activated
        for s in strategy_gross:
            compression[s] = 1.0
        rationale.append("no_action")
    else:
        # apply gross reduce as proportional scale to all strategies, but could be strategy-specific
        reduce_frac = applied.get("gross_reduce", 0.0)
        for s, _g in strategy_gross.items():
            # weaker strategies (higher vol) get slightly larger reduction
            vol_base = baseline_volatility.get(s, 1.0)
            vol_now = current_volatility.get(s, vol_base)
            vol_mult = vol_now / max(1e-9, vol_base)
            factor = 1.0 - reduce_frac * min(1.0, vol_mult)
            compression[s] = max(0.0, min(1.0, factor))
        if applied.get("freeze_new"):
            # freeze strategies with smallest gross
            weakest = sorted(strategy_gross.items(), key=lambda x: x[1])[
                : max(1, len(strategy_gross) // 4)
            ]
            freeze_list = [s for s, _ in weakest]
        flatten_assets = applied.get("flatten_assets", [])
        rationale.append(f"ladder_applied_dd={current_drawdown:.2%}")

    # Re-risk gating
    re_risk_allowed = True
    # volatility normalized check
    vol_flag = any(
        (
            current_volatility.get(s, 0.0)
            > baseline_volatility.get(s, 1.0) * cfg.volatility_mult_threshold
        )
        for s in strategy_gross
    )
    if vol_flag:
        re_risk_allowed = False
        rationale.append("volatility_high")

    if correlation_score >= cfg.correlation_threshold:
        re_risk_allowed = False
        rationale.append("correlation_stress")

    if incidents_since > 0:
        re_risk_allowed = False
        rationale.append("recent_incidents")

    if not paper_gates_ok:
        re_risk_allowed = False
        rationale.append("paper_gates_fail")

    return {
        "compression": compression,
        "freeze_list": freeze_list,
        "flatten_assets": flatten_assets,
        "rationale": rationale,
        "re_risk_allowed": re_risk_allowed,
    }
