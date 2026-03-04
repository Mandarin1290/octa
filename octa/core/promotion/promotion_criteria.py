"""PromotionCriteria — deterministic, fail-closed eligibility gate.

Evaluates whether a paper-run model is eligible for live promotion based on
quantitative metrics, drift guard state, and risk incident count.

Fail-closed contract:
  - Any missing required metric → eligible=False, reason MISSING_METRIC:<key>
  - Any open drift breach     → eligible=False, reason DRIFT_BREACH:<model_key>
  - risk_incidents > max      → eligible=False, reason RISK_INCIDENTS_PRESENT:<n>
  - survivors < min_survivors → eligible=False, reason INSUFFICIENT_SURVIVORS:<n>

All fields are conservative by default.  Caller must explicitly relax them.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple


# ---------------------------------------------------------------------------
# Required metric keys (must be present; absence → fail-closed)
# ---------------------------------------------------------------------------

_REQUIRED_METRICS: Tuple[str, ...] = (
    "sharpe",
    "max_drawdown",
    "n_trades",
)


# ---------------------------------------------------------------------------
# PromotionCriteria
# ---------------------------------------------------------------------------


@dataclass
class PromotionCriteria:
    """Conservative defaults; caller tightens or relaxes as needed.

    Attributes
    ----------
    min_sharpe:
        Minimum annualised Sharpe ratio (OOS).  Below → ineligible.
    max_drawdown:
        Maximum allowable max drawdown fraction [0, 1].  Above → ineligible.
    min_trades:
        Minimum number of trades in the OOS evaluation window.  Below → ineligible.
    min_survivors:
        Minimum number of timeframes that individually pass all metric gates.
        Evaluated per-symbol across all timeframes supplied to evaluate().
    drift_guard_required:
        If True, any open drift breach for the evaluated model_key → ineligible.
    risk_incidents_max:
        Maximum number of risk incidents allowed (0 = zero tolerance).
    """

    min_sharpe: float = 0.8
    max_drawdown: float = 0.15
    min_trades: int = 30
    min_survivors: int = 1
    drift_guard_required: bool = True
    risk_incidents_max: int = 0


# ---------------------------------------------------------------------------
# evaluate()
# ---------------------------------------------------------------------------


def evaluate(
    metrics_by_tf: Mapping[str, Mapping[str, Any]],
    *,
    criteria: PromotionCriteria,
    drift_breaches: Optional[Mapping[str, bool]] = None,
    risk_incident_count: int = 0,
) -> Tuple[bool, List[str], Dict[str, Any]]:
    """Evaluate promotion eligibility for a single symbol across timeframes.

    Parameters
    ----------
    metrics_by_tf:
        ``{timeframe: {metric_key: value, ...}, ...}``
        e.g. ``{"1D": {"sharpe": 1.2, "max_drawdown": 0.04, "n_trades": 180}}``.
    criteria:
        Policy thresholds.
    drift_breaches:
        ``{model_key: is_breached}``  where ``True`` = active breach.
        If *drift_guard_required* and any key is ``True`` → fail-closed.
    risk_incident_count:
        Number of risk incidents recorded in the run evidence.
        If > ``criteria.risk_incidents_max`` → fail-closed.

    Returns
    -------
    (eligible, reasons, details)
        eligible: bool — True only when all gates pass.
        reasons:  list of short reason codes (empty when eligible=True).
        details:  dict for evidence/audit trail.
    """
    reasons: List[str] = []
    details: Dict[str, Any] = {
        "criteria": {
            "min_sharpe": criteria.min_sharpe,
            "max_drawdown": criteria.max_drawdown,
            "min_trades": criteria.min_trades,
            "min_survivors": criteria.min_survivors,
            "drift_guard_required": criteria.drift_guard_required,
            "risk_incidents_max": criteria.risk_incidents_max,
        },
        "timeframe_results": {},
        "survivors": 0,
        "risk_incident_count": risk_incident_count,
        "drift_breaches": dict(drift_breaches) if drift_breaches else {},
    }

    # --- drift guard (checked first, before per-TF metrics) ---
    if criteria.drift_guard_required and drift_breaches:
        for model_key, breached in drift_breaches.items():
            if breached:
                reasons.append(f"DRIFT_BREACH:{model_key}")

    # --- risk incidents ---
    if risk_incident_count > criteria.risk_incidents_max:
        reasons.append(f"RISK_INCIDENTS_PRESENT:{risk_incident_count}")

    # --- per-timeframe metric gates ---
    survivors = 0
    for tf, metrics in metrics_by_tf.items():
        tf_reasons: List[str] = []

        # Fail-closed: required metrics must be present
        for key in _REQUIRED_METRICS:
            if key not in metrics:
                tf_reasons.append(f"MISSING_METRIC:{key}")

        if not tf_reasons:
            sharpe = float(metrics["sharpe"])
            max_dd = float(metrics["max_drawdown"])
            n_trades = int(metrics["n_trades"])

            if sharpe < criteria.min_sharpe:
                tf_reasons.append(
                    f"SHARPE_TOO_LOW:{sharpe:.4f}<{criteria.min_sharpe}"
                )
            if max_dd > criteria.max_drawdown:
                tf_reasons.append(
                    f"DRAWDOWN_EXCEEDED:{max_dd:.4f}>{criteria.max_drawdown}"
                )
            if n_trades < criteria.min_trades:
                tf_reasons.append(
                    f"INSUFFICIENT_TRADES:{n_trades}<{criteria.min_trades}"
                )

        tf_pass = len(tf_reasons) == 0
        if tf_pass:
            survivors += 1

        details["timeframe_results"][tf] = {
            "pass": tf_pass,
            "reasons": tf_reasons,
            "metrics_snapshot": {
                k: metrics.get(k) for k in _REQUIRED_METRICS
            },
        }

    details["survivors"] = survivors

    if survivors < criteria.min_survivors:
        reasons.append(
            f"INSUFFICIENT_SURVIVORS:{survivors}<{criteria.min_survivors}"
        )

    eligible = len(reasons) == 0
    details["eligible"] = eligible
    details["reasons"] = reasons
    return eligible, reasons, details
