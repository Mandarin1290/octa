from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from octa_core.risk_institutional.opengamma_client import OpenGammaClient


@dataclass(frozen=True)
class RiskSnapshot:
    exposures: Dict[str, Any]
    var_es: Optional[Dict[str, Any]] = None
    stress: Optional[Dict[str, Any]] = None
    source: str = "local"


def aggregate_risk(
    *,
    exposures: Dict[str, Any],
    opengamma: Optional[OpenGammaClient] = None,
    opengamma_required: bool = False,
    confidence: float = 0.975,
    horizon_days: int = 1,
    stress_scenario_id: Optional[str] = None,
) -> RiskSnapshot:
    """Unify risk metrics from internal + external analytics.

    Fail-closed: if OpenGamma is required and unavailable/unhealthy, raise.
    """

    if opengamma is None:
        if opengamma_required:
            raise RuntimeError("opengamma_required_but_not_configured")
        return RiskSnapshot(exposures=exposures, source="local")

    if not opengamma.health_check():
        if opengamma_required:
            raise RuntimeError("opengamma_unhealthy")
        return RiskSnapshot(exposures=exposures, source="local")

    job_id = opengamma.submit_portfolio(exposures)
    req_id = opengamma.request_var_es(confidence=confidence, horizon_days=horizon_days, job_id=job_id)
    var_es = opengamma.fetch_results(request_id=req_id)

    stress = None
    if stress_scenario_id:
        srid = opengamma.request_stress(scenario_id=stress_scenario_id, job_id=job_id)
        stress = opengamma.fetch_results(request_id=srid)

    return RiskSnapshot(exposures=exposures, var_es=var_es, stress=stress, source="opengamma")


__all__ = ["RiskSnapshot", "aggregate_risk"]
